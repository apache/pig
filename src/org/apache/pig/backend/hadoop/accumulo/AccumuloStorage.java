/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.pig.backend.hadoop.accumulo;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Basic PigStorage implementation that uses Accumulo as the backing store.
 * 
 * <p>
 * When writing data, the first entry in the {@link Tuple} is treated as the row
 * in the Accumulo key, while subsequent entries in the tuple are handled as
 * columns in that row. {@link Map}s are expanded, placing the map key in the
 * column family and the map value in the Accumulo value. Scalars are placed
 * directly into the value with an empty column qualifier. If the columns
 * argument on the constructor is omitted, null or the empty String, no column
 * family is provided on the Keys created for Accumulo
 * </p>
 * 
 * <p>
 * When reading data, if aggregateColfams is true, elements in the same row and
 * column family are aggregated into a single {@link Map}. This will result in a
 * {@link Tuple} of length (unique_column_families + 1) for the given row. If
 * aggregateColfams is false, column family and column qualifier are
 * concatenated (separated by a colon), and placed into a {@link Map}. This will
 * result in a {@link Tuple} with two entries, where the latter element has a
 * number of elements equal to the number of columns in the given row.
 * </p>
 */
public class AccumuloStorage extends AbstractAccumuloStorage {
    private static final Logger log = Logger.getLogger(AccumuloStorage.class);
    private static final String COLON = ":", EMPTY = "";
    private static final Text EMPTY_TEXT = new Text(new byte[0]);
    private static final DataByteArray EMPTY_DATA_BYTE_ARRAY = new DataByteArray(
            new byte[0]);

    // Not sure if AccumuloStorage instances need to be thread-safe or not
    final Text _cfHolder = new Text(), _cqHolder = new Text();

    /**
     * Creates an AccumuloStorage which writes all values in a {@link Tuple}
     * with an empty column family and doesn't group column families together on
     * read (creates on {@link Map} for all columns)
     */
    public AccumuloStorage() throws ParseException, IOException {
        this(EMPTY, EMPTY);
    }

    /**
     * Create an AccumuloStorage with a CSV of columns-families to use on write
     * and whether columns in a row should be grouped by family on read.
     * 
     * @param columns
     *            A comma-separated list of column families to use when writing
     *            data, aligned to the n'th entry in the tuple
     * @param aggregateColfams
     *            Should unique column qualifier and value pairs be grouped
     *            together by column family when reading data
     */
    public AccumuloStorage(String columns) throws ParseException, IOException {
        this(columns, EMPTY);
    }

    public AccumuloStorage(String columnStr, String args)
            throws ParseException, IOException {
        super(columnStr, args);
    }

    @Override
    protected Tuple getTuple(Key key, Value value) throws IOException {
        SortedMap<Key, Value> rowKVs = WholeRowIterator.decodeRow(key, value);
        Tuple tuple = TupleFactory.getInstance().newTuple(columns.size() + 1);

        final Text cfHolder = new Text();
        final Text cqHolder = new Text();
        final Text row = key.getRow();
        int tupleOffset = 0;

        tuple.set(
                tupleOffset,
                new DataByteArray(Text.decode(row.getBytes(), 0,
                        row.getLength())));

        for (Column column : this.columns) {
            tupleOffset++;

            switch (column.getType()) {
            case LITERAL:
                cfHolder.set(column.getColumnFamily());
                if (null != column.getColumnQualifier()) {
                    cqHolder.set(column.getColumnQualifier());
                } else {
                    cqHolder.set(EMPTY_TEXT);
                }

                // Get the key where our literal would exist (accounting for
                // "colf:colq" or "colf:" empty colq)
                Key literalStartKey = new Key(row, cfHolder, cqHolder);

                SortedMap<Key, Value> tailMap = rowKVs.tailMap(literalStartKey);

                // Find the element
                if (tailMap.isEmpty()) {
                    tuple.set(tupleOffset, EMPTY_DATA_BYTE_ARRAY);
                } else {
                    Key actualKey = tailMap.firstKey();

                    // Only place it in the tuple if it matches the user
                    // request, avoid using a value from a
                    // key with the wrong colqual
                    if (0 == literalStartKey.compareTo(actualKey,
                            PartialKey.ROW_COLFAM_COLQUAL)) {
                        tuple.set(tupleOffset,
                                new DataByteArray(tailMap.get(actualKey).get()));
                    } else {
                        // This row doesn't have the column we were looking for
                        tuple.set(tupleOffset, EMPTY_DATA_BYTE_ARRAY);
                    }
                }

                break;
            case COLFAM_PREFIX:
                cfHolder.set(column.getColumnFamily());
                Range colfamPrefixRange = Range.prefix(row, cfHolder);
                Key colfamPrefixStartKey = new Key(row, cfHolder);

                SortedMap<Key, Value> cfTailMap = rowKVs
                        .tailMap(colfamPrefixStartKey);

                // Find the element
                if (cfTailMap.isEmpty()) {
                    tuple.set(tupleOffset, EMPTY_DATA_BYTE_ARRAY);
                } else {
                    HashMap<String, DataByteArray> tupleMap = new HashMap<String, DataByteArray>();

                    // Build up a map for all the entries in this row that match
                    // the colfam prefix
                    for (Entry<Key, Value> entry : cfTailMap.entrySet()) {
                        if (colfamPrefixRange.contains(entry.getKey())) {
                            entry.getKey().getColumnFamily(cfHolder);
                            entry.getKey().getColumnQualifier(cqHolder);
                            DataByteArray val = new DataByteArray(entry
                                    .getValue().get());

                            // Avoid adding an extra ':' when colqual is empty
                            if (0 == cqHolder.getLength()) {
                                tupleMap.put(cfHolder.toString(), val);
                            } else {
                                tupleMap.put(cfHolder.toString() + COLON
                                        + cqHolder.toString(), val);
                            }
                        } else {
                            break;
                        }
                    }

                    if (!tupleMap.isEmpty()) {
                        tuple.set(tupleOffset, tupleMap);
                    }
                }

                break;
            case COLQUAL_PREFIX:
                cfHolder.set(column.getColumnFamily());
                cqHolder.set(column.getColumnQualifier());
                Range colqualPrefixRange = Range
                        .prefix(row, cfHolder, cqHolder);
                Key colqualPrefixStartKey = new Key(row, cfHolder, cqHolder);

                SortedMap<Key, Value> cqTailMap = rowKVs
                        .tailMap(colqualPrefixStartKey);
                if (cqTailMap.isEmpty()) {
                    tuple.set(tupleOffset, EMPTY_DATA_BYTE_ARRAY);
                } else {
                    HashMap<String, DataByteArray> tupleMap = new HashMap<String, DataByteArray>();

                    // Build up a map for all the entries in this row that match
                    // the colqual prefix
                    for (Entry<Key, Value> entry : cqTailMap.entrySet()) {
                        if (colqualPrefixRange.contains(entry.getKey())) {
                            entry.getKey().getColumnFamily(cfHolder);
                            entry.getKey().getColumnQualifier(cqHolder);
                            DataByteArray val = new DataByteArray(entry
                                    .getValue().get());

                            // Avoid the extra ':' on empty colqual
                            if (0 == cqHolder.getLength()) {
                                tupleMap.put(cfHolder.toString(), val);
                            } else {
                                tupleMap.put(cfHolder.toString() + COLON
                                        + cqHolder.toString(), val);
                            }
                        } else {
                            break;
                        }
                    }

                    if (!tupleMap.isEmpty()) {
                        tuple.set(tupleOffset, tupleMap);
                    }
                }

                break;
            default:
                break;
            }
        }

        return tuple;
    }

    @Override
    protected void configureInputFormat(Job job) {
        AccumuloInputFormat.addIterator(job, new IteratorSetting(100,
                WholeRowIterator.class));
    }

    @Override
    protected Collection<Mutation> getMutations(Tuple tuple)
            throws ExecException, IOException {
        final ResourceFieldSchema[] fieldSchemas = (schema == null) ? null
                : schema.getFields();

        Iterator<Object> tupleIter = tuple.iterator();

        if (1 >= tuple.size()) {
            log.debug("Ignoring tuple of size " + tuple.size());
            return Collections.emptyList();
        }

        Mutation mutation = new Mutation(objectToText(tupleIter.next(),
                (null == fieldSchemas) ? null : fieldSchemas[0]));

        int tupleOffset = 1;
        Iterator<Column> columnIter = columns.iterator();
        while (tupleIter.hasNext() && columnIter.hasNext()) {
            Object o = tupleIter.next();
            Column column = columnIter.next();

            // Grab the type for this field
            final byte type = schemaToType(o, (null == fieldSchemas) ? null
                    : fieldSchemas[tupleOffset]);

            switch (column.getType()) {
            case LITERAL:
                byte[] bytes = objToBytes(o, type);

                if (null != bytes) {
                    Value value = new Value(bytes);

                    // We don't have any column name from non-Maps
                    addColumn(mutation, column.getColumnFamily(),
                            column.getColumnQualifier(), value);
                }
                break;
            case COLFAM_PREFIX:
            case COLQUAL_PREFIX:
                Map<String, Object> map;
                try {
                    map = (Map<String, Object>) o;
                } catch (ClassCastException e) {
                    log.error("Expected Map at tuple offset " + tupleOffset
                            + " but was " + o.getClass().getSimpleName());
                    throw e;
                }

                for (Entry<String, Object> entry : map.entrySet()) {
                    String key = entry.getKey();
                    Object objValue = entry.getValue();

                    byte valueType = DataType.findType(objValue);
                    byte[] mapValue = objToBytes(objValue, valueType);

                    if (Column.Type.COLFAM_PREFIX == column.getType()) {
                        addColumn(mutation, column.getColumnFamily() + key,
                                null, new Value(mapValue));
                    } else if (Column.Type.COLQUAL_PREFIX == column.getType()) {
                        addColumn(mutation, column.getColumnFamily(),
                                column.getColumnQualifier() + key, new Value(
                                        mapValue));
                    } else {
                        throw new IOException("Unknown column type");
                    }
                }
                break;
            default:
                log.info("Ignoring unhandled column type");
                continue;
            }

            tupleOffset++;
        }

        if (0 == mutation.size()) {
            return Collections.emptyList();
        }

        return Collections.singletonList(mutation);
    }

    /**
     * Adds the given column family, column qualifier and value to the given
     * mutation
     * 
     * @param mutation
     * @param colfam
     * @param colqual
     * @param columnValue
     */
    protected void addColumn(Mutation mutation, String colfam, String colqual,
            Value columnValue) {
        if (null != colfam) {
            _cfHolder.set(colfam);
        } else {
            _cfHolder.clear();
        }

        if (null != colqual) {
            _cqHolder.set(colqual);
        } else {
            _cqHolder.clear();
        }

        mutation.put(_cfHolder, _cqHolder, columnValue);
    }
}
