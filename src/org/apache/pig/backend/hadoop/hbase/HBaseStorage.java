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
package org.apache.pig.backend.hadoop.hbase;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.Slice;
import org.apache.pig.Slicer;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * A <code>Slicer</code> that split the hbase table into {@link HBaseSlice}s.
 * And a load function will provided to do none load operations, the actually
 * load operatrions will be done in {@link HBaseSlice}.
 */
public class HBaseStorage extends Utf8StorageConverter implements Slicer,
        LoadFunc {

    private byte[][] m_cols;
    private HTable m_table;
    private HBaseConfiguration m_conf;

    private static final Log LOG = LogFactory.getLog(HBaseStorage.class);

    // HBase Slicer
    // Creates a slice per region of a specified table.

    /**
     * Constructor. Construct a HBase Table loader to load the cells of the
     * provided columns.
     * 
     * @param columnList
     *            columnlist that is a presented string delimited by space.
     */
    public HBaseStorage(String columnList) {
        String[] colNames = columnList.split(" ");
        m_cols = new byte[colNames.length][];
        for (int i = 0; i < m_cols.length; i++) {
            m_cols[i] = Bytes.toBytes(colNames[i]);
        }

        m_conf = new HBaseConfiguration();
    }

    @Override
    public Slice[] slice(DataStorage store, String tablename)
            throws IOException {
        validate(store, tablename);

        byte[][] startKeys = m_table.getStartKeys();
        if (startKeys == null || startKeys.length == 0) {
            throw new IOException("Expecting at least one region");
        }
        if (m_cols == null || m_cols.length == 0) {
            throw new IOException("Expecting at least one column");
        }
        // one region one slice
        Slice[] slices = new Slice[startKeys.length];
        for (int i = 0; i < startKeys.length; i++) {
            String regionLocation = m_table.getRegionLocation(startKeys[i])
                    .getServerAddress().getHostname();
            slices[i] = new HBaseSlice(m_table.getTableName(), startKeys[i],
                    ((i + 1) < startKeys.length) ? startKeys[i + 1]
                            : HConstants.EMPTY_START_ROW, m_cols,
                    regionLocation);
            LOG.info("slice: " + i + "->" + slices[i]);
        }

        return slices;
    }

    @Override
    public void validate(DataStorage store, String tablename)
            throws IOException {
        ensureTable(tablename);
    }

    private void ensureTable(String tablename) throws IOException {
        LOG.info("tablename: "+tablename);

        // We're looking for the right scheme here (actually, we don't
        // care what the scheme is as long as it is one and it's
        // different from hdfs and file. If the user specified to use
        // the multiquery feature and did not specify a scheme we will
        // have transformed it to an absolute path. In that case we'll
        // take the last component and guess that's what was
        // meant. We'll print a warning in that case.
        int index;
        if(-1 != (index = tablename.indexOf("://"))) {
            if (tablename.startsWith("hdfs:") 
                || tablename.startsWith("file:")) {
                index = tablename.lastIndexOf("/");
                if (-1 == index) {
                    index = tablename.lastIndexOf("\\");
                }

                if (-1 == index) {
                    throw new IOException("Got tablename: "+tablename
                        +". Either turn off multiquery (-no_multiquery)"
                        +" or specify load path as \"hbase://<tablename>\".");
                } else {
                    String in = tablename;
                    tablename = tablename.substring(index+1);
                    LOG.warn("Got tablename: "+in+" Assuming you meant table: "
                             +tablename+". Either turn off multiquery (-no_multiquery) "
                             +"or specify load path as \"hbase://<tablename>\" "
                             +"to avoid this warning.");
                }
            } else {
                tablename = tablename.substring(index+3);
            }
        }

        if (m_table == null) {
            m_table = new HTable(m_conf, tablename);
        }
    }

    // HBase LoadFunc
    // It is just a mock class to let the UDF be casted to a LOADFUNC during
    // parsing.
    
    @Override
    public void bindTo(String fileName, BufferedPositionedInputStream is,
            long offset, long end) throws IOException {
        // do nothing
    }

    @Override
    public Schema determineSchema(String fileName, ExecType execType,
            DataStorage storage) throws IOException {
        // do nothing
        return null;
    }

    @Override
    public void fieldsToRead(Schema schema) {
        // do nothing
    }

    @Override
    public Tuple getNext() throws IOException {
        // do nothing
        return null;
    }
}
