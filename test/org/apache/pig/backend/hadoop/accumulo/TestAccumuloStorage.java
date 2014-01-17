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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.cli.ParseException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.InternalMap;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TestAccumuloStorage {

    @Test
    public void testWrite1Tuple() throws IOException, ParseException {
        AccumuloStorage storage = new AccumuloStorage();

        Tuple t = TupleFactory.getInstance().newTuple(1);
        t.set(0, "row");

        Collection<Mutation> mutations = storage.getMutations(t);

        Assert.assertEquals(0, mutations.size());
    }

    @Test(expected = ClassCastException.class)
    public void testWriteLiteralAsMap() throws IOException, ParseException {
        AccumuloStorage storage = new AccumuloStorage();

        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, "row");
        t.set(1, "value");

        storage.getMutations(t).size();
    }

    @Test(expected = ClassCastException.class)
    public void testWriteLiteralAsMapWithAsterisk() throws IOException,
            ParseException {
        AccumuloStorage storage = new AccumuloStorage("*");

        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, "row");
        t.set(1, "value");

        storage.getMutations(t).size();
    }

    @Test
    public void testWrite2TupleWithColumn() throws IOException, ParseException {
        AccumuloStorage storage = new AccumuloStorage("col");

        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, "row");
        t.set(1, "value");

        Collection<Mutation> mutations = storage.getMutations(t);

        Assert.assertEquals(1, mutations.size());

        Mutation m = mutations.iterator().next();

        Assert.assertTrue("Rows not equal",
                Arrays.equals(m.getRow(), ((String) t.get(0)).getBytes()));

        List<ColumnUpdate> colUpdates = m.getUpdates();
        Assert.assertEquals(1, colUpdates.size());

        ColumnUpdate colUpdate = colUpdates.get(0);
        Assert.assertTrue("CF not equal",
                Arrays.equals(colUpdate.getColumnFamily(), "col".getBytes()));
        Assert.assertTrue("CQ not equal",
                Arrays.equals(colUpdate.getColumnQualifier(), new byte[0]));
        Assert.assertTrue("Values not equal",
                Arrays.equals(colUpdate.getValue(), "value".getBytes()));
    }

    @Test
    public void testWrite2TupleWithColumnQual() throws IOException,
            ParseException {
        AccumuloStorage storage = new AccumuloStorage("col:qual");

        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, "row");
        t.set(1, "value");

        Collection<Mutation> mutations = storage.getMutations(t);

        Assert.assertEquals(1, mutations.size());

        Mutation m = mutations.iterator().next();

        Assert.assertTrue("Rows not equal",
                Arrays.equals(m.getRow(), ((String) t.get(0)).getBytes()));

        List<ColumnUpdate> colUpdates = m.getUpdates();
        Assert.assertEquals(1, colUpdates.size());

        ColumnUpdate colUpdate = colUpdates.get(0);
        Assert.assertTrue("CF not equal",
                Arrays.equals(colUpdate.getColumnFamily(), "col".getBytes()));
        Assert.assertTrue("CQ not equal", Arrays.equals(
                colUpdate.getColumnQualifier(), "qual".getBytes()));
        Assert.assertTrue("Values not equal",
                Arrays.equals(colUpdate.getValue(), "value".getBytes()));
    }

    @Test
    public void testWrite2TupleWithMixedColumns() throws IOException,
            ParseException {
        AccumuloStorage storage = new AccumuloStorage(
                "col1,col1:qual,col2:qual,col2");

        Tuple t = TupleFactory.getInstance().newTuple(5);
        t.set(0, "row");
        t.set(1, "value1");
        t.set(2, "value2");
        t.set(3, "value3");
        t.set(4, "value4");

        Collection<Mutation> mutations = storage.getMutations(t);

        Assert.assertEquals(1, mutations.size());

        Mutation m = mutations.iterator().next();

        Assert.assertTrue("Rows not equal",
                Arrays.equals(m.getRow(), ((String) t.get(0)).getBytes()));

        List<ColumnUpdate> colUpdates = m.getUpdates();
        Assert.assertEquals(4, colUpdates.size());

        ColumnUpdate colUpdate = colUpdates.get(0);
        Assert.assertTrue("CF not equal",
                Arrays.equals(colUpdate.getColumnFamily(), "col1".getBytes()));
        Assert.assertTrue("CQ not equal",
                Arrays.equals(colUpdate.getColumnQualifier(), new byte[0]));
        Assert.assertTrue("Values not equal",
                Arrays.equals(colUpdate.getValue(), "value1".getBytes()));

        colUpdate = colUpdates.get(1);
        Assert.assertTrue("CF not equal",
                Arrays.equals(colUpdate.getColumnFamily(), "col1".getBytes()));
        Assert.assertTrue("CQ not equal", Arrays.equals(
                colUpdate.getColumnQualifier(), "qual".getBytes()));
        Assert.assertTrue("Values not equal",
                Arrays.equals(colUpdate.getValue(), "value2".getBytes()));

        colUpdate = colUpdates.get(2);
        Assert.assertTrue("CF not equal",
                Arrays.equals(colUpdate.getColumnFamily(), "col2".getBytes()));
        Assert.assertTrue("CQ not equal", Arrays.equals(
                colUpdate.getColumnQualifier(), "qual".getBytes()));
        Assert.assertTrue("Values not equal",
                Arrays.equals(colUpdate.getValue(), "value3".getBytes()));

        colUpdate = colUpdates.get(3);
        Assert.assertTrue("CF not equal",
                Arrays.equals(colUpdate.getColumnFamily(), "col2".getBytes()));
        Assert.assertTrue("CQ not equal",
                Arrays.equals(colUpdate.getColumnQualifier(), new byte[0]));
        Assert.assertTrue("Values not equal",
                Arrays.equals(colUpdate.getValue(), "value4".getBytes()));
    }

    @Test
    public void testWriteIgnoredExtraColumns() throws IOException,
            ParseException {
        AccumuloStorage storage = new AccumuloStorage("col");

        Tuple t = TupleFactory.getInstance().newTuple(3);
        t.set(0, "row");
        t.set(1, "value1");
        t.set(2, "value2");

        Collection<Mutation> mutations = storage.getMutations(t);

        Assert.assertEquals(1, mutations.size());

        Mutation m = mutations.iterator().next();

        Assert.assertTrue("Rows not equal",
                Arrays.equals(m.getRow(), ((String) t.get(0)).getBytes()));

        List<ColumnUpdate> colUpdates = m.getUpdates();
        Assert.assertEquals(1, colUpdates.size());

        ColumnUpdate colUpdate = colUpdates.get(0);
        Assert.assertTrue("CF not equal",
                Arrays.equals(colUpdate.getColumnFamily(), "col".getBytes()));
        Assert.assertTrue("CQ not equal",
                Arrays.equals(colUpdate.getColumnQualifier(), new byte[0]));
        Assert.assertTrue("Values not equal",
                Arrays.equals(colUpdate.getValue(), "value1".getBytes()));
    }

    @Test
    public void testWriteIgnoredExtraMap() throws IOException, ParseException {
        AccumuloStorage storage = new AccumuloStorage("col1");

        Map<String, Object> map = Maps.newHashMap();

        map.put("mapcol1", "mapval1");
        map.put("mapcol2", "mapval2");
        map.put("mapcol3", "mapval3");
        map.put("mapcol4", "mapval4");

        Tuple t = TupleFactory.getInstance().newTuple(3);
        t.set(0, "row");
        t.set(1, "value1");
        t.set(2, map);

        Collection<Mutation> mutations = storage.getMutations(t);

        Assert.assertEquals(1, mutations.size());

        Mutation m = mutations.iterator().next();

        Assert.assertTrue("Rows not equal",
                Arrays.equals(m.getRow(), ((String) t.get(0)).getBytes()));

        List<ColumnUpdate> colUpdates = m.getUpdates();
        Assert.assertEquals(1, colUpdates.size());

        ColumnUpdate update = colUpdates.get(0);
        Assert.assertEquals("col1", new String(update.getColumnFamily()));
        Assert.assertEquals("", new String(update.getColumnQualifier()));
        Assert.assertEquals("value1", new String(update.getValue()));
    }

    @Test
    public void testWriteMultipleColumnsWithNonExpandedMap()
            throws IOException, ParseException {
        AccumuloStorage storage = new AccumuloStorage("col1,col2");

        Map<String, Object> map = Maps.newHashMap();

        map.put("mapcol1", "mapval1");
        map.put("mapcol2", "mapval2");
        map.put("mapcol3", "mapval3");
        map.put("mapcol4", "mapval4");

        Tuple t = TupleFactory.getInstance().newTuple(3);
        t.set(0, "row");
        t.set(1, "value1");
        t.set(2, map);

        Collection<Mutation> mutations = storage.getMutations(t);

        Assert.assertEquals(1, mutations.size());

        Mutation m = mutations.iterator().next();

        Assert.assertTrue("Rows not equal",
                Arrays.equals(m.getRow(), ((String) t.get(0)).getBytes()));

        List<ColumnUpdate> colUpdates = m.getUpdates();
        Assert.assertEquals(2, colUpdates.size());

        ColumnUpdate update = colUpdates.get(0);
        Assert.assertEquals("col1", new String(update.getColumnFamily()));
        Assert.assertEquals("", new String(update.getColumnQualifier()));
        Assert.assertEquals("value1", new String(update.getValue()));

        update = colUpdates.get(1);
        Assert.assertEquals("col2", new String(update.getColumnFamily()));
        Assert.assertEquals("", new String(update.getColumnQualifier()));
        Assert.assertArrayEquals(storage.objToBytes(map, DataType.MAP),
                update.getValue());
    }

    @Test
    public void testWriteMultipleColumnsWithExpandedMap() throws IOException,
            ParseException {
        AccumuloStorage storage = new AccumuloStorage("col1,col2:");

        Map<String, Object> map = Maps.newHashMap();

        map.put("mapcol1", "mapval1");
        map.put("mapcol2", "mapval2");
        map.put("mapcol3", "mapval3");
        map.put("mapcol4", "mapval4");

        Tuple t = TupleFactory.getInstance().newTuple(3);
        t.set(0, "row");
        t.set(1, "value1");
        t.set(2, map);

        Collection<Mutation> mutations = storage.getMutations(t);

        Assert.assertEquals(1, mutations.size());

        Mutation m = mutations.iterator().next();

        Assert.assertTrue("Rows not equal",
                Arrays.equals(m.getRow(), ((String) t.get(0)).getBytes()));

        List<ColumnUpdate> colUpdates = m.getUpdates();
        Assert.assertEquals(5, colUpdates.size());

        ColumnUpdate update = colUpdates.get(0);
        Assert.assertEquals("col1", new String(update.getColumnFamily()));
        Assert.assertEquals("", new String(update.getColumnQualifier()));
        Assert.assertEquals("value1", new String(update.getValue()));

        Map<Entry<String, String>, String> expectations = Maps.newHashMap();
        expectations.put(Maps.immutableEntry("col2", "mapcol1"), "mapval1");
        expectations.put(Maps.immutableEntry("col2", "mapcol2"), "mapval2");
        expectations.put(Maps.immutableEntry("col2", "mapcol3"), "mapval3");
        expectations.put(Maps.immutableEntry("col2", "mapcol4"), "mapval4");

        for (int i = 1; i < 5; i++) {
            update = colUpdates.get(i);
            Entry<String, String> key = Maps.immutableEntry(
                    new String(update.getColumnFamily()),
                    new String(update.getColumnQualifier()));
            String value = new String(update.getValue());
            Assert.assertTrue("Did not find expected key: " + key,
                    expectations.containsKey(key));

            String actual = expectations.remove(key);
            Assert.assertEquals(value, actual);
        }

        Assert.assertTrue("Did not find all expectations",
                expectations.isEmpty());
    }

    @Test
    public void testWriteMapWithColFamWithColon() throws IOException,
            ParseException {
        AccumuloStorage storage = new AccumuloStorage("col:");

        Map<String, Object> map = Maps.newHashMap();

        map.put("mapcol1", "mapval1");
        map.put("mapcol2", "mapval2");
        map.put("mapcol3", "mapval3");
        map.put("mapcol4", "mapval4");

        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, "row");
        t.set(1, map);

        Collection<Mutation> mutations = storage.getMutations(t);

        Assert.assertEquals(1, mutations.size());

        Mutation m = mutations.iterator().next();

        Assert.assertTrue("Rows not equal",
                Arrays.equals(m.getRow(), ((String) t.get(0)).getBytes()));

        List<ColumnUpdate> colUpdates = m.getUpdates();
        Assert.assertEquals(4, colUpdates.size());

        Map<Entry<String, String>, String> expectations = Maps.newHashMap();
        expectations.put(Maps.immutableEntry("col", "mapcol1"), "mapval1");
        expectations.put(Maps.immutableEntry("col", "mapcol2"), "mapval2");
        expectations.put(Maps.immutableEntry("col", "mapcol3"), "mapval3");
        expectations.put(Maps.immutableEntry("col", "mapcol4"), "mapval4");

        for (ColumnUpdate update : colUpdates) {
            Entry<String, String> key = Maps.immutableEntry(
                    new String(update.getColumnFamily()),
                    new String(update.getColumnQualifier()));
            String value = new String(update.getValue());
            Assert.assertTrue("Did not find expected key: " + key,
                    expectations.containsKey(key));

            String actual = expectations.remove(key);
            Assert.assertEquals(value, actual);
        }

        Assert.assertTrue("Did not find all expectations",
                expectations.isEmpty());
    }

    @Test
    public void testWriteMapWithColFamWithColonAsterisk() throws IOException,
            ParseException {
        AccumuloStorage storage = new AccumuloStorage("col:*");

        Map<String, Object> map = Maps.newHashMap();

        map.put("mapcol1", "mapval1");
        map.put("mapcol2", "mapval2");
        map.put("mapcol3", "mapval3");
        map.put("mapcol4", "mapval4");

        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, "row");
        t.set(1, map);

        Collection<Mutation> mutations = storage.getMutations(t);

        Assert.assertEquals(1, mutations.size());

        Mutation m = mutations.iterator().next();

        Assert.assertTrue("Rows not equal",
                Arrays.equals(m.getRow(), ((String) t.get(0)).getBytes()));

        List<ColumnUpdate> colUpdates = m.getUpdates();
        Assert.assertEquals(4, colUpdates.size());

        Map<Entry<String, String>, String> expectations = Maps.newHashMap();
        expectations.put(Maps.immutableEntry("col", "mapcol1"), "mapval1");
        expectations.put(Maps.immutableEntry("col", "mapcol2"), "mapval2");
        expectations.put(Maps.immutableEntry("col", "mapcol3"), "mapval3");
        expectations.put(Maps.immutableEntry("col", "mapcol4"), "mapval4");

        for (ColumnUpdate update : colUpdates) {
            Entry<String, String> key = Maps.immutableEntry(
                    new String(update.getColumnFamily()),
                    new String(update.getColumnQualifier()));
            String value = new String(update.getValue());
            Assert.assertTrue("Did not find expected key: " + key,
                    expectations.containsKey(key));

            String actual = expectations.remove(key);
            Assert.assertEquals(value, actual);
        }

        Assert.assertTrue("Did not find all expectations",
                expectations.isEmpty());
    }

    @Test
    public void testWriteMapWithColFamColQualPrefix() throws IOException,
            ParseException {
        AccumuloStorage storage = new AccumuloStorage("col:qual_*");

        Map<String, Object> map = Maps.newHashMap();

        map.put("mapcol1", "mapval1");
        map.put("mapcol2", "mapval2");
        map.put("mapcol3", "mapval3");
        map.put("mapcol4", "mapval4");

        Tuple t = TupleFactory.getInstance().newTuple(2);
        t.set(0, "row");
        t.set(1, map);

        Collection<Mutation> mutations = storage.getMutations(t);

        Assert.assertEquals(1, mutations.size());

        Mutation m = mutations.iterator().next();

        Assert.assertTrue("Rows not equal",
                Arrays.equals(m.getRow(), ((String) t.get(0)).getBytes()));

        List<ColumnUpdate> colUpdates = m.getUpdates();
        Assert.assertEquals(4, colUpdates.size());

        Map<Entry<String, String>, String> expectations = Maps.newHashMap();
        expectations.put(Maps.immutableEntry("col", "qual_mapcol1"), "mapval1");
        expectations.put(Maps.immutableEntry("col", "qual_mapcol2"), "mapval2");
        expectations.put(Maps.immutableEntry("col", "qual_mapcol3"), "mapval3");
        expectations.put(Maps.immutableEntry("col", "qual_mapcol4"), "mapval4");

        for (ColumnUpdate update : colUpdates) {
            Entry<String, String> key = Maps.immutableEntry(
                    new String(update.getColumnFamily()),
                    new String(update.getColumnQualifier()));
            String value = new String(update.getValue());
            Assert.assertTrue(expectations.containsKey(key));

            String actual = expectations.remove(key);
            Assert.assertEquals(value, actual);
        }

        Assert.assertTrue("Did not find all expectations",
                expectations.isEmpty());
    }

    @Test
    public void testReadSingleKey() throws IOException, ParseException {
        AccumuloStorage storage = new AccumuloStorage();

        List<Key> keys = Lists.newArrayList();
        List<Value> values = Lists.newArrayList();

        keys.add(new Key("1", "", "col1"));
        values.add(new Value("value1".getBytes()));

        Key k = new Key("1");
        Value v = WholeRowIterator.encodeRow(keys, values);

        Tuple t = storage.getTuple(k, v);

        Assert.assertEquals(2, t.size());

        Assert.assertEquals("1", t.get(0).toString());

        InternalMap map = new InternalMap();
        map.put(":col1", new DataByteArray("value1"));

        Assert.assertEquals(map, t.get(1));
    }

    @Test
    public void testReadSingleColumn() throws IOException, ParseException {
        AccumuloStorage storage = new AccumuloStorage();

        List<Key> keys = Lists.newArrayList();
        List<Value> values = Lists.newArrayList();

        keys.add(new Key("1", "col1", "cq1"));
        keys.add(new Key("1", "col1", "cq2"));
        keys.add(new Key("1", "col1", "cq3"));

        values.add(new Value("value1".getBytes()));
        values.add(new Value("value2".getBytes()));
        values.add(new Value("value3".getBytes()));

        Key k = new Key("1");
        Value v = WholeRowIterator.encodeRow(keys, values);

        Tuple t = storage.getTuple(k, v);

        Assert.assertEquals(2, t.size());

        Assert.assertEquals("1", t.get(0).toString());

        InternalMap map = new InternalMap();
        map.put("col1:cq1", new DataByteArray("value1"));
        map.put("col1:cq2", new DataByteArray("value2"));
        map.put("col1:cq3", new DataByteArray("value3"));

        Assert.assertEquals(map, t.get(1));
    }

    @Test
    public void testReadMultipleColumnsAggregateColfamsAsterisk()
            throws IOException, ParseException {
        AccumuloStorage storage = new AccumuloStorage("*");

        List<Key> keys = Lists.newArrayList();
        List<Value> values = Lists.newArrayList();

        keys.add(new Key("1", "col1", "cq1"));
        keys.add(new Key("1", "col1", "cq2"));
        keys.add(new Key("1", "col1", "cq3"));
        keys.add(new Key("1", "col2", "cq1"));
        keys.add(new Key("1", "col3", "cq1"));
        keys.add(new Key("1", "col3", "cq2"));

        values.add(new Value("value1".getBytes()));
        values.add(new Value("value2".getBytes()));
        values.add(new Value("value3".getBytes()));
        values.add(new Value("value1".getBytes()));
        values.add(new Value("value1".getBytes()));
        values.add(new Value("value2".getBytes()));

        Key k = new Key("1");
        Value v = WholeRowIterator.encodeRow(keys, values);

        Tuple t = storage.getTuple(k, v);

        Assert.assertEquals(2, t.size());

        Assert.assertEquals("1", t.get(0).toString());

        InternalMap map = new InternalMap();
        map.put("col1:cq1", new DataByteArray("value1"));
        map.put("col1:cq2", new DataByteArray("value2"));
        map.put("col1:cq3", new DataByteArray("value3"));
        map.put("col2:cq1", new DataByteArray("value1"));
        map.put("col3:cq1", new DataByteArray("value1"));
        map.put("col3:cq2", new DataByteArray("value2"));

        Assert.assertEquals(map, t.get(1));
    }

    @Test
    public void testReadMultipleColumnsAggregateColfamsAsteriskEmptyColfam()
            throws IOException, ParseException {
        AccumuloStorage storage = new AccumuloStorage("*");

        List<Key> keys = Lists.newArrayList();
        List<Value> values = Lists.newArrayList();

        keys.add(new Key("1", "col1", ""));
        keys.add(new Key("1", "col2", ""));
        keys.add(new Key("1", "col3", ""));

        values.add(new Value("value1".getBytes()));
        values.add(new Value("value2".getBytes()));
        values.add(new Value("value3".getBytes()));

        Key k = new Key("1");
        Value v = WholeRowIterator.encodeRow(keys, values);

        Tuple t = storage.getTuple(k, v);

        Assert.assertEquals(2, t.size());

        Assert.assertEquals("1", t.get(0).toString());

        InternalMap map = new InternalMap();
        map.put("col1", new DataByteArray("value1"));
        map.put("col2", new DataByteArray("value2"));
        map.put("col3", new DataByteArray("value3"));

        Assert.assertEquals(map, t.get(1));
    }

    @Test
    public void testReadMultipleColumnsEmptyString() throws IOException,
            ParseException {
        AccumuloStorage storage = new AccumuloStorage("");

        List<Key> keys = Lists.newArrayList();
        List<Value> values = Lists.newArrayList();

        keys.add(new Key("1", "col1", "cq1"));
        keys.add(new Key("1", "col1", "cq2"));
        keys.add(new Key("1", "col1", "cq3"));
        keys.add(new Key("1", "col2", "cq1"));
        keys.add(new Key("1", "col3", "cq1"));
        keys.add(new Key("1", "col3", "cq2"));

        values.add(new Value("value1".getBytes()));
        values.add(new Value("value2".getBytes()));
        values.add(new Value("value3".getBytes()));
        values.add(new Value("value1".getBytes()));
        values.add(new Value("value1".getBytes()));
        values.add(new Value("value2".getBytes()));

        Key k = new Key("1");
        Value v = WholeRowIterator.encodeRow(keys, values);

        Tuple t = storage.getTuple(k, v);

        Assert.assertEquals(2, t.size());

        Assert.assertEquals("1", t.get(0).toString());

        InternalMap map = new InternalMap();
        map.put("col1:cq1", new DataByteArray("value1"));
        map.put("col1:cq2", new DataByteArray("value2"));
        map.put("col1:cq3", new DataByteArray("value3"));
        map.put("col2:cq1", new DataByteArray("value1"));
        map.put("col3:cq1", new DataByteArray("value1"));
        map.put("col3:cq2", new DataByteArray("value2"));

        Assert.assertEquals(map, t.get(1));
    }

    @Test
    public void testReadMultipleColumnsNoColfamAggregate() throws IOException,
            ParseException {
        AccumuloStorage storage = new AccumuloStorage();

        List<Key> keys = Lists.newArrayList();
        List<Value> values = Lists.newArrayList();

        keys.add(new Key("1", "col1", "cq1"));
        keys.add(new Key("1", "col1", "cq2"));
        keys.add(new Key("1", "col1", "cq3"));
        keys.add(new Key("1", "col2", "cq1"));
        keys.add(new Key("1", "col3", "cq1"));
        keys.add(new Key("1", "col3", "cq2"));

        values.add(new Value("value1".getBytes()));
        values.add(new Value("value2".getBytes()));
        values.add(new Value("value3".getBytes()));
        values.add(new Value("value1".getBytes()));
        values.add(new Value("value1".getBytes()));
        values.add(new Value("value2".getBytes()));

        Key k = new Key("1");
        Value v = WholeRowIterator.encodeRow(keys, values);

        Tuple t = storage.getTuple(k, v);

        Assert.assertEquals(2, t.size());

        Assert.assertEquals("1", t.get(0).toString());

        InternalMap map = new InternalMap();
        map.put("col1:cq1", new DataByteArray("value1"));
        map.put("col1:cq2", new DataByteArray("value2"));
        map.put("col1:cq3", new DataByteArray("value3"));
        map.put("col2:cq1", new DataByteArray("value1"));
        map.put("col3:cq1", new DataByteArray("value1"));
        map.put("col3:cq2", new DataByteArray("value2"));

        Assert.assertEquals(map, t.get(1));
    }

    @Test
    public void testReadMultipleScalars() throws IOException, ParseException {
        AccumuloStorage storage = new AccumuloStorage("col1,col3,col5");

        List<Key> keys = Lists.newArrayList();
        List<Value> values = Lists.newArrayList();

        // Filtering by AccumuloInputFormat isn't applied here since we're
        // shortcircuiting things
        keys.add(new Key("1", "col1", ""));
        // keys.add(new Key("1", "col2", ""));
        keys.add(new Key("1", "col3", ""));
        // keys.add(new Key("1", "col4", ""));
        keys.add(new Key("1", "col5", ""));

        values.add(new Value("value1".getBytes()));
        // values.add(new Value("value2".getBytes()));
        values.add(new Value("value3".getBytes()));
        // values.add(new Value("value4".getBytes()));
        values.add(new Value("value5".getBytes()));

        Key k = new Key("1");
        Value v = WholeRowIterator.encodeRow(keys, values);

        Tuple t = storage.getTuple(k, v);

        Assert.assertEquals(4, t.size());

        Assert.assertEquals("1", t.get(0).toString());

        Assert.assertEquals("value1", t.get(1).toString());
        Assert.assertEquals("value3", t.get(2).toString());
        Assert.assertEquals("value5", t.get(3).toString());
    }

    @Test
    public void testUnsortedColumnList() throws IOException, ParseException {
        AccumuloStorage storage = new AccumuloStorage("z,a");

        List<Key> keys = Lists.newArrayList();
        List<Value> values = Lists.newArrayList();

        keys.add(new Key("1", "a", ""));
        keys.add(new Key("1", "z", ""));

        values.add(new Value("a".getBytes()));
        values.add(new Value("z".getBytes()));

        Key k = new Key("1");
        Value v = WholeRowIterator.encodeRow(keys, values);

        Tuple t = storage.getTuple(k, v);

        Assert.assertEquals(3, t.size());

        Assert.assertEquals("z", t.get(1).toString());
        Assert.assertEquals("a", t.get(2).toString());
    }

    @Test
    public void testReadMultipleScalarsAndMaps() throws IOException,
            ParseException {
        AccumuloStorage storage = new AccumuloStorage("z,r:,m:2,b:");

        List<Key> keys = Lists.newArrayList();
        List<Value> values = Lists.newArrayList();

        keys.add(new Key("1", "a", "1"));
        keys.add(new Key("1", "b", "1"));
        keys.add(new Key("1", "b", "2"));
        keys.add(new Key("1", "f", "1"));
        keys.add(new Key("1", "f", "2"));
        keys.add(new Key("1", "m", "1"));
        keys.add(new Key("1", "m", "2"));
        keys.add(new Key("1", "r", "1"));
        keys.add(new Key("1", "r", "2"));
        keys.add(new Key("1", "r", "3"));
        keys.add(new Key("1", "z", ""));

        values.add(new Value("a1".getBytes()));
        values.add(new Value("b1".getBytes()));
        values.add(new Value("b2".getBytes()));
        values.add(new Value("f1".getBytes()));
        values.add(new Value("f2".getBytes()));
        values.add(new Value("m1".getBytes()));
        values.add(new Value("m2".getBytes()));
        values.add(new Value("r1".getBytes()));
        values.add(new Value("r2".getBytes()));
        values.add(new Value("r3".getBytes()));
        values.add(new Value("z1".getBytes()));

        Key k = new Key("1");
        Value v = WholeRowIterator.encodeRow(keys, values);

        Tuple t = storage.getTuple(k, v);

        Assert.assertEquals(5, t.size());

        Assert.assertEquals(new DataByteArray("z1".getBytes()), t.get(1));

        HashMap<String, DataByteArray> rMap = new HashMap<String, DataByteArray>();
        rMap.put("r:1", new DataByteArray("r1".getBytes()));
        rMap.put("r:2", new DataByteArray("r2".getBytes()));
        rMap.put("r:3", new DataByteArray("r3".getBytes()));
        Assert.assertEquals(rMap, t.get(2));

        Assert.assertEquals(new DataByteArray("m2".getBytes()), t.get(3));

        HashMap<String, DataByteArray> bMap = new HashMap<String, DataByteArray>();
        bMap.put("b:1", new DataByteArray("b1".getBytes()));
        bMap.put("b:2", new DataByteArray("b2".getBytes()));
        Assert.assertEquals(bMap, t.get(4));
    }

}
