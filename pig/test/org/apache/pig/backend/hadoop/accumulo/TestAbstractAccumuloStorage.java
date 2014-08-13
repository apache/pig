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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.junit.Assert;
import org.junit.Test;

public class TestAbstractAccumuloStorage {

    public static void assertConfigurationsEqual(Configuration expectedConf,
            Configuration actualConf) {
        // Make sure the values in both confs are equal
        Iterator<Entry<String, String>> expectedIter = expectedConf.iterator();
        while (expectedIter.hasNext()) {
            Entry<String, String> e = expectedIter.next();
            assertEquals("Values differed for " + e.getKey(),
                    expectedConf.get(e.getKey()), actualConf.get(e.getKey()));
        }

        Iterator<Entry<String, String>> actualIter = actualConf.iterator();
        while (actualIter.hasNext()) {
            Entry<String, String> e = actualIter.next();
            assertEquals("Values differed for " + e.getKey(),
                    expectedConf.get(e.getKey()), actualConf.get(e.getKey()));
        }
    }

    public static void assertKeyValueEqualsTuple(Key key, Value value,
            Tuple tuple) throws ExecException {
        assertTrue(Arrays.equals(key.getRow().getBytes(),
                ((DataByteArray) tuple.get(0)).get()));
        assertTrue(Arrays.equals(key.getColumnFamily().getBytes(),
                ((DataByteArray) tuple.get(1)).get()));
        assertTrue(Arrays.equals(key.getColumnQualifier().getBytes(),
                ((DataByteArray) tuple.get(2)).get()));
        assertTrue(Arrays.equals(key.getColumnVisibility().getBytes(),
                ((DataByteArray) tuple.get(3)).get()));
        assertEquals(key.getTimestamp(), ((Long) tuple.get(4)).longValue());
        assertTrue(Arrays.equals(value.get(),
                ((DataByteArray) tuple.get(5)).get()));
    }

    public static void assertWholeRowKeyValueEqualsTuple(Key key, Value value,
            Tuple mainTuple) throws IOException {
        assertTrue(Arrays.equals(key.getRow().getBytes(),
                ((DataByteArray) mainTuple.get(0)).get()));

        DefaultDataBag bag = (DefaultDataBag) mainTuple.get(1);
        Iterator<Tuple> iter = bag.iterator();

        for (Entry<Key, Value> e : WholeRowIterator.decodeRow(key, value)
                .entrySet()) {
            Tuple tuple = iter.next();

            assertTrue(Arrays.equals(e.getKey().getColumnFamily().getBytes(),
                    ((DataByteArray) tuple.get(0)).get()));
            assertTrue(Arrays.equals(
                    e.getKey().getColumnQualifier().getBytes(),
                    ((DataByteArray) tuple.get(1)).get()));
            assertTrue(Arrays.equals(e.getKey().getColumnVisibility()
                    .getBytes(), ((DataByteArray) tuple.get(2)).get()));
            assertEquals(e.getKey().getTimestamp(),
                    ((Long) tuple.get(3)).longValue());
            assertTrue(Arrays.equals(e.getValue().get(),
                    ((DataByteArray) tuple.get(4)).get()));
        }
    }

    public Job getExpectedLoadJob(String inst, String zookeepers, String user,
            String password, String table, String start, String end,
            Authorizations authorizations,
            List<Pair<Text, Text>> columnFamilyColumnQualifierPairs)
            throws IOException {
        Collection<Range> ranges = new LinkedList<Range>();
        ranges.add(new Range(start, end));

        Job expected = new Job(new Configuration());

        try {
            AccumuloInputFormat.setConnectorInfo(expected, user,
                    new PasswordToken(password));
        } catch (AccumuloSecurityException e) {
            Assert.fail(e.getMessage());
        }
        AccumuloInputFormat.setInputTableName(expected, table);
        AccumuloInputFormat.setScanAuthorizations(expected, authorizations);
        AccumuloInputFormat.setZooKeeperInstance(expected, inst, zookeepers);
        AccumuloInputFormat.fetchColumns(expected,
                columnFamilyColumnQualifierPairs);
        AccumuloInputFormat.setRanges(expected, ranges);

        return expected;
    }

    public Job getDefaultExpectedLoadJob() throws IOException {
        String inst = "myinstance";
        String zookeepers = "127.0.0.1:2181";
        String user = "root";
        String password = "secret";
        String table = "table1";
        String start = "abc";
        String end = "z";
        Authorizations authorizations = new Authorizations(
                "PRIVATE,PUBLIC".split(","));

        List<Pair<Text, Text>> columnFamilyColumnQualifierPairs = new LinkedList<Pair<Text, Text>>();
        columnFamilyColumnQualifierPairs.add(new Pair<Text, Text>(new Text(
                "col1"), new Text("cq1")));
        columnFamilyColumnQualifierPairs.add(new Pair<Text, Text>(new Text(
                "col2"), new Text("cq2")));
        columnFamilyColumnQualifierPairs.add(new Pair<Text, Text>(new Text(
                "col3"), null));

        Job expected = getExpectedLoadJob(inst, zookeepers, user, password,
                table, start, end, authorizations,
                columnFamilyColumnQualifierPairs);
        return expected;
    }

    public Job getExpectedStoreJob(String inst, String zookeepers, String user,
            String password, String table, long maxWriteBufferSize,
            int writeThreads, int maxWriteLatencyMS) throws IOException {

        Job expected = new Job(new Configuration());

        try {
            AccumuloOutputFormat.setConnectorInfo(expected, user,
                    new PasswordToken(password));
        } catch (AccumuloSecurityException e) {
            Assert.fail(e.getMessage());
        }

        AccumuloOutputFormat.setZooKeeperInstance(expected, inst, zookeepers);
        AccumuloOutputFormat.setCreateTables(expected, true);

        BatchWriterConfig bwConfig = new BatchWriterConfig();
        bwConfig.setMaxLatency(maxWriteLatencyMS, TimeUnit.MILLISECONDS);
        bwConfig.setMaxMemory(maxWriteBufferSize);
        bwConfig.setMaxWriteThreads(writeThreads);

        AccumuloOutputFormat.setBatchWriterOptions(expected, bwConfig);

        return expected;
    }

    public Job getDefaultExpectedStoreJob() throws IOException {
        String inst = "myinstance";
        String zookeepers = "127.0.0.1:2181";
        String user = "root";
        String password = "secret";
        String table = "table1";
        long maxWriteBufferSize = 1234000;
        int writeThreads = 7;
        int maxWriteLatencyMS = 30000;

        Job expected = getExpectedStoreJob(inst, zookeepers, user, password,
                table, maxWriteBufferSize, writeThreads, maxWriteLatencyMS);
        return expected;
    }

    public String getDefaultLoadLocation() {
        return "accumulo://table1?instance=myinstance&user=root&password=secret&zookeepers=127.0.0.1:2181&auths=PRIVATE,PUBLIC&fetch_columns=col1:cq1,col2:cq2,col3&start=abc&end=z";
    }

    public String getDefaultStoreLocation() {
        return "accumulo://table1?instance=myinstance&user=root&password=secret&zookeepers=127.0.0.1:2181&write_buffer_size_bytes=1234000&write_threads=7&write_latency_ms=30000";
    }

    public static AbstractAccumuloStorage getAbstractAccumuloStorage()
            throws ParseException, IOException {
        return getAbstractAccumuloStorage("");
    }

    public static AbstractAccumuloStorage getAbstractAccumuloStorage(
            String columns) throws ParseException, IOException {
        return getAbstractAccumuloStorage(columns, "");
    }

    public static AbstractAccumuloStorage getAbstractAccumuloStorage(
            String columns, String args) throws ParseException, IOException {
        return new AbstractAccumuloStorage(columns, args) {

            @Override
            public Collection<Mutation> getMutations(Tuple tuple) {
                return null;
            }

            @Override
            protected Tuple getTuple(Key key, Value value) throws IOException {
                return null;
            }
        };
    }

    @Test
    public void testSetLoadLocation() throws IOException, ParseException {
        AbstractAccumuloStorage s = getAbstractAccumuloStorage();

        Job actual = new Job();
        s.setLocation(getDefaultLoadLocation(), actual);
        Configuration actualConf = actual.getConfiguration();

        Job expected = getDefaultExpectedLoadJob();
        Configuration expectedConf = expected.getConfiguration();

        s.loadDependentJars(expectedConf);

        assertConfigurationsEqual(expectedConf, actualConf);
    }

    @Test
    public void testSetStoreLocation() throws IOException, ParseException {
        AbstractAccumuloStorage s = getAbstractAccumuloStorage();

        Job actual = new Job();
        s.setStoreLocation(getDefaultStoreLocation(), actual);
        Configuration actualConf = actual.getConfiguration();

        Job expected = getDefaultExpectedStoreJob();
        Configuration expectedConf = expected.getConfiguration();

        s.loadDependentJars(expectedConf);

        assertConfigurationsEqual(expectedConf, actualConf);
    }
}
