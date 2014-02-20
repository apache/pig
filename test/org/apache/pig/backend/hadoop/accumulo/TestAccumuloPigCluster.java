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

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.MiniCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class TestAccumuloPigCluster {

    @SuppressWarnings("unchecked")
    private static final List<ImmutableMap<String, String>> AIRPORTS = Lists
            .newArrayList(
                    ImmutableMap.of("code", "SJC", "name", "San Jose"),
                    ImmutableMap.of("code", "SFO", "name", "San Francisco"),
                    ImmutableMap.of("code", "MDO", "name", "Orlando"),
                    ImmutableMap.of("code", "MDW", "name", "Chicago-Midway"),
                    ImmutableMap.of("code", "JFK", "name", "JFK International"),
                    ImmutableMap.of("code", "BWI", "name",
                            "Baltimore-Washington"));

    @SuppressWarnings("unchecked")
    private static final List<ImmutableMap<String, String>> flightData = Lists
            .newArrayList(
                    ImmutableMap.of("origin", "BWI", "destination", "SFO"),
                    ImmutableMap.of("origin", "BWI", "destination", "SJC"),
                    ImmutableMap.of("origin", "MDW", "destination", "MDO"),
                    ImmutableMap.of("origin", "MDO", "destination", "SJC"),
                    ImmutableMap.of("origin", "SJC", "destination", "JFK"),
                    ImmutableMap.of("origin", "JFK", "destination", "MDW"));

    private static final Logger log = Logger
            .getLogger(TestAccumuloPigCluster.class);
    private static final File tmpdir = Files.createTempDir();
    private static MiniAccumuloCluster accumuloCluster;
    private static MiniCluster cluster;
    private static Configuration conf;
    private PigServer pig;

    @BeforeClass
    public static void setupClusters() throws Exception {
        MiniAccumuloConfig macConf = new MiniAccumuloConfig(tmpdir, "password");
        macConf.setNumTservers(1);

        accumuloCluster = new MiniAccumuloCluster(macConf);
        accumuloCluster.start();

        // This is needed by Pig
        cluster = MiniCluster.buildCluster();
        conf = cluster.getConfiguration();
    }

    @Before
    public void beforeTest() throws Exception {
        pig = new PigServer(ExecType.LOCAL, conf);
    }

    @AfterClass
    public static void stopClusters() throws Exception {
        cluster.shutDown();
        accumuloCluster.stop();
        FileUtils.deleteDirectory(tmpdir);
    }

    private void loadTestData() throws Exception {
        ZooKeeperInstance inst = new ZooKeeperInstance(
                accumuloCluster.getInstanceName(),
                accumuloCluster.getZooKeepers());
        Connector c = inst.getConnector("root", new PasswordToken("password"));

        TableOperations tops = c.tableOperations();
        if (!tops.exists("airports")) {
            tops.create("airports");
        }

        if (!tops.exists("flights")) {
            tops.create("flights");
        }

        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxWriteThreads(1);
        config.setMaxLatency(100000l, TimeUnit.MILLISECONDS);
        config.setMaxMemory(10000l);

        BatchWriter bw = c.createBatchWriter("airports", config);
        try {
            int i = 1;
            for (Map<String, String> record : AIRPORTS) {
                Mutation m = new Mutation(Integer.toString(i));

                for (Entry<String, String> entry : record.entrySet()) {
                    m.put(entry.getKey(), "", entry.getValue());
                }

                bw.addMutation(m);
                i++;
            }
        } finally {
            if (null != bw) {
                bw.close();
            }
        }

        bw = c.createBatchWriter("flights", config);
        try {
            int i = 1;
            for (Map<String, String> record : flightData) {
                Mutation m = new Mutation(Integer.toString(i));

                for (Entry<String, String> entry : record.entrySet()) {
                    m.put(entry.getKey(), "", entry.getValue());
                }

                bw.addMutation(m);
                i++;
            }
        } finally {
            if (null != bw) {
                bw.close();
            }
        }
    }

    @Test
    public void test() throws Exception {
        loadTestData();

        final String loadFlights = "flights = LOAD 'accumulo://flights?instance="
                + accumuloCluster.getInstanceName()
                + "&user=root&password=password&zookeepers="
                + accumuloCluster.getZooKeepers()
                + "' using org.apache.pig.backend.hadoop.accumulo.AccumuloStorage()"
                + " as (rowKey:chararray, column_map:map[]);";

        final String loadAirports = "airports = LOAD 'accumulo://airports?instance="
                + accumuloCluster.getInstanceName()
                + "&user=root&password=password&zookeepers="
                + accumuloCluster.getZooKeepers()
                + "' using org.apache.pig.backend.hadoop.accumulo.AccumuloStorage()"
                + " as (rowKey:chararray, column_map:map[]);";

        final String joinQuery = "joined = JOIN flights BY column_map#'origin', airports BY column_map#'code';";

        pig.registerQuery(loadFlights);
        pig.registerQuery(loadAirports);
        pig.registerQuery(joinQuery);

        Iterator<Tuple> it = pig.openIterator("joined");

        int i = 0;
        while (it.hasNext()) {
            Tuple t = it.next();

            // id and map for each dataset we joined
            Assert.assertEquals(4, t.size());

            Object o = t.get(1);

            Map<String, String> airport = null, flight = null;

            Assert.assertTrue(Map.class.isAssignableFrom(o.getClass()));
            @SuppressWarnings("unchecked")
            Map<String, String> data1 = (Map<String, String>) o;
            Assert.assertTrue(!data1.isEmpty());

            if (data1.containsKey("origin")) {
                flight = data1;
            } else if (data1.containsKey("code")) {
                airport = data1;
            } else {
                Assert.fail("Received map which did not contain an expected key");
            }

            o = t.get(3);

            Assert.assertTrue(Map.class.isAssignableFrom(o.getClass()));
            @SuppressWarnings("unchecked")
            Map<String, String> data2 = (Map<String, String>) o;
            Assert.assertTrue(!data2.isEmpty());

            if (null == flight && data2.containsKey("origin")) {
                flight = data2;
            } else if (null == airport && data2.containsKey("code")) {
                airport = data2;
            } else {
                Assert.fail("Received map which did not contain an expected key");
            }

            Assert.assertTrue(null != airport && null != flight);

            Assert.assertEquals(airport.get("code"), flight.get("origin"));

            i++;
        }

        Assert.assertEquals(6, i);
    }

}
