/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.test;

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestParser {

    protected final Log log = LogFactory.getLog(getClass());

    private static MiniGenericCluster cluster = MiniGenericCluster.buildCluster();
    private static ExecType[] execTypes = new ExecType[] { ExecType.LOCAL, cluster.getExecType() };
    protected PigServer pigServer;

    @After
    public void tearDown() throws Exception {
        pigServer.shutdown();
        Util.resetStateForExecModeSwitch();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        if (cluster != null)
            cluster.shutDown();
    }

    public void setUp(ExecType execType) throws Exception{
        Util.resetStateForExecModeSwitch();
        if (execType == cluster.getExecType()) {
            pigServer = new PigServer(cluster.getExecType(), cluster.getProperties());
        } else {
            pigServer = new PigServer(Util.getLocalTestMode());
        }
    }

    @Test(expected = IOException.class)
    public void testLoadingNonexistentFile() throws Exception {
        for (ExecType execType : execTypes) {
            setUp(execType);
            pigServer.registerQuery("vals = load 'nonexistentfile';");
            pigServer.openIterator("vals");
        }
    }

    @Test
    public void testRemoteServerList() throws Exception {
        for (ExecType execType : execTypes) {
            setUp(execType);
            Properties pigProperties = pigServer.getPigContext().getProperties();
            pigProperties.setProperty("fs.defaultFS", "hdfs://a.com:8020");
            Configuration conf;

            Data data = Storage.resetData(pigServer.getPigContext());
            data.set("/user/pig/1.txt");// no data

            pigServer.registerQuery("a = load '/user/pig/1.txt' using mock.Storage;");
            conf = ConfigurationUtil.toConfiguration(pigProperties);
            assertTrue(conf.get(MRConfiguration.JOB_HDFS_SERVERS) == null ||
                    conf.get(MRConfiguration.JOB_HDFS_SERVERS).equals(pigProperties.get("fs.defaultFS"))||
                    conf.get(MRConfiguration.JOB_HDFS_SERVERS).equals(pigProperties.get("fs.defaultFS")));

            pigServer.registerQuery("a = load 'hdfs://a.com/user/pig/1.txt' using mock.Storage;");
            conf = ConfigurationUtil.toConfiguration(pigProperties);
            assertTrue(pigProperties.getProperty(MRConfiguration.JOB_HDFS_SERVERS) == null ||
                    conf.get(MRConfiguration.JOB_HDFS_SERVERS).equals(pigProperties.get("fs.defaultFS"))||
                    conf.get(MRConfiguration.JOB_HDFS_SERVERS).equals(pigProperties.get("fs.defaultFS")));

            pigServer.registerQuery("a = load 'har:///1.txt' using mock.Storage;");
            conf = ConfigurationUtil.toConfiguration(pigProperties);
            assertTrue(pigProperties.getProperty(MRConfiguration.JOB_HDFS_SERVERS) == null ||
                    conf.get(MRConfiguration.JOB_HDFS_SERVERS).equals(pigProperties.get("fs.defaultFS"))||
                    conf.get(MRConfiguration.JOB_HDFS_SERVERS).equals(pigProperties.get("fs.defaultFS")));

            pigServer.registerQuery("a = load 'hdfs://b.com/user/pig/1.txt' using mock.Storage;");
            conf = ConfigurationUtil.toConfiguration(pigProperties);
            assertTrue(conf.get(MRConfiguration.JOB_HDFS_SERVERS) != null &&
                    conf.get(MRConfiguration.JOB_HDFS_SERVERS).contains("hdfs://b.com"));

            pigServer.registerQuery("a = load 'har://hdfs-c.com/user/pig/1.txt' using mock.Storage;");
            conf = ConfigurationUtil.toConfiguration(pigProperties);
            assertTrue(conf.get(MRConfiguration.JOB_HDFS_SERVERS) != null &&
                    conf.get(MRConfiguration.JOB_HDFS_SERVERS).contains("hdfs://c.com"));

            pigServer.registerQuery("a = load 'hdfs://d.com:8020/user/pig/1.txt' using mock.Storage;");
            conf = ConfigurationUtil.toConfiguration(pigProperties);
            assertTrue(conf.get(MRConfiguration.JOB_HDFS_SERVERS) != null &&
                    conf.get(MRConfiguration.JOB_HDFS_SERVERS).contains("hdfs://d.com:8020"));
        }
    }

    @Test
    public void testRemoteServerList2() throws Exception {
        pigServer = new PigServer(Util.getLocalTestMode());
        Properties pigProperties = pigServer.getPigContext().getProperties();
        pigProperties.setProperty("fs.defaultFS", "hdfs://a.com:8020");
        Configuration conf;

        pigServer.setBatchOn();

        Data data = Storage.resetData(pigServer.getPigContext());
        data.set("/user/pig/1.txt");// no data

        pigServer.registerQuery("a = load '/user/pig/1.txt' using mock.Storage;");
        pigServer.registerQuery("store a into '/user/pig/1.txt';");

        System.out.println("hdfs-servers: "
                + pigProperties.getProperty(MRConfiguration.JOB_HDFS_SERVERS));
        conf = ConfigurationUtil.toConfiguration(pigProperties);
        assertTrue(conf.get(MRConfiguration.JOB_HDFS_SERVERS) == null ||
                conf.get(MRConfiguration.JOB_HDFS_SERVERS).equals(pigProperties.get("fs.defaultFS"))||
                conf.get(MRConfiguration.JOB_HDFS_SERVERS).equals(pigProperties.get("fs.defaultFS")));

        pigServer.registerQuery("store a into 'hdfs://b.com/user/pig/1.txt' using mock.Storage;");
        System.out.println("hdfs-servers: "
                + pigProperties.getProperty(MRConfiguration.JOB_HDFS_SERVERS));
        conf = ConfigurationUtil.toConfiguration(pigProperties);
        assertTrue(conf.get(MRConfiguration.JOB_HDFS_SERVERS) != null &&
                conf.get(MRConfiguration.JOB_HDFS_SERVERS).contains("hdfs://b.com"));

        pigServer.registerQuery("store a into 'har://hdfs-c.com:8020/user/pig/1.txt' using mock.Storage;");
        System.out.println("hdfs-servers: "
                + pigProperties.getProperty(MRConfiguration.JOB_HDFS_SERVERS));
        conf = ConfigurationUtil.toConfiguration(pigProperties);
        assertTrue(conf.get(MRConfiguration.JOB_HDFS_SERVERS) != null &&
                conf.get(MRConfiguration.JOB_HDFS_SERVERS).contains("hdfs://c.com:8020"));

        pigServer.registerQuery("store a into 'hdfs://d.com:8020/user/pig/1.txt' using mock.Storage;");
        System.out.println("hdfs-servers: "
                + pigProperties.getProperty(MRConfiguration.JOB_HDFS_SERVERS));
        conf = ConfigurationUtil.toConfiguration(pigProperties);
        assertTrue(conf.get(MRConfiguration.JOB_HDFS_SERVERS) != null &&
                conf.get(MRConfiguration.JOB_HDFS_SERVERS).contains("hdfs://d.com:8020"));

    }

    @Test
    public void testRestrictedColumnNamesWhitelist() throws Exception {
        pigServer = new PigServer(Util.getLocalTestMode());
        Data data = resetData(pigServer);

        Set<Tuple> tuples = Sets.newHashSet(tuple(1),tuple(2),tuple(3));
        data.set("foo",
            "x:int",
            tuples
            );

        pigServer.registerQuery("a = load 'foo' using mock.Storage();");
        pigServer.registerQuery("a = foreach a generate x as rank;");
        pigServer.registerQuery("a = foreach a generate rank as cube;");
        pigServer.registerQuery("a = foreach a generate cube as y;");
        pigServer.registerQuery("rank = a;");
        pigServer.registerQuery("cube = rank;");
        pigServer.registerQuery("rank = cube;");
        pigServer.registerQuery("cube = foreach rank generate y as cube;");
        pigServer.registerQuery("store cube into 'baz' using mock.Storage();");
        List<Tuple> tuples2 = data.get("baz");
        assertEquals(tuples.size(), tuples2.size());
        for (Tuple t : tuples2) {
            tuples.remove(t);
        }
        assertTrue(tuples.isEmpty());

    }
}

