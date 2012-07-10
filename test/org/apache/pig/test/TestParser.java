/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.test;

import static org.apache.pig.ExecType.LOCAL;
import static org.apache.pig.ExecType.MAPREDUCE;

import java.io.IOException;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.PigServer;
import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestParser extends TestCase {

protected final Log log = LogFactory.getLog(getClass());

    protected ExecType execType = MAPREDUCE;

    private static MiniCluster cluster;
    protected PigServer pigServer;

    @Before
    @Override
    public void setUp() throws Exception {

        String execTypeString = System.getProperty("test.exectype");
        if(execTypeString!=null && execTypeString.length()>0){
            execType = ExecType.fromString(execTypeString);
        }
        if(execType == MAPREDUCE) {
            cluster = MiniCluster.buildCluster();
            pigServer = new PigServer(MAPREDUCE, cluster.getProperties());
        } else {
            pigServer = new PigServer(LOCAL);
        }
    }

    @Override
    @After
    public void tearDown() throws Exception {
        pigServer.shutdown();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        if(cluster != null)
            cluster.shutDown();
    }

    @Test
    public void testLoadingNonexistentFile() throws ExecException, IOException {
        try {
            // FIXME : this should be tested in all modes
            if(execType == ExecType.LOCAL)
                return;
            pigServer.registerQuery("vals = load 'nonexistentfile';");
            pigServer.openIterator("vals");
            fail("Loading a  nonexistent file should throw an IOException at parse time");
        } catch (IOException io) {
        }
    }

    @Test
    public void testRemoteServerList() throws ExecException, IOException {
        try {
            Properties pigProperties = pigServer.getPigContext().getProperties();
            pigProperties.setProperty("fs.default.name", "hdfs://a.com:8020");
            Configuration conf;

            pigServer.registerQuery("a = load '/user/pig/1.txt';");
            conf = ConfigurationUtil.toConfiguration(pigProperties);
            assertTrue(conf.get("mapreduce.job.hdfs-servers")==null||
                    conf.get("mapreduce.job.hdfs-servers").equals("hdfs://a.com:8020"));

            pigServer.registerQuery("a = load 'hdfs://a.com/user/pig/1.txt';");
            conf = ConfigurationUtil.toConfiguration(pigProperties);
            assertTrue(pigProperties.getProperty("mapreduce.job.hdfs-servers")==null||
                    conf.get("mapreduce.job.hdfs-servers").equals("hdfs://a.com:8020"));

            pigServer.registerQuery("a = load 'har:///1.txt';");
            conf = ConfigurationUtil.toConfiguration(pigProperties);
            assertTrue(pigProperties.getProperty("mapreduce.job.hdfs-servers")==null||
                    conf.get("mapreduce.job.hdfs-servers").equals("hdfs://a.com:8020"));

            pigServer.registerQuery("a = load 'hdfs://b.com/user/pig/1.txt';");
            conf = ConfigurationUtil.toConfiguration(pigProperties);
            assertTrue(conf.get("mapreduce.job.hdfs-servers")!=null &&
                    conf.get("mapreduce.job.hdfs-servers").contains("hdfs://b.com"));

            pigServer.registerQuery("a = load 'har://hdfs-c.com/user/pig/1.txt';");
            conf = ConfigurationUtil.toConfiguration(pigProperties);
            assertTrue(conf.get("mapreduce.job.hdfs-servers")!=null &&
                    conf.get("mapreduce.job.hdfs-servers").contains("hdfs://c.com"));

            pigServer.registerQuery("a = load 'hdfs://d.com:8020/user/pig/1.txt';");
            conf = ConfigurationUtil.toConfiguration(pigProperties);
            assertTrue(conf.get("mapreduce.job.hdfs-servers")!=null &&
                    conf.get("mapreduce.job.hdfs-servers").contains("hdfs://d.com:8020"));


        } catch (IOException io) {
        }
    }

    @Test
    public void testRemoteServerList2() throws ExecException, IOException {

        Properties pigProperties = pigServer.getPigContext().getProperties();
        pigProperties.setProperty("fs.default.name", "hdfs://a.com:8020");
        Configuration conf;

        pigServer.setBatchOn();

        Data data = Storage.resetData(pigServer.getPigContext());
        data.set("/user/pig/1.txt");// no data

        pigServer.registerQuery("a = load '/user/pig/1.txt' using mock.Storage;");
        pigServer.registerQuery("store a into '/user/pig/1.txt';");

        System.out.println("hdfs-servers: " + pigProperties.getProperty("mapreduce.job.hdfs-servers"));
        conf = ConfigurationUtil.toConfiguration(pigProperties);
        assertTrue(conf.get("mapreduce.job.hdfs-servers")==null||
                conf.get("mapreduce.job.hdfs-servers").equals("hdfs://a.com:8020"));

        pigServer.registerQuery("store a into 'hdfs://b.com/user/pig/1.txt';");
        System.out.println("hdfs-servers: " + pigProperties.getProperty("mapreduce.job.hdfs-servers"));
        conf = ConfigurationUtil.toConfiguration(pigProperties);
        assertTrue(conf.get("mapreduce.job.hdfs-servers")!=null &&
                conf.get("mapreduce.job.hdfs-servers").contains("hdfs://b.com"));

        pigServer.registerQuery("store a into 'har://hdfs-c.com:8020/user/pig/1.txt';");
        System.out.println("hdfs-servers: " + pigProperties.getProperty("mapreduce.job.hdfs-servers"));
        conf = ConfigurationUtil.toConfiguration(pigProperties);
        assertTrue(conf.get("mapreduce.job.hdfs-servers")!=null &&
                conf.get("mapreduce.job.hdfs-servers").contains("hdfs://c.com:8020"));

        pigServer.registerQuery("store a into 'hdfs://d.com:8020/user/pig/1.txt';");
        System.out.println("hdfs-servers: " + pigProperties.getProperty("mapreduce.job.hdfs-servers"));
        conf = ConfigurationUtil.toConfiguration(pigProperties);
        assertTrue(conf.get("mapreduce.job.hdfs-servers")!=null &&
                conf.get("mapreduce.job.hdfs-servers").contains("hdfs://d.com:8020"));

    }
}
