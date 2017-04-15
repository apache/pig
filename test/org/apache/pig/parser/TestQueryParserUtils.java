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
package org.apache.pig.parser;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.NonFSLoadFunc;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.test.Util;
import org.junit.Test;

public class TestQueryParserUtils {

    @Test
    public void testSetHDFSServers() throws Exception {
        Properties props = new Properties();
        props.setProperty("fs.defaultFS", "hdfs://nn1:8020/tmp");
        PigContext pc = new PigContext(ExecType.LOCAL, props);

        //No scheme/host
        QueryParserUtils.setHdfsServers("hdfs:///tmp", pc);
        assertEquals(null, props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));
        QueryParserUtils.setHdfsServers("/tmp", pc);
        assertEquals(null, props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));
        QueryParserUtils.setHdfsServers("tmp", pc);
        assertEquals(null, props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));

        // Same as default host and scheme
        QueryParserUtils.setHdfsServers("hdfs://nn1/tmp", pc);
        assertEquals(null, props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));
        QueryParserUtils.setHdfsServers("hdfs://nn1:8020/tmp", pc);
        assertEquals(null, props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));

        // Same host different scheme
        QueryParserUtils.setHdfsServers("hftp://nn1/tmp", pc);
        assertEquals("hftp://nn1", props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));
        QueryParserUtils.setHdfsServers("hftp://nn1:50070/tmp", pc);
        assertEquals("hftp://nn1,hftp://nn1:50070", props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));
        // There should be no duplicates
        QueryParserUtils.setHdfsServers("hftp://nn1:50070/tmp", pc);
        assertEquals("hftp://nn1,hftp://nn1:50070", props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));

        // har
        props.remove(MRConfiguration.JOB_HDFS_SERVERS);
        QueryParserUtils.setHdfsServers("har:///tmp", pc);
        assertEquals(null, props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));
        QueryParserUtils.setHdfsServers("har://hdfs-nn1:8020/tmp", pc);
        assertEquals(null, props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));
        QueryParserUtils.setHdfsServers("har://hdfs-nn1/tmp", pc);
        assertEquals("hdfs://nn1", props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));

        // Non existing filesystem scheme
        props.remove(MRConfiguration.JOB_HDFS_SERVERS);
        QueryParserUtils.setHdfsServers("hello://nn1/tmp", pc);
        assertEquals(null, props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));

        // webhdfs
        props.remove(MRConfiguration.JOB_HDFS_SERVERS);
        QueryParserUtils.setHdfsServers("webhdfs://nn1/tmp", pc);
        assertEquals("webhdfs://nn1", props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));
        QueryParserUtils.setHdfsServers("webhdfs://nn1:50070/tmp", pc);
        assertEquals("webhdfs://nn1,webhdfs://nn1:50070", props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));

        // har with webhfs
        QueryParserUtils.setHdfsServers("har://webhdfs-nn1:50070/tmp", pc);
        assertEquals("webhdfs://nn1,webhdfs://nn1:50070", props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));
        QueryParserUtils.setHdfsServers("har://webhdfs-nn2:50070/tmp", pc);
        assertEquals("webhdfs://nn1,webhdfs://nn1:50070,webhdfs://nn2:50070", props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));
        props.remove(MRConfiguration.JOB_HDFS_SERVERS);
        QueryParserUtils.setHdfsServers("har://webhdfs-nn1/tmp", pc);
        assertEquals("webhdfs://nn1", props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));

        //viewfs
        props.remove(MRConfiguration.JOB_HDFS_SERVERS);
        QueryParserUtils.setHdfsServers("viewfs:/tmp", pc);
        assertEquals("viewfs://", props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));
        QueryParserUtils.setHdfsServers("viewfs:///tmp", pc);
        assertEquals("viewfs://", props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));
        QueryParserUtils.setHdfsServers("viewfs://cluster1/tmp", pc);
        assertEquals("viewfs://,viewfs://cluster1", props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));

        //har with viewfs
        props.remove(MRConfiguration.JOB_HDFS_SERVERS);
        QueryParserUtils.setHdfsServers("har://viewfs/tmp", pc);
        assertEquals("viewfs://", props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));
        QueryParserUtils.setHdfsServers("har://viewfs-cluster1/tmp", pc);
        assertEquals("viewfs://,viewfs://cluster1", props.getProperty(MRConfiguration.JOB_HDFS_SERVERS));

    }


    @Test
    public void testNonFSLoadFunc() throws Exception {
        PigServer pigServer = new PigServer(Util.getLocalTestMode(), new Properties());
        pigServer.registerQuery("A =  load 'hbase://query/SELECT ID, NAME, DATE FROM HIRES WHERE DATE > TO_DATE(\"1990-12-21 05:55:00.000\")' using org.apache.pig.parser.TestQueryParserUtils$DummyNonFSLoader();");
        pigServer.shutdown();
    }

    /**
     * Test class for testNonFSLoadFuncNoSetHdfsServersCall test case
     */
    public static class DummyNonFSLoader extends LoadFunc implements NonFSLoadFunc {

        @Override
        public void setLocation(String location, Job job) throws IOException {
            throw new RuntimeException("Should not be called");
        }

        @Override
        public InputFormat getInputFormat() throws IOException {
            throw new RuntimeException("Should not be called");
        }

        @Override
        public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
            throw new RuntimeException("Should not be called");
        }

        @Override
        public Tuple getNext() throws IOException {
            throw new RuntimeException("Should not be called");
        }

        @Override
        public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
            return location;
        }
    }
}
