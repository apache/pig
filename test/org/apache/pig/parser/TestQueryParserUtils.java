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

import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.test.Util;
import org.junit.Test;

public class TestQueryParserUtils {

    @Test
    public void testSetHDFSServers() throws Exception {
        Properties props = new Properties();
        props.setProperty("fs.default.name", "hdfs://nn1:8020/tmp");
        PigContext pc = new PigContext(ExecType.LOCAL, props);

        //No scheme/host
        QueryParserUtils.setHdfsServers("hdfs:///tmp", pc);
        assertEquals(null, props.getProperty("mapreduce.job.hdfs-servers"));
        QueryParserUtils.setHdfsServers("/tmp", pc);
        assertEquals(null, props.getProperty("mapreduce.job.hdfs-servers"));
        QueryParserUtils.setHdfsServers("tmp", pc);
        assertEquals(null, props.getProperty("mapreduce.job.hdfs-servers"));

        // Same as default host and scheme
        QueryParserUtils.setHdfsServers("hdfs://nn1/tmp", pc);
        assertEquals(null, props.getProperty("mapreduce.job.hdfs-servers"));
        QueryParserUtils.setHdfsServers("hdfs://nn1:8020/tmp", pc);
        assertEquals(null, props.getProperty("mapreduce.job.hdfs-servers"));

        // Same host different scheme
        QueryParserUtils.setHdfsServers("hftp://nn1/tmp", pc);
        assertEquals("hftp://nn1", props.getProperty("mapreduce.job.hdfs-servers"));
        QueryParserUtils.setHdfsServers("hftp://nn1:50070/tmp", pc);
        assertEquals("hftp://nn1,hftp://nn1:50070", props.getProperty("mapreduce.job.hdfs-servers"));
        // There should be no duplicates
        QueryParserUtils.setHdfsServers("hftp://nn1:50070/tmp", pc);
        assertEquals("hftp://nn1,hftp://nn1:50070", props.getProperty("mapreduce.job.hdfs-servers"));

        // har
        props.remove("mapreduce.job.hdfs-servers");
        QueryParserUtils.setHdfsServers("har:///tmp", pc);
        assertEquals(null, props.getProperty("mapreduce.job.hdfs-servers"));
        QueryParserUtils.setHdfsServers("har://hdfs-nn1:8020/tmp", pc);
        assertEquals(null, props.getProperty("mapreduce.job.hdfs-servers"));
        QueryParserUtils.setHdfsServers("har://hdfs-nn1/tmp", pc);
        assertEquals("hdfs://nn1", props.getProperty("mapreduce.job.hdfs-servers"));

        // Non existing filesystem scheme
        props.remove("mapreduce.job.hdfs-servers");
        QueryParserUtils.setHdfsServers("hello://nn1/tmp", pc);
        assertEquals(null, props.getProperty("mapreduce.job.hdfs-servers"));

        if(Util.isHadoop23() || Util.isHadoop2_0()) {
            // webhdfs
            props.remove("mapreduce.job.hdfs-servers");
            QueryParserUtils.setHdfsServers("webhdfs://nn1/tmp", pc);
            assertEquals("webhdfs://nn1", props.getProperty("mapreduce.job.hdfs-servers"));
            QueryParserUtils.setHdfsServers("webhdfs://nn1:50070/tmp", pc);
            assertEquals("webhdfs://nn1,webhdfs://nn1:50070", props.getProperty("mapreduce.job.hdfs-servers"));

            // har with webhfs
            QueryParserUtils.setHdfsServers("har://webhdfs-nn1:50070/tmp", pc);
            assertEquals("webhdfs://nn1,webhdfs://nn1:50070", props.getProperty("mapreduce.job.hdfs-servers"));
            QueryParserUtils.setHdfsServers("har://webhdfs-nn2:50070/tmp", pc);
            assertEquals("webhdfs://nn1,webhdfs://nn1:50070,webhdfs://nn2:50070", props.getProperty("mapreduce.job.hdfs-servers"));
            props.remove("mapreduce.job.hdfs-servers");
            QueryParserUtils.setHdfsServers("har://webhdfs-nn1/tmp", pc);
            assertEquals("webhdfs://nn1", props.getProperty("mapreduce.job.hdfs-servers"));

            //viewfs
            props.remove("mapreduce.job.hdfs-servers");
            QueryParserUtils.setHdfsServers("viewfs:/tmp", pc);
            assertEquals("viewfs://", props.getProperty("mapreduce.job.hdfs-servers"));
            QueryParserUtils.setHdfsServers("viewfs:///tmp", pc);
            assertEquals("viewfs://", props.getProperty("mapreduce.job.hdfs-servers"));
            QueryParserUtils.setHdfsServers("viewfs://cluster1/tmp", pc);
            assertEquals("viewfs://,viewfs://cluster1", props.getProperty("mapreduce.job.hdfs-servers"));

            //har with viewfs
            props.remove("mapreduce.job.hdfs-servers");
            QueryParserUtils.setHdfsServers("har://viewfs/tmp", pc);
            assertEquals("viewfs://", props.getProperty("mapreduce.job.hdfs-servers"));
            QueryParserUtils.setHdfsServers("har://viewfs-cluster1/tmp", pc);
            assertEquals("viewfs://,viewfs://cluster1", props.getProperty("mapreduce.job.hdfs-servers"));


        }


    }

}
