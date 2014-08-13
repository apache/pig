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


import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.fs.Path;
import org.apache.pig.LoadFunc;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLoadFunc {

    private static Path curHdfsDir;
    
    private static String curHdfsRoot = 
        "hdfs://localhost.localdomain:12345"; 
            
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        curHdfsDir = new Path(curHdfsRoot + "/user/pig/");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }
    
    @Test
    public void testGetAbsolutePath() throws IOException {
        // test case: simple absolute path 
        Assert.assertEquals("/hadoop/test/passwd",
                LoadFunc.getAbsolutePath("/hadoop/test/passwd", curHdfsDir));      
    }

    @Test
    public void testGetAbsolutePath2() throws IOException {
        // test case: simple relative path
        Assert.assertEquals(curHdfsRoot + "/user/pig/data/passwd",
                LoadFunc.getAbsolutePath("data/passwd", curHdfsDir));      
    }
    
    @Test
    public void testGetAbsolutePath3() throws IOException {
        // test case: remote hdfs path
        String absPath = "hdfs://myhost.mydomain:37765/data/passwd";
        Assert.assertEquals(absPath,
                LoadFunc.getAbsolutePath(absPath, curHdfsDir));      
    }
    
    @Test
    public void testGetAbsolutePath4() throws IOException {
        // test case: non dfs scheme
        Assert.assertEquals("http://myhost:12345/data/passwd",
                LoadFunc.getAbsolutePath("http://myhost:12345/data/passwd", 
                curHdfsDir));      
    }
    
    @Test
    public void testCommaSeparatedString() throws Exception {
        // test case: comma separated absolute paths
        Assert.assertEquals("/usr/pig/a,/usr/pig/b,/usr/pig/c",
                LoadFunc.getAbsolutePath("/usr/pig/a,/usr/pig/b,/usr/pig/c", 
                        curHdfsDir));
    }

    @Test
    public void testCommaSeparatedString2() throws Exception {
        // test case: comma separated relative paths
        Assert.assertEquals(curHdfsRoot + "/user/pig/t?s*," + 
                curHdfsRoot + "/user/pig/test", 
                LoadFunc.getAbsolutePath("t?s*,test", curHdfsDir));
    }

    @Test
    public void testCommaSeparatedString4() throws Exception {
        // test case: comma separated paths with hadoop glob
        Assert.assertEquals(curHdfsRoot + "/user/pig/test/{a,c}," + 
                curHdfsRoot + "/user/pig/test/b",
                LoadFunc.getAbsolutePath("test/{a,c},test/b", curHdfsDir));
    }

    @Test
    public void testCommaSeparatedString5() throws Exception {
        // test case: comma separated paths with hadoop glob
        Assert.assertEquals("/test/data/{a,c}," +
                curHdfsRoot + "/user/pig/test/b",
                LoadFunc.getAbsolutePath("/test/data/{a,c},test/b", 
                        curHdfsDir));
    }
    
    @Test
    public void testCommaSeparatedString6() throws Exception {
        // test case: comma separated paths with hasoop glob
        Assert.assertEquals(curHdfsRoot + "/user/pig/test/{a,c},/test/data/b",
                LoadFunc.getAbsolutePath("test/{a,c},/test/data/b",
                        curHdfsDir));
    }    

    @Test
    public void testCommaSeparatedString7() throws Exception {
        // test case: comma separated paths with white spaces
        Assert.assertEquals(curHdfsRoot + "/user/pig/test/{a,c},/test/data/b",
                LoadFunc.getAbsolutePath("test/{a,c}, /test/data/b",
                        curHdfsDir));
    }    

    @Test(expected=IllegalArgumentException.class)
    public void testCommaSeparatedString8() throws Exception {
        // test case: comma separated paths with empty string
        Assert.assertEquals(curHdfsRoot + "/user/pig/," + 
                curHdfsRoot + "/test/data/b",
                LoadFunc.getAbsolutePath(", /test/data/b",
                        curHdfsDir));
    }
    
    @Test
    public void testHarUrl() throws Exception {
        // test case: input location is a har:// url
        Assert.assertEquals("har:///user/pig/harfile",
                LoadFunc.getAbsolutePath("har:///user/pig/harfile",
                        curHdfsDir));
    }   
    
}
