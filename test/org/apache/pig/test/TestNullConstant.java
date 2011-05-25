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


import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class TestNullConstant extends TestCase {
    
    static MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer;

    @Before
    @Override
    public void setUp() throws Exception{
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }
    
    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
    
    @Test
    public void testArithExpressions() throws IOException, ExecException {
        String inputFileName = "testArithExpressions-input.txt";
        Util.createInputFile(cluster, inputFileName, 
                new String[] {"10\t11.0"});
        pigServer.registerQuery("a = load '" + inputFileName + "' as (x:int, y:double);");
        pigServer.registerQuery("b = foreach a generate x + null, x * null, x / null, x - null, null % x, " +
        		"y + null, y * null, y / null, y - null;");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        for (int i = 0; i < 9; i++) {
            assertEquals(null, t.get(i));
        }
        Util.deleteFile(cluster, inputFileName);
    }
    
    @Test
    public void testBinCond() throws IOException, ExecException {
        String inputFileName = "testBinCond-input.txt";
        Util.createInputFile(cluster, inputFileName, 
                new String[] {"10\t11.0"});
        pigServer.registerQuery("a = load '" + inputFileName + "' as (x:int, y:double);");
        pigServer.registerQuery("b = foreach a generate (2 > 1? null : 1), ( 2 < 1 ? null : 1), (2 > 1 ? 1 : null), ( 2 < 1 ? 1 : null);");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        Object[] result = new Object[] { null, 1, 1, null};
        for (int i = 0; i < 4; i++) {
            assertEquals(result[i], t.get(i));
        }
        
        // is null and is not null test
        pigServer.registerQuery("b = foreach a generate (null is null ? 1 : 2), ( null is not null ? 2 : 1);");
        it = pigServer.openIterator("b");
        t = it.next();
        for (int i = 0; i < 2; i++) {
            assertEquals(1, t.get(i));
        }
        Util.deleteFile(cluster, inputFileName);
        
    }

    @Test
    public void testForeachGenerate() throws ExecException, IOException {
        String inputFileName = "testForeachGenerate-input.txt";
        Util.createInputFile(cluster, inputFileName, 
                new String[] {"10\t11.0"});
        pigServer.registerQuery("a = load '" + inputFileName + "' as (x:int, y:double);");
        pigServer.registerQuery("b = foreach a generate x, null, y, null;");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        Object[] result = new Object[] { 10, null, 11.0, null};
        for (int i = 0; i < 4; i++) {
            assertEquals(result[i], t.get(i));
        }
        Util.deleteFile(cluster, inputFileName);
        
    }
    
    @Test
    public void testOuterJoin() throws IOException, ExecException {
        String inputFileName1 = "testOuterJoin-input1.txt";
        Util.createInputFile(cluster, inputFileName1, 
                new String[] {"10\twill_join", "11\twill_not_join"});
        String inputFileName2 = "testOuterJoin-input2.txt";
        Util.createInputFile(cluster, inputFileName2, 
                new String[] {"10\twill_join", "12\twill_not_join"});
        pigServer.registerQuery("a = load '" + inputFileName1 + "' as (x:int, y:chararray);");
        pigServer.registerQuery("b = load '" + inputFileName2 + "' as (u:int, v:chararray);");
        pigServer.registerQuery("c = cogroup a by x, b by u;");
        pigServer.registerQuery("d = foreach c generate flatten((SIZE(a) == 0 ? null: a)), flatten((SIZE(b) == 0 ? null : b));");
        Iterator<Tuple> it = pigServer.openIterator("d");
        Object[][] results = new Object[][]{{10, "will_join", 10, "will_join"}, {11, "will_not_join", null}, {null, 12, "will_not_join"}};
        int i = 0;
        while(it.hasNext()) {
          
            Tuple t = it.next();
            Object[] result = results[i++];
            assertEquals(result.length, t.size());
            for (int j = 0; j < result.length; j++) {
                assertEquals(result[j], t.get(j));
            }
        }
        Util.deleteFile(cluster, inputFileName1);
        Util.deleteFile(cluster, inputFileName2);
    }
    
    @Test
    public void testConcatAndSize() throws IOException, ExecException {
        String inputFileName = "testConcatAndSize-input.txt";
        Util.createInputFile(cluster, inputFileName, 
                new String[] {"10\t11.0\tstring"});
        pigServer.registerQuery("a = load '" + inputFileName + "' as (x:int, y:double, str:chararray);");
        pigServer.registerQuery("b = foreach a generate SIZE(null), CONCAT(str, null), " +
        		"CONCAT(null, str);");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        for (int i = 0; i < 3; i++) {
            assertEquals(null, t.get(i));
        }
        Util.deleteFile(cluster, inputFileName);
    }

    @Test
    public void testExplicitCast() throws IOException, ExecException {
        String inputFileName = "testExplicitCast-input.txt";
        Util.createInputFile(cluster, inputFileName, 
                new String[] {"10\t11.0\tstring"});
        pigServer.registerQuery("a = load '" + inputFileName + "' as (x:int, y:double, str:chararray);");
        pigServer.registerQuery("b = foreach a generate (int)null, (double)null, (chararray)null, (map[])null;");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        for (int i = 0; i < 3; i++) {
            assertEquals(null, t.get(i));
        }
        Util.deleteFile(cluster, inputFileName);
    }
    
    @Test
    public void testComplexNullConstants() throws IOException, ExecException {
        String inputFileName = "testComplexNullConstants-input.txt";
        Util.createInputFile(cluster, inputFileName, 
                new String[] {"10\t11.0\tstring"});
        pigServer.registerQuery("a = load '" + inputFileName + "' as (x:int, y:double, str:chararray);");
        pigServer.registerQuery("b = foreach a generate {(null)}, ['2'#null];");
        Iterator<Tuple> it = pigServer.openIterator("b");
        Tuple t = it.next();
        assertEquals(null, ((DataBag)t.get(0)).iterator().next().get(0));
        assertEquals(null, ((Map<String, Object>)t.get(1)).get("2"));
        Util.deleteFile(cluster, inputFileName);
    }

    @Test
    public void testMapNullKeyFailure() throws IOException {
        String inputFileName = "testMapNullKeyFailure-input.txt";
        Util.createInputFile(cluster, inputFileName, 
                new String[] {"10\t11.0\tstring"});
        pigServer.registerQuery("a = load '" + inputFileName + "' as (x:int, y:double, str:chararray);");

        boolean exceptionOccured = false;
        try {
            pigServer.registerQuery("b = foreach a generate [null#'2'];");
        } catch(Exception e) {
            exceptionOccured = true;
        }
        Util.deleteFile(cluster, inputFileName);
        if(!exceptionOccured) fail();        
    }
}
