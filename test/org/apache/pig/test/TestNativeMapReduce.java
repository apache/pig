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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestNativeMapReduce  {
    static MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pigServer;
    // the jar has been created using the source at
    // http://svn.apache.org/repos/asf/hadoop/mapreduce/trunk/src/examples/org/apache/hadoop/examples/WordCount.java:816822
    private String jarFileName = "test//org/apache/pig/test/data/TestWordCount.jar";
    private String exp_msg_prefix = "Check if expected results contains: ";
    TupleFactory mTf = TupleFactory.getInstance();
    BagFactory mBf = BagFactory.getInstance();

    /**
     * TODO - Move to runtime jar creation approach
    private void createWordCountJar() {
    }*/

    @Before
    public void setUp() throws Exception{
        FileLocalizer.setR(new Random());
        //FileLocalizer.setInitialized(false);
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

        //createWordCountJar();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    // See PIG-506
    @Test
    public void testNativeMRJobSimple() throws Exception{
        String[] input = {
                "one",
                "two",
                "three",
                "three",
                "two",
                "three"
        };
        Util.createInputFile(cluster, "table_testNativeMRJobSimple", input);

        Collection<String> results = new HashSet<String>();
        results.add("(one,1)");
        results.add("(two,2)");
        results.add("(three,3)");
        
        pigServer.setBatchOn();
        pigServer.registerQuery("A = load 'table_testNativeMRJobSimple';");
        pigServer.registerQuery("B = mapreduce '" + jarFileName + "' " +
                "Store A into 'table_testNativeMRJobSimple_input' "+
                "Load 'table_testNativeMRJobSimple_output' "+
        "`WordCount table_testNativeMRJobSimple_input table_testNativeMRJobSimple_output`;");
        pigServer.registerQuery("Store B into 'table_testNativeMRJobSimpleDir';");
        pigServer.executeBatch();

        // Check the output
        pigServer.registerQuery("C = load 'table_testNativeMRJobSimpleDir';");

        Iterator<Tuple> iter = pigServer.openIterator("C");
        Tuple t;

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertFalse(iter.hasNext());
        
        // We have to manually delete intermediate mapreduce files
        Util.deleteFile(cluster, "table_testNativeMRJobSimple_input");
        Util.deleteFile(cluster, "table_testNativeMRJobSimple_output");
        
        // check in interactive mode
        iter = pigServer.openIterator("B");

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertFalse(iter.hasNext());
        
        // We have to manually delete intermediate mapreduce files
        Util.deleteFile(cluster, "table_testNativeMRJobSimple_input");
        Util.deleteFile(cluster, "table_testNativeMRJobSimple_output");
        
        Util.deleteFile(cluster, "table_testNativeMRJobSimple");
        Util.deleteFile(cluster, "table_testNativeMRJobSimpleDir");
    }

    // See PIG-506
    @Test
    public void testNativeMRJobMultiStoreOnPred() throws Exception{
        String[] input = {
                "one",
                "two",
                "three",
                "three",
                "two",
                "three"
        };
        Util.createInputFile(cluster, "table_testNativeMRJobMultiStoreOnPred", input);

        Collection<String> results = new HashSet<String>();
        results.add("(one,1)");
        results.add("(two,2)");
        results.add("(three,3)");
        
        pigServer.setBatchOn();
        pigServer.registerQuery("A = load 'table_testNativeMRJobMultiStoreOnPred';");
        pigServer.registerQuery("Store A into 'testNativeMRJobMultiStoreOnPredTemp';");
        pigServer.registerQuery("B = mapreduce '" + jarFileName + "' " +
                "Store A into 'table_testNativeMRJobMultiStoreOnPred_input' "+
                "Load 'table_testNativeMRJobMultiStoreOnPred_output' "+
        "`WordCount table_testNativeMRJobMultiStoreOnPred_input table_testNativeMRJobMultiStoreOnPred_output`;");
        pigServer.registerQuery("Store B into 'table_testNativeMRJobMultiStoreOnPredDir';");
        pigServer.executeBatch();

        // Check the output
        pigServer.registerQuery("C = load 'table_testNativeMRJobMultiStoreOnPredDir';");

        Iterator<Tuple> iter = pigServer.openIterator("C");
        Tuple t;

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertFalse(iter.hasNext());
        
        Util.deleteFile(cluster, "table_testNativeMRJobMultiStoreOnPred_input");
        Util.deleteFile(cluster, "table_testNativeMRJobMultiStoreOnPred_output");

        // check in interactive mode
        iter = pigServer.openIterator("B");

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertFalse(iter.hasNext());
        
        Util.deleteFile(cluster, "table_testNativeMRJobMultiStoreOnPred_input");
        Util.deleteFile(cluster, "table_testNativeMRJobMultiStoreOnPred_output");
        
        Util.deleteFile(cluster, "table_testNativeMRJobMultiStoreOnPred");
        Util.deleteFile(cluster, "table_testNativeMRJobMultiStoreOnPredDir");
    }

    // See PIG-506
    @Test
    public void testNativeMRJobMultiQueryOpt() throws Exception{
        String[] input = {
                "one",
                "two",
                "three",
                "three",
                "two",
                "three"
        };
        Util.createInputFile(cluster, "table_testNativeMRJobMultiQueryOpt", input);

        Collection<String> results = new HashSet<String>();
        results.add("(one,1)");
        results.add("(two,2)");
        results.add("(three,3)");
        
        pigServer.registerQuery("A = load 'table_testNativeMRJobMultiQueryOpt';");
        pigServer.registerQuery("B = mapreduce '" + jarFileName + "' " +
                "Store A into 'table_testNativeMRJobMultiQueryOpt_inputB' "+
                "Load 'table_testNativeMRJobMultiQueryOpt_outputB' "+
        "`WordCount table_testNativeMRJobMultiQueryOpt_inputB table_testNativeMRJobMultiQueryOpt_outputB`;");
        pigServer.registerQuery("C = mapreduce '" + jarFileName + "' " +
                "Store A into 'table_testNativeMRJobMultiQueryOpt_inputC' "+
                "Load 'table_testNativeMRJobMultiQueryOpt_outputC' "+
        "`WordCount table_testNativeMRJobMultiQueryOpt_inputC table_testNativeMRJobMultiQueryOpt_outputC`;");

        Iterator<Tuple> iter = pigServer.openIterator("C");
        Tuple t;

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertFalse(iter.hasNext());

        iter = pigServer.openIterator("B");

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertFalse(iter.hasNext());
        
        Util.deleteFile(cluster, "table_testNativeMRJobMultiQueryOpt");
        Util.deleteFile(cluster, "table_testNativeMRJobMultiQueryOpt_inputB");
        Util.deleteFile(cluster, "table_testNativeMRJobMultiQueryOpt_outputB");
        Util.deleteFile(cluster, "table_testNativeMRJobMultiQueryOpt_inputC");
        Util.deleteFile(cluster, "table_testNativeMRJobMultiQueryOpt_outputC");
    }
    
    // See PIG-506
    @Test
    public void testNativeMRJobTypeCastInserter() throws Exception{
        String[] input = {
                "one",
                "two",
                "three",
                "three",
                "two",
                "three"
        };
        Util.createInputFile(cluster, "table_testNativeMRJobTypeCastInserter", input);

        Collection<String> results = new HashSet<String>();
        results.add("(2)");
        results.add("(3)");
        results.add("(4)");
        
        pigServer.registerQuery("A = load 'table_testNativeMRJobTypeCastInserter';");
        pigServer.registerQuery("B = mapreduce '" + jarFileName + "' " +
                "Store A into 'table_testNativeMRJobTypeCastInserter_input' "+
                "Load 'table_testNativeMRJobTypeCastInserter_output' as (name:chararray, count: int)"+
        "`WordCount table_testNativeMRJobTypeCastInserter_input table_testNativeMRJobTypeCastInserter_output`;");
        pigServer.registerQuery("C = foreach B generate count+1;");
        
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Tuple t;
        
        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertTrue("iter.hasNext()",iter.hasNext());
        t = iter.next();
        assertTrue(exp_msg_prefix + t, results.contains(t.toString()));

        assertFalse(iter.hasNext());
        
        Util.deleteFile(cluster, "table_testNativeMRJobTypeCastInserter");
        Util.deleteFile(cluster, "table_testNativeMRJobTypeCastInserter_input");
        Util.deleteFile(cluster, "table_testNativeMRJobTypeCastInserter_output");
    }
}
