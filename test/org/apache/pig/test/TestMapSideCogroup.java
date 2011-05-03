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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.CollectableLoadFunc;
import org.apache.pig.ExecType;
import org.apache.pig.IndexableLoadFunc;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeCogroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestMapSideCogroup {
    private static final String INPUT_FILE1 = "testCogrpInput1.txt";
    private static final String INPUT_FILE2 = "testCogrpInput2.txt";
    private static final String INPUT_FILE3 = "testCogrpInput3.txt";
    private static final String INPUT_FILE4 = "testCogrpInput4.txt";
    private static final String INPUT_FILE5 = "testCogrpInput5.txt";
    private static final String EMPTY_FILE = "empty.txt";
    private static final String DATA_WITH_NULL_KEYS = "null.txt";

    private static MiniCluster cluster = MiniCluster.buildCluster();

    @Before
    public void setUp() throws Exception {

        Util.deleteFile(cluster, INPUT_FILE1);
        Util.deleteFile(cluster, INPUT_FILE2);
        Util.deleteFile(cluster, INPUT_FILE3);
        Util.deleteFile(cluster, INPUT_FILE4);
        Util.deleteFile(cluster, INPUT_FILE5);
        Util.deleteFile(cluster, EMPTY_FILE);
        int LOOP_SIZE = 3;
        String[] input = new String[LOOP_SIZE*LOOP_SIZE];
        int k = 0;
        for(int i = 1; i <= LOOP_SIZE; i++) {
            String si = i + "";
            for(int j=1;j<=LOOP_SIZE;j++)
                input[k++] = si + "\t" + j;
        }

        String[] input2 = new String[LOOP_SIZE*2*LOOP_SIZE];
        k = 0;
        for(int i = LOOP_SIZE + 1; i <= 3*LOOP_SIZE ; i++) {
            String si = i + "";
            for(int j=1;j<=LOOP_SIZE;j++)
                input2[k++] = si + "\t" + j;
        }

        String[] input3 = new String[LOOP_SIZE*LOOP_SIZE];
        k = 0;
        for(int i = LOOP_SIZE ; i < 2*LOOP_SIZE ; i++) {
            String si = i + "";
            for(int j=1;j<=LOOP_SIZE;j++)
                input3[k++] = si + "\t" + j;
        }

        String[] dataWithNullKeys = new String[LOOP_SIZE*LOOP_SIZE];
        k = 0;
        for(int i = 1; i <= LOOP_SIZE ; i++) {
            String si;
            if(i == 1)
                si = "";
            else
                si = i + "";
            for(int j=1;j<=LOOP_SIZE;j++){
                dataWithNullKeys[k++] = si + "\t" + j;                
            }

        }

        Util.createInputFile(cluster, INPUT_FILE1, input);
        Util.createInputFile(cluster, INPUT_FILE2, input);
        Util.createInputFile(cluster, INPUT_FILE3, input);
        Util.createInputFile(cluster, INPUT_FILE4, input2);
        Util.createInputFile(cluster, INPUT_FILE5, input3);
        Util.createInputFile(cluster, EMPTY_FILE, new String[]{});
        Util.createInputFile(cluster, DATA_WITH_NULL_KEYS, dataWithNullKeys);


    }

    @After
    public void tearDown() throws Exception {
        Util.deleteFile(cluster, INPUT_FILE1);
        Util.deleteFile(cluster, INPUT_FILE2);
        Util.deleteFile(cluster, INPUT_FILE3);
        Util.deleteFile(cluster, INPUT_FILE4);
        Util.deleteFile(cluster, INPUT_FILE5);
        Util.deleteFile(cluster, EMPTY_FILE);
        Util.deleteFile(cluster, DATA_WITH_NULL_KEYS);
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
    
    @Test
    public void testCompilation(){
        try{
            PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
            String query = "A = LOAD 'data1' using "+ DummyCollectableLoader.class.getName() +"() as (id, name, grade);" + 
            "B = LOAD 'data2' using "+ DummyIndexableLoader.class.getName() +"() as (id, name, grade);" +
            "D = LOAD 'data2' using "+ DummyIndexableLoader.class.getName() +"() as (id, name, grade);" +
            "C = cogroup A by id, B by id, D by id using 'merge';" +
            "store C into 'output';";
            LogicalPlan lp = Util.buildLp(pigServer, query);
            Operator op = lp.getSinks().get(0);
            LOCogroup cogrp = (LOCogroup)lp.getPredecessors(op).get(0);
            assertEquals(LOCogroup.GROUPTYPE.MERGE, cogrp.getGroupType());

            PigContext pc = new PigContext(ExecType.MAPREDUCE,cluster.getProperties());
            pc.connect();
            PhysicalPlan phyP = Util.buildPp(pigServer, query);
            PhysicalOperator phyOp = phyP.getLeaves().get(0);
            assertTrue(phyOp instanceof POStore);
            phyOp = phyOp.getInputs().get(0);
            assertTrue(phyOp instanceof POMergeCogroup);

            MROperPlan mrPlan = Util.buildMRPlan(phyP,pc);            
            assertEquals(2,mrPlan.size());

            Iterator<MapReduceOper> itr = mrPlan.iterator();
            MapReduceOper oper = itr.next();
            assertTrue(oper.reducePlan.isEmpty());
            assertFalse(oper.mapPlan.isEmpty());

            oper = itr.next();
            assertFalse(oper.reducePlan.isEmpty());
            assertFalse(oper.mapPlan.isEmpty());
        } catch(Exception e){
            e.printStackTrace();
            fail("Compilation of merged cogroup failed.");
        }

    }

//    @Test // PIG-2018
//    public void testFailure1() throws Exception{
//        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
//        String query = "A = LOAD 'data1' using "+ DummyCollectableLoader.class.getName() +"() as (id, name, grade);" +
//        "E = group A by id;" +
//        "B = LOAD 'data2' using "+ DummyIndexableLoader.class.getName() +"() as (id, name, grade);" +
//        "D = LOAD 'data2' using "+ DummyIndexableLoader.class.getName() +"() as (id, name, grade);" +
//        "C = cogroup E by A.id, B by id, D by id using 'merge';" +
//        "store C into 'output';";
//        LogicalPlan lp = Util.buildLp(pigServer, query);
//        Operator op = lp.getSinks().get(0);
//        LOCogroup cogrp = (LOCogroup)lp.getPredecessors(op).get(0);
//        assertEquals(LOCogroup.GROUPTYPE.MERGE, cogrp.getGroupType());
//
//        PigContext pc = new PigContext(ExecType.MAPREDUCE,cluster.getProperties());
//        pc.connect();
//        boolean exceptionCaught = false;
//        try{
//            Util.buildPp(pigServer, query);   
//        }catch (java.lang.reflect.InvocationTargetException e){
//        	FrontendException ex = (FrontendException)e.getTargetException();
//            assertEquals(1103,ex.getErrorCode());
//            exceptionCaught = true;
//        }
//        assertTrue(exceptionCaught);
//    }
    
    @Test
    public void testFailure2() throws Exception{
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        String query = "A = LOAD 'data1' using "+ DummyCollectableLoader.class.getName() +"() as (id, name, grade);" +
        "B = LOAD 'data2' using "+ DummyIndexableLoader.class.getName() +"() as (id, name, grade);" +
        "D = LOAD 'data2' using "+ DummyIndexableLoader.class.getName() +"() as (id, name, grade);" +
        "C = cogroup A by id inner, B by id, D by id inner using 'merge';" +
        "store C into 'output';";
        LogicalPlan lp = Util.buildLp(pigServer, query);
        Operator op = lp.getSinks().get(0);
        LOCogroup cogrp = (LOCogroup)lp.getPredecessors(op).get(0);
        assertEquals(LOCogroup.GROUPTYPE.MERGE, cogrp.getGroupType());

        PigContext pc = new PigContext(ExecType.MAPREDUCE,cluster.getProperties());
        pc.connect();
        boolean exceptionCaught = false;
        try{
            Util.buildPp(pigServer, query);   
        }catch (java.lang.reflect.InvocationTargetException e){
            exceptionCaught = true;
        }
        assertTrue(exceptionCaught);
    }
    
    @Test
    public void testSimple() throws Exception{

        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE1 + "' using "+ DummyCollectableLoader.class.getName() +"() as (c1:chararray,c2:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE2 + "' using "+ DummyIndexableLoader.class.getName()   +"() as (c1:chararray,c2:int);");

        DataBag dbMergeCogrp = BagFactory.getInstance().newDefaultBag();
        pigServer.registerQuery("C = cogroup A by c1, B by c1 using 'merge';");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        while(iter.hasNext()) {
            Tuple t = iter.next();
            dbMergeCogrp.add(t);
        }

        String[] results = new String[]{
                "(1,{(1,1),(1,2),(1,3)},{(1,1),(1,2),(1,3)})",
                "(2,{(2,2),(2,1),(2,3)},{(2,1),(2,2),(2,3)})",
                "(3,{(3,3),(3,2),(3,1)},{(3,1),(3,2),(3,3)})"
        };

        assertEquals(3, dbMergeCogrp.size());
        Iterator<Tuple> itr = dbMergeCogrp.iterator();
        for(int i=0; i<3; i++){
            assertEquals(itr.next().toString(), results[i]);   
        }
        assertFalse(itr.hasNext());
    }

    @Test
    public void test3Way() throws Exception{

        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE1 + "' using "+ DummyCollectableLoader.class.getName() +"() as (c1:chararray,c2:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE2 + "' using "+ DummyIndexableLoader.class.getName()   +"() as (c1:chararray,c2:int);");
        pigServer.registerQuery("E = LOAD '" + INPUT_FILE3 + "' using "+ DummyIndexableLoader.class.getName()   +"() as (c1:chararray,c2:int);");

        DataBag dbMergeCogrp = BagFactory.getInstance().newDefaultBag();

        pigServer.registerQuery("C = cogroup A by c1, B by c1, E by c1 using 'merge';");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        while(iter.hasNext()) {
            Tuple t = iter.next();
            dbMergeCogrp.add(t);
        }


        String[] results = new String[]{
                "(1,{(1,1),(1,2),(1,3)},{(1,1),(1,2),(1,3)},{(1,1),(1,2),(1,3)})",
                "(2,{(2,2),(2,1),(2,3)},{(2,1),(2,2),(2,3)},{(2,1),(2,2),(2,3)})",
                "(3,{(3,2),(3,3),(3,1)},{(3,1),(3,2),(3,3)},{(3,1),(3,2),(3,3)})"
        };

        assertEquals(3, dbMergeCogrp.size());
        Iterator<Tuple> itr = dbMergeCogrp.iterator();
        for(int i=0; i<3; i++){
            assertEquals(itr.next().toString(), results[i]);   
        }
        assertFalse(itr.hasNext());

    }

    @Test
    public void testMultiSplits() throws Exception{

        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE1 + "," + INPUT_FILE4 + "' using "+ DummyCollectableLoader.class.getName() +"() as (c1:chararray,c2:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE5 + "' using "+ DummyIndexableLoader.class.getName()   +"() as (c1:chararray,c2:int);");

        DataBag dbMergeCogrp = BagFactory.getInstance().newDefaultBag();

        pigServer.registerQuery("C = cogroup A by c1, B by c1 using 'merge';");
        Iterator<Tuple> iter = pigServer.openIterator("C");


        while(iter.hasNext()) {
            Tuple t = iter.next();
            dbMergeCogrp.add(t);
        }


        String[] results = new String[]{
                "(4,{(4,1),(4,2),(4,3)},{(4,1),(4,2),(4,3)})",
                "(5,{(5,2),(5,1),(5,3)},{(5,1),(5,2),(5,3)})",
                "(6,{(6,1),(6,2),(6,3)},{})",
                "(7,{(7,1),(7,2),(7,3)},{})",
                "(8,{(8,1),(8,2),(8,3)},{})",
                "(9,{(9,1),(9,2),(9,3)},{})",
                "(1,{(1,1),(1,2),(1,3)},{})",
                "(2,{(2,1),(2,2),(2,3)},{})",
                "(3,{(3,3),(3,2),(3,1)},{(3,1),(3,2),(3,3)})"
        };

        assertEquals(9, dbMergeCogrp.size());
        Iterator<Tuple> itr = dbMergeCogrp.iterator();
        for(int i=0; i<9; i++){
            assertEquals(itr.next().toString(), results[i]);   
        }
        assertFalse(itr.hasNext());
    }

    @Test
    public void testCogrpOnMultiKeys() throws Exception{

        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE1 + "' using "+ DummyCollectableLoader.class.getName() +"() as (c1:chararray,c2:chararray);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE2 + "' using "+ DummyIndexableLoader.class.getName()   +"() as (c1:chararray,c2:chararray);");

        DataBag dbMergeCogrp = BagFactory.getInstance().newDefaultBag();
        pigServer.registerQuery("C = cogroup A by (c1,c2) , B by (c1,c2) using 'merge' ;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        while(iter.hasNext()) {
            Tuple t = iter.next();
            dbMergeCogrp.add(t);
        }

        String[] results = new String[]{
                "((1,1),{(1,1)},{(1,1)})",
                "((1,2),{(1,2)},{(1,2)})",
                "((1,3),{(1,3)},{(1,3)})",
                "((2,1),{(2,1)},{(2,1)})",
                "((2,2),{(2,2)},{(2,2)})",
                "((2,3),{(2,3)},{(2,3)})",
                "((3,1),{(3,1)},{(3,1)})",
                "((3,2),{(3,2)},{(3,2)})",
                "((3,3),{(3,3)},{(3,3)})"
        };

        assertEquals(9, dbMergeCogrp.size());
        Iterator<Tuple> itr = dbMergeCogrp.iterator();
        for(int i=0; i<9; i++){
            assertEquals(itr.next().toString(), results[i]);   
        }
        assertFalse(itr.hasNext());
    }
    
    @Test
    public void testEmptyDeltaFile() throws Exception{

        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE1 + "' using "+ DummyCollectableLoader.class.getName() +"() as (c1:chararray,c2:int);");
        pigServer.registerQuery("B = LOAD '" + EMPTY_FILE + "' using "+ DummyIndexableLoader.class.getName()   +"() as (c1:chararray,c2:int);");

        DataBag dbMergeCogrp = BagFactory.getInstance().newDefaultBag();

        pigServer.registerQuery("C = cogroup A by c1, B by c1 using 'merge';");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        while(iter.hasNext()) {
            Tuple t = iter.next();
            dbMergeCogrp.add(t);
        }

        String[] results = new String[]{
                "(1,{(1,1),(1,2),(1,3)},{})",
                "(2,{(2,1),(2,2),(2,3)},{})",
                "(3,{(3,1),(3,2),(3,3)},{})"
        };

        assertEquals(3, dbMergeCogrp.size());
        Iterator<Tuple> itr = dbMergeCogrp.iterator();
        for(int i=0; i<3; i++){
            assertEquals(itr.next().toString(), results[i]);   
        }
        assertFalse(itr.hasNext());
    }

    @Test
    public void testDataWithNullKeys() throws Exception{

        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.registerQuery("A = LOAD '" + DATA_WITH_NULL_KEYS + "' using "+ DummyCollectableLoader.class.getName() +"() as (c1:chararray,c2:int);");
        pigServer.registerQuery("B = LOAD '" + DATA_WITH_NULL_KEYS + "' using "+ DummyIndexableLoader.class.getName()   +"() as (c1:chararray,c2:int);");

        String[] results = new String[]{
                "(,{(,1),(,2),(,3)},{})",
                "(,{},{(,1),(,2),(,3)})",
                "(2,{(2,3),(2,1),(2,2)},{(2,1),(2,2),(2,3)})",
                "(3,{(3,3),(3,1),(3,2)},{(3,1),(3,2),(3,3)})"
        };   

        DataBag dbMergeCogrp = BagFactory.getInstance().newDefaultBag();

        pigServer.registerQuery("C = cogroup A by c1, B by c1 using 'merge';");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        while(iter.hasNext()) {
            Tuple t = iter.next();
            dbMergeCogrp.add(t);
        }

        assertEquals(4, dbMergeCogrp.size());
        Iterator<Tuple> itr = dbMergeCogrp.iterator();
        for(int i=0; i<4; i++){
            assertEquals(itr.next().toString(), results[i]);   
        }
        assertFalse(itr.hasNext());

    }

    /**
     * A dummy loader which implements {@link CollectableLoadFunc}
     */
    public static class DummyCollectableLoader extends PigStorage implements CollectableLoadFunc{

        public DummyCollectableLoader() {
        }

        @Override
        public void ensureAllKeyInstancesInSameSplit() throws IOException {
        }
    }

    /**
     * A dummy loader which implements {@link IndexableLoadFunc} 
     */
    public static class DummyIndexableLoader extends PigStorage implements IndexableLoadFunc {

        private String loc;
        private FSDataInputStream is;

        public DummyIndexableLoader() {
        }

        @Override
        public void close() throws IOException {
            is.close();
        }

        @Override
        public void seekNear(Tuple keys) throws IOException {

        }

        @Override
        public void initialize(Configuration conf) throws IOException {
            is = FileSystem.get(conf).open(new Path(loc));
        }

        @Override
        public void setLocation(String location, Job job) throws IOException {
            super.setLocation(location, job);
            loc = location;
        }

        @Override
        public Tuple getNext() throws IOException {
            String line = is.readLine();
            if(line == null)
                return null;
            String[] members = line.split("\t");
            DefaultTuple tuple = new DefaultTuple();
            for(String member : members)
                tuple.append(member);
            return tuple;
        }
    }
}
