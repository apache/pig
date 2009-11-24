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


import java.io.*;
import java.util.Iterator;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.Before;

public class TestSkewedJoin extends TestCase{
    private static final String INPUT_FILE1 = "SkewedJoinInput1.txt";
    private static final String INPUT_FILE2 = "SkewedJoinInput2.txt";
    private static final String INPUT_FILE3 = "SkewedJoinInput3.txt";
    private static final String INPUT_FILE4 = "SkewedJoinInput4.txt";
    private static final String INPUT_FILE5 = "SkewedJoinInput5.txt";
    
    private PigServer pigServer;
    private MiniCluster cluster = MiniCluster.buildCluster();
    
    public TestSkewedJoin() throws ExecException, IOException{
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        // pigServer = new PigServer(ExecType.LOCAL);
        pigServer.getPigContext().getProperties().setProperty("pig.skewedjoin.reduce.maxtuple", "5");     
        pigServer.getPigContext().getProperties().setProperty("pig.skewedjoin.reduce.memusage", "0.01");
    }
    
    @Before
    public void setUp() throws Exception {
        createFiles();
    }

    private void createFiles() throws IOException {
    	PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE1));
    	    	
    	int k = 0;
    	for(int j=0; j<120; j++) {   	           	        
   	        w.println("100\tapple1\taaa" + k);
    	    k++;
    	    w.println("200\torange1\tbbb" + k);
    	    k++;
    	    w.println("300\tstrawberry\tccc" + k);
    	    k++;    	        	    
    	}
    	
    	w.close();

    	PrintWriter w2 = new PrintWriter(new FileWriter(INPUT_FILE2));
    	w2.println("100\tapple1");
    	w2.println("100\tapple2");
    	w2.println("100\tapple2");
    	w2.println("200\torange1");
    	w2.println("200\torange2");
    	w2.println("300\tstrawberry");    	
    	w2.println("400\tpear");

    	w2.close();
    	
    	PrintWriter w3 = new PrintWriter(new FileWriter(INPUT_FILE3));
    	w3.println("100\tapple1");
    	w3.println("100\tapple2");
    	w3.println("200\torange1");
    	w3.println("200\torange2");
    	w3.println("300\tstrawberry");
    	w3.println("300\tstrawberry2");
    	w3.println("400\tpear");

    	w3.close();
    	
    	PrintWriter w4 = new PrintWriter(new FileWriter(INPUT_FILE4));
        for(int i=0; i < 100; i++) {
            w4.println("[a100#apple1,a100#apple2,a200#orange1,a200#orange2,a300#strawberry,a300#strawberry2,a400#pear]");
        }
    	w4.close();
    	
    	// Create a file with null keys
    	PrintWriter w5 = new PrintWriter(new FileWriter(INPUT_FILE5));
        for(int i=0; i < 10; i++) {
        	w5.println("\tapple1");
        }
        w5.println("100\tapple2");
        for(int i=0; i < 10; i++) {
        	w5.println("\torange1");
        }
        w5.println("\t");
        w5.println("100\t");
        w5.close();
        
    	Util.copyFromLocalToCluster(cluster, INPUT_FILE1, INPUT_FILE1);
    	Util.copyFromLocalToCluster(cluster, INPUT_FILE2, INPUT_FILE2);
    	Util.copyFromLocalToCluster(cluster, INPUT_FILE3, INPUT_FILE3);
    	Util.copyFromLocalToCluster(cluster, INPUT_FILE4, INPUT_FILE4);
    	Util.copyFromLocalToCluster(cluster, INPUT_FILE5, INPUT_FILE5);

    }
    
    @After
    public void tearDown() throws Exception {
    	new File(INPUT_FILE1).delete();
    	new File(INPUT_FILE2).delete();
    	new File(INPUT_FILE3).delete();
        new File(INPUT_FILE4).delete();
        Util.deleteDirectory(new File("skewedjoin"));
    	
        Util.deleteFile(cluster, INPUT_FILE1);
        Util.deleteFile(cluster, INPUT_FILE2);
        Util.deleteFile(cluster, INPUT_FILE3);
        Util.deleteFile(cluster, INPUT_FILE4);
        Util.deleteFile(cluster, INPUT_FILE5);

    }
    
    public void testSkewedJoinWithGroup() throws IOException{
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE1 + "' as (id, name, n);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE2 + "' as (id, name);");
        pigServer.registerQuery("C = GROUP A by id;");
        pigServer.registerQuery("D = GROUP B by id;");
        
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("E = join C by group, D by group using \"skewed\" parallel 5;");
            Iterator<Tuple> iter = pigServer.openIterator("E");
            
            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("E = join C by group, D by group;");
            Iterator<Tuple> iter = pigServer.openIterator("E");
            
            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        Assert.assertTrue(dbfrj.size()>0 && dbshj.size()>0);
        Assert.assertEquals(true, TestHelper.compareBags(dbfrj, dbshj));
    }      
    
    public void testSkewedJoinWithNoProperties() throws IOException{
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

        pigServer.registerQuery("A = LOAD '" + INPUT_FILE1 + "' as (id, name, n);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE2 + "' as (id, name);");
        try {
            DataBag dbfrj = BagFactory.getInstance().newDefaultBag();
            DataBag dbshj = BagFactory.getInstance().newDefaultBag();
            {
                pigServer.registerQuery("C = join A by (id, name), B by (id, name) using \"skewed\" parallel 5;");
                Iterator<Tuple> iter = pigServer.openIterator("C");

                while(iter.hasNext()) {
                    dbfrj.add(iter.next());
                }
            }
	    {
           	 pigServer.registerQuery("E = join A by(id, name), B by (id, name);");
           	 Iterator<Tuple> iter = pigServer.openIterator("E");

            	while(iter.hasNext()) {
                    dbshj.add(iter.next());
        	}
            }
            Assert.assertTrue(dbfrj.size()>0 && dbshj.size()>0);
            Assert.assertEquals(true, TestHelper.compareBags(dbfrj, dbshj));

        }catch(Exception e) {
             fail(e.getMessage());
        }
    }

    public void testSkewedJoinReducers() throws IOException{
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE1 + "' as (id, name, n);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE2 + "' as (id, name);");
        try {
            DataBag dbfrj = BagFactory.getInstance().newDefaultBag();
            {
                pigServer.registerQuery("C = join A by id, B by id using \"skewed\" parallel 1;");
                Iterator<Tuple> iter = pigServer.openIterator("C");
                
                while(iter.hasNext()) {
                    dbfrj.add(iter.next());
                }
            }
        }catch(Exception e) {
        	fail("Should not throw exception, should continue execution");
        }
        
    }
    
    public void testSkewedJoin3Way() throws IOException{
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE1 + "' as (id, name, n);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE2 + "' as (id, name);");
        pigServer.registerQuery("C = LOAD '" + INPUT_FILE3 + "' as (id, name);");
        try {
            DataBag dbfrj = BagFactory.getInstance().newDefaultBag();
            {
                pigServer.registerQuery("D = join A by id, B by id, C by id using \"skewed\" parallel 5;");
                Iterator<Tuple> iter = pigServer.openIterator("D");
                
                while(iter.hasNext()) {
                    dbfrj.add(iter.next());
                }
            }
        }catch(Exception e) {
        	return;
        }
        
        fail("Should throw exception, do not support 3 way join");
    }       

    public void testSkewedJoinMapKey() throws IOException{
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE4 + "' as (m:[]);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE4 + "' as (n:[]);");
        try {
            DataBag dbfrj = BagFactory.getInstance().newDefaultBag();
            {
                pigServer.registerQuery("C = join A by (chararray)m#'a100', B by (chararray)n#'a100' using \"skewed\" parallel 20;");
                Iterator<Tuple> iter = pigServer.openIterator("C");
                
                while(iter.hasNext()) {
                    dbfrj.add(iter.next());
                }
            }
        }catch(Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
        	fail("Should support maps and expression operators as keys");
        }
        
       	return;
	}


    public void testSkewedJoinKeyPartition() throws IOException {
    	try{
    	     Util.deleteFile(cluster, "skewedjoin");
    	}catch(Exception e){
    		// it is ok if directory not exist
    	}
    	 
    	 pigServer.registerQuery("A = LOAD '" + INPUT_FILE1 + "' as (id, name, n);");
         pigServer.registerQuery("B = LOAD '" + INPUT_FILE2 + "' as (id, name);");
           
        
         pigServer.registerQuery("E = join A by id, B by id using \"skewed\" parallel 7;");
         pigServer.store("E", "skewedjoin");
         
         int[][] lineCount = new int[3][7];
         
         new File("skewedjoin").mkdir();
         // check how many times a key appear in each part- file
         for(int i=0; i<7; i++) {
        	 Util.copyFromClusterToLocal(cluster, "skewedjoin/part-0000"+i, "skewedjoin/part-0000"+i);
        	 
        	 BufferedReader reader = new BufferedReader(new FileReader("skewedjoin/part-0000"+i));
      	     String line = null;      	     
      	     while((line = reader.readLine()) != null) {
      	        String[] cols = line.split("\t");
      	        int key = Integer.parseInt(cols[0])/100 -1;
      	        lineCount[key][i] ++;
      	    }
         }
         
         int fc = 0;
         for(int i=0; i<3; i++) {
        	 for(int j=0; j<7; j++) {
        	     if (lineCount[i][j] > 0) {
        			 fc ++;
        		 }
        	 }
         }
         // atleast one key should be a skewed key
         // check atleast one key should appear in more than 1 part- file
         assertTrue(fc > 3);
    }
    
    public void testSkewedJoinNullKeys() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE5 + "' as (id,name);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE5 + "' as (id,name);");
        try {
            DataBag dbfrj = BagFactory.getInstance().newDefaultBag();
            {
                pigServer.registerQuery("C = join A by id, B by id using \"skewed\";");
                Iterator<Tuple> iter = pigServer.openIterator("C");
                
                while(iter.hasNext()) {
                    dbfrj.add(iter.next());
                }
            }
        } catch(Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
        	fail("Should support null keys in skewed join");
        }
        return;
    }
    
    public void testSkewedJoinOuter() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE5 + "' as (id,name);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE5 + "' as (id,name);");
        try {
            DataBag dbfrj = BagFactory.getInstance().newDefaultBag();
            {
                pigServer.registerQuery("C = join A by id left, B by id using \"skewed\";");
                Iterator<Tuple> iter = pigServer.openIterator("C");
                
                while(iter.hasNext()) {
                    dbfrj.add(iter.next());
                }
            }
            {
                pigServer.registerQuery("C = join A by id right, B by id using \"skewed\";");
                Iterator<Tuple> iter = pigServer.openIterator("C");
                
                while(iter.hasNext()) {
                    dbfrj.add(iter.next());
                }
            }
            {
                pigServer.registerQuery("C = join A by id full, B by id using \"skewed\";");
                Iterator<Tuple> iter = pigServer.openIterator("C");
                
                while(iter.hasNext()) {
                    dbfrj.add(iter.next());
                }
            }
        } catch(Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            fail("Should support outer join in skewed join");
        }
        return;
    }
    
    // pig 1048
    public void testSkewedJoinOneValue() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE3 + "' as (id,name);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE3 + "' as (id,name);");
        // Filter key with a single value

        pigServer.registerQuery("C = FILTER A by id == 400;");
        pigServer.registerQuery("D = FILTER B by id == 400;");

        
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbrj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.registerQuery("E = join C by id, D by id using \"skewed\";");
            Iterator<Tuple> iter = pigServer.openIterator("E");
                
            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
        	pigServer.registerQuery("E = join C by id, D by id;");
        	Iterator<Tuple> iter = pigServer.openIterator("E");
        
        	while(iter.hasNext()) {
        		dbrj.add(iter.next());
        	}
        }
        Assert.assertEquals(dbfrj.size(), dbrj.size());
        Assert.assertEquals(true, TestHelper.compareBags(dbfrj, dbrj));       
       
    }
}
