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
import java.util.HashMap;
import java.util.Iterator;
import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.Before;

public class TestAccumulator extends TestCase{
    private static final String INPUT_FILE = "AccumulatorInput.txt";
    private static final String INPUT_FILE2 = "AccumulatorInput2.txt";
    private static final String INPUT_FILE3 = "AccumulatorInput3.txt";
 
    private PigServer pigServer;
    private MiniCluster cluster = MiniCluster.buildCluster();
    
    public TestAccumulator() throws ExecException, IOException{
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        // pigServer = new PigServer(ExecType.LOCAL);
        pigServer.getPigContext().getProperties().setProperty("pig.accumulative.batchsize", "2");     
        pigServer.getPigContext().getProperties().setProperty("pig.exec.batchsize", "2");
        pigServer.getPigContext().getProperties().setProperty("pig.exec.nocombiner", "true");
        // reducing the number of retry attempts to speed up test completion
        pigServer.getPigContext().getProperties().setProperty("mapred.map.max.attempts","1");
        pigServer.getPigContext().getProperties().setProperty("mapred.reduce.max.attempts","1");
    }
    
    @Before
    public void setUp() throws Exception {
        createFiles();
    }

    private void createFiles() throws IOException {
        PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
                
        w.println("100\tapple");    	    	
        w.println("200\torange");    	
        w.println("300\tstrawberry");    	
        w.println("300\tpear");
        w.println("100\tapple");
        w.println("300\tpear");
        w.println("400\tapple");    
        w.close();   
        
        Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);
        
        w = new PrintWriter(new FileWriter(INPUT_FILE2));
        
        w.println("100\t");    	
        w.println("100\t");
        w.println("200\t");    	
        w.println("200\t");    	
        w.println("300\tstrawberry");
        w.close();   
        
        Util.copyFromLocalToCluster(cluster, INPUT_FILE2, INPUT_FILE2);
        
        w = new PrintWriter(new FileWriter(INPUT_FILE3));
        
        w.println("100\t1.0");    	
        w.println("100\t2.0");
        w.println("200\t1.1");    	
        w.println("200\t2.1");
        w.println("100\t3.0");    	
        w.println("100\t4.0");
        w.println("200\t3.1");
        w.println("100\t5.0");
        w.println("300\t3.3");
        w.println("400\t");
        w.println("400\t");
        w.close();   
        
        Util.copyFromLocalToCluster(cluster, INPUT_FILE3, INPUT_FILE3);
    }
    
    @After
    public void tearDown() throws Exception {
        new File(INPUT_FILE).delete();
        Util.deleteFile(cluster, INPUT_FILE);
        new File(INPUT_FILE2).delete();
        Util.deleteFile(cluster, INPUT_FILE2);
        new File(INPUT_FILE3).delete();
        Util.deleteFile(cluster, INPUT_FILE3);
    }
    
    public void testAccumBasic() throws IOException{
        // test group by
        pigServer.registerQuery("A = load '" + INPUT_FILE + "' as (id:int, fruit);");
        pigServer.registerQuery("B = group A by id;");
        pigServer.registerQuery("C = foreach B generate group,  org.apache.pig.test.utils.AccumulatorBagCount(A);");                     

        HashMap<Integer, Integer> expected = new HashMap<Integer, Integer>();
        expected.put(100, 2);
        expected.put(200, 1);
        expected.put(300, 3);
        expected.put(400, 1);
        
                  
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        while(iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(expected.get((Integer)t.get(0)), (Integer)t.get(1));                
        }            
        
        pigServer.registerQuery("B = group A by id;");
        pigServer.registerQuery("C = foreach B generate group,  " +
                "org.apache.pig.test.utils.AccumulatorBagCount(A), org.apache.pig.test.utils.BagCount(A);");                     
        
        try{
            iter = pigServer.openIterator("C");
        
            while(iter.hasNext()) {
                Tuple t = iter.next();
                assertEquals(expected.get((Integer)t.get(0)), (Integer)t.get(1));                
            }      
            fail("accumulator should not be called.");
        }catch(IOException e) {
            // should throw exception from AccumulatorBagCount.
        }
        
        // test cogroup
        pigServer.registerQuery("A = load '" + INPUT_FILE + "' as (id:int, fruit);");
        pigServer.registerQuery("B = load '" + INPUT_FILE + "' as (id:int, fruit);");
        pigServer.registerQuery("C = cogroup A by id, B by id;");
        pigServer.registerQuery("D = foreach C generate group,  " +
                "org.apache.pig.test.utils.AccumulatorBagCount(A), org.apache.pig.test.utils.AccumulatorBagCount(B);");                     

        HashMap<Integer, String> expected2 = new HashMap<Integer, String>();
        expected2.put(100, "2,2");
        expected2.put(200, "1,1");
        expected2.put(300, "3,3");
        expected2.put(400, "1,1");
        
                  
        iter = pigServer.openIterator("D");
        
        while(iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(expected2.get((Integer)t.get(0)), t.get(1).toString()+","+t.get(2).toString());                
        }            
    }      
    
    public void testAccumWithNegative() throws IOException{
        pigServer.registerQuery("A = load '" + INPUT_FILE + "' as (id:int, fruit);");
        pigServer.registerQuery("B = group A by id;");
        pigServer.registerQuery("C = foreach B generate group,  -org.apache.pig.test.utils.AccumulatorBagCount(A);");                     

        HashMap<Integer, Integer> expected = new HashMap<Integer, Integer>();
        expected.put(100, -2);
        expected.put(200, -1);
        expected.put(300, -3);
        expected.put(400, -1);
        
                  
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        while(iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(expected.get((Integer)t.get(0)), (Integer)t.get(1));                
        }            
    }
    
    public void testAccumWithAdd() throws IOException{
        pigServer.registerQuery("A = load '" + INPUT_FILE + "' as (id:int, fruit);");
        pigServer.registerQuery("B = group A by id;");
        pigServer.registerQuery("C = foreach B generate group,  org.apache.pig.test.utils.AccumulatorBagCount(A)+1.0;");                     
        
        {
            HashMap<Integer, Double> expected = new HashMap<Integer, Double>();
            expected.put(100, 3.0);
            expected.put(200, 2.0);
            expected.put(300, 4.0);
            expected.put(400, 2.0);
            
                      
            Iterator<Tuple> iter = pigServer.openIterator("C");
            
            while(iter.hasNext()) {
                Tuple t = iter.next();
                assertEquals(expected.get((Integer)t.get(0)), (Double)t.get(1));                
            }                            
        }
        
        {
            pigServer.registerQuery("C = foreach B generate group,  " +
            "org.apache.pig.test.utils.AccumulatorBagCount(A)+org.apache.pig.test.utils.AccumulatorBagCount(A);");                     

            HashMap<Integer, Integer>expected = new HashMap<Integer, Integer>();
            expected.put(100, 4);
            expected.put(200, 2);
            expected.put(300, 6);
            expected.put(400, 2);
    
              
            Iterator<Tuple> iter = pigServer.openIterator("C");
    
            while(iter.hasNext()) {
                Tuple t = iter.next();
                assertEquals(expected.get((Integer)t.get(0)), (Integer)t.get(1));                
            }
        }
    }      
    
    public void testAccumWithMinus() throws IOException{
        pigServer.registerQuery("A = load '" + INPUT_FILE + "' as (id:int, fruit);");
        pigServer.registerQuery("B = group A by id;");
        pigServer.registerQuery("C = foreach B generate group, " +
                " org.apache.pig.test.utils.AccumulatorBagCount(A)*3.0-org.apache.pig.test.utils.AccumulatorBagCount(A);");                     

        HashMap<Integer, Double> expected = new HashMap<Integer, Double>();
        expected.put(100, 4.0);
        expected.put(200, 2.0);
        expected.put(300, 6.0);
        expected.put(400, 2.0);
        
                  
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        while(iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(expected.get((Integer)t.get(0)), (Double)t.get(1));                
        }                                   
    }              
    
    public void testAccumWithMod() throws IOException{
        pigServer.registerQuery("A = load '" + INPUT_FILE + "' as (id:int, fruit);");
        pigServer.registerQuery("B = group A by id;");
        pigServer.registerQuery("C = foreach B generate group,  " +
                "org.apache.pig.test.utils.AccumulatorBagCount(A) % 2;");                     

        HashMap<Integer, Integer> expected = new HashMap<Integer, Integer>();
        expected.put(100, 0);
        expected.put(200, 1);
        expected.put(300, 1);
        expected.put(400, 1);
        
                  
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        while(iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(expected.get((Integer)t.get(0)), (Integer)t.get(1));                
        }                                   
    }             
    
    public void testAccumWithDivide() throws IOException{
        pigServer.registerQuery("A = load '" + INPUT_FILE + "' as (id:int, fruit);");
        pigServer.registerQuery("B = group A by id;");
        pigServer.registerQuery("C = foreach B generate group,  " +
                "org.apache.pig.test.utils.AccumulatorBagCount(A)/2;");                     

        HashMap<Integer, Integer> expected = new HashMap<Integer, Integer>();
        expected.put(100, 1);
        expected.put(200, 0);
        expected.put(300, 1);
        expected.put(400, 0);
        
                  
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        while(iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(expected.get((Integer)t.get(0)), (Integer)t.get(1));                
        }                                   
    }        
    
    public void testAccumWithAnd() throws IOException{
        pigServer.registerQuery("A = load '" + INPUT_FILE + "' as (id:int, fruit);");
        pigServer.registerQuery("B = group A by id;");
        pigServer.registerQuery("C = foreach B generate group,  " +
                "((org.apache.pig.test.utils.AccumulatorBagCount(A)>1 and " +
                "org.apache.pig.test.utils.AccumulatorBagCount(A)<3)?0:1);");                     

        HashMap<Integer, Integer> expected = new HashMap<Integer, Integer>();
        expected.put(100, 0);
        expected.put(200, 1);
        expected.put(300, 1);
        expected.put(400, 1);
        
                  
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        while(iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(expected.get((Integer)t.get(0)), (Integer)t.get(1));                
        }                                   
    }          
    
    public void testAccumWithOr() throws IOException{
        pigServer.registerQuery("A = load '" + INPUT_FILE + "' as (id:int, fruit);");
        pigServer.registerQuery("B = group A by id;");
        pigServer.registerQuery("C = foreach B generate group,  " +
                "((org.apache.pig.test.utils.AccumulatorBagCount(A)>3 or " +
                "org.apache.pig.test.utils.AccumulatorBagCount(A)<2)?0:1);");                     

        HashMap<Integer, Integer> expected = new HashMap<Integer, Integer>();
        expected.put(100, 1);
        expected.put(200, 0);
        expected.put(300, 1);
        expected.put(400, 0);
        
                  
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        while(iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(expected.get((Integer)t.get(0)), (Integer)t.get(1));                
        }                                   
    }  
    
    public void testAccumWithRegexp() throws IOException{
        pigServer.registerQuery("A = load '" + INPUT_FILE + "' as (id:int, fruit);");
        pigServer.registerQuery("B = group A by id;");
        pigServer.registerQuery("C = foreach B generate group,  " +
                "(((chararray)org.apache.pig.test.utils.AccumulatorBagCount(A)) matches '1*' ?0:1);");                     

        HashMap<Integer, Integer> expected = new HashMap<Integer, Integer>();
        expected.put(100, 1);
        expected.put(200, 0);
        expected.put(300, 1);
        expected.put(400, 0);
        
                  
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        while(iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(expected.get((Integer)t.get(0)), (Integer)t.get(1));                
        }                                   
    }              
    

    public void testAccumWithIsNull() throws IOException{
        pigServer.registerQuery("A = load '" + INPUT_FILE2 + "' as (id:int, fruit);");
        pigServer.registerQuery("B = group A by id;");
        pigServer.registerQuery("C = foreach B generate group,  " +
                "((chararray)org.apache.pig.test.utils.AccumulativeSumBag(A) is null?0:1);");                     

        HashMap<Integer, Integer> expected = new HashMap<Integer, Integer>();
        expected.put(100, 0);
        expected.put(200, 0);
        expected.put(300, 1);                
                  
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        while(iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(expected.get((Integer)t.get(0)), (Integer)t.get(1));                
        }                                   
    }              
    
    public void testAccumWithDistinct() throws IOException{
        pigServer.registerQuery("A = load '" + INPUT_FILE + "' as (id:int, f);");
        pigServer.registerQuery("B = group A by id;");
        pigServer.registerQuery("C = foreach B { D = distinct A; generate group, org.apache.pig.test.utils.AccumulatorBagCount(D)+1;};");                     

        HashMap<Integer, Integer> expected = new HashMap<Integer, Integer>();
        expected.put(100, 2);
        expected.put(200, 2);
        expected.put(300, 3);
        expected.put(400, 2);
                  
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        while(iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(expected.get((Integer)t.get(0)), (Integer)t.get(1));                
        }                                   
    }             
    
    public void testAccumWithSort() throws IOException{
        pigServer.registerQuery("A = load '" + INPUT_FILE + "' as (id:int, f);");
        pigServer.registerQuery("B = foreach A generate id, f, id as t;");
        pigServer.registerQuery("C = group B by id;");
        pigServer.registerQuery("D = foreach C { E = order B by f; F = E.f; generate group, org.apache.pig.test.utils.AccumulativeSumBag(F);};");                     

        HashMap<Integer, String> expected = new HashMap<Integer, String>();
        expected.put(100, "(apple)(apple)");
        expected.put(200, "(orange)");
        expected.put(300, "(pear)(pear)(strawberry)");
        expected.put(400, "(apple)");
                  
        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        while(iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(expected.get((Integer)t.get(0)), (String)t.get(1));                
        }                                   
    }
    
    public void testAccumWithBuildinAvg() throws IOException {
      HashMap<Integer, Double> expected = new HashMap<Integer, Double>();
      expected.put(100, 3.0);
      expected.put(200, 2.1);
      expected.put(300, 3.3);
      expected.put(400, null);
      // Test all the averages for correct behaviour with null values
      String[] types = { "double", "float", "int", "long" };
      for (int i = 0; i < types.length; i++) {
        if (i > 1) { // adjust decimal error for non real types
          expected.put(200, 2.0);
          expected.put(300, 3.0);
        }
        pigServer.registerQuery("A = load '" + INPUT_FILE3 + "' as (id:int, v:"
            + types[i] + ");");
        pigServer.registerQuery("C = group A by id;");
        pigServer.registerQuery("D = foreach C generate group, AVG(A.v);");
        Iterator<Tuple> iter = pigServer.openIterator("D");

        while (iter.hasNext()) {
          Tuple t = iter.next();
          Double v = expected.get((Integer) t.get(0));
          if (v != null) {
            assertEquals(v.doubleValue(), ((Number) t.get(1)).doubleValue(),
                0.0001);
          } else {
            assertEquals(null, t.get(1));
          }
        }
      }
    }
    
    public void testAccumWithBuildin() throws IOException{
        pigServer.registerQuery("A = load '" + INPUT_FILE3 + "' as (id:int, v:double);");
        pigServer.registerQuery("C = group A by id;");
        // moving AVG accumulator test to separate test case
        pigServer.registerQuery("D = foreach C generate group, SUM(A.v), COUNT(A.v), MIN(A.v), MAX(A.v);");       

        HashMap<Integer, Double[]> expected = new HashMap<Integer, Double[]>();
        expected.put(100, new Double[]{15.0, 5.0, 1.0, 5.0});
        expected.put(200, new Double[]{6.3, 3.0, 1.1, 3.1});
        expected.put(300, new Double[]{3.3, 1.0, 3.3, 3.3});
        expected.put(400, new Double[] { null, 0.0, null, null });
                  
        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        while(iter.hasNext()) {
            Tuple t = iter.next();
            Double[] v = expected.get((Integer)t.get(0));
            for(int i=0; i<v.length; i++) {
              if (v[i] != null) {
                assertEquals(v[i].doubleValue(), ((Number) t.get(i + 1))
                    .doubleValue(), 0.0001);
              } else {
                assertEquals(null, t.get(i + 1));
              }
            }            
        }    
    }
    
    public void testAccumWithMultiBuildin() throws IOException{
        pigServer.registerQuery("A = load '" + INPUT_FILE + "' as (id:int, c:chararray);");
        pigServer.registerQuery("C = group A by 1;");
        pigServer.registerQuery("D = foreach C generate SUM(A.id), 1+SUM(A.id)+SUM(A.id);");                     

        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        while(iter.hasNext()) {
            Tuple t = iter.next();    
            t.get(0).toString().equals("1700");
            t.get(1).toString().equals("3401");   
        }    
    }

	// Pig 1105
    public void testAccumCountStar() throws IOException{
        pigServer.registerQuery("A = load '" + INPUT_FILE3 + "' as (id:int, v:double);");
        pigServer.registerQuery("C = group A by id;");
        pigServer.registerQuery("D = foreach C generate group, COUNT_STAR(A.id);");

		try {
			Iterator<Tuple> iter = pigServer.openIterator("D");
		} catch (Exception e) {
			fail("COUNT_STAR should be supported by accumulator interface");
		}      
	}
	
}
