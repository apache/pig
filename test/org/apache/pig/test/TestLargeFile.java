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

import static org.apache.pig.PigServer.ExecType.MAPREDUCE;
import org.apache.pig.PigServer;

import org.apache.pig.data.Tuple;

import org.apache.pig.impl.io.FileLocalizer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.io.FileOutputStream;
import java.util.Iterator;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;

import org.apache.hadoop.conf.Configuration;
//This class tests pig behavior with large file spanning multiple blocks along with group and count functions
//Order and Distinct functions are also tested.
//This test would take a long time because of the large test files.

public class TestLargeFile extends TestCase {
	
	File datFile;
	
	private long defaultBlockSize = (new Configuration()).getLong("dfs.block.size", 0);
		
	private long total = defaultBlockSize >> 1;
	private int max_rand = 500;
//	private double sum = 0.0, sumIn = 0.0;
	MiniCluster cluster = MiniCluster.buildCluster();
	
	Integer [] COUNT = new Integer[max_rand];

	
	PigServer pig;
	String fileName, tmpFile1;
	
	@Override
	@Before
	protected void setUp() throws Exception{

        System.out.println("Generating test data...");
        System.out.println("Default block size = " + defaultBlockSize);
        System.out.println("Total no. of iterations to run for the test data = " + total);
        
        datFile = File.createTempFile("StoreTest", ".dat");
        
        FileOutputStream dat = new FileOutputStream(datFile);
        
        Random rand = new Random();
        
        for(int i = 0; i < max_rand; i++) {
        	COUNT[i] = 0;
        }
        
        
        for(long i = 0; i < total; i++) {

            Integer x = new Integer(rand.nextInt(max_rand));
            COUNT[x.intValue()] ++;
            dat.write((x.toString() + "\n").getBytes());
        }
        
        dat.close();
    
        try {
            pig = new PigServer(MAPREDUCE, cluster.getProperties());
        }
        catch (ExecException e) {
        	IOException ioe = new IOException("Failed to create Pig server");
        	ioe.initCause(e);
            throw ioe;
        }
		fileName = "'" + FileLocalizer.hadoopify(datFile.toString(), pig.getPigContext()) + "'";
		tmpFile1 = "'" + FileLocalizer.getTemporaryPath(null, pig.getPigContext()).toString() + "'";

        datFile.delete();
    }
	
	@Override
	@After
	protected void tearDown() throws Exception {

		
	}


	@Test
	public void testLargeFile () throws Exception {
		System.out.println("Running testLargeFile...");
		pig.registerQuery("A = load " + Util.encodeEscape(fileName.toString()) + ";");
		pig.registerQuery("A = group A by $0;");
		pig.store("A", Util.encodeEscape(tmpFile1.toString()), "BinStorage()");
//		pig.store("A", tmpFile1);
		pig.registerQuery("B = foreach A generate group, COUNT($1);");
		
		Iterator <Tuple> B = pig.openIterator("B");
		
		while(B.hasNext()) {
			Tuple temp = B.next();
			int index = temp.getAtomField(0).numval().intValue();
			int value = temp.getAtomField(1).numval().intValue();
			System.out.println("COUNT [" + index + "] = " + COUNT[index] + " B[" + index + "] = " + value);
			
			assertEquals(COUNT[index].intValue(), value);
			
		}
				
	}
	
	@Test
	public void testOrder () throws Exception {
		System.out.println("Running testOrder...");
		int N = 0, Nplus1 = 0;
		pig.registerQuery("A = load " + Util.encodeEscape(fileName.toString()) + ";");
		pig.registerQuery("B = order A by $0;");
		
		Iterator <Tuple> B = pig.openIterator("B");
		
		if(B.hasNext()) {
			N = B.next().getAtomField(0).numval().intValue();
		}
		
		while(B.hasNext()) {
			int flag = 0;
			Nplus1 = B.next().getAtomField(0).numval().intValue();
			if(Nplus1 >= N) {
				flag = 1;
			}
			assertEquals(flag, 1);
			
			N = Nplus1;
			
		}
		
		
	}
	
	@Test
	public void testDistinct () throws Exception {
		System.out.println("Running testDistinct...");
		pig.registerQuery("A = load " + Util.encodeEscape(fileName.toString()) + ";");
		pig.registerQuery("B = distinct A;");
		
		Iterator <Tuple> B = pig.openIterator("B");
		
		Integer [] COUNT_Test = new Integer [max_rand];
		Integer [] COUNT_Data = new Integer [max_rand];
		
		for(int i = 0; i < max_rand; i++) {
        	COUNT_Test[i] = 0;
        	if (COUNT[i] > 0) {
        		COUNT_Data[i] = 1;
        	} else {
        		COUNT_Data[i] = 0;
        	}
        }
		
		while(B.hasNext()) {
			int temp = B.next().getAtomField(0).numval().intValue();
			COUNT_Test[temp] ++;
		}
		
		for(int i = 0; i < max_rand; i++) {
			assertEquals(COUNT_Test[i].intValue(), COUNT_Data[i].intValue());
		}
		
	}

}
