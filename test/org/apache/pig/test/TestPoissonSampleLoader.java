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
import java.util.ArrayList;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.builtin.PoissonSampleLoader;
import org.apache.pig.impl.builtin.SampleLoader;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;

import java.util.Properties;


public class TestPoissonSampleLoader extends TestCase{
    private static final String INPUT_FILE1 = "SkewedJoinInput1.txt";

    private PigServer pigServer;
    private MiniCluster cluster = MiniCluster.buildCluster();
    
    public TestPoissonSampleLoader() throws ExecException, IOException{
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pigServer.getPigContext().getProperties().setProperty("pig.skewedjoin.reduce.maxtuple", "5");     
        pigServer.getPigContext().getProperties().setProperty("pig.skewedjoin.reduce.memusage", "0.0001");
        pigServer.getPigContext().getProperties().setProperty("mapred.child.java.opts", "-Xmx512m");

        pigServer.getPigContext().getProperties().setProperty("pig.mapsplits.count", "5");
    }
    
    
    @Before
    public void setUp() throws Exception {
        createFiles();
    }

    private void createFiles() throws IOException {
    	PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE1));
    	    	
    	int k = 0;
    	for(int j=0; j<100; j++) {   	           	        
   	        w.println("100\tapple1\taaa" + k);
    	    k++;
    	    w.println("200\torange1\tbbb" + k);
    	    k++;
    	    w.println("300\tstrawberry\tccc" + k);
    	    k++;    	        	    
    	}
    	
    	w.close();
    	
    	Util.copyFromLocalToCluster(cluster, INPUT_FILE1, INPUT_FILE1);
    }
    
    
    @After
    public void tearDown() throws Exception {
    	new File(INPUT_FILE1).delete();
    	
        Util.deleteFile(cluster, INPUT_FILE1);
    }
    
    
    public void testComputeSamples() throws IOException{
 		FileSpec fs = new FileSpec(INPUT_FILE1, new FuncSpec(PigStorage.class.getName()));
  		
  		ArrayList<Pair<FileSpec, Boolean>> inputs = new ArrayList<Pair<FileSpec, Boolean> >();
  		inputs.add(new Pair<FileSpec, Boolean>(fs, true));
  		Properties p = UDFContext.getUDFContext().getUDFProperties(SampleLoader.class);
  		p.setProperty("pig.input.0.size", Long.toString(Util.getSize(cluster, INPUT_FILE1)));
  		
        // Use 100 as a default value;
        PoissonSampleLoader ps = new PoissonSampleLoader((new FuncSpec(PigStorage.class.getName())).toString(), "100");

        // Get the number of samples for the file
        ps.computeSamples(inputs, pigServer.getPigContext());
        
        if (ps.getNumSamples() != 3) {
        	fail("Compute samples returned the wrong number of samples: " + ps.getNumSamples() + " instead of 3");
        }
    }
	
}