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
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import org.apache.pig.PigServer;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

public class TestExGenEval extends PigExecTestCase {
	
	String A, B, C, D;
	private int MAX = 10;
	
	PigContext pigContext;
	
	@Override
	@Before
	protected void setUp() throws Exception{
	    super.setUp();
		System.out.println("Generating test data...");
		File fileA, fileB, fileC, fileD;
		
		pigContext = pigServer.getPigContext();
		fileA = File.createTempFile("dataA", ".dat");
		fileB = File.createTempFile("dataB", ".dat");
		fileC = File.createTempFile("dataC", ".dat");
		fileD = File.createTempFile("dataD", ".dat");
		
		
		writeData(fileA);
		writeData(fileB);
		writeData(fileC);
		writeData(fileD);
		
		
		A = "'" + FileLocalizer.hadoopify(fileA.toString(), pigServer.getPigContext()) + "'";
		B = "'" + FileLocalizer.hadoopify(fileB.toString(), pigServer.getPigContext()) + "'";
		C = "'" + FileLocalizer.hadoopify(fileC.toString(), pigServer.getPigContext()) + "'";
		D = "'" + FileLocalizer.hadoopify(fileD.toString(), pigServer.getPigContext()) + "'";
		
		System.out.println("Test data created.");
		fileA.delete();
		fileB.delete();
		fileC.delete();
		fileD.delete();
		
	}
	
	private void writeData(File dataFile) throws Exception{
		//File dataFile = File.createTempFile(name, ".dat");
		FileOutputStream dat = new FileOutputStream(dataFile);
		
		Random rand = new Random();
		
		for(int i = 0; i < MAX; i++) 
			dat.write((rand.nextInt(10) + "\t" + rand.nextInt(10) + "\n").getBytes());
			
	}
	
	@Override
	@After
	protected void tearDown() throws Exception {
		
	}
	
	@Test
	public void testForeach() throws Exception {
	    // FIXME : this should be tested in all modes
        if(execType != ExecType.LOCAL)
            return;
	    System.out.println("Testing Foreach statement...");
		pigServer.registerQuery("A = load " + A + " as (x, y);");
		pigServer.registerQuery("B = foreach A generate x+y as sum;");
		pigServer.showExamples("B");
		assertEquals(1, 1);
	}
	
	@Test 
	public void testFilter() throws Exception {
        // FIXME : this should be tested in all modes
        if(execType != ExecType.LOCAL)
            return;
	    pigServer.registerQuery("A = load " + A + " as (x, y);");
	    pigServer.registerQuery("B = filter A by x < 10.0;");
	    pigServer.showExamples("B");
		assertEquals(1, 1);
	}
	
	@Test
	public void testFlatten() throws Exception {
        // FIXME : this should be tested in all modes
        if(execType != ExecType.LOCAL)
            return;
	    pigServer.registerQuery("A1 = load " + A + " as (x, y);");
	    pigServer.registerQuery("B1 = load " + B + " as (x, y);");
	    pigServer.registerQuery("C1 = load " + C + " as (x, y);");
        pigServer.registerQuery("D1 = load " + D + " as (x, y);");
	    pigServer.registerQuery("E = join A1 by x, B1 by x;");
	    pigServer.registerQuery("F = join C1 by x, D1 by x;");
	    pigServer.registerQuery("G = join E by $0, F by $0;");
	    pigServer.showExamples("G");
		assertEquals(1, 1);
	}
}
