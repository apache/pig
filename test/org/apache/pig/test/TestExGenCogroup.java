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
import java.util.Random;

import org.apache.pig.PigServer.ExecType;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestExGenCogroup extends PigExecTestCase{
	String A, B;
	private int MAX = 10;
	@Override
	@Before
	protected void setUp() throws Exception{
	    super.setUp();
	    
		File fileA, fileB;
		System.out.println("Generating test data...");
		fileA = File.createTempFile("dataA", ".dat");
		fileB = File.createTempFile("dataB", ".dat");
		
		writeData(fileA);
		writeData(fileB);
		
		A = "'" + FileLocalizer.hadoopify(fileA.toString(), pigServer.getPigContext()) + "'";
		B = "'" + FileLocalizer.hadoopify(fileB.toString(), pigServer.getPigContext()) + "'";
		System.out.println("A : " + A  + "\n" + "B : " + B);
		System.out.println("Test data created.");
		
	}
	
	private void writeData(File dataFile) throws Exception{
		//File dataFile = File.createTempFile(name, ".dat");
		FileOutputStream dat = new FileOutputStream(dataFile);
		
		Random rand = new Random();
		
		for(int i = 0; i < MAX; i++) 
			dat.write((rand.nextInt(10) + "\t" + rand.nextInt(10) + "\n").getBytes());
		dat.close();
			
	}
	
	@Override
	@After
	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	@Test
	public void testCogroupMultipleCols() throws Exception {
        // FIXME : this should be tested in all modes
	    if(execType != ExecType.LOCAL)
	        return;
		pigServer.registerQuery("A = load " + A + " as (x, y);");
		pigServer.registerQuery("B = load " + B + " as (x, y);");
		pigServer.registerQuery("C = cogroup A by (x, y), B by (x, y);");
		pigServer.showExamples("C");
	}
	
	@Test
	public void testCogroup() throws Exception {
	    // FIXME : this should be tested in all modes
        if(execType != ExecType.LOCAL)
            return;
		pigServer.registerQuery("A = load " + A + " as (x, y);");
		pigServer.registerQuery("B = load " + B + " as (x, y);");
		pigServer.registerQuery("C = cogroup A by x, B by x;");
		pigServer.showExamples("C");
	}
	
	@Test
	public void testGroup() throws Exception {
        // FIXME : this should be tested in all modes
        if(execType != ExecType.LOCAL)
            return;
		pigServer.registerQuery("A = load " + A.toString() + " as (x, y);");
		pigServer.registerQuery("B = group A by x;");
		pigServer.showExamples("B");
		
	}
	
	@Test
	public void testComplexGroup() throws Exception {
        // FIXME : this should be tested in all modes
        if(execType != ExecType.LOCAL)
            return;
	    pigServer.registerQuery("A = load " + A.toString() + " as (x, y);");
	    pigServer.registerQuery("B = load " + B.toString() + " as (x, y);");
	    pigServer.registerQuery("C = cogroup A by x, B by x;");
	    pigServer.registerQuery("D = cogroup A by y, B by y;");
	    pigServer.registerQuery("E = cogroup C by $0, D by $0;");
	    pigServer.showExamples("E");
	}
	
}
