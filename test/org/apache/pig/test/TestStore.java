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
import java.io.PrintWriter;
import java.util.Iterator;

import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.backend.executionengine.ExecException;
import static org.apache.pig.PigServer.ExecType.MAPREDUCE;

import junit.framework.TestCase;

public class TestStore extends PigExecTestCase {

	private int LOOP_COUNT = 1024;
	String fileName;
	String tmpFile1, tmpFile2;

	public void testSingleStore() throws Exception{
		pigServer.registerQuery("A = load " + fileName + ";");
		
		pigServer.store("A", tmpFile1);
		
		pigServer.registerQuery("B = load " + tmpFile1 + ";");
		Iterator<Tuple> iter  = pigServer.openIterator("B");
		
		int i =0;
		while (iter.hasNext()){
			Tuple t = iter.next();
			assertEquals(t.getAtomField(0).numval().intValue(),i);
			assertEquals(t.getAtomField(1).numval().intValue(),i);
			i++;
		}
	}
	
	public void testMultipleStore() throws Exception{
		pigServer.registerQuery("A = load " + fileName + ";");
		
		pigServer.store("A", tmpFile1);
		
		pigServer.registerQuery("B = foreach (group A by $0) generate $0, SUM($1);");
		pigServer.store("B", tmpFile2);
		pigServer.registerQuery("C = load " + tmpFile2 + ";");
		Iterator<Tuple> iter  = pigServer.openIterator("C");
		
		int i =0;
		while (iter.hasNext()){
			Tuple t = iter.next();
			i++;
			
		}
		
		assertEquals(LOOP_COUNT, i);
		
	}
	
	public void testStoreWithMultipleMRJobs() throws Exception{
		pigServer.registerQuery("A = load " + fileName + ";");		
		pigServer.registerQuery("B = foreach (group A by $0) generate $0, SUM($1);");
		pigServer.registerQuery("C = foreach (group B by $0) generate $0, SUM($1);");
		pigServer.registerQuery("D = foreach (group C by $0) generate $0, SUM($1);");

		pigServer.store("D", tmpFile2);
		pigServer.registerQuery("E = load " + tmpFile2 + ";");
		Iterator<Tuple> iter  = pigServer.openIterator("E");
		
		int i =0;
		while (iter.hasNext()){
			Tuple t = iter.next();
			i++;
		}
		
		assertEquals(LOOP_COUNT, i);
		
	}

	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		File f = File.createTempFile("tmp", "");
		PrintWriter pw = new PrintWriter(f);
		for (int i=0;i<LOOP_COUNT; i++){
			pw.println(i + "\t" + i);
		}
		pw.close();
		
		fileName = "'" + FileLocalizer.hadoopify(f.toString(), pigServer.getPigContext()) + "'";
		tmpFile1 = "'" + FileLocalizer.getTemporaryPath(null, pigServer.getPigContext()).toString() + "'";
		tmpFile2 = "'" + FileLocalizer.getTemporaryPath(null, pigServer.getPigContext()).toString() + "'";
		f.delete();
	}


    public void testDelimiter() throws IOException{
        System.out.println("Temp files: " + tmpFile1 + ", " + tmpFile2);
        pigServer.registerQuery("A = load " + fileName + ";");
        pigServer.store("A", tmpFile1, "PigStorage('\u0001')");
        pigServer.registerQuery("B = load " + tmpFile1 + "using PigStorage('\\u0001') ;");
        pigServer.registerQuery("C = foreach B generate $0, $1;");
        pigServer.store("C", tmpFile2);
        pigServer.registerQuery("E = load " + tmpFile2 + ";");
        Iterator<Tuple> iter = pigServer.openIterator("E");
        int i =0;
        while (iter.hasNext()) {
            Tuple t = iter.next();
            assertEquals(t.getAtomField(0).numval().intValue(),i);
            assertEquals(t.getAtomField(1).numval().intValue(),i); i++; 
        }
    }

 }
