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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Iterator;

import org.junit.Test;

import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.backend.executionengine.ExecException;

import junit.framework.TestCase;

public class TestPigSplit extends TestCase {
	PigServer pig;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		
		try {
		    pig = new PigServer();
		}
		catch (ExecException e) {
			IOException ioe = new IOException("Failed to create Pig Server");
			ioe.initCause(e);
		    throw ioe;
		}
	}
	@Test
	public void testLongEvalSpec() throws Exception{
		File f = File.createTempFile("tmp", "");
		
		PrintWriter pw = new PrintWriter(f);
		pw.println("0\ta");
		pw.close();
		
		pig.registerQuery("a = load 'file:" + f + "';");
		for (int i=0; i< 500; i++){
			pig.registerQuery("a = filter a by $0 == '1';");
		}
		Iterator<Tuple> iter = pig.openIterator("a");
		while (iter.hasNext()){
			throw new Exception();
		}
		f.delete();
	}
	
}
