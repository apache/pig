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
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import org.junit.Test;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigServer;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.TextLoader;
import org.apache.pig.data.*;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.PigFile;
import static org.apache.pig.PigServer.ExecType.MAPREDUCE;

import org.apache.pig.backend.executionengine.ExecException;

import junit.framework.TestCase;

public class TestEvalPipeline extends PigExecTestCase {
		
	static public class MyBagFunction extends EvalFunc<DataBag>{
		@Override
		public void exec(Tuple input, DataBag output) throws IOException {
			output.add(new Tuple("a"));
			output.add(new Tuple("a"));
			output.add(new Tuple("a"));
			
		}
	}
	
	private File createFile(String[] data) throws Exception{
		File f = File.createTempFile("tmp", "");
		PrintWriter pw = new PrintWriter(f);
		for (int i=0; i<data.length; i++){
			pw.println(data[i]);
		}
		pw.close();
		return f;
	}

	@Test
	public void testFunctionInsideFunction() throws Throwable {
		File f1 = createFile(new String[]{"a:1","b:1","a:1"});
		pigServer.registerQuery("a = load 'file:" + Util.encodeEscape(f1.toString()) + "' using " + PigStorage.class.getName() + "(':');");
		pigServer.registerQuery("b = foreach a generate '1'-'1'/'1';");
		Iterator<Tuple> iter  = pigServer.openIterator("b");
		
		for (int i=0 ;i<3; i++){
			assertEquals(iter.next().getAtomField(0).numval(), 0.0);
		}
		
	}
	
	@Test
	public void testJoin() throws Throwable {
		File f1 = createFile(new String[]{"a:1","b:1","a:1"});
		File f2 = createFile(new String[]{"b","b","a"});
		
		pigServer.registerQuery("a = load 'file:" + Util.encodeEscape(f1.toString()) + "' using " + PigStorage.class.getName() + "(':');");
		pigServer.registerQuery("b = load 'file:" + Util.encodeEscape(f2.toString()) + "';");
		pigServer.registerQuery("c = cogroup a by $0, b by $0;");		
		pigServer.registerQuery("d = foreach c generate flatten($1),flatten($2);");
		
		Iterator<Tuple> iter = pigServer.openIterator("d");
		int count = 0;
		while(iter.hasNext()){
			Tuple t = iter.next();
			assertTrue(t.getAtomField(0).strval().equals(t.getAtomField(2).strval()));
			count++;
		}
		assertEquals(count, 4);
	}
	
	@Test
	public void testDriverMethod() throws Throwable {
		File f = File.createTempFile("tmp", "");
		PrintWriter pw = new PrintWriter(f);
		pw.println("a");
		pw.println("a");
		pw.close();
		pigServer.registerQuery("a = foreach (load 'file:" + Util.encodeEscape(f.toString()) + "') generate '1', flatten(" + MyBagFunction.class.getName() + "(*));");
		pigServer.registerQuery("b = foreach a generate $0, flatten($1);");
		Iterator<Tuple> iter = pigServer.openIterator("a");
		int count = 0;
		while(iter.hasNext()){
			Tuple t = iter.next();
			assertTrue(t.getAtomField(0).strval().equals("1"));
			assertTrue(t.getAtomField(1).strval().equals("a"));
			count++;
		}
		assertEquals(count, 6);
		f.delete();
	}
	
	
	@Test
	public void testMapLookup() throws Throwable {
		DataBag b = BagFactory.getInstance().newDefaultBag();
		DataMap colors = new DataMap();
		colors.put("apple","red");
		colors.put("orange","orange");
		
		DataMap weights = new DataMap();
		weights.put("apple","0.1");
		weights.put("orange","0.3");
		
		Tuple t = new Tuple();
		t.appendField(colors);
		t.appendField(weights);
		b.add(t);
		
		String fileName = "file:"+File.createTempFile("tmp", "");
		PigFile f = new PigFile(fileName);
		f.store(b, new BinStorage(), pigServer.getPigContext());
		
		
		pigServer.registerQuery("a = load '" + Util.encodeEscape(fileName.toString()) + "' using BinStorage();");
		pigServer.registerQuery("b = foreach a generate $0#'apple',flatten($1#'orange');");
		Iterator<Tuple> iter = pigServer.openIterator("b");
		t = iter.next();
		assertEquals(t.getAtomField(0).strval(), "red");
		assertEquals(t.getAtomField(1).numval(), 0.3);
		assertFalse(iter.hasNext());
	}
	
	
	static public class TitleNGrams extends EvalFunc<DataBag> {
		
		@Override
		public void exec(Tuple input, DataBag output) throws IOException {	
		    String str = input.getAtomField(0).strval();
			
			String title = str;

			if (title != null) {
				List<String> nGrams = makeNGrams(title);
				
				for (Iterator<String> it = nGrams.iterator(); it.hasNext(); ) {
					Tuple t = new Tuple(1);
					t.setField(0, it.next());
					output.add(t);
				}
			}
	    }
		
		
		List<String> makeNGrams(String str) {
			List<String> tokens = new ArrayList<String>();
			
			StringTokenizer st = new StringTokenizer(str);
			while (st.hasMoreTokens())
				tokens.add(st.nextToken());
			
			return nGramHelper(tokens, new ArrayList<String>());
		}
		
		ArrayList<String> nGramHelper(List<String> str, ArrayList<String> nGrams) {
			if (str.size() == 0)
				return nGrams;
			
			for (int i = 0; i < str.size(); i++)
				nGrams.add(makeString(str.subList(0, i+1)));
			
			return nGramHelper(str.subList(1, str.size()), nGrams);
		}
		
		String makeString(List<String> list) {
			StringBuilder sb = new StringBuilder();
			for (Iterator<String> it = list.iterator(); it.hasNext(); ) {
				sb.append(it.next());
				if (it.hasNext())
					sb.append(" ");
			}
			return sb.toString();
		}
	}

	@Test
	public void testBagFunctionWithFlattening() throws Throwable {
		File queryLogFile = createFile(
					new String[]{ 
						"stanford\tdeer\tsighting",
						"bush\tpresident",
						"stanford\tbush",
						"conference\tyahoo",
						"world\tcup\tcricket",
						"bush\twins",
						"stanford\tpresident",
					}
				);
				
		File newsFile = createFile(
					new String[]{
						"deer seen at stanford",
						"george bush visits stanford", 
						"yahoo hosting a conference in the bay area", 
						"who will win the world cup"
					}
				);	
		
		Map<String, Integer> expectedResults = new HashMap<String, Integer>();
		expectedResults.put("bush", 2);
		expectedResults.put("stanford", 3);
		expectedResults.put("world", 1);
		expectedResults.put("conference", 1);
		
		pigServer.registerQuery("newsArticles = LOAD 'file:" + Util.encodeEscape(newsFile.toString()) + "' USING " + TextLoader.class.getName() + "();");
		pigServer.registerQuery("queryLog = LOAD 'file:" + Util.encodeEscape(queryLogFile.toString()) + "';");

	    pigServer.registerQuery("titleNGrams = FOREACH newsArticles GENERATE flatten(" + TitleNGrams.class.getName() + "(*));");
	    pigServer.registerQuery("cogrouped = COGROUP titleNGrams BY $0 INNER, queryLog BY $0 INNER;");
	    pigServer.registerQuery("answer = FOREACH cogrouped GENERATE COUNT(queryLog),group;");
		
	    Iterator<Tuple> iter = pigServer.openIterator("answer");
	    while(iter.hasNext()){
	    	Tuple t = iter.next();
	    	assertEquals(expectedResults.get(t.getAtomField(1).strval()).doubleValue(),t.getAtomField(0).numval().doubleValue());
	    }
	}
	
	@Test
	public void testSort() throws Throwable {
		testSortDistinct(false);
	}
	
	@Test
	public void testDistinct() throws Throwable {
		testSortDistinct(true);
	}

	private void testSortDistinct(boolean eliminateDuplicates) throws Throwable {
		int LOOP_SIZE = 1024*16;
		File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        Random r = new Random();
        for(int i = 0; i < LOOP_SIZE; i++) {
            ps.println(r.nextInt(LOOP_SIZE/2) + "\t" + i);
        }
        ps.close(); 
		
        String tmpOutputFile = FileLocalizer.getTemporaryPath(null, pigServer.getPigContext()).toString();
		pigServer.registerQuery("A = LOAD 'file:" + Util.encodeEscape(tmpFile.toString()) + "';");
		if (eliminateDuplicates){
			pigServer.registerQuery("B = DISTINCT (FOREACH A GENERATE $0) PARALLEL 10;");
		}else{
			pigServer.registerQuery("B = ORDER A BY $0 PARALLEL 10;");
		}
		pigServer.store("B", tmpOutputFile);
		
		pigServer.registerQuery("A = load '" + Util.encodeEscape(tmpOutputFile.toString()) + "';");
		Iterator<Tuple> iter = pigServer.openIterator("A");
		int last = -1;
		while (iter.hasNext()){
			Tuple t = iter.next();
			if (eliminateDuplicates){
				assertTrue(last < t.getAtomField(0).numval().intValue());
			}else{
				assertTrue(last <= t.getAtomField(0).numval().intValue());
				assertEquals(t.arity(), 2);
			}
		}
		
	}
	

}
