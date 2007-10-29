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
import java.io.PrintWriter;
import java.util.Iterator;

import junit.framework.TestCase;

import org.junit.Test;

import org.apache.pig.FilterFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigServer;
import org.apache.pig.EvalFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.builtin.*;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataMap;
import org.apache.pig.data.Tuple;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.impl.builtin.ShellBagEvalFunc;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.PigContext;

public class TestBuiltin extends TestCase {
	
	private String initString = "local";
    
    // Builtin MATH Functions
    // =======================
    @Test
    public void testAVG() throws Exception {
        int input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        double expected = 5.5;

        EvalFunc<DataAtom> avg = new AVG();
        Tuple tup = Util.loadNestTuple(new Tuple(1), input);
        DataAtom output = new DataAtom();
        avg.exec(tup, output);
        
        double actual = (new Double(output.strval())).doubleValue();
        assertTrue(actual == expected);
    }

    @Test
    public void testCOUNT() throws Exception {
        int input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        double expected = input.length;

        EvalFunc<DataAtom> count = new COUNT();
        Tuple tup = Util.loadNestTuple(new Tuple(1), input);
        DataAtom output = new DataAtom();
        count.exec(tup, output);

        double actual = (new Double(output.strval())).doubleValue();
        assertTrue(actual == expected);
    }

    @Test
    public void testCOUNTMap() throws Exception {
        DataMap map = new DataMap();
        
        Tuple tup = new Tuple();
        tup.appendField(map);
        DataAtom output = new DataAtom();
        
        
        EvalFunc<DataAtom> count = new COUNT();
        FilterFunc isEmpty = new IsEmpty();
        
        assertTrue(isEmpty.exec(tup));
        count.exec(tup,output);
        assertTrue(output.numval() == 0);
        
        map.put("a", new DataAtom("a"));

        assertFalse(isEmpty.exec(tup));
        count.exec(tup,output);
        assertTrue(output.numval() == 1);

        
        map.put("b", new Tuple());

        assertFalse(isEmpty.exec(tup));
        count.exec(tup,output);
        assertTrue(output.numval() == 2);
        
    }
    
    @Test
    public void testSUM() throws Exception {
        int input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        double expected = 55;

        EvalFunc<DataAtom> sum = new SUM();
        Tuple tup = Util.loadNestTuple(new Tuple(1), input);
        DataAtom output = new DataAtom();
        sum.exec(tup, output);

        double actual = (new Double(output.strval())).doubleValue();

        assertTrue(actual == expected);
    }

    // Builtin APPLY Functions
    // ========================

    


    // Builtin LOAD Functions
    // =======================
    @Test
    public void testLFPig() throws Exception {
        String input1 = "this:is:delimited:by:a:colon\n";
        int arity1 = 6;

        LoadFunc p1 = new PigStorage(":");
        FakeFSInputStream ffis1 = new FakeFSInputStream(input1.getBytes());
        p1.bindTo(null, new BufferedPositionedInputStream(ffis1), 0, input1.getBytes().length);
        Tuple f1 = p1.getNext();
        assertTrue(f1.arity() == arity1);

        String input2 = ":this:has:a:leading:colon\n";
        int arity2 = 6;

        LoadFunc p2 = new PigStorage(":");
        FakeFSInputStream ffis2 = new FakeFSInputStream(input2.getBytes());
        p2.bindTo(null, new BufferedPositionedInputStream(ffis2), 0, input2.getBytes().length);
        Tuple f2 = p2.getNext();
        assertTrue(f2.arity() == arity2);

        String input3 = "this:has:a:trailing:colon:\n";
        int arity3 = 6;

        LoadFunc p3 = new PigStorage(":");
        FakeFSInputStream ffis3 = new FakeFSInputStream(input3.getBytes());
        p3.bindTo(null, new BufferedPositionedInputStream(ffis3), 0, input1.getBytes().length);
        Tuple f3 = p3.getNext();
        assertTrue(f3.arity() == arity3);
    }

    @Test
    public void testLFBin() throws Exception {

        BagFactory.init(new File("/tmp"));
        
        
        Tuple t1 = new Tuple(4);
        DataAtom a = new DataAtom("a");
        DataAtom b = new DataAtom("b");
        Tuple t2 = new Tuple(1);
        t2.setField(0,a);
        Tuple t3 = new Tuple(1);
        t3.setField(0, b);
        DataBag bag = BagFactory.getInstance().getNewBigBag();
        bag.add(t2);
        bag.add(t3);
        Tuple t4 = new Tuple(2);
        t4.setField(0, t2);
        t4.setField(1, t3);
        
        t1.setField(0, a);
        t1.setField(1, t2);
        t1.setField(2, bag);
        t1.setField(3, t4);
        
        Tuple t5 = new Tuple(4);
        DataAtom c = new DataAtom("the quick brown fox");
        DataAtom d = new DataAtom("jumps over the lazy dog");
        Tuple t6 = new Tuple(1);
        t6.setField(0,c);
        Tuple t7 = new Tuple(1);
        t7.setField(0, d);
        DataBag bag2 = BagFactory.getInstance().getNewBigBag();    
        for(int i = 0; i < 10; i ++) {
            bag2.add(t6);
            bag2.add(t7);
        }
        Tuple t8 = new Tuple(2);
        t8.setField(0, t6);
        t8.setField(1, t7);
        
        t5.setField(0, c);
        t5.setField(1, t6);
        t5.setField(2, bag2);
        t5.setField(3, t8);
        
        
        OutputStream os = new FileOutputStream("/tmp/bintest.bin");
        StoreFunc s = new BinStorage();
        s.bindTo(os);
        s.putNext(t1);
        s.putNext(t5);
        s.finish();
        
        LoadFunc l = new BinStorage();
        InputStream is = FileLocalizer.open("/tmp/bintest.bin", new PigContext(ExecType.LOCAL));
        l.bindTo("/tmp/bintest.bin", new BufferedPositionedInputStream(is), 0, Long.MAX_VALUE);
        Tuple r1 = l.getNext();
        Tuple r2 = l.getNext();
        
        assertTrue(r1.equals(t1));
        assertTrue(r2.equals(t5));
    }

    
    @Test
    public void testLFText() throws Exception {
        String input1 = "This is some text.\nWith a newline in it.\n";
        String expected1 = "This is some text.";
        String expected2 = "With a newline in it.";
        FakeFSInputStream ffis1 = new FakeFSInputStream(input1.getBytes());
        LoadFunc text1 = new TextLoader();
        text1.bindTo(null, new BufferedPositionedInputStream(ffis1), 0, input1.getBytes().length);
        Tuple f1 = text1.getNext();
        Tuple f2 = text1.getNext();
        assertTrue(expected1.equals(f1.getAtomField(0).strval()) && expected2.equals(f2.getAtomField(0).strval()));

        String input2 = "";
        FakeFSInputStream ffis2 = new FakeFSInputStream(input2.getBytes());
        LoadFunc text2 = new TextLoader();
        text2.bindTo(null, new BufferedPositionedInputStream(ffis2), 0, input2.getBytes().length);
        Tuple f3 = text2.getNext();
        assertTrue(f3 == null);
    }

    @Test
    public void testSFPig() throws Exception {
        byte[] buf = new byte[1024];
        FakeFSOutputStream os = new FakeFSOutputStream(buf);
        StoreFunc sfunc = new PigStorage("\t");
        sfunc.bindTo(os);

        int[] input = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        Tuple f1 = Util.loadFlatTuple(new Tuple(input.length), input);

        sfunc.putNext(f1);
        sfunc.finish();
        
        FakeFSInputStream is = new FakeFSInputStream(buf);
        LoadFunc lfunc = new PigStorage();
        lfunc.bindTo(null, new BufferedPositionedInputStream(is), 0, buf.length);
        Tuple f2 = lfunc.getNext();
        
        assertTrue(f1.equals(f2));        
    }
    
    @Test
    public void testShellFuncSingle() throws Exception {
    	//ShellBagEvalFunc func = new ShellBagEvalFunc("tr o 0");
    	PigServer pig = new PigServer(initString);
    	
    	File tempFile = File.createTempFile("tmp", ".dat");
    	PrintWriter writer = new PrintWriter(tempFile);
    	writer.println("foo");
    	writer.println("boo");
    	writer.close();
    	
    	pig.registerFunction("myTr",ShellBagEvalFunc.class.getName() + "('tr o 0')");
    	pig.registerQuery("a = load 'file:" + tempFile + "';");
    	pig.registerQuery("b = foreach a generate myTr(*);");
    	Iterator<Tuple> iter = pig.openIterator("b");
    	    	
    	Tuple t;
    	
    	assertTrue(iter.hasNext());
    	t = iter.next();
    	assertEquals("f00", t.getAtomField(0).strval());
    	assertTrue(iter.hasNext());
    	t = iter.next();
    	assertEquals("b00", t.getAtomField(0).strval());
    	assertFalse(iter.hasNext());
    	tempFile.delete();
    }
    
    @Test
    public void testShellFuncMultiple() throws Exception {

    	PigServer pig = new PigServer(initString);
    	final int numTimes = 100;
    	
    	File tempFile = File.createTempFile("tmp", ".dat");
    	PrintWriter writer = new PrintWriter(tempFile);
    	for (int i=0; i< numTimes; i++){
    		writer.println(i+"oo");
    	}
    	writer.close();
    	
    	pig.registerFunction("tr1",ShellBagEvalFunc.class.getName() + "('tr o A')");
    	pig.registerFunction("tr2",ShellBagEvalFunc.class.getName() + "('tr o B')");
    	pig.registerQuery("a = load 'file:" + tempFile + "';");
    	pig.registerQuery("b = foreach a generate tr1(*),tr2(*);");
    	Iterator<Tuple> iter = pig.openIterator("b");
    	
    	for (int i=0; i< numTimes; i++){
    		Tuple t = iter.next();
    		
    		assertEquals(i+"AA", t.getBagField(0).content().next().getAtomField(0).strval());
    		assertEquals(i+"BB", t.getBagField(1).content().next().getAtomField(0).strval());
    		
    	}
    	
    	assertFalse(iter.hasNext());
    	tempFile.delete();
    }
 
    
    

}
