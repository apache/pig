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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Test;

import org.apache.pig.FilterFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigServer;
import org.apache.pig.EvalFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.builtin.*;
import org.apache.pig.data.*;
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

        EvalFunc<Double> avg = new AVG();
        Tuple tup =
            Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);
        Double output = avg.exec(tup);
        
        assertTrue(output == expected);
    }

    @Test
    public void testAVGInitial() throws Exception {
        int input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        EvalFunc<Tuple> avg = new AVG.Initial();
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);
        Tuple output = avg.exec(tup);

        Double f1 = DataType.toDouble(output.get(0));
        assertEquals("Expected sum to be 55.0", 55.0, f1);
        Long f2 = DataType.toLong(output.get(1));
        assertEquals("Expected count to be 10", 10, f2.longValue());
    }

    @Test
    public void testAVGFinal() throws Exception {
        Tuple t1 = TupleFactory.getInstance().newTuple(2);
        t1.set(0, 55.0);
        t1.set(1, 10L);
        Tuple t2 = TupleFactory.getInstance().newTuple(2);
        t2.set(0, 28.0);
        t2.set(1, 7L);
        Tuple t3 = TupleFactory.getInstance().newTuple(2);
        t3.set(0, 82.0);
        t3.set(1, 17L);
        DataBag bag = BagFactory.getInstance().newDefaultBag();
        bag.add(t1);
        bag.add(t2);
        bag.add(t3);
        
        Tuple tup = TupleFactory.getInstance().newTuple(bag);

        EvalFunc<Double> avg = new AVG.Final();
        Double output = avg.exec(tup);

        assertEquals("Expected average to be 4.852941176470588",
            4.852941176470588, output);
    }


    @Test
    public void testCOUNT() throws Exception {
        int input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        long expected = input.length;

        EvalFunc<Long> count = new COUNT();
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);
        Long output = count.exec(tup);

        assertTrue(output == expected);
    }

    @Test
    public void testCOUNTMap() throws Exception {
        Map<Object, Object> map = new HashMap<Object, Object>();
        
        Tuple tup = TupleFactory.getInstance().newTuple();
        tup.append(map);
        
        EvalFunc<Long> count = new COUNT();
        FilterFunc isEmpty = new IsEmpty();
        
        assertTrue(isEmpty.exec(tup));
        Long output = count.exec(tup);
        assertTrue(output == 0);
        
        map.put("a", "a");

        assertFalse(isEmpty.exec(tup));
        output = count.exec(tup);
        assertTrue(output == 1);

        
        map.put("b", TupleFactory.getInstance().newTuple());

        assertFalse(isEmpty.exec(tup));
        output = count.exec(tup);
        assertTrue(output == 2);
        
    }

    @Test
    public void testCOUNTInitial() throws Exception {
        int input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        EvalFunc<Tuple> count = new COUNT.Initial();
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);
        Tuple output = count.exec(tup);

        Long f1 = DataType.toLong(output.get(0));
        assertEquals("Expected count to be 10", 10, f1.longValue());
    }

    @Test
    public void testCOUNTFinal() throws Exception {
        long input[] = { 23, 38, 39 };
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);

        EvalFunc<Long> count = new COUNT.Final();
        Long output = count.exec(tup);

        assertEquals("Expected count to be 100", 100, output.longValue());
    }

    @Test
    public void testSUM() throws Exception {
        int input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        double expected = 55;

        EvalFunc<Double> sum = new SUM();
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);
        Double output = sum.exec(tup);

        assertTrue(output == expected);
    }

    @Test
    public void testSUMInitial() throws Exception {
        int input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        EvalFunc<Tuple> sum = new SUM.Initial();
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);
        Tuple output = sum.exec(tup);

        assertEquals("Expected sum to be 55.0", 55.0, DataType.toDouble(output.get(0)));
    }

    @Test
    public void testSUMFinal() throws Exception {
        int input[] = { 23, 38, 39 };
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);

        EvalFunc<Double> sum = new SUM.Final();
        Double output = sum.exec(tup);

        assertEquals("Expected sum to be 100.0", 100.0, output);
    }

    @Test
    public void testMIN() throws Exception {
        int input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        EvalFunc<Double> min = new MIN();
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);
        Double output = min.exec(tup);

        assertEquals("Expected min to be 1.0", 1.0, output);
    }


    @Test
    public void testMINInitial() throws Exception {
        int input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        EvalFunc<Tuple> min = new MIN.Initial();
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);
        Tuple output = min.exec(tup);

        assertEquals("Expected min to be 1.0", 1.0, DataType.toDouble(output.get(0)));
    }

    @Test
    public void testMINFinal() throws Exception {
        int input[] = { 23, 38, 39 };
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);

        EvalFunc<Double> min = new MIN.Final();
        Double output = min.exec(tup);

        assertEquals("Expected sum to be 23.0", 23.0, output);
    }

    @Test
    public void testMAX() throws Exception {
        int input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        EvalFunc<Double> max = new MAX();
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);
        Double output = max.exec(tup);

        assertEquals("Expected max to be 10.0", 10.0, output);
    }


    @Test
    public void testMAXInitial() throws Exception {
        int input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        EvalFunc<Tuple> max = new MAX.Initial();
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);
        Tuple output = max.exec(tup);

        assertEquals("Expected max to be 10.0", 10.0, DataType.toDouble(output.get(0)));
    }

    @Test
    public void testMAXFinal() throws Exception {
        int input[] = { 23, 38, 39 };
        Tuple tup = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1), input);

        EvalFunc<Double> max = new MAX.Final();
        Double output = max.exec(tup);

        assertEquals("Expected sum to be 39.0", 39.0, output);
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
        assertTrue(f1.size() == arity1);

        String input2 = ":this:has:a:leading:colon\n";
        int arity2 = 6;

        LoadFunc p2 = new PigStorage(":");
        FakeFSInputStream ffis2 = new FakeFSInputStream(input2.getBytes());
        p2.bindTo(null, new BufferedPositionedInputStream(ffis2), 0, input2.getBytes().length);
        Tuple f2 = p2.getNext();
        assertTrue(f2.size() == arity2);

        String input3 = "this:has:a:trailing:colon:\n";
        int arity3 = 6;

        LoadFunc p3 = new PigStorage(":");
        FakeFSInputStream ffis3 = new FakeFSInputStream(input3.getBytes());
        p3.bindTo(null, new BufferedPositionedInputStream(ffis3), 0, input1.getBytes().length);
        Tuple f3 = p3.getNext();
        assertTrue(f3.size() == arity3);
    }

    /*
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
    */

    
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
        assertTrue(expected1.equals(f1.get(0).toString()) &&
            expected2.equals(f2.get(0).toString()));

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

        DataByteArray[] input = { new DataByteArray("amy"),
            new DataByteArray("bob"), new DataByteArray("charlene"),
            new DataByteArray("david"), new DataByteArray("erin"),
            new DataByteArray("frank") };
        Tuple f1 = Util.loadTuple(TupleFactory.getInstance().newTuple(input.length), input);

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
    	assertEquals("{(f00)}", t.get(0).toString());
    	assertTrue(iter.hasNext());
    	t = iter.next();
    	assertEquals("{(b00)}", t.get(0).toString());
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
            DataBag b = DataType.toBag(t.get(0));
            Tuple t1 = b.iterator().next();
    		assertEquals(i+"AA", t1.get(0).toString());
            b = DataType.toBag(t.get(1));
            t1 = b.iterator().next();
    		assertEquals(i+"BB", t1.get(0).toString());
    	}
    	
    	assertFalse(iter.hasNext());
    	tempFile.delete();
    }
 
    
    

}
