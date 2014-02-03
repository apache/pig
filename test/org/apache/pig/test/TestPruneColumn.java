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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.pig.FilterFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigImplConstants;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.newplan.logical.rules.ColumnPruneVisitor;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPruneColumn {
    private PigServer pigServer;
    File tmpFile1;
    File tmpFile2;
    File tmpFile3;
    File tmpFile4;
    File tmpFile5;
    File tmpFile6;
    File tmpFile7;
    File tmpFile8;
    File tmpFile9;
    File tmpFile10;
    File tmpFile11;
    File tmpFile12;
    File tmpFile13;
    File logFile;

    private static final String simpleEchoStreamingCommand;
    static {
        String quote = "'";
        if (Util.WINDOWS) {
            quote = "\"";
        }

        simpleEchoStreamingCommand = "perl -ne " + quote + "print $_" + quote;
    }

    static public class MyFilterFunc extends FilterFunc {
        @Override
        public Boolean exec(Tuple input) {
            return true;
        }
    }

    @Before
    public void setUp() throws Exception{
        Logger logger = Logger.getLogger(ColumnPruneVisitor.class);
        logger.removeAllAppenders();
        logger.setLevel(Level.INFO);
        SimpleLayout layout = new SimpleLayout();
        logFile = File.createTempFile("log", "");
        FileAppender appender = new FileAppender(layout, logFile.toString(), false, false, 0);
        logger.addAppender(appender);

        pigServer = new PigServer("local");
        tmpFile1 = File.createTempFile("prune", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile1));
        ps.println("1\t2\t3");
        ps.println("2\t5\t2");
        ps.close();

        tmpFile2 = File.createTempFile("prune", "txt");
        ps = new PrintStream(new FileOutputStream(tmpFile2));
        ps.println("1\t1");
        ps.println("2\t2");
        ps.close();

        tmpFile3 = File.createTempFile("prune", "txt");
        ps = new PrintStream(new FileOutputStream(tmpFile3));
        ps.println("1\t[key1#1,key2#2]");
        ps.println("2\t[key1#2,key2#4]");
        ps.close();

        tmpFile4 = File.createTempFile("prune", "txt");
        ps = new PrintStream(new FileOutputStream(tmpFile4));
        ps.println("1\t2\t3");
        ps.println("1\t2\t3");
        ps.close();

        tmpFile5 = File.createTempFile("prune", "txt");
        ps = new PrintStream(new FileOutputStream(tmpFile5));
        ps.println("1\t2\t3\t4");
        ps.println("2\t3\t4\t5");
        ps.close();

        tmpFile6 = File.createTempFile("prune", "txt");
        ps = new PrintStream(new FileOutputStream(tmpFile6));
        ps.println("\t2\t3");
        ps.println("2\t3\t4");
        ps.close();

        tmpFile7 = File.createTempFile("prune", "txt");
        ps = new PrintStream(new FileOutputStream(tmpFile7));
        ps.println("1\t1\t1");
        ps.println("2\t2\t2");
        ps.close();

        tmpFile8 = File.createTempFile("prune", "txt");
        ps = new PrintStream(new FileOutputStream(tmpFile8));
        ps.println("1\t2\t3\t4");
        ps.println("2\t5\t2\t3");
        ps.close();

        tmpFile9 = File.createTempFile("prune", "txt");
        ps = new PrintStream(new FileOutputStream(tmpFile9));
        ps.println("1\t[key1#1,key2#2]\t[key3#8,key4#9]");
        ps.println("2\t[key1#2,key2#4]\t[key3#8,key4#9]");
        ps.close();

        tmpFile10 = File.createTempFile("prune", "txt");
        ps = new PrintStream(new FileOutputStream(tmpFile10));
        ps.println("1\t[1#1,2#1]\t2");
        ps.close();

        tmpFile11 = File.createTempFile("prune", "txt");
        ps = new PrintStream(new FileOutputStream(tmpFile11));
        ps.println("1\t2\t3");
        ps.println("1\t3\t2");
        ps.println("2\t5\t2");
        ps.close();

        tmpFile12 = File.createTempFile("prune", "txt");
        ps = new PrintStream(new FileOutputStream(tmpFile12));
        ps.println("[key1#1,key2#2,cond#1]");
        ps.println("[key1#2,key2#3,cond#1]");
        ps.close();
        
        tmpFile13 = File.createTempFile("prune", "txt");
        ps = new PrintStream(new FileOutputStream(tmpFile13));
        ps.println("3\ti");
        ps.println("3\ti");
        ps.println("1\ti");
        ps.println("2\ti");
        ps.println("2\ti");
        ps.println("3\ti");
        ps.close();
    }

    @After
    public void tearDown() throws Exception{
        tmpFile1.delete();
        tmpFile2.delete();
        tmpFile3.delete();
        tmpFile4.delete();
        tmpFile5.delete();
        tmpFile6.delete();
        tmpFile7.delete();
        tmpFile8.delete();
        tmpFile9.delete();
        tmpFile10.delete();
        tmpFile11.delete();
        tmpFile12.delete();
        logFile.delete();
    }

    public boolean checkLogFileMessage(String[] messages) {
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(logFile));
            List<String> logMessages=new ArrayList<String>();
            String line;
            while ((line=reader.readLine())!=null)
            {
                logMessages.add(line);
            }
            if (logMessages.size() > 0) {
                logMessages = TestHelper.sortSubFields(logMessages);
            }

            // Check if all messages appear in the log
            for (int i=0;i<messages.length;i++)
            {
                boolean found = false;
                for (int j=0;j<logMessages.size();j++)
                if (logMessages.get(j).contains(messages[i])) {
                    found = true;
                    break;
                }
                if (!found)
                    return false;
            }

            // Check no other log besides messages
            for (int i=0;i<logMessages.size();i++) {
                boolean found = false;
                for (int j=0;j<messages.length;j++) {
                    if (logMessages.get(i).contains(messages[j])) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    if (logMessages.get(i).contains("Columns pruned for")||
                            logMessages.get(i).contains("Map key required for")) {
                        return false;
                    }
                }
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean emptyLogFileMessage()
    {
        if (!logFile.exists())
            return true;
        BufferedReader reader = null;
        String line;
        try {
            reader = new BufferedReader(new FileReader(logFile));
            while ((line=reader.readLine())!=null)
            {
                if (line!=null && !line.equals(""))
                    return false;
            }
            return true;
        }
        catch (IOException e) {
            return false;
        }
    }

    @Test
    public void testLoadForEach1() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = foreach A generate a1, a2;");
        Iterator<Tuple> iter = pigServer.openIterator("B");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(2, t.size());
        assertEquals(2, t.get(0));
        assertEquals(3, t.get(1));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertEquals(5, t.get(0));
        assertEquals(2, t.get(1));

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0"}));
    }

    @Test
    public void testLoadForEach2() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = foreach A generate a0, a2;");
        Iterator<Tuple> iter = pigServer.openIterator("B");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(2, t.size());
        assertEquals(1, t.get(0));
        assertEquals(3, t.get(1));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertEquals(2, t.get(0));
        assertEquals(2, t.get(1));

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1"}));
    }

    @Test
    public void testLoadForEach3() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = foreach A generate a0, a1;");
        Iterator<Tuple> iter = pigServer.openIterator("B");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(2, t.size());
        assertEquals(1, t.get(0));
        assertEquals(2, t.get(1));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertEquals(2, t.get(0));
        assertEquals(5, t.get(1));

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $2"}));
    }

    @Test
    public void testJoin1() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile2.toString()), pigServer.getPigContext()) + "' as (b0:int, b1:int);");
        pigServer.registerQuery("C = join A by a1, B by b1;");
        pigServer.registerQuery("D = foreach C generate a1, a2, b0, b1;");

        Iterator<Tuple> iter = pigServer.openIterator("D");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(4, t.size());
        assertEquals(2, t.get(0));
        assertEquals(3, t.get(1));
        assertEquals(2, t.get(2));
        assertEquals(2, t.get(3));

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0"}));
    }

    @Test
    public void testJoin2() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile2.toString()), pigServer.getPigContext()) + "' as (b0:int, b1:int);");
        pigServer.registerQuery("C = join A by a1, B by b1;");
        pigServer.registerQuery("D = foreach C generate a1, a2, b1;");

        Iterator<Tuple> iter = pigServer.openIterator("D");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(3, t.size());
        assertEquals(2, t.get(0));
        assertEquals(3, t.get(1));
        assertEquals(2, t.get(2));

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0",
                "Columns pruned for B: $0"}));
    }

    @Test
    public void testForEachFilter() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = filter A by a2==3;");
        pigServer.registerQuery("C = foreach B generate a0, a1;");

        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(2, t.size());
        assertEquals(1, t.get(0));
        assertEquals(2, t.get(1));

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testForEach1() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = foreach A generate a0, a1+a2;");

        Iterator<Tuple> iter = pigServer.openIterator("B");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(2, t.size());
        assertEquals(1, t.get(0));
        assertEquals(5, t.get(1));

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(2, t.size());
        assertEquals(2, t.get(0));
        assertEquals(7, t.get(1));

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testForEach2() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = foreach A generate a0 as b0, *;");

        Iterator<Tuple> iter = pigServer.openIterator("B");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(4, t.size());
        assertEquals(1, t.get(0));
        assertEquals(1, t.get(1));
        assertEquals(2, t.get(2));
        assertEquals(3, t.get(3));

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(4, t.size());
        assertEquals(2, t.get(0));
        assertEquals(2, t.get(1));
        assertEquals(5, t.get(2));
        assertEquals(2, t.get(3));

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testSplit1() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' as (a0, a1, a2);");
        pigServer.registerQuery("split A into B if $0<=1, C if $0>1;");
        pigServer.registerQuery("D = foreach B generate $1;");

        Iterator<Tuple> iter = pigServer.openIterator("D");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(1, t.size());
        assertEquals("2", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $2"}));
    }

    @Test
    public void testSplit2() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' as (a0, a1, a2);");
        pigServer.registerQuery("split A into B if $0<=1, C if $0>1;");
        pigServer.registerQuery("D = foreach B generate $1;");

        Iterator<Tuple> iter = pigServer.openIterator("D");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(1, t.size());
        assertEquals("2", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $2"}));
    }

    @Test
    public void testForeachNoSchema1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "';");
        pigServer.registerQuery("B = foreach A generate $1, $2;");
        Iterator<Tuple> iter = pigServer.openIterator("B");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(2, t.size());
        assertEquals("2", t.get(0).toString());
        assertEquals("3", t.get(1).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertEquals("5", t.get(0).toString());
        assertEquals("2", t.get(1).toString());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testForeachNoSchema2() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "';");
        pigServer.registerQuery("B = foreach A generate $1, 'aoeuaoeu';");
        Iterator<Tuple> iter = pigServer.openIterator("B");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(2, t.size());
        assertEquals("2", t.get(0).toString());
        assertEquals("aoeuaoeu", t.get(1).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertEquals("5", t.get(0).toString());
        assertEquals("aoeuaoeu", t.get(1).toString());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testCoGroup1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1:int, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile2.toString()), pigServer.getPigContext()) + "' AS (b0, b1:int);");
        pigServer.registerQuery("C = cogroup A by $1, B by $1;");
        pigServer.registerQuery("D = foreach C generate AVG($1.$1);");
        Iterator<Tuple> iter = pigServer.openIterator("D");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(1, t.size());
        assertNull(t.get(0));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertEquals("2.0", t.get(0).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertEquals("5.0", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for B: $0"}));
    }

    @Test
    public void testCoGroup2() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1:int, a2);");
        pigServer.registerQuery("B = group A all;");
        pigServer.registerQuery("C = foreach B generate $1;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        String[] expected = new String[] {
                "({(1,2,3),(2,5,2)})"
        };
        assertTrue(iter.hasNext());
        Util.checkQueryOutputsAfterSortRecursive(iter, expected, org.apache.pig.newplan.logical.Util.translateSchema(pigServer.dumpSchema("C")));

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testCoGroup3() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1:int, a2);");
        pigServer.registerQuery("B = group A by $1;");
        pigServer.registerQuery("C = foreach B generate $1, '1';");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(2, t.size());
        assertEquals("{(1,2,3)}", t.get(0).toString());
        assertEquals("1", t.get(1).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertEquals("{(2,5,2)}", t.get(0).toString());
        assertEquals("1", t.get(1).toString());

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testCoGroup4() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1:int, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile2.toString()), pigServer.getPigContext()) + "' AS (b0, b1:int);");
        pigServer.registerQuery("C = cogroup A by ($1), B by ($1);");
        pigServer.registerQuery("D = foreach C generate $1.$1, $2.$1;");
        Iterator<Tuple> iter = pigServer.openIterator("D");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(2, t.size());
        assertEquals("{}", t.get(0).toString());
        assertEquals("{(1)}", t.get(1).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertEquals("{(2)}", t.get(0).toString());
        assertEquals("{(2)}", t.get(1).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertEquals("{(5)}", t.get(0).toString());
        assertEquals("{}", t.get(1).toString());

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testCoGroup5() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = group A by (a0, a1);");
        pigServer.registerQuery("C = foreach B generate flatten(group);");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(2, t.size());
        assertEquals("1", t.get(0).toString());
        assertEquals("2", t.get(1).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertEquals("2", t.get(0).toString());
        assertEquals("5", t.get(1).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $2"}));
    }

    @Test
    public void testDistinct1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile4.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = distinct A;");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(1, t.size());
        assertEquals("1", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testStream1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = stream A through `" + simpleEchoStreamingCommand + "`;");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(1, t.size());
        assertEquals("1", t.get(0).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertEquals("2", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testBinCond1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile5.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2, a3);");
        pigServer.registerQuery("B = foreach A generate ($1 == '2'? $2 : $3);");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(1, t.size());
        assertEquals("3", t.get(0).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertEquals("5", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0"}));
    }

    @Test
    public void testCoGroup6() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile2.toString()), pigServer.getPigContext()) + "' AS (b0, b1);");
        pigServer.registerQuery("C = cogroup A by ($1), B by ($1);");
        pigServer.registerQuery("D = foreach C generate A, flatten(B.($0, $1));");
        Iterator<Tuple> iter = pigServer.openIterator("D");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(3, t.size());
        assertEquals("{}", t.get(0).toString());
        assertEquals("1", t.get(1).toString());
        assertEquals("1", t.get(2).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(3, t.size());
        assertEquals("{(1,2,3)}", t.get(0).toString());
        assertEquals("2", t.get(1).toString());
        assertEquals("2", t.get(2).toString());

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testCoGroup7() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile2.toString()), pigServer.getPigContext()) + "' AS (b0, b1);");
        pigServer.registerQuery("C = cogroup A by ($1), B by ($1);");
        pigServer.registerQuery("D = foreach C {B = order B by $0;generate FLATTEN(A), B.($1);};");
        Iterator<Tuple> iter = pigServer.openIterator("D");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(4, t.size());
        assertEquals("1", t.get(0).toString());
        assertEquals("2", t.get(1).toString());
        assertEquals("3", t.get(2).toString());
        assertEquals("{(2)}", t.get(3).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(4, t.size());
        assertEquals("2", t.get(0).toString());
        assertEquals("5", t.get(1).toString());
        assertEquals("2", t.get(2).toString());
        assertEquals("{}", t.get(3).toString());

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testCross1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile2.toString()), pigServer.getPigContext()) + "' AS (b0, b1);");
        pigServer.registerQuery("C = cross A, B;");
        pigServer.registerQuery("D = foreach C generate $0, $3;");
        Iterator<Tuple> iter = pigServer.openIterator("D");

        Collection<String> results = new HashSet<String>();
        results.add("(1,1)");
        results.add("(2,1)");
        results.add("(1,2)");
        results.add("(2,2)");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(2, t.size());
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertTrue(results.contains(t.toString()));

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1, $2",
            "Columns pruned for B: $1"}));
    }

    @Test
    public void testUnion1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile4.toString()), pigServer.getPigContext()) + "' AS (b0, b1, b2);");
        pigServer.registerQuery("C = union A, B;");
        pigServer.registerQuery("D = foreach C generate $0, $2;");
        Iterator<Tuple> iter = pigServer.openIterator("D");

        Collection<String> results = new HashSet<String>();
        results.add("(1,3)");
        results.add("(2,2)");
        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(2, t.size());
        results.contains(t.toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        results.contains(t.toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        results.contains(t.toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        results.contains(t.toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1",
            "Columns pruned for B: $1"}));
    }

    @Test
    public void testFRJoin1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile2.toString()), pigServer.getPigContext()) + "' AS (b0, b1);");
        pigServer.registerQuery("C = join A by $0, B by $0 using 'replicated';");
        pigServer.registerQuery("D = foreach C generate $0, $3;");
        Iterator<Tuple> iter = pigServer.openIterator("D");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(2, t.size());
        assertEquals("1", t.get(0).toString());
        assertEquals("1", t.get(1).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertEquals("2", t.get(0).toString());
        assertEquals("2", t.get(1).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1, $2",
            "Columns pruned for B: $1"}));
    }

    @Test
    public void testFilter1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = order A by a1;");
        pigServer.registerQuery("C = limit B 10;");
        pigServer.registerQuery("D = foreach C generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("D");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(1, t.size());
        assertEquals("1", t.get(0).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertEquals("2", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $2"}));
    }

    @Test
    public void testFilter2() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = filter A by a0+a2 == 4;");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(1, t.size());
        assertEquals("1", t.get(0).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertEquals("2", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1"}));
    }

    @Test
    public void testOrderBy1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = order A by $0;");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(1, t.size());
        assertEquals("1", t.get(0).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertEquals("2", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1, $2"}));
    }

    @Test
    public void testOrderBy2() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = order A by *;");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(1, t.size());
        assertEquals("1", t.get(0).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertEquals("2", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testCogroup8() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = group A by *;");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(1, t.size());
        assertEquals("(1,2,3)", t.get(0).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertEquals("(2,5,2)", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testJoin3() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile4.toString()), pigServer.getPigContext()) + "' AS (b0, b1, b2);");
        pigServer.registerQuery("C = join A by *, B by * using 'replicated';");
        pigServer.registerQuery("D = foreach C generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("D");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(1, t.size());
        assertEquals("1", t.get(0).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertEquals("1", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testLoadForEach4() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = foreach A generate *;");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(1, t.size());
        assertEquals("1", t.get(0).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertEquals("2", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1, $2"}));
    }

    @Test
    public void testForEachUDF() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0:chararray, a1:chararray, a2:chararray);");
        pigServer.registerQuery("B = foreach A generate StringSize(*);");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(1, t.size());
        assertEquals("1", t.get(0).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertEquals("1", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testOutJoin1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile6.toString()), pigServer.getPigContext()) + "' AS (a0:chararray, a1:chararray, a2:chararray);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile2.toString()), pigServer.getPigContext()) + "' AS (a0:chararray, a1:chararray, a2:chararray);");
        pigServer.registerQuery("C = join A by $0 left, B by $0;");
        pigServer.registerQuery("D = foreach C generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("D");

        Collection<String> results = new HashSet<String>();
        results.add("(2)");
        results.add("()");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(1, t.size());
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertTrue(results.contains(t.toString()));

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1, $2",
            "Columns pruned for B: $1, $2"}));
    }

    @Test
    public void testFilter3() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = filter A by " + MyFilterFunc.class.getName() + "(*) ;");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(1, t.size());
        assertEquals("1", t.get(0).toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertEquals("2", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testMapKey1() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile3.toString()), pigServer.getPigContext()) + "' as (a0:int, a1:map[]);");
        pigServer.registerQuery("B = foreach A generate a0, a1#'key1';");

        Iterator<Tuple> iter = pigServer.openIterator("B");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(2, t.size());
        assertEquals(1, t.get(0));
        assertEquals("1", t.get(1).toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(2, t.size());
        assertEquals(2, t.get(0));
        assertEquals("2", t.get(1).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Map key required for A: $1->[key1]"}));
    }

    @Test
    public void testMapKey2() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile3.toString()), pigServer.getPigContext()) + "' as (a0:int, a1:map[]);");
        pigServer.registerQuery("B = foreach A generate a1, a1#'key1';");
        pigServer.registerQuery("C = foreach B generate $0#'key2', $1;");

        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(2, t.size());
        assertEquals("2", t.get(0).toString());
        assertEquals("1", t.get(1).toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(2, t.size());
        assertEquals("4", t.get(0).toString());
        assertEquals("2", t.get(1).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0",
                "Map key required for A: $1->[key1, key2]"}));
    }

    @Test
    public void testMapKey3() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile3.toString()), pigServer.getPigContext()) + "' as (a0:int, a1:map[]);");
        pigServer.registerQuery("B = foreach A generate a1, a1#'key1';");
        pigServer.registerQuery("C = group B all;");

        Iterator<Tuple> iter = pigServer.openIterator("C");

        String[] expected = new String[] {
                "(all,{([key2#2,key1#1],1),([key2#4,key1#2],2)})"
        };

        Util.checkQueryOutputsAfterSortRecursive(iter, expected, org.apache.pig.newplan.logical.Util.translateSchema(pigServer.dumpSchema("C")));

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0"}));
    }

    @Test
    public void testMapKey4() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile3.toString()), pigServer.getPigContext()) + "' as (a0:int, a1:map[]);");
        pigServer.registerQuery("B = limit A 10;");
        pigServer.registerQuery("C = foreach B generate $0, $1#'key1';");

        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(2, t.size());
        assertEquals("1", t.get(0).toString());
        assertEquals("1", t.get(1).toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(2, t.size());
        assertEquals("2", t.get(0).toString());
        assertEquals("2", t.get(1).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Map key required for A: $1->[key1]"}));
    }

    @Test
    public void testMapKey5() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile3.toString()), pigServer.getPigContext()) + "' as (a0:int, a1:map[]);");
        pigServer.registerQuery("B = foreach A generate $0, $1#'key1';");
        pigServer.registerQuery("C = stream B through `" + simpleEchoStreamingCommand + "`;");

        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(2, t.size());
        assertEquals("1", t.get(0).toString());
        assertEquals("1", t.get(1).toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(2, t.size());
        assertEquals("2", t.get(0).toString());
        assertEquals("2", t.get(1).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Map key required for A: $1->[key1]"}));
    }

    @Test
    public void testMapKeyInSplit1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile12.toString()), pigServer.getPigContext()) + "' as (m:map[]);");
        pigServer.registerQuery("B = foreach A generate m#'key1' as key1;");
        pigServer.registerQuery("C = foreach A generate m#'key2' as key2;");
        pigServer.registerQuery("D = join B by key1, C by key2;");

        Iterator<Tuple> iter = pigServer.openIterator("D");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(2, t.size());
        assertEquals("2", t.get(0).toString());
        assertEquals("2", t.get(1).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Map key required for A: $0->[key1, key2]"}));
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testMapKeyInSplit2() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile12.toString()), pigServer.getPigContext()) + "' as (m:map[]);");
        pigServer.registerQuery("B = filter A by m#'cond'==1;");
        pigServer.registerQuery("C = filter B by m#'key1'==1;");
        pigServer.registerQuery("D = filter B by m#'key2'==2;");
        pigServer.registerQuery("E = join C by m#'key1', D by m#'key1';");

        Iterator<Tuple> iter = pigServer.openIterator("E");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(2, t.size());
        assertEquals("1", ((Map)t.get(0)).get("key1").toString());
        assertEquals("2", ((Map)t.get(0)).get("key2").toString());
        assertEquals("1", ((Map)t.get(0)).get("cond").toString());
        assertEquals("1", ((Map)t.get(1)).get("key1").toString());
        assertEquals("2", ((Map)t.get(1)).get("key2").toString());
        assertEquals("1", ((Map)t.get(1)).get("cond").toString());

        assertFalse(iter.hasNext());

        assertTrue(this.emptyLogFileMessage());
    }

    @Test
    public void testConstantPlan() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' as (a0, a1, a2);");
        pigServer.registerQuery("B = foreach A generate 1, a2;");

        Iterator<Tuple> iter = pigServer.openIterator("B");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(2, t.size());
        assertEquals("1", t.get(0).toString());
        assertEquals("3", t.get(1).toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(2, t.size());
        assertEquals("1", t.get(0).toString());
        assertEquals("2", t.get(1).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0, $1"}));
    }

    @Test
    public void testPlainPlan() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' as (a0, a1, a2);");
        pigServer.registerQuery("B = order A by $0;");

        Iterator<Tuple> iter = pigServer.openIterator("B");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(3, t.size());
        assertEquals("1", t.get(0).toString());
        assertEquals("2", t.get(1).toString());
        assertEquals("3", t.get(2).toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(3, t.size());
        assertEquals("2", t.get(0).toString());
        assertEquals("5", t.get(1).toString());
        assertEquals("2", t.get(2).toString());

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testBinStorage1() throws Exception {
        // get a temp intermediate filename
        File intermediateFile = File.createTempFile("intemediate", "txt");
        intermediateFile.delete(); // delete since we don't want the file to be present
        String clusterPath = Util.removeColon(intermediateFile.getAbsolutePath());

        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' as (a0, a1, a2);");
        pigServer.store("A", clusterPath, "BinStorage()");

        pigServer.registerQuery("A = load '"+ Util.encodeEscape(clusterPath)
                + "' using BinStorage() as (a0, a1, a2);");

        pigServer.registerQuery("B = foreach A generate a0;");

        Iterator<Tuple> iter = pigServer.openIterator("B");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(1, t.size());
        assertEquals("1", t.get(0).toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(1, t.size());
        assertEquals("2", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1, $2"}));
    }

    @Test
    public void testBinStorage2() throws Exception {
        File intermediateFile = File.createTempFile("intemediate", "txt");
        intermediateFile.delete(); // delete since we don't want the file to be present
        String clusterPath = Util.removeColon(intermediateFile.getAbsolutePath());

        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' as (a0, a1, a2);");
        pigServer.store("A", clusterPath, "BinStorage()");

        pigServer.registerQuery("A = load '"+ Util.encodeEscape(clusterPath)
                + "' using BinStorage() as (a0, a1, a2);");

        pigServer.registerQuery("B = foreach A generate a2, a0, a1;");
        pigServer.registerQuery("C = foreach B generate a0, a2;");

        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(2, t.size());
        assertEquals("1", t.get(0).toString());
        assertEquals("3", t.get(1).toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(2, t.size());
        assertEquals("2", t.get(0).toString());
        assertEquals("2", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1"}));
    }


    @Test
    public void testProjectCastKeyLookup() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile3.toString()), pigServer.getPigContext())
                + "' as (a0, a1);");

        pigServer.registerQuery("B = foreach A generate a1#'key1';");

        Iterator<Tuple> iter = pigServer.openIterator("B");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(1, t.size());
        assertEquals("1", t.get(0).toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(1, t.size());
        assertEquals("2", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0",
                "Map key required for A: $1->[key1]"}));

    }

    @Test
    public void testRelayFlattenMap() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile3.toString()), pigServer.getPigContext())
                + "' as (a0, a1:map[]);");

        pigServer.registerQuery("B = foreach A generate flatten(a1);");
        pigServer.registerQuery("C = foreach B generate a1#'key1';");

        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(1, t.size());
        assertEquals("1", t.get(0).toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(1, t.size());
        assertEquals("2", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0",
                "Map key required for A: $1->[key1]"}));
    }

    @Test
    public void testCrossAtLeastOneColumnOneInput() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile2.toString()), pigServer.getPigContext()) + "' as (b0:int, b1:int);");
        pigServer.registerQuery("C = cross A, B;");
        pigServer.registerQuery("D = foreach C generate $0;");

        Iterator<Tuple> iter = pigServer.openIterator("D");
        Collection<String> results = new HashSet<String>();
        results.add("(1)");
        results.add("(2)");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(1, t.size());
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(1, t.size());
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(1, t.size());
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(1, t.size());
        assertTrue(results.contains(t.toString()));

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1, $2",
                "Columns pruned for B: $1"}));
    }

    @Test
    public void testComplex1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile7.toString()), pigServer.getPigContext()) + "' as (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile8.toString()), pigServer.getPigContext()) + "' as (b0, b1, b2, b3);");
        pigServer.registerQuery("B1 = foreach B generate b2, b0+b3;");
        pigServer.registerQuery("C = join A by $0, B1 by $0;");
        pigServer.registerQuery("D = order C by $4;");
        pigServer.registerQuery("E = foreach D generate $0, $2;");
        pigServer.registerQuery("F = filter E by $1<10;");
        pigServer.registerQuery("G = group F by $0;");
        pigServer.registerQuery("H = foreach G generate $1;");

        Iterator<Tuple> iter = pigServer.openIterator("H");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(1, t.size());
        assertEquals("{(2,2)}", t.get(0).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1",
                "Columns pruned for B: $1"}));
    }
    
    @Test
    public void testComplex2() throws Exception {
        HashSet<String> optimizerRules = new HashSet<String>();
        optimizerRules.add("PushUpFilter");
        pigServer.getPigContext().getProperties().setProperty(
                PigImplConstants.PIG_OPTIMIZER_RULES_KEY,
                ObjectSerializer.serialize(optimizerRules));
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile13.toString()), pigServer.getPigContext()) + "' as (a:int, b:chararray);");
        pigServer.registerQuery("B = FOREACH A generate a;");
        pigServer.registerQuery("C = GROUP B by a;");
        pigServer.registerQuery("D = filter C by group > 0 and group < 100;");
        pigServer.registerQuery("E = FOREACH D {F = LIMIT B 1 ;GENERATE B.a as mya, FLATTEN(F.a) as setting;}");
        pigServer.registerQuery("G = FOREACH E GENERATE mya, setting as setting;");

        Iterator<Tuple> iter = pigServer.openIterator("G");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals("({(1)},1)", t.toString());
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals("({(2),(2)},2)", t.toString());
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals("({(3),(3),(3)},3)", t.toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1"}));
        
        pigServer.getPigContext().getProperties().remove(PigImplConstants.PIG_OPTIMIZER_RULES_KEY);
    }

    @Test
    public void testCoGroup8() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1:int, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile2.toString()), pigServer.getPigContext()) + "' AS (b0, b1:int);");
        pigServer.registerQuery("C = cogroup A by ($1), B by ($1);");
        pigServer.registerQuery("D = foreach C generate $0, $1;");

        Iterator<Tuple> iter = pigServer.openIterator("D");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals(2, t.size());
        assertEquals("1", t.get(0).toString());
        assertEquals("{}", t.get(1).toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(2, t.size());
        assertEquals("2", t.get(0).toString());
        assertEquals("{(1,2,3)}", t.get(1).toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals(2, t.size());
        assertEquals("5", t.get(0).toString());
        assertEquals("{(2,5,2)}", t.get(1).toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for B: $0"}));
    }

    // See PIG-1128
    @Test
    public void testUserDefinedSchema() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS ( c1 : chararray, c2 : int);");
        pigServer.registerQuery("B = foreach A generate c1 as c1 : chararray, c2 as c2 : int, 'CA' as state : chararray;");
        pigServer.registerQuery("C = foreach B generate c1 as c1 : chararray;");

        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals("(1)", t.toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals("(2)", t.toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1"}));
    }

    // See PIG-1127
    @Test
    public void testSharedSchemaObject() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile10.toString()), pigServer.getPigContext()) + "' AS (a0, a1:map[], a2);");
        pigServer.registerQuery("B = foreach A generate a1;");
        pigServer.registerQuery("C = limit B 10;");

        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals("([1#1, 2#1])", TestHelper.sortString("\\[(.*)\\]", t.toString(), ","));

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0, $2"}));
    }

    // See PIG-1142
    @Test
    public void testJoin4() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (b0, b1, b2);");
        pigServer.registerQuery("C = join A by a2, B by b2;");
        pigServer.registerQuery("D = foreach C generate $0,  $1,  $2;");

        Iterator<Tuple> iter = pigServer.openIterator("D");
        Collection<String> results = new HashSet<String>();
        results.add("(1,2,3)");
        results.add("(2,5,2)");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(results.contains(t.toString()));

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for B: $0, $1"}));
    }

    @Test
    public void testFilter4() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2:int);");
        pigServer.registerQuery("B = filter A by a2==3;");
        pigServer.registerQuery("C = foreach B generate $2;");

        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals("(3)", t.toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0, $1"}));
    }

    @Test
    public void testSplit3() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2:int);");
        pigServer.registerQuery("split A into B if a2==3, C if a2<3;");
        pigServer.registerQuery("C = foreach B generate $2;");

        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals("(3)", t.toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0, $1"}));
    }

    @Test
    public void testOrderBy3() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = order A by a2;");
        pigServer.registerQuery("C = foreach B generate a2;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(1, t.size());
        assertEquals("(2)", t.toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertEquals("(3)", t.toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0, $1"}));
    }

    @Test
    public void testCogroup9() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (b0, b1, b2);");
        pigServer.registerQuery("C = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (c0, c1, c2);");
        pigServer.registerQuery("D = cogroup A by a2, B by b2, C by c2;");
        pigServer.registerQuery("E = foreach D generate $1, $2;");
        Iterator<Tuple> iter = pigServer.openIterator("E");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(2, t.size());
        assertEquals("({(2,5,2)},{(2,5,2)})", t.toString());

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertEquals("({(1,2,3)},{(1,2,3)})", t.toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for C: $0, $1"}));
    }

    // See PIG-1165
    @Test
    public void testOrderbyWrongSignature() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile2.toString()), pigServer.getPigContext()) + "' AS (b0, b1);");
        pigServer.registerQuery("C = order A by a1;");
        pigServer.registerQuery("D = join C by a1, B by b0;");
        pigServer.registerQuery("E = foreach D generate a1, b0, b1;");
        Iterator<Tuple> iter = pigServer.openIterator("E");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(3, t.size());
        assertEquals("(2,2,2)", t.toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0, $2"}));
    }

    // See PIG-1146
    @Test
    public void testUnionMixedPruning() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1:chararray, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(Util.encodeEscape(tmpFile2.toString()), pigServer.getPigContext()) + "' AS (b0, b2);");
        pigServer.registerQuery("C = foreach B generate b0, 'hello', b2;");
        pigServer.registerQuery("D = union A, C;");
        pigServer.registerQuery("E = foreach D generate $0, $2;");
        Iterator<Tuple> iter = pigServer.openIterator("E");
        Collection<String> results = new HashSet<String>();
        results.add("(1,3)");
        results.add("(2,2)");
        results.add("(1,1)");
        results.add("(2,2)");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(2, t.size());
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(2, t.size());
        assertTrue(results.contains(t.toString()));

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1"}));
    }

    // See PIG-1176
    @Test
    public void testUnionMixedSchemaPruning() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = foreach A generate a0;;");
        pigServer.registerQuery("C = load '"+ Util.generateURI(Util.encodeEscape(tmpFile2.toString()), pigServer.getPigContext()) + "';");
        pigServer.registerQuery("D = foreach C generate $0;");
        pigServer.registerQuery("E = union B, D;");
        Iterator<Tuple> iter = pigServer.openIterator("E");
        Collection<String> results = new HashSet<String>();
        results.add("(1)");
        results.add("(2)");
        results.add("(1)");
        results.add("(2)");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(1, t.size());
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertEquals(1, t.size());
        assertTrue(results.contains(t.toString()));

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }

    // See PIG-1184
    @Test
    public void testForEachFlatten() throws Exception {
        File inputFile = Util.createInputFile("table_testForEachFlatten", "", new String[]{"oiue\tM\t{(3),(4)}\t{(toronto),(montreal)}"});

        pigServer.registerQuery("A = load '"+Util.encodeEscape(inputFile.toString())+"' as (a0:chararray, a1:chararray, a2:bag{t:tuple(id:chararray)}, a3:bag{t:tuple(loc:chararray)});");
        pigServer.registerQuery("B = foreach A generate a0, a1, flatten(a2), flatten(a3), 10;");
        pigServer.registerQuery("C = foreach B generate a0, $4;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals("(oiue,10)", t.toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals("(oiue,10)", t.toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals("(oiue,10)", t.toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals("(oiue,10)", t.toString());

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1"}));
    }

    // See PIG-1210
    @Test
    public void testFieldsToReadDuplicatedEntry() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = foreach A generate a0+a0, a1, a2;");
        Iterator<Tuple> iter = pigServer.openIterator("B");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals("(2.0,2,3)", t.toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals("(4.0,5,2)", t.toString());

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }

    // See PIG-1272
    @Test
    public void testSplit4() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = foreach A generate a0;");
        pigServer.registerQuery("C = join A by a0, B by a0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals("(1,2,3,1)", t.toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals("(2,5,2,2)", t.toString());

        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testSplit5() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile11.toString()), pigServer.getPigContext()) + "' AS (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = foreach A generate a0, a1;");
        pigServer.registerQuery("C = join A by a0, B by a0;");
        pigServer.registerQuery("D = filter C by A::a1>=B::a1;");
        Iterator<Tuple> iter = pigServer.openIterator("D");

        String [] expected = new String[] {
                "(1,2,3,1,2)",
                "(1,3,2,1,2)",
                "(1,3,2,1,3)",
                "(2,5,2,2,5)"
        };

        Util.checkQueryOutputsAfterSortRecursive(iter, expected, org.apache.pig.newplan.logical.Util.translateSchema(pigServer.dumpSchema("D")));

        assertTrue(emptyLogFileMessage());
    }


    // See PIG-1493
    @Test
    public void testInconsistentPruning() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' AS (a0:chararray, a1:chararray, a2);");
        pigServer.registerQuery("B = foreach A generate CONCAT(a0,a1) as b0, a0, a2;");
        pigServer.registerQuery("C = foreach B generate a0, a2;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertEquals("(1,3)", t.toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        assertEquals("(2,2)", t.toString());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1"}));
    }

    // See PIG-1644
    @Test
    public void testSplitOutputWithForEach() throws Exception {
        Path output1 = FileLocalizer.getTemporaryPath(pigServer.getPigContext());
        Path output2 = FileLocalizer.getTemporaryPath(pigServer.getPigContext());
        pigServer.setBatchOn();
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile5.toString()), pigServer.getPigContext()) + "' AS (a0, a1, a2, a3);");
        pigServer.registerQuery("B = foreach A generate a0, a1, a2;");
        pigServer.registerQuery("store B into '" + Util.generateURI(Util.encodeEscape(output1.toString()), pigServer.getPigContext()) + "';");
        pigServer.registerQuery("C = order B by a2;");
        pigServer.registerQuery("D = foreach C generate a2;");
        pigServer.registerQuery("store D into '" + Util.generateURI(Util.encodeEscape(output2.toString()), pigServer.getPigContext()) + "';");
        pigServer.executeBatch();

        BufferedReader reader1 = new BufferedReader(new InputStreamReader(FileLocalizer.openDFSFile(output1.toString(), pigServer.getPigContext().getProperties())));
        String line = reader1.readLine();
        assertEquals("1\t2\t3", line);

        line = reader1.readLine();
        assertEquals("2\t3\t4", line);

        assertNull(reader1.readLine());

        BufferedReader reader2 = new BufferedReader(new InputStreamReader(FileLocalizer.openDFSFile(output2.toString(), pigServer.getPigContext().getProperties())));
        line = reader2.readLine();
        assertEquals("3", line);

        line = reader2.readLine();
        assertEquals("4", line);

        assertNull(reader2.readLine());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $3"}));

        reader1.close();
        reader2.close();
    }

    static public class PruneColumnEvalFunc extends LoadFunc implements LoadPushDown {
        String[] aliases;
        String signature;
        public PruneColumnEvalFunc() {}
        @Override
        public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) throws FrontendException {
            aliases = new String[requiredFieldList.getFields().size()];
            for (int i=0; i<requiredFieldList.getFields().size(); i++) {
                RequiredField fs = requiredFieldList.getFields().get(i);
                aliases[i] = fs.getAlias();
            }
            try {
                UDFContext.getUDFContext().getUDFProperties(this.getClass()).setProperty(signature, ObjectSerializer.serialize(aliases));
            } catch (IOException e) {
                throw new FrontendException(e);
            }
            return new RequiredFieldResponse(true);
        }

        @Override
        public List<OperatorSet> getFeatures() {
            return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
        }

        @Override
        public void setLocation(String location, Job job) throws IOException {
            FileInputFormat.setInputPaths(job, location);
        }

        @Override
        public InputFormat getInputFormat() throws IOException {
            return new PigTextInputFormat();
        }

        @Override
        public void prepareToRead(RecordReader reader, PigSplit split)
                throws IOException {
        }

        @Override
        public void setUDFContextSignature(String signature) {
            this.signature = signature;
        }

        @Override
        public Tuple getNext() throws IOException {
            if (aliases==null) {
                aliases = (String[])ObjectSerializer.deserialize(UDFContext.getUDFContext().getUDFProperties(this.getClass()).getProperty(signature));
                Tuple t = TupleFactory.getInstance().newTuple();
                for (String s : aliases)
                    t.append(s);
                return t;
            }
            return null;
        }
    }

    public void testAliasInRequiredFieldList() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile1.toString()), pigServer.getPigContext()) + "' using "
                + PruneColumnEvalFunc.class.getName() +"() as (a0, a1, a2);");
        pigServer.registerQuery("B = foreach A generate a1, a2;");
        Iterator<Tuple> iter = pigServer.openIterator("B");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();

        assertEquals(2, t.size());
        assertEquals("a1", t.get(0));
        assertEquals("a2", t.get(1));

        assertFalse(iter.hasNext());
    }

    @Test
    public void testCogroup10() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(Util.encodeEscape(tmpFile2.toString()), pigServer.getPigContext()) + "' AS (a0, a1:double);");
        pigServer.registerQuery("B = foreach A generate a0, a1, 0 as joinField;");
        pigServer.registerQuery("C = group B all;");
        pigServer.registerQuery("D = foreach C generate 0 as joinField, SUM(B.a1) as total;");
        pigServer.registerQuery("E = join B by joinField, D by joinField;");
        pigServer.registerQuery("F = foreach E generate a0;");
        Iterator<Tuple> iter = pigServer.openIterator("F");

        String[] expected = new String[] {"(1)", "(2)"};

        Util.checkQueryOutputsAfterSortRecursive(iter, expected, org.apache.pig.newplan.logical.Util.translateSchema(pigServer.dumpSchema("F")));

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1"}));
    }

    // See PIG-2534
    @Test
    public void testStream2() throws Exception {
        File input1 = File.createTempFile("tmp", "");
        input1.delete();
        File input2 = File.createTempFile("tmp", "");
        input2.delete();

        Util.createLocalInputFile(input1.getAbsolutePath(), new String[]
                {"[key1#0,key2#5,key3#val3,key4#val4,key5#val5]"});
        Util.createLocalInputFile(input2.getAbsolutePath(), new String[]
                {"[key1#0,key2#5,key3#val3,key4#val4,key5#val5]"});

        pigServer.registerQuery("event_serve = LOAD '" + Util.encodeEscape(input1.getAbsolutePath()) +
                "' AS (s, m, l);");
        pigServer.registerQuery("cm_data_raw = LOAD '" + Util.encodeEscape(input2.getAbsolutePath()) +
                "' AS (s, m, l);");
        pigServer.registerQuery("cm_serve = FOREACH cm_data_raw GENERATE  s#'key3' AS f1,  s#'key4' AS f2, s#'key5' AS f3 ;");
        pigServer.registerQuery("cm_serve_lowercase = stream cm_serve through `tr '[:upper:]' '[:lower:]'`;");
        pigServer.registerQuery("cm_serve_final = FOREACH cm_serve_lowercase GENERATE  $0 AS cm_event_guid, $1 AS cm_receive_time, $2 AS cm_ctx_url;");
        pigServer.registerQuery("event_serve_project = FOREACH  event_serve GENERATE  s#'key3' AS event_guid, s#'key4' AS receive_time;");
        pigServer.registerQuery("event_serve_join = join cm_serve_final by (cm_event_guid), event_serve_project by (event_guid);");
        Iterator<Tuple> iter = pigServer.openIterator("event_serve_join");

        String[] expected = new String[] {"(val3,val4,val5,val3,val4)"};

        Util.checkQueryOutputsAfterSortRecursive(iter, expected, org.apache.pig.newplan.logical.Util.translateSchema(pigServer.dumpSchema("event_serve_join")));

        assertTrue(checkLogFileMessage(new String[]{"Map key required for event_serve: $0->[key3, key4]",
                "Map key required for cm_data_raw: $0->[key3, key4, key5]"}));
    }

    // See PIG-2535
    @Test
    public void testStream3() throws Exception {
        pigServer.registerQuery("event_serve = LOAD 'input1' AS (s, m, l);");
        pigServer.registerQuery("raw = LOAD 'input2' AS (s, m, l);");

        pigServer.registerQuery("SPLIT raw INTO " +
            "serve_raw IF (( (chararray) (s#'type') == '0') AND ( (chararray) (s#'source') == '5'))," +
            "cm_click_raw IF (( (chararray) (s#'type') == '1') AND ( (chararray) (s#'source') == '5'));");
        pigServer.registerQuery("cm_serve = FOREACH serve_raw GENERATE  s#'cm_serve_id' AS cm_event_guid,  s#'cm_serve_timestamp_ms' AS cm_receive_time, s#'p_url' AS ctx ;");
        pigServer.registerQuery("cm_serve_lowercase = stream cm_serve through `tr '[:upper:]' '[:lower:]'`;");
        pigServer.registerQuery("cm_serve_final = FOREACH cm_serve_lowercase GENERATE  $0 AS cm_event_guid, $1 AS cm_receive_time, $2 AS ctx;");
        pigServer.registerQuery("filtered = FILTER event_serve BY (chararray) (s#'filter_key') neq 'xxxx' AND (chararray) (s#'filter_key') neq 'yyyy';");
        pigServer.registerQuery("event_serve_project = FOREACH filtered GENERATE s#'event_guid' AS event_guid, s#'receive_time' AS receive_time;");
        pigServer.registerQuery("event_serve_join = join cm_serve_final by (cm_event_guid), event_serve_project by (event_guid);");


        pigServer.explain("event_serve_join", System.out);

        assertTrue(checkLogFileMessage(new String[]{"Map key required for event_serve: $0->[event_guid, filter_key, receive_time]",
                "Map key required for raw: $0->[cm_serve_id, cm_serve_timestamp_ms, p_url, source, type]"}));
    }
}