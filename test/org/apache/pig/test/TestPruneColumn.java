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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.pig.ExecType;
import org.apache.pig.FilterFunc;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.rules.ColumnPruneVisitor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

public class TestPruneColumn extends TestCase {
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
    File logFile;

    static public class PigStorageWithTrace extends PigStorage {

        /**
         * @param delimiter
         */
        public PigStorageWithTrace() {
            super();
        }
        @Override
        public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) throws FrontendException {
            RequiredFieldResponse response = super.pushProjection(requiredFieldList);
            Logger logger = Logger.getLogger(this.getClass());
            logger.info(requiredFieldList);
            return response;
        }

    }
    private static final String simpleEchoStreamingCommand;
    static {
        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
            simpleEchoStreamingCommand = "perl -ne 'print \\\"$_\\\"'";
        else
            simpleEchoStreamingCommand = "perl -ne 'print \"$_\"'";
    }

    static public class MyFilterFunc extends FilterFunc {
        
        @Override
        public Boolean exec(Tuple input) {
            return true;
        }
    }
    
    @Before
    @Override
    public void setUp() throws Exception{
        Logger logger = Logger.getLogger(ColumnPruneVisitor.class);
        logger.removeAllAppenders();
        logger.setLevel(Level.INFO);
        SimpleLayout layout = new SimpleLayout();
        logFile = File.createTempFile("log", "");
        FileAppender appender = new FileAppender(layout, logFile.toString(), false, false, 0);
        logger.addAppender(appender);
        
        Logger pigStorageWithTraceLogger = Logger.getLogger(PigStorageWithTrace.class);
        pigStorageWithTraceLogger.setLevel(Level.INFO);
        pigStorageWithTraceLogger.addAppender(appender);
        
        pigServer = new PigServer("local");
        //pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
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
    }
    
    @After
    @Override
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
        logFile.delete();
    }
    
    public boolean checkLogFileMessage(String[] messages)
    {
        BufferedReader reader = null;
        
        try {
            reader = new BufferedReader(new FileReader(logFile));
            List<String> logMessages=new ArrayList<String>();
            String line;
            while ((line=reader.readLine())!=null)
            {
                logMessages.add(line);
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
        }
        catch (IOException e) {
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
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = foreach A generate a1, a2;");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue((Integer)t.get(0) == 2);
        assertTrue((Integer)t.get(1) == 3);
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue((Integer)t.get(0) == 5);
        assertTrue((Integer)t.get(1) == 2);
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0"}));
    }
    
    @Test
    public void testLoadForEach2() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = foreach A generate a0, a2;");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue((Integer)t.get(0) == 1);
        assertTrue((Integer)t.get(1) == 3);
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue((Integer)t.get(0) == 2);
        assertTrue((Integer)t.get(1) == 2);
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1"}));
    }
    
    @Test
    public void testLoadForEach3() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = foreach A generate a0, a1;");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue((Integer)t.get(0) == 1);
        assertTrue((Integer)t.get(1) == 2);
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue((Integer)t.get(0) == 2);
        assertTrue((Integer)t.get(1) == 5);
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $2"}));
    }
    
    @Test
    public void testJoin1() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile2.toString(), pigServer.getPigContext()) + "' as (b0:int, b1:int);");
        pigServer.registerQuery("C = join A by a1, B by b1;");
        pigServer.registerQuery("D = foreach C generate a1, a2, b0, b1;");
        
        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==4);
        assertTrue(t.get(0).equals(2));
        assertTrue(t.get(1).equals(3));
        assertTrue(t.get(2).equals(2));
        assertTrue(t.get(3).equals(2));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0"}));
    }
    
    @Test
    public void testJoin2() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile2.toString(), pigServer.getPigContext()) + "' as (b0:int, b1:int);");
        pigServer.registerQuery("C = join A by a1, B by b1;");
        pigServer.registerQuery("D = foreach C generate a1, a2, b1;");
        
        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==3);
        assertTrue(t.get(0).equals(2));
        assertTrue(t.get(1).equals(3));
        assertTrue(t.get(2).equals(2));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0",
                "Columns pruned for B: $0"}));
    }
    
    @Test
    public void testForEachFilter() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = filter A by a2==3;");
        pigServer.registerQuery("C = foreach B generate a0, a1;");
        
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).equals(1));
        assertTrue(t.get(1).equals(2));
        
        assertFalse(iter.hasNext());
        
        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testForEach1() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = foreach A generate a0, a1+a2;");
        
        Iterator<Tuple> iter = pigServer.openIterator("B");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).equals(1));
        assertTrue(t.get(1).equals(5));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).equals(2));
        assertTrue(t.get(1).equals(7));
        
        assertFalse(iter.hasNext());
        
        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testForEach2() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = foreach A generate a0 as b0, *;");
        
        Iterator<Tuple> iter = pigServer.openIterator("B");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==4);
        assertTrue(t.get(0).equals(1));
        assertTrue(t.get(1).equals(1));
        assertTrue(t.get(2).equals(2));
        assertTrue(t.get(3).equals(3));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.size()==4);
        assertTrue(t.get(0).equals(2));
        assertTrue(t.get(1).equals(2));
        assertTrue(t.get(2).equals(5));
        assertTrue(t.get(3).equals(2));
        
        assertFalse(iter.hasNext());
        
        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testSplit1() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' as (a0, a1, a2);");
        pigServer.registerQuery("split A into B if $0<=1, C if $0>1;");
        pigServer.registerQuery("D = foreach B generate $1;");
        
        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("2"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $2"}));
    }
    
    @Test
    public void testSplit2() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' as (a0, a1, a2);");
        pigServer.registerQuery("split A into B if $0<=1, C if $0>1;");
        pigServer.registerQuery("D = foreach B generate $1;");
        
        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("2"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $2"}));
    }
    
    @Test
    public void testForeachNoSchema1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "';");
        pigServer.registerQuery("B = foreach A generate $1, $2;");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("2"));
        assertTrue(t.get(1).toString().equals("3"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("5"));
        assertTrue(t.get(1).toString().equals("2"));
        
        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testForeachNoSchema2() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "';");
        pigServer.registerQuery("B = foreach A generate $1, 'aoeuaoeu';");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("2"));
        assertTrue(t.get(1).toString().equals("aoeuaoeu"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("5"));
        assertTrue(t.get(1).toString().equals("aoeuaoeu"));
        
        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testCoGroup1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1:int, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile2.toString(), pigServer.getPigContext()) + "' AS (b0, b1:int);");
        pigServer.registerQuery("C = cogroup A by $1, B by $1;");
        pigServer.registerQuery("D = foreach C generate AVG($1.$1);");
        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0)==null);
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("2.0"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("5.0"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for B: $0"}));
    }
    
    @Test
    public void testCoGroup2() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1:int, a2);");
        pigServer.registerQuery("B = group A all;");
        pigServer.registerQuery("C = foreach B generate $1;");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("{(1,2,3),(2,5,2)}"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testCoGroup3() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1:int, a2);");
        pigServer.registerQuery("B = group A by $1;");
        pigServer.registerQuery("C = foreach B generate $1, '1';");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("{(1,2,3)}"));
        assertTrue(t.get(1).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("{(2,5,2)}"));
        assertTrue(t.get(1).toString().equals("1"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testCoGroup4() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1:int, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile2.toString(), pigServer.getPigContext()) + "' AS (b0, b1:int);");
        pigServer.registerQuery("C = cogroup A by ($1), B by ($1);");
        pigServer.registerQuery("D = foreach C generate $1.$1, $2.$1;");
        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("{}"));
        assertTrue(t.get(1).toString().equals("{(1)}"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("{(2)}"));
        assertTrue(t.get(1).toString().equals("{(2)}"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("{(5)}"));
        assertTrue(t.get(1).toString().equals("{}"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testCoGroup5() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = group A by (a0, a1);");
        pigServer.registerQuery("C = foreach B generate flatten(group);");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("1"));
        assertTrue(t.get(1).toString().equals("2"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("2"));
        assertTrue(t.get(1).toString().equals("5"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $2"}));
    }
    
    @Test
    public void testDistinct1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile4.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = distinct A;");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("1"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testStream1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = stream A through `" + simpleEchoStreamingCommand + "`;");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("2"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testBinCond1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile5.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2, a3);");
        pigServer.registerQuery("B = foreach A generate ($1 == '2'? $2 : $3);");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("3"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("5"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0"}));
    }
    
    @Test
    public void testCoGroup6() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile2.toString(), pigServer.getPigContext()) + "' AS (b0, b1);");
        pigServer.registerQuery("C = cogroup A by ($1), B by ($1);");
        pigServer.registerQuery("D = foreach C generate A, flatten(B.($0, $1));");
        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==3);
        assertTrue(t.get(0).toString().equals("{}"));
        assertTrue(t.get(1).toString().equals("1"));
        assertTrue(t.get(2).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==3);
        assertTrue(t.get(0).toString().equals("{(1,2,3)}"));
        assertTrue(t.get(1).toString().equals("2"));
        assertTrue(t.get(2).toString().equals("2"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testCoGroup7() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile2.toString(), pigServer.getPigContext()) + "' AS (b0, b1);");
        pigServer.registerQuery("C = cogroup A by ($1), B by ($1);");
        pigServer.registerQuery("D = foreach C {B = order B by $0;generate FLATTEN(A), B.($1);};");
        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==4);
        assertTrue(t.get(0).toString().equals("1"));
        assertTrue(t.get(1).toString().equals("2"));
        assertTrue(t.get(2).toString().equals("3"));
        assertTrue(t.get(3).toString().equals("{(2)}"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==4);
        assertTrue(t.get(0).toString().equals("2"));
        assertTrue(t.get(1).toString().equals("5"));
        assertTrue(t.get(2).toString().equals("2"));
        assertTrue(t.get(3).toString().equals("{}"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testCross1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile2.toString(), pigServer.getPigContext()) + "' AS (b0, b1);");
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
        
        assertTrue(t.size()==2);
        assertTrue(results.contains(t.toString()));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(results.contains(t.toString()));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(results.contains(t.toString()));

        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1, $2", 
            "Columns pruned for B: $1"}));
    }
    
    @Test
    public void testUnion1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile4.toString(), pigServer.getPigContext()) + "' AS (b0, b1, b2);");
        pigServer.registerQuery("C = union A, B;");
        pigServer.registerQuery("D = foreach C generate $0, $2;");
        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        Collection<String> results = new HashSet<String>();
        results.add("(1,3)");
        results.add("(2,2)");
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==2);
        results.contains(t.toString());
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==2);
        results.contains(t.toString());

        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==2);
        results.contains(t.toString());
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==2);
        results.contains(t.toString());

        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1", 
            "Columns pruned for B: $1"}));
    }

    @Test
    public void testFRJoin1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile2.toString(), pigServer.getPigContext()) + "' AS (b0, b1);");
        pigServer.registerQuery("C = join A by $0, B by $0 using \"replicated\";");
        pigServer.registerQuery("D = foreach C generate $0, $3;");
        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("1"));
        assertTrue(t.get(1).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("2"));
        assertTrue(t.get(1).toString().equals("2"));

        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1, $2", 
            "Columns pruned for B: $1"}));
    }
    
    @Test
    public void testFilter1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = order A by a1;");
        pigServer.registerQuery("C = limit B 10;");
        pigServer.registerQuery("D = foreach C generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("2"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $2"}));
    }
    
    @Test
    public void testFilter2() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = filter A by a0+a2 == 4;");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("2"));

        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1"}));
    }
    
    @Test
    public void testOrderBy1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = order A by $0;");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("2"));

        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1, $2"}));
    }
    
    @Test
    public void testOrderBy2() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = order A by *;");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("2"));

        assertFalse(iter.hasNext());
        
        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testCogroup8() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = group A by *;");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("(1,2,3)"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("(2,5,2)"));

        assertFalse(iter.hasNext());
        
        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testJoin3() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile4.toString(), pigServer.getPigContext()) + "' AS (b0, b1, b2);");
        pigServer.registerQuery("C = join A by *, B by * using \"replicated\";");
        pigServer.registerQuery("D = foreach C generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("1"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testLoadForEach4() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = foreach A generate *;");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("2"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1, $2"}));
    }
    
    @Test
    public void testForEachUDF() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0:chararray, a1:chararray, a2:chararray);");
        pigServer.registerQuery("B = foreach A generate StringSize(*);");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("1"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testOutJoin1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile6.toString(), pigServer.getPigContext()) + "' AS (a0:chararray, a1:chararray, a2:chararray);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile2.toString(), pigServer.getPigContext()) + "' AS (a0:chararray, a1:chararray, a2:chararray);");
        pigServer.registerQuery("C = join A by $0 left, B by $0;");
        pigServer.registerQuery("D = foreach C generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        Collection<String> results = new HashSet<String>();
        results.add("(2)");
        results.add("()");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(results.contains(t.toString()));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(results.contains(t.toString()));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1, $2", 
            "Columns pruned for B: $1, $2"}));
    }
    
    @Test
    public void testFilter3() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = filter A by " + MyFilterFunc.class.getName() + "(*) ;");
        pigServer.registerQuery("C = foreach B generate $0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("2"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(emptyLogFileMessage());
    }

    @Test
    public void testMapKey1() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile3.toString(), pigServer.getPigContext()) + "' as (a0:int, a1:map[]);");
        pigServer.registerQuery("B = foreach A generate a0, a1#'key1';");
        
        Iterator<Tuple> iter = pigServer.openIterator("B");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).equals(1));
        assertTrue(t.get(1).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).equals(2));
        assertTrue(t.get(1).toString().equals("2"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Map key required for A: $1->[key1]"}));
    }
    
    @Test
    public void testMapKey2() throws Exception{
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile3.toString(), pigServer.getPigContext()) + "' as (a0:int, a1:map[]);");
        pigServer.registerQuery("B = foreach A generate a1, a1#'key1';");
        pigServer.registerQuery("C = foreach B generate $0#'key2', $1;");
        
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("2"));
        assertTrue(t.get(1).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("4"));
        assertTrue(t.get(1).toString().equals("2"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0", 
                "Map key required for A: $1->[key2, key1]"}));
    }
    
    @Test
    public void testMapKey3() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile3.toString(), pigServer.getPigContext()) + "' as (a0:int, a1:map[]);");
        pigServer.registerQuery("B = foreach A generate a1, a1#'key1';");
        pigServer.registerQuery("C = group B all;");
        
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("all"));
        assertTrue(t.get(1).toString().equals("{([key2#2,key1#1],1),([key2#4,key1#2],2)}"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0"}));
    }
    
    @Test
    public void testMapKey4() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile3.toString(), pigServer.getPigContext()) + "' as (a0:int, a1:map[]);");
        pigServer.registerQuery("B = limit A 10;");
        pigServer.registerQuery("C = foreach B generate $0, $1#'key1';");
        
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("1"));
        assertTrue(t.get(1).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("2"));
        assertTrue(t.get(1).toString().equals("2"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Map key required for A: $1->[key1]"}));
    }
    
    @Test
    public void testMapKey5() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile3.toString(), pigServer.getPigContext()) + "' as (a0:int, a1:map[]);");
        pigServer.registerQuery("B = foreach A generate $0, $1#'key1';");
        pigServer.registerQuery("C = stream B through `" + simpleEchoStreamingCommand + "`;");
        
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("1"));
        assertTrue(t.get(1).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("2"));
        assertTrue(t.get(1).toString().equals("2"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Map key required for A: $1->[key1]"}));
    }
    
    @Test
    public void testConstantPlan() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' as (a0, a1, a2);");
        pigServer.registerQuery("B = foreach A generate 1, a2;");
        
        Iterator<Tuple> iter = pigServer.openIterator("B");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("1"));
        assertTrue(t.get(1).toString().equals("3"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("1"));
        assertTrue(t.get(1).toString().equals("2"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0, $1"}));
    }
    
    @Test
    public void testPlainPlan() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' as (a0, a1, a2);");
        pigServer.registerQuery("B = order A by $0;");
        
        Iterator<Tuple> iter = pigServer.openIterator("B");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==3);
        assertTrue(t.get(0).toString().equals("1"));
        assertTrue(t.get(1).toString().equals("2"));
        assertTrue(t.get(2).toString().equals("3"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.size()==3);
        assertTrue(t.get(0).toString().equals("2"));
        assertTrue(t.get(1).toString().equals("5"));
        assertTrue(t.get(2).toString().equals("2"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testBinStorage1() throws Exception {
        // get a temp intermediate filename
        File intermediateFile = File.createTempFile("intemediate", "txt");
        intermediateFile.delete(); // delete since we don't want the file to be present
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' as (a0, a1, a2);");
        pigServer.store("A", intermediateFile.toString(), "BinStorage()");
        
        pigServer.registerQuery("A = load '"+ intermediateFile.toString() 
                + "' using BinStorage() as (a0, a1, a2);");
        
        pigServer.registerQuery("B = foreach A generate a0;");
        
        Iterator<Tuple> iter = pigServer.openIterator("B");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("2"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1, $2"}));
    }
    
    @Test
    public void testBinStorage2() throws Exception {
        // get a temp intermediate filename
        File intermediateFile = File.createTempFile("intemediate", "txt");
        intermediateFile.delete(); // delete since we don't want the file to be present
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' as (a0, a1, a2);");
        pigServer.store("A", intermediateFile.toString(), "BinStorage()");
        
        pigServer.registerQuery("A = load '"+ intermediateFile.toString() 
                + "' using BinStorage() as (a0, a1, a2);");
        
        pigServer.registerQuery("B = foreach A generate a2, a0, a1;");
        pigServer.registerQuery("C = foreach B generate a0, a2;");
        
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("1"));
        assertTrue(t.get(1).toString().equals("3"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("2"));
        assertTrue(t.get(0).toString().equals("2"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1"}));
        
    }

    
    @Test
    public void testProjectCastKeyLookup() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile3.toString(), pigServer.getPigContext()) 
                + "' as (a0, a1);");
        
        pigServer.registerQuery("B = foreach A generate a1#'key1';");
        
        Iterator<Tuple> iter = pigServer.openIterator("B");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("2"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0", 
                "Map key required for A: $1->[key1]"}));
    }
    
    @Test
    public void testRelayFlattenMap() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile3.toString(), pigServer.getPigContext()) 
                + "' as (a0, a1:map[]);");
        
        pigServer.registerQuery("B = foreach A generate flatten(a1);");
        pigServer.registerQuery("C = foreach B generate a1#'key1';");
        
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("1"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("2"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0", 
                "Map key required for A: $1->[key1]"}));
    }
    
    @Test
    public void testCrossAtLeastOneColumnOneInput() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' as (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile2.toString(), pigServer.getPigContext()) + "' as (b0:int, b1:int);");
        pigServer.registerQuery("C = cross A, B;");
        pigServer.registerQuery("D = foreach C generate $0;");
        
        Iterator<Tuple> iter = pigServer.openIterator("D");
        Collection<String> results = new HashSet<String>();
        results.add("(1)");
        results.add("(2)");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==1);
        assertTrue(results.contains(t.toString()));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.size()==1);
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.size()==1);
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.size()==1);
        assertTrue(results.contains(t.toString()));

        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1, $2", 
                "Columns pruned for B: $1"}));
    }
    
    @Test
    public void testComplex1() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile7.toString(), pigServer.getPigContext()) + "' as (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile8.toString(), pigServer.getPigContext()) + "' as (b0, b1, b2, b3);");
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
        assertTrue(t.size()==1);
        assertTrue(t.get(0).toString().equals("{(2,2)}"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1", 
                "Columns pruned for B: $1"}));
    }
    
    @Test
    public void testCoGroup8() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1:int, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile2.toString(), pigServer.getPigContext()) + "' AS (b0, b1:int);");
        pigServer.registerQuery("C = cogroup A by ($1), B by ($1);");
        pigServer.registerQuery("D = foreach C generate $0, $1;");
        
        Iterator<Tuple> iter = pigServer.openIterator("D");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("1"));
        assertTrue(t.get(1).toString().equals("{}"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("2"));
        assertTrue(t.get(1).toString().equals("{(1,2,3)}"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.size()==2);
        assertTrue(t.get(0).toString().equals("5"));
        assertTrue(t.get(1).toString().equals("{(2,5,2)}"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for B: $0"}));
    }
    
    // See PIG-1128
    @Test
    public void testUserDefinedSchema() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS ( c1 : chararray, c2 : int);");
        pigServer.registerQuery("B = foreach A generate c1 as c1 : chararray, c2 as c2 : int, 'CA' as state : chararray;");
        pigServer.registerQuery("C = foreach B generate c1 as c1 : chararray;");
        
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.toString().equals("(1)"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.toString().equals("(2)"));

        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1"}));
    }

    // See PIG-1127
    @Test
    public void testSharedSchemaObject() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile10.toString(), pigServer.getPigContext()) + "' AS (a0, a1:map[], a2);");
        pigServer.registerQuery("B = foreach A generate a1;");
        pigServer.registerQuery("C = limit B 10;");
        
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.toString().equals("([2#1,1#1])"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0, $2"}));
    }
    
    // See PIG-1142
    @Test
    public void testJoin4() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (b0, b1, b2);");
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
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2:int);");
        pigServer.registerQuery("B = filter A by a2==3;");
        pigServer.registerQuery("C = foreach B generate $2;");
        
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.toString().equals("(3)"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0, $1"}));
    }
    
    @Test
    public void testSplit3() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2:int);");
        pigServer.registerQuery("split A into B if a2==3, C if a2<3;");
        pigServer.registerQuery("C = foreach B generate $2;");
        
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.toString().equals("(3)"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0, $1"}));
    }
    
    @Test
    public void testOrderBy3() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = order A by a2;");
        pigServer.registerQuery("C = foreach B generate a2;");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.toString().equals("(2)"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==1);
        assertTrue(t.toString().equals("(3)"));

        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0, $1"}));
    }
    
    @Test
    public void testCogroup9() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (b0, b1, b2);");
        pigServer.registerQuery("C = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (c0, c1, c2);");
        pigServer.registerQuery("D = cogroup A by a2, B by b2, C by c2;");
        pigServer.registerQuery("E = foreach D generate $1, $2;");
        Iterator<Tuple> iter = pigServer.openIterator("E");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(t.toString().equals("({(2,5,2)},{(2,5,2)})"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        
        assertTrue(t.size()==2);
        assertTrue(t.toString().equals("({(1,2,3)},{(1,2,3)})"));

        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for C: $0, $1"}));
    }

    // See PIG-1165
    @Test
    public void testOrderbyWrongSignature() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile2.toString(), pigServer.getPigContext()) + "' AS (b0, b1);");
        pigServer.registerQuery("C = order A by a1;");
        pigServer.registerQuery("D = join C by a1, B by b0;");
        pigServer.registerQuery("E = foreach D generate a1, b0, b1;");
        Iterator<Tuple> iter = pigServer.openIterator("E");
        
        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        
        assertTrue(t.size()==3);
        assertTrue(t.toString().equals("(2,2,2)"));
        
        assertFalse(iter.hasNext());
        
        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $0, $2"}));
    }
    
    // See PIG-1146
    @Test
    public void testUnionMixedPruning() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1:chararray, a2);");
        pigServer.registerQuery("B = load '"+ Util.generateURI(tmpFile2.toString(), pigServer.getPigContext()) + "' AS (b0, b2);");
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

        assertTrue(t.size()==2);
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.size()==2);
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.size()==2);
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.size()==2);
        assertTrue(results.contains(t.toString()));

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1"}));
    }
    
    // See PIG-1176
    @Test
    public void testUnionMixedSchemaPruning() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = foreach A generate a0;;");
        pigServer.registerQuery("C = load '"+ Util.generateURI(tmpFile2.toString(), pigServer.getPigContext()) + "';");
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

        assertTrue(t.size()==1);
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.size()==1);
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.size()==1);
        assertTrue(results.contains(t.toString()));

        assertTrue(iter.hasNext());
        t = iter.next();

        assertTrue(t.size()==1);
        assertTrue(results.contains(t.toString()));

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }
    
    // See PIG-1184
    @Test
    public void testForEachFlatten() throws Exception {
        File inputFile = Util.createInputFile("table_testForEachFlatten", "", new String[]{"oiue\tM\t{(3),(4)}\t{(toronto),(montreal)}"});
        
        pigServer.registerQuery("A = load '"+inputFile.toString()+"' as (a0:chararray, a1:chararray, a2:bag{t:tuple(id:chararray)}, a3:bag{t:tuple(loc:chararray)});");
        pigServer.registerQuery("B = foreach A generate a0, a1, flatten(a2), flatten(a3), 10;");
        pigServer.registerQuery("C = foreach B generate a0, $4;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.toString().equals("(oiue,10)"));

        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.toString().equals("(oiue,10)"));

        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.toString().equals("(oiue,10)"));

        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.toString().equals("(oiue,10)"));

        assertFalse(iter.hasNext());

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1"}));
    }
    
    // See PIG-1210
    @Test
    public void testFieldsToReadDuplicatedEntry() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' using "+PigStorageWithTrace.class.getName()
                +" AS (a0, a1, a2);");
        pigServer.registerQuery("B = foreach A generate a0+a0, a1, a2;");
        Iterator<Tuple> iter = pigServer.openIterator("B");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.toString().equals("(2.0,2,3)"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.toString().equals("(4.0,5,2)"));

        assertFalse(iter.hasNext());

        assertTrue(emptyLogFileMessage());
    }
    
    // See PIG-1272
    @Test
    public void testSplit4() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2);");
        pigServer.registerQuery("B = foreach A generate a0;");
        pigServer.registerQuery("C = join A by a0, B by a0;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.toString().equals("(1,2,3,1)"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.toString().equals("(2,5,2,2)"));

        assertTrue(emptyLogFileMessage());
    }
    
    @Test
    public void testSplit5() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile11.toString(), pigServer.getPigContext()) + "' AS (a0:int, a1:int, a2:int);");
        pigServer.registerQuery("B = foreach A generate a0, a1;");
        pigServer.registerQuery("C = join A by a0, B by a0;");
        pigServer.registerQuery("D = filter C by A::a1>=B::a1;");
        Iterator<Tuple> iter = pigServer.openIterator("D");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.toString().equals("(1,2,3,1,2)"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.toString().equals("(1,3,2,1,2)"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.toString().equals("(1,3,2,1,3)"));

        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.toString().equals("(2,5,2,2,5)"));
        
        assertTrue(emptyLogFileMessage());
    }


    // See PIG-1493
    @Test
    public void testInconsistentPruning() throws Exception {
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile1.toString(), pigServer.getPigContext()) + "' AS (a0:chararray, a1:chararray, a2);");
        pigServer.registerQuery("B = foreach A generate CONCAT(a0,a1) as b0, a0, a2;");
        pigServer.registerQuery("C = foreach B generate a0, a2;");
        Iterator<Tuple> iter = pigServer.openIterator("C");

        assertTrue(iter.hasNext());
        Tuple t = iter.next();
        assertTrue(t.toString().equals("(1,3)"));
        
        assertTrue(iter.hasNext());
        t = iter.next();
        assertTrue(t.toString().equals("(2,2)"));

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $1"}));
    }
    
    // See PIG-1644
    @Test
    public void testSplitOutputWithForEach() throws Exception {
        Path output1 = FileLocalizer.getTemporaryPath(pigServer.getPigContext());
        Path output2 = FileLocalizer.getTemporaryPath(pigServer.getPigContext());
        pigServer.setBatchOn();
        pigServer.registerQuery("A = load '"+ Util.generateURI(tmpFile5.toString(), pigServer.getPigContext()) + "' AS (a0, a1, a2, a3);");
        pigServer.registerQuery("B = foreach A generate a0, a1, a2;");
        pigServer.registerQuery("store B into '" + Util.generateURI(output1.toString(), pigServer.getPigContext()) + "';");
        pigServer.registerQuery("C = order B by a2;");
        pigServer.registerQuery("D = foreach C generate a2;");
        pigServer.registerQuery("store D into '" + Util.generateURI(output2.toString(), pigServer.getPigContext()) + "';");
        pigServer.executeBatch();

        BufferedReader reader1 = new BufferedReader(new InputStreamReader(FileLocalizer.openDFSFile(output1.toString())));
        String line = reader1.readLine();
        assertTrue(line.equals("1\t2\t3"));
        
        line = reader1.readLine();
        assertTrue(line.equals("2\t3\t4"));
        
        assertTrue(reader1.readLine()==null);
        
        BufferedReader reader2 = new BufferedReader(new InputStreamReader(FileLocalizer.openDFSFile(output2.toString())));
        line = reader2.readLine();
        assertTrue(line.equals("3"));
        
        line = reader2.readLine();
        assertTrue(line.equals("4"));
        
        assertTrue(reader2.readLine()==null);

        assertTrue(checkLogFileMessage(new String[]{"Columns pruned for A: $3"}));
        
        reader1.close();
        reader2.close();
    }
}
