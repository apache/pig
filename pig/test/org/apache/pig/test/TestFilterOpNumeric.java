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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.junit.Before;
import org.junit.Test;

public class TestFilterOpNumeric {

    private final Log log = LogFactory.getLog(getClass());

    private static int LOOP_COUNT = 1024;
    private PigServer pig;

    @Before
    public void setUp() throws Exception {
        pig = new PigServer(ExecType.LOCAL);
    }

    @Test
    public void testNumericEq() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println(i + ":" + (double)i);
            } else {
                ps.println(i + ":" + (i-1));
            }
        }
        ps.close();
        pig.registerQuery("A=load '"
                + Util.encodeEscape(Util.generateURI(tmpFile.toString(), pig.getPigContext()))
                + "' using "+PigStorage.class.getName() +"(':');");
        String query = "A = filter A by ($0 == $1 and $0 <= $1);";
        log.info(query);
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = it.next();
            Double first = Double.valueOf(t.get(0).toString());
            Double second = Double.valueOf(t.get(1).toString());
            assertEquals(first, second);

            String sfirst = t.get(0).toString();
            String ssecond = t.get(1).toString();
            assertFalse(sfirst.equals(ssecond));
        }
    }

    @Test
    public void testNumericNeq() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("1:1");
            } else {
                ps.println("2:3");
            }
        }
        ps.close();
        pig.registerQuery("A=load '"
                + Util.encodeEscape(Util.generateURI(tmpFile.toString(), pig.getPigContext()))
                + "' using " + PigStorage.class.getName() + "(':');");
        String query = "A = filter A by $0 != $1;";
        log.info(query);
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = it.next();
            Double first = Double.valueOf(t.get(0).toString());
            Double second = Double.valueOf(t.get(1).toString());
            assertFalse(first.equals(second));
        }
    }

    @Test
    public void testNumericGt() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println(i + ":" + (double)i);
            } else {
                ps.println(i+1 + ":" + (double)(i));
            }
        }
        ps.close();
        pig.registerQuery("A=load '"
                + Util.encodeEscape(Util.generateURI(tmpFile.toString(), pig.getPigContext())) + "' using "
                + PigStorage.class.getName() + "(':') as (f1: double, f2:double);");
        String query = "A = filter A by ($0 > $1 and $0 >= $1);";

        log.info(query);
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = it.next();
            Double first = Double.valueOf(t.get(0).toString());
            Double second = Double.valueOf(t.get(1).toString());
            assertTrue(first.compareTo(second) > 0);
        }
    }

    @Test
    public void testBinCond() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            ps.println(i + "\t" + i + "\t1");
        }
        ps.close();
        pig.registerQuery("A=load '"
                + Util.encodeEscape(Util.generateURI(tmpFile.toString(), pig.getPigContext())) + "';");
        String query = "A = foreach A generate ($1 >= "+ LOOP_COUNT+"-10?'1':'0');";
        log.info(query);
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        tmpFile.delete();
        int count =0;
        while(it.hasNext()) {
            Tuple t = it.next();
            Double first = Double.valueOf(t.get(0).toString());
            if (first == 1)
                count++;
            else
                assertTrue(first == 0);

        }
        assertEquals("expected count of 10", 10, count);
    }


    @Test
    public void testNestedBinCond() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            ps.println(i + "\t" + i + "\t1");
        }
        ps.close();
        pig.registerQuery("A=load '"
                + Util.encodeEscape(Util.generateURI(tmpFile.toString(), pig.getPigContext())) + "';");
        String query = "A = foreach A generate (($0 < 10 or $0 < 9)?(($1 >= 5 and $1 >= 4) ? 2: 1) : 0);";
        log.info(query);
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        tmpFile.delete();
        int count =0;
        while(it.hasNext()) {
            Tuple t = it.next();
            Integer first = (Integer)t.get(0);
            count+=first;
            assertTrue(first == 1 || first == 2 || first == 0);

        }
        assertEquals("expected count of 15", 15, count);
    }

    @Test
    public void testNumericLt() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println(i + ":" + (double)i);
            } else {
                ps.println(i + ":" + (double)(i+1));
            }
        }
        ps.close();
        pig.registerQuery("A=load '"
                + Util.encodeEscape(Util.generateURI(tmpFile.toString(), pig.getPigContext()))
                + "' using " + PigStorage.class.getName() + "(':') as (a: double, b:double);");
        String query = "A = filter A by ($0 <= $1 and $0 < $1);";

        log.info(query);
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = it.next();
            Double first = Double.valueOf(t.get(0).toString());
            Double second = Double.valueOf(t.get(1).toString());
            assertTrue(first.compareTo(second) < 0);
        }

    }

    @Test
    public void testNumericGte() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println(i + ":" + (double)i);
            } else if(i % 3 == 0){
                ps.println(i-1 + ":" + (double)(i));
            } else {
                ps.println(i+1 + ":" + (double)(i));
            }
        }
        ps.close();
        pig.registerQuery("A=load '"
                + Util.encodeEscape(Util.generateURI(tmpFile.toString(), pig.getPigContext()))
                + "' using " + PigStorage.class.getName() + "(':');");
        String query = "A = filter A by ($0 > $1 or $0 >= $1);";

        log.info(query);
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = it.next();
            Double first = Double.valueOf(t.get(0).toString());
            Double second = Double.valueOf(t.get(1).toString());
            assertTrue(first.compareTo(second) >= 0);
        }
    }

    @Test
    public void testNumericLte() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println(i + ":" + (double)i);
            } else if(i % 3 == 0){
                ps.println(i-1 + ":" + (double)(i));
            } else {
                ps.println(i+1 + ":" + (double)(i));
            }
        }
        ps.close();
        pig.registerQuery("A=load '"
                + Util.encodeEscape(Util.generateURI(tmpFile.toString(), pig.getPigContext()))
                + "' using " + PigStorage.class.getName() + "(':') as (a: double, b:double);");
        String query = "A = filter A by ($0 <= $1 or $0 < $1);";

        log.info(query);
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = it.next();
            Double first = Double.valueOf(t.get(0).toString());
            Double second = Double.valueOf(t.get(1).toString());
            assertTrue(first.compareTo(second) <= 0);
        }
    }
}