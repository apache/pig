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
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.Before;
import org.junit.Test;

public class TestFilterOpString {

    private final Log log = LogFactory.getLog(getClass());
    private static int LOOP_COUNT = 1024;

    private PigServer pig;

    @Before
    public void setUp() throws Exception {
        FileLocalizer.deleteTempFiles();
        pig = new PigServer(ExecType.LOCAL);
    }

    @Test
    public void testStringEq() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int expectedCount = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("a:" + i);
                // test with nulls
                ps.println("a:");
                ps.println(":a");
                ps.println(":");
            } else {
                ps.println("ab:ab");
                expectedCount++;
            }
        }
        ps.close();
        pig.registerQuery("A=load '"
                + Util.encodeEscape(Util.generateURI(tmpFile.toString(), pig.getPigContext()))
                + "' using " + PigStorage.class.getName() + "(':');");
        String query = "A = filter A by $0 eq $1;";

        log.info(query);
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        tmpFile.delete();
        int count = 0;
        while(it.hasNext()) {
            Tuple t = it.next();
            String first = t.get(0).toString();
            String second = t.get(1).toString();
            count++;
            assertEquals(first, second);
        }
        assertEquals(expectedCount, count);
    }

    @Test
    public void testStringNeq() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int expectedCount = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("ab:ab");
            } else if (i % 3 == 0) {
                ps.println("ab:abc");
                expectedCount++;
            } else {
                // test with nulls
                ps.println(":");
                ps.println("ab:");
                ps.println(":ab");
            }
        }
        ps.close();
        pig.registerQuery("A=load '"
                + Util.encodeEscape(Util.generateURI(tmpFile.toString(), pig.getPigContext()))
                + "' using " + PigStorage.class.getName() + "(':');");
        String query = "A = filter A by $0 neq $1;";

        log.info(query);
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        tmpFile.delete();
        int count = 0;
        while(it.hasNext()) {
            Tuple t = it.next();
            String first = t.get(0).toString();
            String second = t.get(1).toString();
            assertFalse(first.equals(second));
            count++;
        }
        assertEquals(expectedCount, count);
    }

    @Test
    public void testStringGt() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int expectedCount = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("b:a");
                expectedCount++;
            } else {
                ps.println("a:b");
                // test with nulls
                ps.println("a:");
                ps.println(":b");
                ps.println(":");

            }
        }
        ps.close();
        pig.registerQuery("A=load '"
                + Util.encodeEscape(Util.generateURI(tmpFile.toString(), pig.getPigContext()))
                + "' using " + PigStorage.class.getName() + "(':');");
        String query = "A = filter A by $0 gt $1;";

        log.info(query);
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        tmpFile.delete();
        int count = 0;
        while(it.hasNext()) {
            Tuple t = it.next();
            String first = t.get(0).toString();
            String second = t.get(1).toString();
            assertTrue(first.compareTo(second) > 0);
            count++;
        }
        assertEquals(expectedCount, count);
    }



    @Test
    public void testStringGte() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int expectedCount = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("b:a");
                expectedCount++;
            }else if(i % 3 == 0) {
                ps.println("b:b");
                expectedCount++;
            } else {
                ps.println("a:b");
                // test with nulls
                ps.println("a:");
                ps.println(":b");
                ps.println(":");
            }
        }
        ps.close();

        pig.registerQuery("A=load '"
                + Util.encodeEscape(Util.generateURI(tmpFile.toString(), pig.getPigContext()))
                + "' using " + PigStorage.class.getName() + "(':');");
        String query = "A = filter A by $0 gte $1;";

        log.info(query);
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        tmpFile.delete();
        int count = 0;
        while(it.hasNext()) {
            Tuple t = it.next();
            String first = t.get(0).toString();
            String second = t.get(1).toString();
            assertTrue(first.compareTo(second) >= 0);
            count++;
        }
        assertEquals(expectedCount, count);
    }

    @Test
    public void testStringLt() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int expectedCount = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("b:a");
                // test with nulls
                ps.println("a:");
                ps.println(":b");
                ps.println(":");
            } else {
                ps.println("a:b");
                expectedCount++;
            }
        }
        ps.close();

        pig.registerQuery("A=load '"
                + Util.encodeEscape(Util.generateURI(tmpFile.toString(), pig.getPigContext()))
                + "' using " + PigStorage.class.getName() + "(':');");
        String query = "A = filter A by $0 lt $1;";

        log.info(query);
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        tmpFile.delete();
        int count = 0;
        while(it.hasNext()) {
            Tuple t = it.next();
            String first = t.get(0).toString();
            String second = t.get(1).toString();
            assertTrue(first.compareTo(second) < 0);
            count++;
        }
        assertEquals(expectedCount, count);
    }

    @Test
    public void testStringLte() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int expectedCount = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("b:a");
                // test with nulls
                ps.println("a:");
                ps.println(":b");
                ps.println(":");
            }else if(i % 3 == 0) {
                ps.println("b:b");
                expectedCount++;
            } else {
                ps.println("a:b");
                expectedCount++;
            }
        }
        ps.close();

        pig.registerQuery("A=load '"
                + Util.encodeEscape(Util.generateURI(tmpFile.toString(), pig.getPigContext()))
                + "' using " + PigStorage.class.getName() + "(':');");
        String query = "A = filter A by $0 lte $1;";

        log.info(query);
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        tmpFile.delete();
        int count = 0;
        while(it.hasNext()) {
            Tuple t = it.next();
            String first = t.get(0).toString();
            String second = t.get(1).toString();
            assertTrue(first.compareTo(second) <= 0);
            count++;
        }
        assertEquals(expectedCount, count);
    }
}