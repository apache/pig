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

import static org.apache.pig.PigServer.ExecType.MAPREDUCE;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;

import junit.framework.TestCase;

public class TestFilterOpString extends PigExecTestCase {

    private final Log log = LogFactory.getLog(getClass());

    private static int LOOP_COUNT = 1024;

    @Test
    public void testStringEq() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("a:" + i);
            } else {
                ps.println("ab:ab");
            }
        }
        ps.close();
        pigServer.registerQuery("A=load 'file:" + Util.encodeEscape(tmpFile.toString()) + "' using " + PigStorage.class.getName() + "(':');");
        String query = "A = filter A by $0 eq $1;";

        log.info(query);
        pigServer.registerQuery(query);
        Iterator it = pigServer.openIterator("A");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = (Tuple)it.next();
            String first = t.getAtomField(0).strval();
            String second = t.getAtomField(1).strval();
            assertTrue(first.equals(second));
        }
    }
    
    @Test
    public void testStringNeq() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("ab:ab");
            } else {
                ps.println("ab:abc");
            }
        }
        ps.close();
        pigServer.registerQuery("A=load 'file:" + Util.encodeEscape(tmpFile.toString()) + "' using " + PigStorage.class.getName() + "(':');");
        String query = "A = filter A by $0 neq $1;";

        log.info(query);
        pigServer.registerQuery(query);
        Iterator it = pigServer.openIterator("A");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = (Tuple)it.next();
            String first = t.getAtomField(0).strval();
            String second = t.getAtomField(1).strval();
            assertFalse(first.equals(second));
        }
    }

    @Test
    public void testStringGt() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("b:a");
            } else {
                ps.println("a:b");
            }
        }
        ps.close();
        pigServer.registerQuery("A=load 'file:" + Util.encodeEscape(tmpFile.toString()) + "' using " + PigStorage.class.getName() + "(':');");
        String query = "A = filter A by $0 gt $1;";

        log.info(query);
        pigServer.registerQuery(query);
        Iterator it = pigServer.openIterator("A");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = (Tuple)it.next();
            String first = t.getAtomField(0).strval();
            String second = t.getAtomField(1).strval();
            assertTrue(first.compareTo(second) > 0);
        }
    }

    

    @Test
    public void testStringGte() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("b:a");
            }else if(i % 3 == 0) {
                ps.println("b:b");
            } else {
                ps.println("a:b");
            }
        }
        ps.close();
        
        pigServer.registerQuery("A=load 'file:" + Util.encodeEscape(tmpFile.toString()) + "' using " + PigStorage.class.getName() + "(':');");
        String query = "A = filter A by $0 gte $1;";

        log.info(query);
        pigServer.registerQuery(query);
        Iterator it = pigServer.openIterator("A");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = (Tuple)it.next();
            String first = t.getAtomField(0).strval();
            String second = t.getAtomField(1).strval();
            assertTrue(first.compareTo(second) >= 0);
        }
    }

    @Test
    public void testStringLt() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("b:a");
            } else {
                ps.println("a:b");
            }
        }
        ps.close();
        
        pigServer.registerQuery("A=load 'file:" + Util.encodeEscape(tmpFile.toString()) + "' using " + PigStorage.class.getName() + "(':');");
        String query = "A = filter A by $0 lt $1;";

        log.info(query);
        pigServer.registerQuery(query);
        Iterator it = pigServer.openIterator("A");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = (Tuple)it.next();
            String first = t.getAtomField(0).strval();
            String second = t.getAtomField(1).strval();
            assertTrue(first.compareTo(second) < 0);
        }
    }

    @Test
    public void testStringLte() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i % 5 == 0) {
                ps.println("b:a");
            }else if(i % 3 == 0) {
                ps.println("b:b");
            } else {
                ps.println("a:b");
            }
        }
        ps.close();
        
        pigServer.registerQuery("A=load 'file:" + Util.encodeEscape(tmpFile.toString()) + "' using " + PigStorage.class.getName() + "(':');");
        String query = "A = filter A by $0 lte $1;";

        log.info(query);
        pigServer.registerQuery(query);
        Iterator it = pigServer.openIterator("A");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = (Tuple)it.next();
            String first = t.getAtomField(0).strval();
            String second = t.getAtomField(1).strval();
            assertTrue(first.compareTo(second) <= 0);
        }
    }

}
