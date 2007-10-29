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
import java.io.PrintStream;
import java.util.Iterator;

import org.junit.Test;

import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;

import junit.framework.TestCase;

public class TestInfixArithmetic extends TestCase {

    private static int LOOP_COUNT = 1024;    
    private String initString = "mapreduce";
    
    @Test
    public void testAdd() throws Exception {
        PigServer pig = new PigServer(initString);
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            ps.println(i + ":" + i);
        }
        ps.close();
        String query = "A = foreach (load 'file:" + tmpFile + "' using " + PigStorage.class.getName() + "(':')) generate $0, $0 + $1, $1;";
        System.out.println(query);
        pig.registerQuery(query);
        Iterator it = pig.openIterator("A");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = (Tuple)it.next();
            Double first = t.getAtomField(0).numval();
            Double second = t.getAtomField(1).numval();
            assertTrue(second.equals(first + first));
        }
    }
 
    @Test
    public void testSubtract() throws Exception {
        PigServer pig = new PigServer(initString);
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            ps.println(i + ":" + i);
        }
        ps.close();
        String query = "A = foreach (load 'file:" + tmpFile + "' using " + PigStorage.class.getName() + "(':')) generate $0, $0 - $1, $1 ;";
        System.out.println(query);
        pig.registerQuery(query);
        Iterator it = pig.openIterator("A");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = (Tuple)it.next();
            Double second = t.getAtomField(1).numval();
            assertTrue(second.equals(0.0));
        }
    }
 
    @Test
    public void testMultiply() throws Exception {
        PigServer pig = new PigServer(initString);
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            ps.println(i + ":" + i);
        }
        ps.close();
        String query = "A = foreach (load 'file:" + tmpFile + "' using " + PigStorage.class.getName() + "(':')) generate $0, $0 * $1, $1 ;";
        System.out.println(query);
        pig.registerQuery(query);
        Iterator it = pig.openIterator("A");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = (Tuple)it.next();
            Double first = t.getAtomField(0).numval();
            Double second = t.getAtomField(1).numval();
            assertTrue(second.equals(first * first));
        }
    }
    
    @Test
    public void testDivide() throws Exception {
        PigServer pig = new PigServer(initString);
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 1; i < LOOP_COUNT; i++) {
            ps.println(i + ":" + i);
        }
        ps.close();
        String query = "A =  foreach (load 'file:" + tmpFile + "' using " + PigStorage.class.getName() + "(':')) generate $0, $0 / $1, $1;";
        System.out.println(query);
        pig.registerQuery(query);
        Iterator it = pig.openIterator("A");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = (Tuple)it.next();
            Double second = t.getAtomField(1).numval();
            assertTrue(second.equals(1.0));
        }
    }
    
    
    
    
}
