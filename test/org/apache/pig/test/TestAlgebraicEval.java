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

import junit.framework.TestCase;

import org.junit.Test;

import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

public class TestAlgebraicEval extends TestCase {
    
	private String initString = "mapreduce";
    
    @Test
    public void testGroupCountWithMultipleFields() throws Exception {
        int LOOP_COUNT = 1024;
        PigServer pig = new PigServer(initString);
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            for(int j=0; j< LOOP_COUNT; j++) {
                ps.println(i + "\t" + i + "\t" + j%2);
            }
        }
        ps.close();
        String query = "myid = foreach (group (load 'file:" + tmpFile + "') all) generate group, COUNT($1) ;";
        System.out.println(query);
        pig.registerQuery(" a = group (load 'file:" + tmpFile + "') by ($0,$1);");
        pig.registerQuery("b = foreach a generate flatten(group), SUM($1.$2);");
        Iterator<Tuple> it = pig.openIterator("b");
        tmpFile.delete();
        int count = 0;
        while(it.hasNext()){
            DataByteArray a = (DataByteArray)it.next().get(2);
            int sum = Double.valueOf(a.toString()).intValue();
            assertEquals(LOOP_COUNT/2, sum);
            count++;
        }
        assertEquals(count, LOOP_COUNT);
    }
    
    
    
    @Test
    public void testSimpleCount() throws Exception {
        int LOOP_COUNT = 1024;
        PigServer pig = new PigServer(initString);
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            ps.println(i);
        }
        ps.close();
        String query = "myid =  foreach (group (load 'file:" + tmpFile + "') all) generate COUNT($1);";
        System.out.println(query);
        pig.registerQuery(query);
        Iterator it = pig.openIterator("myid");
        tmpFile.delete();
        Tuple t = (Tuple)it.next();
        DataByteArray a = (DataByteArray)t.get(0);
        Double count = Double.valueOf(a.toString());
        assertEquals(count, (double)LOOP_COUNT);
    }

    @Test
    public void testGroupCount() throws Exception {
        int LOOP_COUNT = 1024;
        PigServer pig = new PigServer(initString);
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            ps.println(i);
        }
        ps.close();
        String query = "myid = foreach (group (load 'file:" + tmpFile + "') all) generate group, COUNT($1) ;";
        System.out.println(query);
        pig.registerQuery(query);
        Iterator it = pig.openIterator("myid");
        tmpFile.delete();
        Tuple t = (Tuple)it.next();
        DataByteArray a = (DataByteArray)t.get(1);
        Double count = Double.valueOf(a.toString());
        assertEquals(count, (double)LOOP_COUNT);
    }
    
    
    
    @Test
    public void testGroupReorderCount() throws Exception {
        int LOOP_COUNT = 1024;
        PigServer pig = new PigServer(initString);
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < LOOP_COUNT; i++) {
            ps.println(i);
        }
        ps.close();
        String query = "myid = foreach (group (load 'file:" + tmpFile + "') all) generate COUNT($1), group ;";
        System.out.println(query);
        pig.registerQuery(query);
        Iterator it = pig.openIterator("myid");
        tmpFile.delete();
        Tuple t = (Tuple)it.next();
        DataByteArray a = (DataByteArray)t.get(0);
        Double count = Double.valueOf(a.toString());
        assertEquals(count, (double)LOOP_COUNT);
    }



    @Test
    public void testGroupUniqueColumnCount() throws Exception {
        int LOOP_COUNT = 1024;
        PigServer pig = new PigServer(initString);
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int groupsize = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i%10 == 0) groupsize++;
            ps.println(i%10 + ":" + i);
        }
        ps.close();
        String query = "myid = foreach (group (load 'file:" + tmpFile + "' using " + PigStorage.class.getName() + "(':')) by $0) generate group, COUNT($1.$1) ;";
        System.out.println(query);
        pig.registerQuery(query);
        Iterator it = pig.openIterator("myid");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = (Tuple)it.next();
            DataByteArray a = (DataByteArray)t.get(0);
            Double group = Double.valueOf(a.toString());
            if(group == 0.0) {
                DataByteArray b = (DataByteArray)t.get(1);
                Double count = Double.valueOf(b.toString());
                assertEquals(count, (double)groupsize);
                break;
            }
        }   
    }

    @Test
    public void testGroupDuplicateColumnCount() throws Exception {
        int LOOP_COUNT = 1024;
        PigServer pig = new PigServer(initString);
        File tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        int groupsize = 0;
        for(int i = 0; i < LOOP_COUNT; i++) {
            if(i%10 == 0) groupsize++;
            ps.println(i%10 + ":" + i);
        }
        ps.close();
        String query = "myid = foreach (group (load 'file:" + tmpFile + "' using " + PigStorage.class.getName() + "(':')) by $0) generate group, COUNT($1.$1), COUNT($1.$0) ;";
        System.out.println(query);
        pig.registerQuery(query);
        Iterator it = pig.openIterator("myid");
        tmpFile.delete();
        while(it.hasNext()) {
            Tuple t = (Tuple)it.next();
            DataByteArray a = (DataByteArray)t.get(0);
            Double group = Double.valueOf(a.toString());
            if(group == 0.0) {
                DataByteArray b = (DataByteArray)t.get(1);
                Double count = Double.valueOf(b.toString());
                assertEquals(count, (double)groupsize);
                break;
            }
        }
    }

}
