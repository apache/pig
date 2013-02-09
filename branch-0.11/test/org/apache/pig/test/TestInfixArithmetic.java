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
import java.util.Random;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;



import junit.framework.TestCase;

@RunWith(JUnit4.class)
public class TestInfixArithmetic extends TestCase {

    private final Log log = LogFactory.getLog(getClass());

    private static int LOOP_COUNT = 1024;    
    static MiniCluster cluster = MiniCluster.buildCluster();

    private PigServer pig;
    
    @Before
    @Override
    public void setUp() throws Exception {
        pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }
    
    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
    
    Boolean[] nullFlags = new Boolean[] { false, true };

    @Test
    public void testAdd() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        
        for (int i = 0; i < nullFlags.length; i++) {
            System.err.println("Testing with nulls: " + nullFlags[i]);
            PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
            generateInput(ps, nullFlags[i]);
            String query = "A = foreach (load '"
                    + Util.generateURI(tmpFile.toString(), pig.getPigContext())
                    + "' using " + PigStorage.class.getName()
                    + "(':')) generate $0, $0 + $1, $1;";
            log.info(query);
            pig.registerQuery(query);
            Iterator<Tuple> it = pig.openIterator("A");
            tmpFile.delete();
            while(it.hasNext()) {
                Tuple t = it.next();
                Double first = (t.get(0) == null ? null :DataType.toDouble(t.get(0)));
                Double second = (t.get(1) == null ? null :DataType.toDouble(t.get(1)));
                Double third = (t.get(2) == null ? null :DataType.toDouble(t.get(2)));
                if(first != null && third != null) {
                    assertTrue(second.equals(first + first));
                } else {
                    assertEquals(null, second);
                }                    
            }
        }        
    }
 
    @Test
    public void testSubtract() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        for (int i = 0; i < nullFlags.length; i++) {
            System.err.println("Testing with nulls: " + nullFlags[i]);
            PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
            generateInput(ps, nullFlags[i]);
            String query = "A = foreach (load '"
                    + Util.generateURI(tmpFile.toString(), pig.getPigContext())
                    + "' using " + PigStorage.class.getName()
                    + "(':')) generate $0, $0 - $1, $1 ;";
            log.info(query);
            pig.registerQuery(query);
            Iterator<Tuple> it = pig.openIterator("A");
            tmpFile.delete();
            while(it.hasNext()) {
                Tuple t = it.next();
                Double first = (t.get(0) == null ? null :DataType.toDouble(t.get(0)));
                Double second = (t.get(1) == null ? null :DataType.toDouble(t.get(1)));
                Double third = (t.get(2) == null ? null :DataType.toDouble(t.get(2)));
                if(first != null && third != null) {
                    assertTrue(second.equals(0.0));
                } else {
                    assertEquals(null, second);
                }                    
            }
        }
    }
 
    @Test
    public void testMultiply() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        for (int i = 0; i < nullFlags.length; i++) {
            System.err.println("Testing with nulls: " + nullFlags[i]);
            PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
            generateInput(ps, nullFlags[i]);
            String query = "A = foreach (load '"
                    + Util.generateURI(tmpFile.toString(), pig.getPigContext())
                    + "' using " + PigStorage.class.getName()
                    + "(':')) generate $0, $0 * $1, $1 ;";
            log.info(query);
            pig.registerQuery(query);
            Iterator<Tuple> it = pig.openIterator("A");
            tmpFile.delete();
            while(it.hasNext()) {
                Tuple t = it.next();
                Double first = (t.get(0) == null ? null :DataType.toDouble(t.get(0)));
                Double second = (t.get(1) == null ? null :DataType.toDouble(t.get(1)));
                Double third = (t.get(2) == null ? null :DataType.toDouble(t.get(2)));
                if(first != null && third != null) {
                    assertTrue(second.equals(first * first));
                } else {
                    assertEquals(null, second);
                }                    
            }
        }
    }
    
    @Test
    public void testDivide() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        for (int i = 0; i < nullFlags.length; i++) {
            System.err.println("Testing with nulls: " + nullFlags[i]);
            PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
            generateInput(ps, nullFlags[i]);
            String query = "A = foreach (load '"
                    + Util.generateURI(tmpFile.toString(), pig.getPigContext())
                    + "' using " + PigStorage.class.getName()
                    + "(':')) generate $0, $0 / $1, $1 ;";
            log.info(query);
            pig.registerQuery(query);
            Iterator<Tuple> it = pig.openIterator("A");
            tmpFile.delete();
            while(it.hasNext()) {
                Tuple t = it.next();
                Double first = (t.get(0) == null ? null :DataType.toDouble(t.get(0)));
                Double second = (t.get(1) == null ? null :DataType.toDouble(t.get(1)));
                Double third = (t.get(2) == null ? null :DataType.toDouble(t.get(2)));
                if(first != null && third != null) {
                    assertTrue(second.equals(1.0));
                } else {
                    assertEquals(null, second);
                }                    
            }
        }
    }
    
    private void generateInput(PrintStream ps, boolean withNulls) {
        if(withNulls) {
            // inject nulls randomly
            for(int i = 1; i < LOOP_COUNT; i++) {
                int rand = new Random().nextInt(LOOP_COUNT);
                if(rand <= (0.2 * LOOP_COUNT) ) {
                    ps.println(":"+i);
                } else if (rand > (0.2 * LOOP_COUNT) && rand <= (0.4 * LOOP_COUNT)) {
                    ps.println(i+":");
                } else if (rand > (0.2 * LOOP_COUNT) && rand <= (0.4 * LOOP_COUNT)) {
                    ps.println(":");
                } else {
                    ps.println(i + ":" + i);
                }            
            }
        } else {
            for(int i = 1; i < LOOP_COUNT; i++) {
                ps.println(i + ":" + i);
            }
        }
        ps.close();
    }    
}
