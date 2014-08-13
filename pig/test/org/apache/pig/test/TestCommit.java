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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Test;

public class TestCommit {

    private PigServer pigServer;

    private static final TupleFactory mTf = TupleFactory.getInstance();

    @Before
    public void setUp() throws Exception{
        pigServer = new PigServer(ExecType.LOCAL);
    }

    @Test
    public void testCheckin1() throws Exception{
        Tuple expected1 = mTf.newTuple(2);
        Tuple expected2 = mTf.newTuple(2);
        expected1.set(0, "independent");
        expected1.set(1, 50.0);
        expected2.set(0, "democrat");
        expected2.set(1, 125.5);
        File studentFile = Util.createInputFile("tmp", "student", new String[]{"joe smith:18:3.5","amy brown:25:2.5","jim fox:20:4.0","leo fu:55:3.0"});
        File voterFile = Util.createInputFile("tmp", "voter", new String[]{"amy brown,25,democrat,25.50","amy brown,25,democrat,100","jim fox,20,independent,50.0"});

        pigServer.registerQuery("a = load '" + Util.encodeEscape(studentFile.getAbsolutePath()) + "' using " + PigStorage.class.getName() + "(':') as (name, age, gpa);");
        pigServer.registerQuery("b = load '" + Util.encodeEscape(voterFile.getAbsolutePath()) + "' using " + PigStorage.class.getName() + "(',') as (name, age, registration, contributions);");
        pigServer.registerQuery("c = filter a by age < 50;");
        pigServer.registerQuery("d = filter b by age < 50;");
        pigServer.registerQuery("e = cogroup c by (name, age), d by (name, age);");
        pigServer.registerQuery("f = foreach @ generate flatten(c), flatten(d);");
        pigServer.registerQuery("g = group @ by registration;");
        pigServer.registerQuery("h = foreach @ generate (chararray)group, SUM(f.d::contributions);");
        pigServer.registerQuery("i = order @ by $1;");

        Iterator<Tuple> iter = pigServer.openIterator("i");
        int count = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            count++;
            if (count == 1) {
                assertEquals(t.get(0), expected1.get(0));
                assertEquals(t.get(1), expected1.get(1));
            } else if (count == 2){
                assertEquals(t.get(0), expected2.get(0));
                assertEquals(t.get(1), expected2.get(1));
            }
        }
        assertEquals(2, count);
    }

    @Test
    public void testCheckin2() throws Exception{
        Tuple expected1 = mTf.newTuple(4);
        Tuple expected2 = mTf.newTuple(4);
        File testFile = Util.createInputFile("tmp", "testCheckin2-input.txt", new String[]{"joe smith:18:3.5","amy brown:18:2.5","jim fox:20:4.0","leo fu:55:3.0", "amy smith:20:3.0"});
        expected1.set(0, 18);
        expected1.set(1, 1L);
        expected1.set(2, "joe smith");
        expected1.set(3, 18);
        expected2.set(0, 55);
        expected2.set(1, 1L);
        expected2.set(2, "leo fu");
        expected2.set(3, 55);

        pigServer.registerQuery("a = load '" + Util.encodeEscape(testFile.getAbsolutePath()) + "' using " + PigStorage.class.getName() + "(':') as (name: chararray, age: int, gpa: float);");
        pigServer.registerQuery("b = group @ by age;");
        pigServer.registerQuery("c = foreach @ { d = filter a by gpa > 2.5;  " +
                                "e = order a by name; f = a.age; g = distinct f; " +
                                " generate group, COUNT(d), MAX (e.name), MIN(g.$0);};");
        pigServer.registerQuery("h = order c by $1;");
        pigServer.registerQuery("i = limit @ 2;");
        pigServer.store("i", "testCheckin2-output.txt");
        pigServer.registerQuery("x = load 'testCheckin2-output.txt' as (age: int, cnt: long, max: chararray, min: int);");
        pigServer.registerQuery("y = foreach @ generate age, cnt, max, min;");
        Iterator<Tuple> iter = pigServer.openIterator("y");
        int count = 0;
        boolean contain1=false, contain2=false;
        while(iter.hasNext()){
            Tuple t = iter.next();
            count++;
            if (t.get(0).equals(expected1.get(0)) && t.get(1).equals(expected1.get(1)) && t.get(2).equals(expected1.get(2)) && t.get(3).equals(expected1.get(3))) {
                contain1 = true;
            }

            if (t.get(0).equals(expected2.get(0)) && t.get(1).equals(expected2.get(1)) && t.get(2).equals(expected2.get(2)) && t.get(3).equals(expected2.get(3))) {
                contain2 = true;
            }

        }
        pigServer.deleteFile("testCheckin2-output.txt");
        assertEquals(2, count);
        assertTrue(contain1);
        assertTrue(contain2);
    }
}
