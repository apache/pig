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

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.junit.Before;
import org.junit.Test;

public class TestAlgebraicEvalLocal {

    private int LOOP_COUNT = 512;

    private PigServer pig;

    @Before
    public void setUp() throws Exception {
        pig = new PigServer(ExecType.LOCAL, new Properties());
    }

    Boolean[] nullFlags = new Boolean[]{ false, true};

    @Test
    public void testGroupCountWithMultipleFields() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        for (int k = 0; k < nullFlags.length; k++) {
            System.err.println("Running testGroupCountWithMultipleFields with nullFlags set to " + nullFlags[k]);
            // flag to indicate if both the keys forming
            // the group key are null
            int groupKeyWithNulls = 0;
            if(nullFlags[k] == false) {
                // generate data with no nulls
                PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
                for(int i = 0; i < LOOP_COUNT; i++) {
                    for(int j=0; j< LOOP_COUNT; j++) {
                            ps.println(i + "\t" + i + "\t" + j%2);
                    }
                }
                ps.close();
            } else {
                // generate data with nulls
                PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
                Random r = new Random();
                for(int i = 0; i < LOOP_COUNT; i++) {
                    int rand = r.nextInt(LOOP_COUNT);
                    if(rand <= (0.2 * LOOP_COUNT) ) {
                        for(int j=0; j< LOOP_COUNT; j++) {
                            ps.println("\t" + i + "\t" + j%2);
                        }
                    } else if (rand > (0.2 * LOOP_COUNT) && rand <= (0.4 * LOOP_COUNT)) {
                        for(int j=0; j< LOOP_COUNT; j++) {
                            ps.println(i + "\t" + "\t" + j%2);
                        }
                    } else if (rand > (0.4 * LOOP_COUNT) && rand <= (0.6 * LOOP_COUNT)) {
                        for(int j=0; j< LOOP_COUNT; j++) {
                            ps.println("\t" + "\t" + j%2);
                        }
                        groupKeyWithNulls++;
                    } else {
                        for(int j=0; j< LOOP_COUNT; j++) {
                            ps.println(i + "\t" + i + "\t" + j%2);
                        }
                    }
                }
                ps.close();
            }
            pig.registerQuery(" a = group (load '"
                    + Util.generateURI(Util.encodeEscape(tmpFile.toString()), pig.getPigContext())
                    + "') by ($0,$1);");
            pig.registerQuery("b = foreach a generate flatten(group), SUM($1.$2);");
            Iterator<Tuple> it = pig.openIterator("b");
            int count = 0;
            System.err.println("XX Starting");
            while(it.hasNext()){
                Tuple t = it.next();
            System.err.println("XX "+ t);
                int sum = ((Double)t.get(2)).intValue();
                // if the first two fields (output of flatten(group))
                // are both nulls then we should change the sum accordingly
                if (t.get(0) == null && t.get(1) == null)
                    assertEquals(
                            "Running testGroupCountWithMultipleFields with nullFlags set to "
                                    + nullFlags[k], (LOOP_COUNT / 2)
                                    * groupKeyWithNulls, sum);
                else
                    assertEquals(
                            "Running testGroupCountWithMultipleFields with nullFlags set to "
                                    + nullFlags[k], LOOP_COUNT / 2, sum);

                count++;
            }
            System.err.println("XX done");
            if (groupKeyWithNulls == 0)
                assertEquals(
                        "Running testGroupCountWithMultipleFields with nullFlags set to "
                                + nullFlags[k], LOOP_COUNT, count);
            else
                assertEquals(
                        "Running testGroupCountWithMultipleFields with nullFlags set to "
                                + nullFlags[k], LOOP_COUNT - groupKeyWithNulls
                                + 1, count);

        }
        tmpFile.delete();

    }

    @Test
    public void testSimpleCount() throws Exception {
        File tmpFile = File.createTempFile("test", "txt");
        for (int i = 0; i < nullFlags.length; i++) {
            System.err.println("Testing testSimpleCount with null flag:" + nullFlags[i]);

            PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
            generateInput(ps, nullFlags[i]);
            String query = "myid =  foreach (group (load '"
                    + Util.generateURI(Util.encodeEscape(tmpFile.toString()), pig.getPigContext())
                    + "') all) generate COUNT($1);";
            System.out.println(query);
            pig.registerQuery(query);
            Iterator<Tuple> it = pig.openIterator("myid");
            tmpFile.delete();
            Tuple t = it.next();
            Long count = DataType.toLong(t.get(0));
            assertEquals(this.getClass().getName() + "with nullFlags set to: "
                    + nullFlags[i], count.longValue(), LOOP_COUNT);
        }
    }

    @Test
    public void testGroupCount() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        for (int i = 0; i < nullFlags.length; i++) {
            System.err.println("Testing testGroupCount with null flag:" + nullFlags[i]);

            PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
            generateInput(ps, nullFlags[i]);
            String query = "myid = foreach (group (load '"
                    + Util.generateURI(Util.encodeEscape(tmpFile.toString()), pig.getPigContext())
                    + "') all) generate group, COUNT($1) ;";
            System.out.println(query);
            pig.registerQuery(query);
            Iterator<Tuple> it = pig.openIterator("myid");
            tmpFile.delete();
            Tuple t = it.next();
            Long count = DataType.toLong(t.get(1));
            assertEquals(this.getClass().getName() + "with nullFlags set to: "
                    + nullFlags[i], count.longValue(), LOOP_COUNT);
        }
    }

    @Test
    public void testGroupReorderCount() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        for (int i = 0; i < nullFlags.length; i++) {
            System.err.println("Testing testGroupCount with null flag:" + nullFlags[i]);
            PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
            generateInput(ps, nullFlags[i]);
            String query = "myid = foreach (group (load '"
                    + Util.generateURI(Util.encodeEscape(tmpFile.toString()), pig.getPigContext())
                    + "') all) generate COUNT($1), group ;";
            System.out.println(query);
            pig.registerQuery(query);
            Iterator<Tuple> it = pig.openIterator("myid");
            tmpFile.delete();
            Tuple t = it.next();
            Long count = DataType.toLong(t.get(0));
            assertEquals(this.getClass().getName() + "with nullFlags set to: "
                    + nullFlags[i], count.longValue(), LOOP_COUNT);
        }
    }



    @Test
    public void testGroupUniqueColumnCount() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        for (int i = 0; i < nullFlags.length; i++) {
            PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
            long groupsize = 0;
            if(nullFlags[i] == false) {
                // generate data without nulls
                for(int j = 0; j < LOOP_COUNT; j++) {
                    if(j%10 == 0) groupsize++;
                    ps.println(j%10 + ":" + j);
                }
            } else {
                // generate data with nulls
                for(int j = 0; j < LOOP_COUNT; j++) {
                    if(j%10 == 0) groupsize++;
                    if(j % 20 == 0) {
                        // for half the groups
                        // emit nulls
                        ps.println(j%10 + ":");
                        groupsize --;
                    } else {
                        ps.println(j%10 + ":" + j);
                    }
                }
            }
            ps.close();
            String query = "myid = foreach (group (load '"
                    + Util.generateURI(Util.encodeEscape(tmpFile.toString()), pig.getPigContext())
                    + "' using " + PigStorage.class.getName()
                    + "(':')) by $0) generate group, COUNT($1.$1) ;";
            System.out.println(query);
            pig.registerQuery(query);
            Iterator<Tuple> it = pig.openIterator("myid");
            tmpFile.delete();
            System.err.println("Output from testGroupUniqueColumnCount");
            while(it.hasNext()) {
                Tuple t = it.next();
                System.err.println(t);
                String a = t.get(0).toString();
                Double group = Double.valueOf(a.toString());
                if(group == 0.0) {
                    Long count = DataType.toLong(t.get(1));
                    assertEquals(this.getClass().getName() + "with nullFlags set to: "
                            + nullFlags[i], groupsize, count.longValue());
                }
            }
        }
    }

    @Test
    public void testGroupDuplicateColumnCount() throws Throwable {
        File tmpFile = File.createTempFile("test", "txt");
        for (int i = 0; i < nullFlags.length; i++) {
            PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
            long groupsize = 0;
            long nonNullCnt = 0;
            if(nullFlags[i] == false) {
                // generate data without nulls
                for(int j = 0; j < LOOP_COUNT; j++) {
                    if(j%10 == 0) {groupsize++; nonNullCnt++;}
                    ps.println(j%10 + ":" + j);
                }
            } else {
                // generate data with nulls
                for(int j = 0; j < LOOP_COUNT; j++) {
                    if(j%10 == 0) {groupsize++; nonNullCnt++;}
                    if(j % 20 == 0) {
                        // for half the groups
                        // emit nulls
                        ps.println(j%10 + ":");
                        nonNullCnt--;
                    } else {
                        ps.println(j%10 + ":" + j);
                    }
                }
            }
            ps.close();
            String query = "myid = foreach (group (load '"
                    + Util.generateURI(Util.encodeEscape(tmpFile.toString()), pig.getPigContext())
                    + "' using "
                    + PigStorage.class.getName()
                    + "(':')) by $0) generate group, COUNT($1.$1), COUNT($1.$0) ;";
            System.out.println(query);
            pig.registerQuery(query);
            Iterator<Tuple> it = pig.openIterator("myid");
            tmpFile.delete();
            System.err.println("Output from testGroupDuplicateColumnCount");
            while(it.hasNext()) {
                Tuple t = it.next();
                System.err.println(t);
                String a = t.get(0).toString();
                Double group = Double.valueOf(a.toString());
                if(group == 0.0) {
                    Long count = DataType.toLong(t.get(2));
                    assertEquals(this.getClass().getName() + "with nullFlags set to: "
                            + nullFlags[i], groupsize, count.longValue());
                    count = DataType.toLong(t.get(1));
                    assertEquals(this.getClass().getName() + "with nullFlags set to: "
                            + nullFlags[i], nonNullCnt, count.longValue());
                }
            }
        }
    }

    private int generateInput(PrintStream ps, boolean withNulls ) {
        int numNulls = 0;
        if(withNulls) {
            // inject nulls randomly
            for(int i = 0; i < LOOP_COUNT; i++) {
                int rand = new Random().nextInt(LOOP_COUNT);
                if(rand <= (0.3 * LOOP_COUNT) ) {
                    ps.println(":");
                    numNulls++;
                } else {
                    ps.println(i + ":" + i);
                }
            }
        } else {
            for(int i = 0; i < LOOP_COUNT; i++) {
                ps.println(i + ":" + i);
            }
        }
        ps.close();
        return numNulls;
    }
}