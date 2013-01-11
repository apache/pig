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
import java.io.PrintWriter;
import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestStoreOld {

    static MiniCluster cluster = MiniCluster.buildCluster();
    private int LOOP_COUNT = 1024;

    String fileName;
    String tmpFile1, tmpFile2;
    PigServer pig;

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testSingleStore() throws Exception{
        pig.registerQuery("A = load " + fileName + ";");

        pig.store("A", tmpFile1);

        pig.registerQuery("B = load " + tmpFile1 + ";");
        Iterator<Tuple> iter  = pig.openIterator("B");

        int i =0;
        while (iter.hasNext()){
            Tuple t = iter.next();
            assertEquals(DataType.toInteger(t.get(0)).intValue(),i);
            assertEquals(DataType.toInteger(t.get(1)).intValue(),i);
            i++;
        }
    }

    @Test
    public void testMultipleStore() throws Exception{
        pig.registerQuery("A = load " + fileName + ";");

        pig.store("A", tmpFile1);

        pig.registerQuery("B = foreach (group A by $0) generate $0, SUM($1.$0);");
        pig.store("B", tmpFile2);
        pig.registerQuery("C = load " + tmpFile2 + ";");
        Iterator<Tuple> iter  = pig.openIterator("C");

        int i =0;
        while (iter.hasNext()){
            Tuple t = iter.next();
            i++;
        }

        assertEquals(LOOP_COUNT, i);

    }

    @Test
    public void testStoreWithMultipleMRJobs() throws Exception{
        pig.registerQuery("A = load " + fileName + ";");
        pig.registerQuery("B = foreach (group A by $0) generate $0, SUM($1.$0);");
        pig.registerQuery("C = foreach (group B by $0) generate $0, SUM($1.$0);");
        pig.registerQuery("D = foreach (group C by $0) generate $0, SUM($1.$0);");

        pig.store("D", tmpFile2);
        pig.registerQuery("E = load " + tmpFile2 + ";");
        Iterator<Tuple> iter  = pig.openIterator("E");

        int i =0;
        while (iter.hasNext()){
            Tuple t = iter.next();
            i++;
        }

        assertEquals(LOOP_COUNT, i);

    }

    @Before
    public void setUp() throws Exception {
        File f = File.createTempFile("tmp", "");
        PrintWriter pw = new PrintWriter(f);
        for (int i=0;i<LOOP_COUNT; i++){
            pw.println(i + "\t" + i);
        }
        pw.close();
        pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        fileName = "'" + FileLocalizer.hadoopify(f.toString(), pig.getPigContext()) + "'";
        tmpFile1 = "'" + FileLocalizer.getTemporaryPath(pig.getPigContext()).toString() + "'";
        tmpFile2 = "'" + FileLocalizer.getTemporaryPath(pig.getPigContext()).toString() + "'";
        f.delete();
    }

}
