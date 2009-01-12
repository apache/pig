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
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.junit.Before;
import org.junit.Test;

public class TestExampleGenerator extends TestCase {

    MiniCluster cluster = MiniCluster.buildCluster();
    PigContext pigContext = new PigContext(ExecType.MAPREDUCE, cluster
            .getProperties());

    Random rand = new Random();
    int MAX = 100;
    String A, B;

    {
        try {
            pigContext.connect();
        } catch (ExecException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() throws Exception {
        File fileA, fileB;

        fileA = File.createTempFile("dataA", ".dat");
        fileB = File.createTempFile("dataB", ".dat");

        writeData(fileA);
        writeData(fileB);

        A = "'" + FileLocalizer.hadoopify(fileA.toString(), pigContext) + "'";
        B = "'" + FileLocalizer.hadoopify(fileB.toString(), pigContext) + "'";
        System.out.println("A : " + A + "\n" + "B : " + B);
        System.out.println("Test data created.");

    }

    private void writeData(File dataFile) throws Exception {
        // File dataFile = File.createTempFile(name, ".dat");
        FileOutputStream dat = new FileOutputStream(dataFile);

        Random rand = new Random();

        for (int i = 0; i < MAX; i++)
            dat.write((rand.nextInt(10) + "\t" + rand.nextInt(10) + "\n")
                    .getBytes());

        dat.close();
    }

    @Test
    public void testFilter() throws Exception {

        PigServer pigserver = new PigServer(pigContext);

        String query = "A = load " + A
                + " using PigStorage() as (x : int, y : int);\n";
        pigserver.registerQuery(query);
        query = "B = filter A by x > 10;";
        pigserver.registerQuery(query);
        Map<LogicalOperator, DataBag> derivedData = pigserver.getExamples("B");

        assertTrue(derivedData != null);

    }

    @Test
    public void testForeach() throws ExecException, IOException {
        PigServer pigServer = new PigServer(pigContext);

        pigServer.registerQuery("A = load " + A
                + " using PigStorage() as (x : int, y : int);");
        pigServer.registerQuery("B = foreach A generate x + y as sum;");

        Map<LogicalOperator, DataBag> derivedData = pigServer.getExamples("B");

        assertTrue(derivedData != null);
    }

    @Test
    public void testJoin() throws IOException, ExecException {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A1 = load " + A + " as (x, y);");
        pigServer.registerQuery("B1 = load " + B + " as (x, y);");

        pigServer.registerQuery("E = join A1 by x, B1 by x;");

        Map<LogicalOperator, DataBag> derivedData = pigServer.getExamples("E");

        assertTrue(derivedData != null);
    }

    @Test
    public void testCogroupMultipleCols() throws Exception {

        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A + " as (x, y);");
        pigServer.registerQuery("B = load " + B + " as (x, y);");
        pigServer.registerQuery("C = cogroup A by (x, y), B by (x, y);");
        Map<LogicalOperator, DataBag> derivedData = pigServer.getExamples("C");

        assertTrue(derivedData != null);
    }

    @Test
    public void testCogroup() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A + " as (x, y);");
        pigServer.registerQuery("B = load " + B + " as (x, y);");
        pigServer.registerQuery("C = cogroup A by x, B by x;");
        Map<LogicalOperator, DataBag> derivedData = pigServer.getExamples("C");

        assertTrue(derivedData != null);
    }

    @Test
    public void testGroup() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A.toString() + " as (x, y);");
        pigServer.registerQuery("B = group A by x;");
        Map<LogicalOperator, DataBag> derivedData = pigServer.getExamples("B");

        assertTrue(derivedData != null);

    }
    
    @Test
    public void testUnion() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A.toString() + " as (x, y);");
        pigServer.registerQuery("B = load " + B.toString() + " as (x, y);");
        pigServer.registerQuery("C = union A, B;");
        Map<LogicalOperator, DataBag> derivedData = pigServer.getExamples("C");

        assertTrue(derivedData != null);
    }

}
