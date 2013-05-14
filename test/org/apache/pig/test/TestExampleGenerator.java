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

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.Operator;
import org.apache.pig.test.utils.UDFContextTestLoaderWithSignature;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestExampleGenerator {

    static PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());

    static int MAX = 100;
    static String A, B;
    static  File fileA, fileB;
    
    @BeforeClass
    public static void oneTimeSetup() throws Exception {
        pigContext.connect();

        fileA = File.createTempFile("dataA", ".dat");
        fileB = File.createTempFile("dataB", ".dat");

        writeData(fileA);
        writeData(fileB);
     

        fileA.deleteOnExit();
        fileB.deleteOnExit();
        A = Util.encodeEscape("'" + fileA.getPath() + "'");
        B = Util.encodeEscape("'" + fileB.getPath() + "'");
        System.out.println("A : " + A + "\n" + "B : " + B);
        System.out.println("Test data created.");
    }

    private static void writeData(File dataFile) throws Exception {
        // File dataFile = File.createTempFile(name, ".dat");
        FileOutputStream dat = new FileOutputStream(dataFile);

        Random rand = new Random();

        for (int i = 0; i < MAX; i++)
            dat.write((rand.nextInt(10) + "\t" + rand.nextInt(10) + "\n")
                    .getBytes());

        dat.close();
    }

    @Test
    public void testLoad() throws Exception {

        PigServer pigserver = new PigServer(pigContext);

        String query = "A = load " + A
                + " using PigStorage() as (x : int, y : int);\n";
        pigserver.registerQuery(query);
        Map<Operator, DataBag> derivedData = pigserver.getExamples("A");

        assertNotNull(derivedData);

    }

    @Test
    public void testFilter() throws Exception {

        PigServer pigserver = new PigServer(pigContext);

        String query = "A = load " + A
                + " using PigStorage() as (x : int, y : int);\n";
        pigserver.registerQuery(query);
        query = "B = filter A by x > 10;";
        pigserver.registerQuery(query);
        Map<Operator, DataBag> derivedData = pigserver.getExamples("B");

        assertNotNull(derivedData);

    }

    @Test
    public void testFilter2() throws Exception {

        PigServer pigserver = new PigServer(pigContext);

        String query = "A = load " + A
                + " using PigStorage() as (x : int, y : int);\n";
        pigserver.registerQuery(query);
        query = "B = filter A by x > 5 AND y < 6;";
        pigserver.registerQuery(query);
        Map<Operator, DataBag> derivedData = pigserver.getExamples("B");

        assertNotNull(derivedData);
    }
    
    @Test
    public void testFilter3() throws Exception {

        PigServer pigserver = new PigServer(pigContext);

        String query = "A = load " + A
                + " using PigStorage() as (x : int, y : int);\n";
        pigserver.registerQuery(query);
        query = "B = filter A by x > 10;";
        pigserver.registerQuery(query);
        query = "C = FOREACH B GENERATE (x+1) as x1, (y+1) as y1;";
        pigserver.registerQuery(query);
        query = "D = FOREACH C GENERATE (x1+1) as x2, (y1+1) as y2;";
        pigserver.registerQuery(query);
        query = "E = DISTINCT D;";
        pigserver.registerQuery(query);
        Map<Operator, DataBag> derivedData = pigserver.getExamples("E");

        assertNotNull(derivedData);

    }
    
    @Test
    public void testForeach() throws ExecException, IOException {
        PigServer pigServer = new PigServer(pigContext);

        pigServer.registerQuery("A = load " + A
                + " using PigStorage() as (x : int, y : int);");
        pigServer.registerQuery("B = foreach A generate x + y as sum;");

        Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

        assertNotNull(derivedData);
    }
    
    //see PIG-2170
    @Test
    public void testForeachBinCondWithBooleanExp() throws ExecException, IOException {
        PigServer pigServer = new PigServer(pigContext);

        pigServer.registerQuery("A = load " + A
                + " using PigStorage() as (x : int, y : int);");
        pigServer.registerQuery("B = foreach A generate  (x + 1 > y ? 0 : 1);");

        Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

        assertNotNull(derivedData);
    }
    
    @Test
    public void testForeachWithTypeCastCounter() throws ExecException, IOException {
        PigServer pigServer = new PigServer(pigContext);
        //cast error results in counter being incremented and was resulting
        // in a NPE exception in illustrate
        pigServer.registerQuery("A = load " + A
                + " using PigStorage() as (x : int, y : int);");
        pigServer.registerQuery("B = foreach A generate x, (int)'InvalidInt';");

        Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

        assertNotNull(derivedData);
    }

    @Test
    public void testJoin() throws IOException, ExecException {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A1 = load " + A + " as (x, y);");
        pigServer.registerQuery("B1 = load " + B + " as (x, y);");

        pigServer.registerQuery("E = join A1 by x, B1 by x;");

        Map<Operator, DataBag> derivedData = pigServer.getExamples("E");

        assertNotNull(derivedData);
    }

    @Test
    public void testJoin2() throws IOException, ExecException {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A1 = load " + A + " as (x, y);");
        pigServer.registerQuery("B1 = load " + A + " as (x, y);");

        pigServer.registerQuery("E = join A1 by x, B1 by x;");

        Map<Operator, DataBag> derivedData = pigServer.getExamples("E");

        assertNotNull(derivedData);
    }

    @Test
    public void testCogroupMultipleCols() throws Exception {

        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A + " as (x, y);");
        pigServer.registerQuery("B = load " + B + " as (x, y);");
        pigServer.registerQuery("C = cogroup A by (x, y), B by (x, y);");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("C");

        assertNotNull(derivedData);
    }

    @Test
    public void testCogroup() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A + " as (x, y);");
        pigServer.registerQuery("B = load " + B + " as (x, y);");
        pigServer.registerQuery("C = cogroup A by x, B by x;");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("C");

        assertNotNull(derivedData);
    }

    @Test
    public void testGroup() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A.toString() + " as (x, y);");
        pigServer.registerQuery("B = group A by x;");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

        assertNotNull(derivedData);

    }
    
    @Test
    public void testGroup2() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A.toString() + " as (x:int, y:int);");
        pigServer.registerQuery("B = group A by x;");
        pigServer.registerQuery("C = foreach B generate group, COUNT(A);");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("C");

        assertNotNull(derivedData);

    }

    @Test
    public void testGroup3() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A.toString() + " as (x:int, y:int);");
        pigServer.registerQuery("B = FILTER A by x  > 3;");
        pigServer.registerQuery("C = group B by y;");
        pigServer.registerQuery("D = foreach C generate group, COUNT(B);");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("D");

        assertNotNull(derivedData);

    }
    
    @Test
    public void testFilterUnion() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A.toString() + " as (x:int, y:int);");
        pigServer.registerQuery("B = FILTER A by x  > 3;");
        pigServer.registerQuery("C = FILTER A by x < 3;");
        pigServer.registerQuery("D = UNION B, C;");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("D");

        assertNotNull(derivedData);

    }
    
    @Test
    public void testForEachNestedBlock() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A.toString() + " as (x:int, y:int);");
        pigServer.registerQuery("B = group A by x;");
        pigServer.registerQuery("C = foreach B { FA = filter A by y == 6; generate group, COUNT(FA);};");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("C");

        assertNotNull(derivedData);

    }

    @Test
    public void testForEachNestedBlock2() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A.toString() + " as (x:int, y:int);");
        pigServer.registerQuery("B = group A by x;");
        pigServer.registerQuery("C = foreach B { FA = filter A by y == 6; DA = DISTINCT FA; generate group, COUNT(DA);};");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("C");

        assertNotNull(derivedData);

    }
    
    @Test
    public void testUnion() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A.toString() + " as (x, y);");
        pigServer.registerQuery("B = load " + B.toString() + " as (x, y);");
        pigServer.registerQuery("C = union A, B;");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("C");

        assertNotNull(derivedData);
    }

    @Test
    public void testDistinct() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A.toString() + " as (x, y);");
        pigServer.registerQuery("B = DISTINCT A;");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

        assertNotNull(derivedData);
    }
    
    @Test
    public void testCross() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A.toString() + " as (x, y);");
        pigServer.registerQuery("B = load " + B.toString() + " as (x, y);");
        pigServer.registerQuery("C = CROSS A, B;");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("C");

        assertNotNull(derivedData);
    }
    
    @Test
    public void testLimit() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A.toString() + " as (x, y);");
        pigServer.registerQuery("B = limit A 5;");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

        assertNotNull(derivedData);
    }
    
    //see PIG-2275
    @Test
    public void testFilterWithIsNull() throws ExecException, IOException {
        PigServer pigServer = new PigServer(pigContext);

        pigServer.registerQuery("A = load " + A
                + " using PigStorage() as (x : int, y : int);");
        pigServer.registerQuery("B = filter A by x is not null;");

        Map<Operator, DataBag> derivedData = pigServer.getExamples("B");

        assertNotNull(derivedData);
    }
    
    @Test
    public void testFilterWithUDF() throws ExecException, IOException {
        PigServer pigServer = new PigServer(pigContext);

        pigServer.registerQuery("A = load " + A
                + " using PigStorage() as (x : int, y : int);");
        pigServer.registerQuery("B = group A by x;");
        pigServer.registerQuery("C = filter B by NOT IsEmpty(A.y);");

        Map<Operator, DataBag> derivedData = pigServer.getExamples("C");

        assertNotNull(derivedData);
    }

    @Test
    public void testFilterGroupCountStore() throws Exception {
        File out = File.createTempFile("testFilterGroupCountStoreOutput", "");
        out.deleteOnExit();
        out.delete();
    
        PigServer pigServer = new PigServer(pigContext);
        pigServer.setBatchOn();
        pigServer.registerQuery("A = load " + A.toString() + " as (x, y);");
        pigServer.registerQuery("B = filter A by x < 5;");
        pigServer.registerQuery("C = group B by x;");
        pigServer.registerQuery("D = foreach C generate group as x, COUNT(B) as the_count;");
        pigServer.registerQuery("store D into '" +  Util.encodeEscape(out.getAbsolutePath()) + "';");
        Map<Operator, DataBag> derivedData = pigServer.getExamples(null);
    
        assertNotNull(derivedData);
    }
    
    @Test
    public void testLoaderWithContext() throws Exception {
        PigServer pigServer = new PigServer(pigContext);
        pigServer.registerQuery("A = load " + A.toString() + " using " + UDFContextTestLoaderWithSignature.class.getName() + "('a') as (x, y);");
        Map<Operator, DataBag> derivedData = pigServer.getExamples("A");
        
        assertNotNull(derivedData);
    }

}
