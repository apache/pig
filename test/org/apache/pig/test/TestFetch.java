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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;

import org.apache.pig.ExecType;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.fetch.FetchLauncher;
import org.apache.pig.backend.hadoop.executionengine.fetch.FetchOptimizer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.parser.ParserTestingUtils;
import org.apache.pig.test.utils.GenPhyOp;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFetch {

    private PigServer pigServer;

    private static File inputFile1;
    private static File inputFile2;

    private static final long SEED = 1013;
    private static final Random r = new Random(SEED);

    @BeforeClass
    public static void setUpOnce() throws Exception {

        String[] data1 = {
            "1 {(1,2,7,8,b),(1,3,3,5,a)}",
            "2 {(2,4,6,6,k)}",
            "3 {(3,7,8,9,p),(3,6,3,1,n)}",
            "5 {(5,1,1,2,c)}"
        };

        String[] data2 = {
            "1 3 a",
            "1 2 b",
            "2 4 k",
            "3 6 n",
            "3 7 p",
            "5 1 c"
        };

        inputFile1 = Util.createInputFile("tmp", "testFetchData1.txt", data1);
        inputFile2 = Util.createInputFile("tmp", "testFetchData2.txt", data2);

    }

    @Before
    public void setUp() throws Exception{
        pigServer = new PigServer(ExecType.LOCAL, new Properties());
    }

    @Test
    public void test1() throws Exception {
        String query =
            "A = load '"+Util.encodeEscape(inputFile1.getAbsolutePath()) +"' " +
                 "using PigStorage(' ') as (a:int, b: " +
                   "{t:(t1:int,t2:int,t3:int,t4:int,c:chararray)});" +
            "C = foreach A {" +
            "  temp1 = foreach b generate t1*100 as (key:int), ((t2+t3)*10) as (r:int);" +
            "  temp2 = filter temp1 by key < 400;" +
            "  temp3 = limit temp2 3;" +
            "  temp4 = foreach temp3 generate key-r as (id:int);" +
            "  temp5 = limit temp4 4;" +
            "  temp6 = filter temp5 by id < 100;" +
            "  generate flatten(temp6) as (id:int), a;" +
            "};" +
            "D = foreach C generate (" +
            "  case id % 4" +
            "    when 0 then true" +
            "    else false" +
            "  end" +
            ") as (check:boolean);";

        LogicalPlan lp = ParserTestingUtils.generateLogicalPlan(query);

        PhysicalPlan pp = ((MRExecutionEngine) pigServer.getPigContext().getExecutionEngine())
                .compile(lp, null);

        boolean planFetchable = FetchOptimizer.isPlanFetchable(pigServer.getPigContext(), pp);
        assertTrue(planFetchable);

    }

    @Test
    public void test2() throws Exception {
        Properties properties = pigServer.getPigContext().getProperties();
        properties.setProperty(PigConfiguration.PIG_TEMP_FILE_COMPRESSION_CODEC, "gz");
        properties.setProperty(PigConfiguration.PIG_ENABLE_TEMP_FILE_COMPRESSION, "true");
        properties.setProperty(PigConfiguration.PIG_TEMP_FILE_COMPRESSION_STORAGE, "tfile");

        String query =
            "A = load '"+Util.encodeEscape(inputFile1.getAbsolutePath()) +"' " +
                 "using PigStorage(' ') as (a:int, b: " +
                   "{t:(t1:int,t2:int,t3:int,t4:int,c:chararray)});" +
            "C = foreach A {" +
            "  temp1 = foreach b generate t1*100 as (key:int), ((t2+t3)*10) as (r:int);" +
            "  temp2 = filter temp1 by key < 400;" +
            "  temp3 = limit temp2 3;" +
            "  temp4 = foreach temp3 generate key-r as (id:int);" +
            "  temp5 = limit temp4 4;" +
            "  temp6 = filter temp5 by id < 100;" +
            "  generate flatten(temp6) as (id:int), a;" +
            "};" +
            "D = foreach C generate (" +
            "  case id % 4" +
            "    when 0 then true" +
            "    else false" +
            "  end" +
            ") as (check:boolean);" +
            "store D into 'out' using org.apache.pig.impl.io.TFileStorage();";

        LogicalPlan lp = ParserTestingUtils.generateLogicalPlan(query);

        PhysicalPlan pp = ((MRExecutionEngine) pigServer.getPigContext().getExecutionEngine())
                .compile(lp, null);

        boolean planFetchable = FetchOptimizer.isPlanFetchable(pigServer.getPigContext(), pp);
        assertFalse(planFetchable);

    }

    @Test
    public void test3() throws Exception {
        File scriptFile = null;
        try {
            String[] script = {
                    "A = load '"+Util.encodeEscape(inputFile1.getAbsolutePath()) +"' ",
                    "using PigStorage(' ') as (a:int, b: ",
                    "{t:(t1:int,t2:int,t3:int,t4:int,c:chararray)});",
                    "C = foreach A {",
                    "  temp1 = foreach b generate t1*100 as (key:int), ((t2+t3)*10) as (r:int);",
                    "  temp2 = filter temp1 by key < 400;",
                    "  temp3 = limit temp2 3;",
                    "  temp4 = foreach temp3 generate key-r as (id:int);",
                    "  temp5 = limit temp4 4;",
                    "  temp6 = filter temp5 by id < 100;",
                    "  generate flatten(temp6) as (id:int), a;",
                    "};",
                    "D = foreach C generate (",
                    "  case id % 4",
                    "    when 0 then true",
                    "    else false",
                    "  end",
                    ") as (check:boolean);"
            };

            scriptFile = Util.createLocalInputFile( "testFetchTest3.pig", script);
            pigServer.registerScript(scriptFile.getAbsolutePath());

            Iterator<Tuple> it = pigServer.openIterator("D");
            while (it.hasNext()) {
                assertEquals(false, it.next().get(0));
                assertEquals(true, it.next().get(0));
            }
        }
        finally {
            if (scriptFile != null) {
                scriptFile.delete();
            }
        }
    }

    @Test
    public void test4() throws Exception {
        File scriptFile = null;
        try {
            String[] script = {
                "A = load '"+Util.encodeEscape(inputFile2.getAbsolutePath()) +"' ",
                     "using PigStorage(' ') as (a:int, b:int, c:chararray);",
                "B = limit A 2;",
                "C = limit A 1;",
                "D = union A,B,C;" //introduces an implicit split operator
            };

            scriptFile = Util.createLocalInputFile( "testFetchTest4.pig", script);
            pigServer.registerScript(scriptFile.getAbsolutePath());
            pigServer.setBatchOn();

            LogicalPlan lp = TestPigStats.getLogicalPlan(pigServer);
            PhysicalPlan pp = ((MRExecutionEngine)
                    pigServer.getPigContext().getExecutionEngine()).compile(lp, null);
            boolean planFetchable = FetchOptimizer.isPlanFetchable(pigServer.getPigContext(), pp);
            assertFalse(planFetchable);

        }
        finally {
            if (scriptFile != null) {
                scriptFile.delete();
            }
        }
    }

    @Test
    public void test5() throws Exception {

        File scriptFile = null;
        try {
            String[] script = {
                "A = load '"+Util.encodeEscape(inputFile2.getAbsolutePath()) +"' ",
                "using PigStorage(' ') as (a:int, b:int, c:chararray);",
                "B = group A by a;"
            };

            scriptFile = Util.createLocalInputFile( "testFetchTest5.pig", script);
            pigServer.registerScript(scriptFile.getAbsolutePath());
            pigServer.setBatchOn();

            LogicalPlan lp = TestPigStats.getLogicalPlan(pigServer);
            PhysicalPlan pp = ((MRExecutionEngine)
                    pigServer.getPigContext().getExecutionEngine()).compile(lp, null);
            boolean planFetchable = FetchOptimizer.isPlanFetchable(pigServer.getPigContext(), pp);
            assertFalse(planFetchable);

        }
        finally {
            if (scriptFile != null) {
                scriptFile.delete();
            }
        }
    }

    @Test
    public void test6() throws Exception {
        PigContext pc = pigServer.getPigContext();

        PhysicalPlan pp = new PhysicalPlan();
        POLoad poLoad = GenPhyOp.topLoadOp();
        pp.add(poLoad);
        POLimit poLimit = new POLimit(new OperatorKey("", r.nextLong()), -1, null);
        pp.add(poLimit);
        pp.connect(poLoad, poLimit);
        POStore poStore = GenPhyOp.topStoreOp();
        pp.addAsLeaf(poStore);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        new FetchLauncher(pc).explain(pp, pc, ps, "xml");
        assertTrue(baos.toString().matches("(?si).*No MR jobs. Fetch only.*"));

    }

    @AfterClass
    public static void tearDownOnce() throws Exception {
        inputFile1.delete();
        inputFile2.delete();
    }

}
