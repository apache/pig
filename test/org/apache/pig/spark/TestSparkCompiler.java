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
package org.apache.pig.spark;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Properties;
import java.util.Random;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.io.FileUtils;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkLauncher;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkLocalExecType;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.DotSparkPrinter;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkPrinter;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.XMLSparkPrinter;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.test.Util;
import org.apache.pig.test.utils.TestHelper;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test cases to test the SparkCompiler. VERY IMPORTANT NOTE: The tests here
 * compare results with a "golden" set of outputs. In each test case here, the
 * operators generated have a random operator key which uses Java's Random
 * class. So if there is a code change which changes the number of operators
 * created in a plan, then  the "golden" file for that test case
 * need to be changed.
 */

public class TestSparkCompiler {
    private static PigContext pc;
    private static PigServer pigServer;
    private static final int MAX_SIZE = 100000;

    private enum PlanPrinter {
        TEXT,
        DOT,
        XML;

        public void doPrint(PrintStream ps, SparkOperPlan plan) throws VisitorException, ParserConfigurationException, TransformerException {
            switch (this) {
                case DOT:
                    (new DotSparkPrinter(plan, ps)).dump();
                    break;
                case XML:
                    XMLSparkPrinter printer = new XMLSparkPrinter(ps, plan);
                    printer.visit();
                    printer.closePlan();
                    break;
                case TEXT:
                default:
                    (new SparkPrinter(ps, plan)).visit();
                    break;
            }
        }
    }

    // If for some reason, the golden files need to be regenerated, set this to
    // true - THIS WILL OVERWRITE THE GOLDEN FILES - So use this with caution
    // and only for the test cases you need and are sure of.
    private boolean generate = false;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        resetFileLocalizer();
        pc = new PigContext(new SparkLocalExecType(), new Properties());
        FileUtils.deleteDirectory(new File("/tmp/pigoutput"));
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        resetFileLocalizer();
    }

    @Before
    public void setUp() throws ExecException {
        resetScope();
        pigServer = new PigServer(pc);
    }

    private void resetScope() {
        NodeIdGenerator.reset();
        PigServer.resetScope();
    }

    private static void resetFileLocalizer() {
        FileLocalizer.deleteTempFiles();
        FileLocalizer.setInitialized(false);
        // Set random seed to generate deterministic temporary paths
        FileLocalizer.setR(new Random(1331L));
    }

    @Test
    public void testStoreLoad() throws Exception {
        String query =
                "a = load 'file:///tmp/input' as (x:int, y:int);" +
                "store a into 'file:///tmp/pigoutput';" +
                "b = load 'file:///tmp/pigoutput' as (x:int, y:int);" +
                "store b into 'file:///tmp/pigoutput1';";

        run(query, "test/org/apache/pig/test/data/GoldenFiles/spark/SPARKC-LoadStore-1-text.gld", PlanPrinter.TEXT);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/spark/SPARKC-LoadStore-1-xml.gld", PlanPrinter.XML);
        run(query, "test/org/apache/pig/test/data/GoldenFiles/spark/SPARKC-LoadStore-1-dot.gld", PlanPrinter.DOT);
    }

    private void run(String query, String expectedFile, PlanPrinter planPrinter) throws Exception {
        PhysicalPlan pp = Util.buildPp(pigServer, query);
        SparkLauncher launcher = new SparkLauncher();
        pc.inExplain = true;
        SparkOperPlan sparkOperPlan = launcher.compile(pp, pc);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        planPrinter.doPrint(ps, sparkOperPlan);
        String compiledPlan = baos.toString();
        System.out.println();
        System.out.println("<<<" + compiledPlan + ">>>");

        if (generate) {
            FileOutputStream fos = new FileOutputStream(expectedFile);
            fos.write(baos.toByteArray());
            fos.close();
            return;
        }
        FileInputStream fis = new FileInputStream(expectedFile);
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        fis.close();
        String goldenPlan = new String(b, 0, len);
        if (goldenPlan.charAt(len-1) == '\n') {
            goldenPlan = goldenPlan.substring(0, len-1);
        }

        System.out.println("-------------");
        System.out.println("Golden");
        System.out.println("<<<" + goldenPlan + ">>>");
        System.out.println("-------------");

        String goldenPlanClean = Util.standardizeNewline(goldenPlan).trim();
        String compiledPlanClean = Util.standardizeNewline(compiledPlan).trim();
        assertEquals(TestHelper.sortUDFs(Util.removeSignature(goldenPlanClean)),
                TestHelper.sortUDFs(Util.removeSignature(compiledPlanClean)));
    }

}

