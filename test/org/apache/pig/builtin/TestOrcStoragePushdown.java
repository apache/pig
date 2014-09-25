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
package org.apache.pig.builtin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.pig.ExecType;
import org.apache.pig.Expression;
import org.apache.pig.Expression.OpType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.impl.PigImplConstants;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PredicatePushDownFilterExtractor;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.rules.ColumnPruneVisitor;
import org.apache.pig.test.MiniGenericCluster;
import org.apache.pig.test.Util;
import org.apache.pig.tools.pigstats.JobStats;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestOrcStoragePushdown {

    private static List<OpType> supportedOpTypes;
    private static MiniGenericCluster cluster;
    private static PigServer pigServer;
    private String query = "a = load 'foo' as (srcid:int, mrkt:chararray, dstid:int, name:chararray, " +
            "age:int, browser:map[], location:tuple(country:chararray, zip:int));";
    private OrcStorage orcStorage;

    private static final String basedir = "test/org/apache/pig/builtin/orc/";
    private static final String inpbasedir = System.getProperty("user.dir") + "/build/test/TestOrcStorage_in/";
    private static final String outbasedir = System.getProperty("user.dir") + "/build/test/TestOrcStorage_out/";
    private static String INPUT = inpbasedir + "TestOrcStorage_1";
    private static String OUTPUT1 = outbasedir + "TestOrcStorage_1";
    private static String OUTPUT2 = outbasedir + "TestOrcStorage_2";
    private static String OUTPUT3 = outbasedir + "TestOrcStorage_3";
    private static String OUTPUT4 = outbasedir + "TestOrcStorage_4";

    private static File logFile;

    @BeforeClass
    public static void oneTimeSetup() throws Exception{
        cluster = MiniGenericCluster.buildCluster();
        Util.copyFromLocalToCluster(cluster, basedir + "orc-file-11-format.orc", basedir + "orc-file-11-format.orc");
        Util.copyFromLocalToCluster(cluster, basedir + "charvarchar.orc", basedir + "charvarchar.orc");
        createInputData();

        if(Util.WINDOWS){
            OUTPUT1 = OUTPUT1.replace("\\", "/");
            OUTPUT2 = OUTPUT2.replace("\\", "/");
            OUTPUT3 = OUTPUT3.replace("\\", "/");
            OUTPUT4 = OUTPUT4.replace("\\", "/");
        }

        supportedOpTypes = new ArrayList<OpType>();
        supportedOpTypes.add(OpType.OP_EQ);
        supportedOpTypes.add(OpType.OP_NE);
        supportedOpTypes.add(OpType.OP_GT);
        supportedOpTypes.add(OpType.OP_GE);
        supportedOpTypes.add(OpType.OP_LT);
        supportedOpTypes.add(OpType.OP_LE);
        supportedOpTypes.add(OpType.OP_IN);
        supportedOpTypes.add(OpType.OP_BETWEEN);
        supportedOpTypes.add(OpType.OP_NULL);
        supportedOpTypes.add(OpType.OP_NOT);
        supportedOpTypes.add(OpType.OP_AND);
        supportedOpTypes.add(OpType.OP_OR);

        Logger logger = Logger.getLogger(ColumnPruneVisitor.class);
        logger.removeAllAppenders();
        logger.setLevel(Level.INFO);
        SimpleLayout layout = new SimpleLayout();
        logFile = File.createTempFile("log", "");
        FileAppender appender = new FileAppender(layout, logFile.toString(), false, false, 0);
        logger.addAppender(appender);
    }

    private static void createInputData() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL);

        new File(inpbasedir).mkdirs();
        new File(outbasedir).mkdirs();
        String inputTxtFile = inpbasedir + File.separator + "input.txt";
        BufferedWriter bw = new BufferedWriter(new FileWriter(inputTxtFile));
        long[] lVal = new long[] {100L, 200L, 300L};
        float[] fVal = new float[] {50.0f, 100.0f, 200.0f, 300.0f};
        double[] dVal = new double[] {1000.11, 2000.22, 3000.33};
        StringBuilder sb = new StringBuilder();
        for (int i=1; i <= 10000; i++) {
            sb.append((i > 6500 && i <= 9000) ? true : false).append("\t"); //boolean
            sb.append((i > 1000 && i < 3000) ? 1 : 5).append("\t"); //byte
            sb.append((i > 2500 && i <= 4500) ? 100 : 200).append("\t"); //short
            sb.append(i).append("\t"); //int
            sb.append(lVal[i%3]).append("\t"); //long
            sb.append(fVal[i%4]).append("\t"); //float
            sb.append((i > 2500 && i < 3500) ? dVal[i%3] : dVal[i%1]).append("\t"); //double
            sb.append((i%2 == 1 ? "" : RandomStringUtils.random(100).replaceAll("\t", " ")
                    .replaceAll("\n", " ").replaceAll("\r", " "))).append("\t"); //bytearray
            sb.append((i%2 == 0 ? "" : RandomStringUtils.random(100).replaceAll("\t", " ")
                    .replaceAll("\n", " ").replaceAll("\r", " "))).append("\t"); //string
            int year;
            if (i > 5000 && i <= 8000) { //datetime
                year = RandomUtils.nextInt(4)+2010;
            } else {
                year = RandomUtils.nextInt(10)+2000;
            }
            sb.append(new DateTime(year, RandomUtils.nextInt(12)+1,
                    RandomUtils.nextInt(28)+1, RandomUtils.nextInt(24), RandomUtils.nextInt(60),
                    DateTimeZone.UTC).toString()).append("\t"); // datetime
            String bigString;
            if (i>7500) {
                bigString = RandomStringUtils.randomNumeric(9) + "." + RandomStringUtils.randomNumeric(5);
            } else {
                bigString = "1" + RandomStringUtils.randomNumeric(9) + "." + RandomStringUtils.randomNumeric(5);
            }
            sb.append(new BigDecimal(bigString)).append("\n"); //bigdecimal
            bw.write(sb.toString());
            sb.setLength(0);
        }
        bw.close();

        // Store only 1000 rows in each row block (MIN_ROW_INDEX_STRIDE is 1000. So can't use less than that)
        pigServer.registerQuery("A = load '" + inputTxtFile + "' as (f1:boolean, f2:int, f3:int, f4:int, f5:long, f6:float, f7:double, f8:bytearray, f9:chararray, f10:datetime, f11:bigdecimal);");
        pigServer.registerQuery("store A into '" + INPUT +"' using OrcStorage('-r 1000 -s 100000');");
        Util.copyFromLocalToCluster(cluster, INPUT, INPUT);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Util.deleteDirectory(new File(inpbasedir));
        if (cluster != null) {
            Util.deleteFile(cluster, inpbasedir);
            cluster.shutDown();
        }
    }

    @Before
    public void setup() throws ExecException{
        Util.resetStateForExecModeSwitch();
        pigServer = new PigServer(ExecType.LOCAL);
        orcStorage = new OrcStorage();
    }

    @After
    public void teardown() throws IOException {
        if(pigServer != null) {
            pigServer.shutdown();
        }
        Util.deleteDirectory(new File(outbasedir));
        if (cluster != null) {
            Util.deleteFile(cluster, outbasedir);
        }
    }

    @Test
    public void testColumnPruning() throws Exception {
        Util.resetStateForExecModeSwitch();
        pigServer = new PigServer(cluster.getExecType(), cluster.getProperties());

        pigServer.registerQuery("A = load '" + basedir + "orc-file-11-format.orc' using OrcStorage();");
        ExecJob job = pigServer.store("A", OUTPUT1);
        JobStats stats = (JobStats) job.getStatistics().getJobGraph().getSources().get(0);
        long bytesWithoutPushdown = stats.getHdfsBytesRead();

        pigServer.registerQuery("PRUNE = load '" + basedir + "orc-file-11-format.orc' using OrcStorage();");
        pigServer.registerQuery("PRUNE = foreach PRUNE generate boolean1;");
        job = pigServer.store("PRUNE", OUTPUT2);
        Util.checkLogFileMessage(logFile, new String[]{"Columns pruned for PRUNE: $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13"}, true);
        stats = (JobStats) job.getStatistics().getJobGraph().getSources().get(0);
        long bytesWithPushdown = stats.getHdfsBytesRead();

        assertTrue((bytesWithoutPushdown - bytesWithPushdown) > 300000);
    }

    @Test
    public void testSimple() throws Exception {
        String q = query + "b = filter a by srcid == 10;" + "store b into 'out';";
        Expression expr = getExpressionForTest(q, Arrays.asList("srcid"));
        SearchArgument sarg = orcStorage.getSearchArgument(expr);
        assertEquals("leaf-0 = (EQUALS srcid 10)\n" +
                "expr = leaf-0", sarg.toString());
    }

    @Test
    public void testAndOr() throws Exception {
        String q = query + "b = filter a by (srcid > 10 or dstid <= 5) and name == 'foo' and mrkt is null;" + "store b into 'out';";
        Expression expr = getExpressionForTest(q, Arrays.asList("srcid", "dstid", "name", "mrkt"));
        SearchArgument sarg = orcStorage.getSearchArgument(expr);
        assertEquals("leaf-0 = (LESS_THAN_EQUALS srcid 10)\n" +
                "leaf-1 = (LESS_THAN_EQUALS dstid 5)\n" +
                "leaf-2 = (EQUALS name foo)\n" +
                "leaf-3 = (IS_NULL mrkt)\n" +
                "expr = (and (or (not leaf-0) leaf-1) leaf-2 leaf-3)", sarg.toString());
    }

    @Test
    public void testNot() throws Exception {
        String q = query + "b = filter a by srcid != 10 and mrkt is not null;" + "store b into 'out';";
        Expression expr = getExpressionForTest(q, Arrays.asList("srcid", "dstid", "name", "mrkt"));
        SearchArgument sarg = orcStorage.getSearchArgument(expr);
        assertEquals("leaf-0 = (EQUALS srcid 10)\n" +
                "leaf-1 = (IS_NULL mrkt)\n" +
                "expr = (and (not leaf-0) (not leaf-1))", sarg.toString());
    }

    @Test
    public void testBetweenExpression() throws Exception {
        // TODO: Add support for OP_BETWEEN expression type
        String q = query + "b = filter a by srcid > 10 or srcid < 20;" + "store b into 'out';";
        Expression expr = getExpressionForTest(q, Arrays.asList("srcid"));
        SearchArgument sarg = orcStorage.getSearchArgument(expr);
        assertEquals("leaf-0 = (LESS_THAN_EQUALS srcid 10)\n" +
                "leaf-1 = (LESS_THAN srcid 20)\n" +
                "expr = (or (not leaf-0) leaf-1)", sarg.toString());
    }

    @Test
    public void testInExpression() throws Exception {
        // TODO: Add support for OP_IN expression type
        String q = query + "b = filter a by srcid == 10 or srcid == 11;" + "store b into 'out';";
        Expression expr = getExpressionForTest(q, Arrays.asList("srcid"));
        SearchArgument sarg = orcStorage.getSearchArgument(expr);
        assertEquals("leaf-0 = (EQUALS srcid 10)\n" +
                "leaf-1 = (EQUALS srcid 11)\n" +
                "expr = (or leaf-0 leaf-1)", sarg.toString());
    }

    @Test
    public void testNegativeMatchesExpr() throws Exception {
        // matches operator is not a supported op type
        String q = query + "b = filter a by name matches 'foo*';" + "store b into 'out';";
        Expression expr = getExpressionForTest(q, Arrays.asList("name"));
        Assert.assertNull(expr);
        SearchArgument sarg = orcStorage.getSearchArgument(expr);
        Assert.assertNull(sarg);

        // AND in LHS/RHS
        q = query + "b = filter a by name matches 'foo*' and srcid == 10;" + "store b into 'out';";
        expr = getExpressionForTest(q, Arrays.asList("srcid", "name"));
        sarg = orcStorage.getSearchArgument(expr);
        assertEquals("leaf-0 = (EQUALS srcid 10)\n" +
                "expr = leaf-0", sarg.toString());

        q = query + "b = filter a by srcid == 10 and name matches 'foo*';" + "store b into 'out';";
        expr = getExpressionForTest(q, Arrays.asList("srcid", "name"));
        sarg = orcStorage.getSearchArgument(expr);
        assertEquals("leaf-0 = (EQUALS srcid 10)\n" +
                "expr = leaf-0", sarg.toString());

        // OR - Nothing should be pushed
        q = query + "b = filter a by name matches 'foo*' or srcid == 10;" + "store b into 'out';";
        expr = getExpressionForTest(q, Arrays.asList("srcid", "name"));
        Assert.assertNull(expr);
        sarg = orcStorage.getSearchArgument(expr);
        Assert.assertNull(sarg);
    }

    @Test
    public void testUnSupportedFields() throws Exception {
        //Struct, Map and Bag are not supported
        // TODO: Change the test to use ORCStorage to test OrcStorage.getPredicateFields()
        String q = query + "b = filter a by srcid == 10 and browser#'type' == 'IE';" +
                "store b into 'out';";
        Expression expr = getExpressionForTest(q, Arrays.asList("srcid"));
        SearchArgument sarg = orcStorage.getSearchArgument(expr);
        assertEquals("leaf-0 = (EQUALS srcid 10)\n" +
                "expr = leaf-0", sarg.toString());
    }

    @Test
    public void testPredicatePushdownBoolean() throws Exception {
        testPredicatePushdown(INPUT, "f1 == true", 2500, 1200000);
    }

    @Test
    public void testPredicatePushdownByteShort() throws Exception {
        testPredicatePushdown(INPUT, "f2 != 5 or f3 == 100", 3500, 1200000);
    }

    @Test
    public void testPredicatePushdownIntLongString() throws Exception {
        testPredicatePushdown(INPUT, "f4 >= 980 and f4 < 1010 and (f5 == 100 or f9 is not null)", 20, 1200000);
    }

    @Test
    public void testPredicatePushdownFloatDouble() throws Exception {
        testPredicatePushdown(INPUT, "f6 == 100.0 and f7 > 2000.00000001", 167, 1600000);
    }

    @Test
    public void testPredicatePushdownBigDecimal() throws Exception {
        testPredicatePushdown(INPUT, "f11 < (bigdecimal)'1000000000';", 2500, 1600000);
    }

    @Test
    public void testPredicatePushdownTimestamp() throws Exception {
        testPredicatePushdown(INPUT, "f10 >= ToDate('20100101', 'yyyyMMdd', 'UTC')", 3000, 400000);
    }

    private Expression getExpressionForTest(String query, List<String> predicateCols) throws Exception {
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        Operator op = newLogicalPlan.getSinks().get(0);
        LOFilter filter = (LOFilter) newLogicalPlan.getPredecessors(op).get(0);
        PredicatePushDownFilterExtractor filterExtractor = new PredicatePushDownFilterExtractor(filter.getFilterPlan(), predicateCols, supportedOpTypes);
        filterExtractor.visit();
        return filterExtractor.getPushDownExpression();
    }

    // For eclipse debugging
    private void testPredicatePushdownLocal(String filterStmt, int expectedRows) throws IOException {

        PigServer pigServer_disabledRule = new PigServer(ExecType.LOCAL);
        // Test with PredicatePushdownOptimizer disabled.
        HashSet<String> disabledOptimizerRules = new HashSet<String>();
        disabledOptimizerRules.add("PredicatePushdownOptimizer");
        pigServer_disabledRule.getPigContext().getProperties().setProperty(PigImplConstants.PIG_OPTIMIZER_RULES_KEY,
                ObjectSerializer.serialize(disabledOptimizerRules));
        pigServer_disabledRule.registerQuery("B = load '" + INPUT + "' using OrcStorage();");
        pigServer_disabledRule.registerQuery("C = filter B by " + filterStmt + ";");

        // Test with PredicatePushdownOptimizer enabled.
        pigServer.registerQuery("D = load '" + INPUT + "' using OrcStorage();");
        pigServer.registerQuery("E = filter D by " + filterStmt + ";");

        //Verify that results are same
        Util.checkQueryOutputs(pigServer_disabledRule.openIterator("C"), pigServer.openIterator("E"), expectedRows);
    }

    private void testPredicatePushdown(String inputFile, String filterStmt, int expectedRows, int expectedBytesReadDiff) throws IOException {

        Util.resetStateForExecModeSwitch();
        // Minicluster is required to get hdfs bytes read counter value
        pigServer = new PigServer(cluster.getExecType(), cluster.getProperties());
        PigServer pigServer_disabledRule = new PigServer(cluster.getExecType(), cluster.getProperties());

        // Test with PredicatePushdownOptimizer disabled. All 3 blocks will be read
        HashSet<String> disabledOptimizerRules = new HashSet<String>();
        disabledOptimizerRules.add("PredicatePushdownOptimizer");
        pigServer_disabledRule.getPigContext().getProperties().setProperty(PigImplConstants.PIG_OPTIMIZER_RULES_KEY,
                ObjectSerializer.serialize(disabledOptimizerRules));
        pigServer_disabledRule.registerQuery("B = load '" + inputFile + "' using OrcStorage();");
        pigServer_disabledRule.registerQuery("C = filter B by " + filterStmt + ";");
        ExecJob job = pigServer_disabledRule.store("C", OUTPUT3);
        //Util.copyFromClusterToLocal(cluster, OUTPUT3 + "/part-m-00000", OUTPUT3);
        JobStats stats = (JobStats) job.getStatistics().getJobGraph().getSources().get(0);
        assertEquals(expectedRows, stats.getRecordWrittern());
        long bytesWithoutPushdown = stats.getHdfsBytesRead();

        // Test with PredicatePushdownOptimizer enabled. Only 2 blocks should be read
        pigServer.registerQuery("D = load '" + inputFile + "' using OrcStorage();");
        pigServer.registerQuery("E = filter D by " + filterStmt + ";");
        job = pigServer.store("E", OUTPUT4);
        //Util.copyFromClusterToLocal(cluster, OUTPUT4 + "/part-m-00000", OUTPUT4);
        stats = (JobStats) job.getStatistics().getJobGraph().getSources().get(0);
        assertEquals(expectedRows, stats.getRecordWrittern());
        long bytesWithPushdown = stats.getHdfsBytesRead();

        System.out.println("bytesWithoutPushdown was " + bytesWithoutPushdown +
                " and bytesWithPushdown was " + bytesWithPushdown);
        assertTrue("BytesWithoutPushdown was " + bytesWithoutPushdown +
                " and bytesWithPushdown was " + bytesWithPushdown,
                (bytesWithoutPushdown - bytesWithPushdown) > expectedBytesReadDiff);
        // Verify that results are same
        Util.checkQueryOutputs(pigServer_disabledRule.openIterator("C"), pigServer.openIterator("E"), expectedRows);
        pigServer_disabledRule.shutdown();

    }

    @Test
    public void testPredicatePushdownChar() throws Exception {
        testPredicatePushdown(basedir + "charvarchar.orc", "$0 == 'ulysses thompson'", 18, 18000);
    }

    @Test
    public void testPredicatePushdownVarchar() throws Exception {
        testPredicatePushdown(basedir + "charvarchar.orc", "$1 == 'alice allen         '", 19, 18000);
    }
}
