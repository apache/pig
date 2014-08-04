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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestOrcStoragePushdown {

    private static List<OpType> supportedOpTypes;
    private static MiniGenericCluster cluster;
    private PigServer pigServer;
    private String query = "a = load 'foo' as (srcid:int, mrkt:chararray, dstid:int, name:chararray, " +
            "age:int, browser:map[], location:tuple(country:chararray, zip:int));";
    private OrcStorage orcStorage;

    private static final String basedir = "test/org/apache/pig/builtin/orc/";
    private static final String outbasedir = System.getProperty("user.dir") + "/build/test/TestOrcStorage/";
    private static String OUTPUT1 = outbasedir + "TestOrcStorage_1";
    private static String OUTPUT2 = outbasedir + "TestOrcStorage_2";
    private static String OUTPUT3 = outbasedir + "TestOrcStorage_3";
    private static String OUTPUT4 = outbasedir + "TestOrcStorage_4";

    private static File logFile;

    @BeforeClass
    public static void oneTimeSetup() throws IOException{
        cluster = MiniGenericCluster.buildCluster();
        Util.copyFromLocalToCluster(cluster, basedir + "orc-file-11-format.orc", basedir + "orc-file-11-format.orc");

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

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if (cluster != null) {
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
        System.out.println(sarg);
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

    // Minicluster tests which verify stats

    @Test
    public void testColumnPruneBytesRead() throws Exception {
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
    public void testPredicatePushdownBytesRead() throws Exception {
        new File(outbasedir).mkdirs();
        BufferedWriter bw = new BufferedWriter(new FileWriter(OUTPUT1));
        long[] f2 = new long[] {100L, 200L, 300L};
        for (int i=1; i <= 10000; i++) {
            bw.write(i + "\t" + f2[i%3] + "\t" + (i%2 == 0 ? "" : RandomStringUtils.random(100))+ "\n");
        }
        bw.close();

        // Store only 1000 rows in each row block (MIN_ROW_INDEX_STRIDE is 1000. So can't use less than that)
        pigServer.registerQuery("A = load '" + OUTPUT1 + "' as (f1:int, f2:long, f3:chararray);");
        pigServer.registerQuery("store A into '" + OUTPUT2 +"' using OrcStorage('-r 1000');");
        Util.copyFromLocalToCluster(cluster, OUTPUT2, OUTPUT2);

        Util.resetStateForExecModeSwitch();
        pigServer = new PigServer(cluster.getExecType(), cluster.getProperties());

        // Test with PredicatePushdownOptimizer disabled. All 3 blocks will be read
        HashSet<String> disabledOptimizerRules = new HashSet<String>();
        disabledOptimizerRules.add("PredicatePushdownOptimizer");
        pigServer.getPigContext().getProperties().setProperty(PigImplConstants.PIG_OPTIMIZER_RULES_KEY,
                ObjectSerializer.serialize(disabledOptimizerRules));
        pigServer.registerQuery("B = load '" + OUTPUT2 + "' using OrcStorage();");
        pigServer.registerQuery("C = filter B by f1 > 980 and f1 < 1010 and (f2 == 100 or f3 is not null);");
        ExecJob job = pigServer.store("C", OUTPUT3);
        JobStats stats = (JobStats) job.getStatistics().getJobGraph().getSources().get(0);
        assertEquals(20, stats.getRecordWrittern());
        long bytesWithoutPushdown = stats.getHdfsBytesRead();

        // Test with PredicatePushdownOptimizer enabled. Only 2 blocks should be read
        pigServer.getPigContext().getProperties().remove(PigImplConstants.PIG_OPTIMIZER_RULES_KEY);
        pigServer.registerQuery("D = load '" + OUTPUT2 + "' using OrcStorage();");
        pigServer.registerQuery("E = filter D by f1 > 980 and f1 < 1010 and (f2 == 100 or f3 is not null);");
        job = pigServer.store("E", OUTPUT4);
        stats = (JobStats) job.getStatistics().getJobGraph().getSources().get(0);
        assertEquals(20, stats.getRecordWrittern());
        long bytesWithPushdown = stats.getHdfsBytesRead();

        assertTrue((bytesWithoutPushdown - bytesWithPushdown) > 300000);
        //Verify that results are same
        Util.checkQueryOutputs(pigServer.openIterator("C"), pigServer.openIterator("E"));
    }

    private Expression getExpressionForTest(String query, List<String> predicateCols) throws Exception {
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        Operator op = newLogicalPlan.getSinks().get(0);
        LOFilter filter = (LOFilter) newLogicalPlan.getPredecessors(op).get(0);
        PredicatePushDownFilterExtractor filterExtractor = new PredicatePushDownFilterExtractor(filter.getFilterPlan(), predicateCols, supportedOpTypes);
        filterExtractor.visit();
        return filterExtractor.getPushDownExpression();
    }

}
