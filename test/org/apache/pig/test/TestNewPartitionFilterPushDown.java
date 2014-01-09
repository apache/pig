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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import junit.framework.AssertionFailedError;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.ExecType;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.FilterExtractor;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.BinaryExpression;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.DereferenceExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.IsNullExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.MapLookupExpression;
import org.apache.pig.newplan.logical.expression.OrExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LogToPhyTranslationVisitor;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.rules.LoadTypeCastInserter;
import org.apache.pig.newplan.logical.rules.PartitionFilterOptimizer;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.parser.ParserException;
import org.junit.Assert;
import org.junit.Test;

/**
 * unit tests to test extracting new partition filter conditions out of the filter
 * condition in the filter following a load which talks to metadata system (.i.e.
 * implements {@link LoadMetadata})
 */
public class TestNewPartitionFilterPushDown {
    static PigContext pc = new PigContext(ExecType.LOCAL, new Properties());
    String query = "a = load 'foo' as (srcid:int, mrkt:chararray, dstid:int, name:chararray, " +
            "age:int, browser:map[], location:tuple(country:chararray, zip:int));";
    String loadquery = "a = load 'foo' using "
            + TestLoader.class.getName() +
            "('srcid:int, mrkt:chararray, dstid:int, name:chararray, " +
            "age:int, browser:map[], location:tuple(country:chararray, zip:int)', " +
            "'%s');";
    String query2 = String.format(loadquery, "srcid,dstid");
    String query3 = String.format(loadquery, "srcid");

    /**
     * test case where there is a single expression on partition columns in
     * the filter expression along with an expression on non partition column
     * @throws Exception
     */
    @Test
    public void testSimpleMixed() throws Exception {
        String q = query + "b = filter a by srcid == 10 and name == 'foo';" + "store b into 'out';";
        test(q, Arrays.asList("srcid"), "(srcid == 10)", "(name == 'foo')");
    }

    /**
     * test case where filter does not contain any condition on partition cols
     * @throws Exception
     */
    @Test
    public void testNoPartFilter() throws Exception {
        String q = query + "b = filter a by age == 20 and name == 'foo';" + "store b into 'out';";
        test(q, Arrays.asList("srcid"), null,
                "((age == 20) and (name == 'foo'))");
    }

    /**
     * test case where filter only contains condition on partition cols
     * @throws Exception
     */
    @Test
    public void testOnlyPartFilter1() throws Exception {
        String q = query + "b = filter a by srcid > 20 and mrkt == 'us';" + "store b into 'out';";
        test(q, Arrays.asList("srcid", "mrkt"),
                "((srcid > 20) and (mrkt == 'us'))", null);

    }

    /**
     * test case where filter only contains condition on partition cols
     * @throws Exception
     */
    @Test
    public void testOnlyPartFilter2() throws Exception {
        String q = query + "b = filter a by mrkt == 'us';" + "store b into 'out';";
        test(q, Arrays.asList("srcid", "mrkt"),
                "(mrkt == 'us')", null);

    }

    /**
     * test case where filter only contains condition on partition cols
     * @throws Exception
     */
    @Test
    public void testOnlyPartFilter3() throws Exception {
        String q = query + "b = filter a by srcid == 20 or mrkt == 'us';" + "store b into 'out';";
        test(q, Arrays.asList("srcid", "mrkt"),
                "((srcid == 20) or (mrkt == 'us'))", null);

    }

    /**
     * test case where filter has both conditions on partition cols and non
     * partition cols and the filter condition will be split to extract the
     * conditions on partition columns
     */
    @Test
    public void testMixed1() throws Exception {
        String q = query + "b = filter a by " +
                "(age < 20 and  mrkt == 'us') and (srcid == 10 and " +
                "name == 'foo');" + "store b into 'out';";
        test(q, Arrays.asList("srcid", "mrkt"),
                "((mrkt == 'us') and (srcid == 10))",
                "((age < 20) and (name == 'foo'))");
    }


    /**
     * test case where filter has both conditions on partition cols and non
     * partition cols and the filter condition will be split to extract the
     * conditions on partition columns
     */
    @Test
    public void testMixed2() throws Exception {
        String q = query + "b = filter a by " +
                "(age >= 20 and  mrkt == 'us') and (srcid == 10 and " +
                "dstid == 15);" + "store b into 'out';";
        test(q, Arrays.asList("srcid", "dstid", "mrkt"),
                "((mrkt == 'us') and ((srcid == 10) and (dstid == 15)))",
                "(age >= 20)");
    }

    /**
     * test case where filter has both conditions on partition cols and non
     * partition cols and the filter condition will be split to extract the
     * conditions on partition columns
     */
    @Test
    public void testMixed3() throws Exception {
        String q = query + "b = filter a by " +
                "age >= 20 and  mrkt == 'us' and srcid == 10;" + "store b into 'out';";
        test(q, Arrays.asList("srcid", "dstid", "mrkt"),
                "((mrkt == 'us') and (srcid == 10))", "(age >= 20)");
    }

    /**
     * test case where filter has both conditions on partition cols and non
     * partition cols and the filter condition will be split to extract the
     * conditions on partition columns - this testcase also has a condition
     * based on comparison of two partition columns
     */
    @Test
    public void testMixed4() throws Exception {
        String q = query + "b = filter a by " +
                "age >= 20 and  mrkt == 'us' and name == 'foo' and " +
                "srcid == dstid;" + "store b into 'out';";
        test(q, Arrays.asList("srcid", "dstid", "mrkt"),
                "((mrkt == 'us') and (srcid == dstid))",
                "((age >= 20) and (name == 'foo'))");
    }

    /**
     * test case where filter has both conditions on partition cols and non
     * partition cols and the filter condition will be split to extract the
     * conditions on partition columns -
     * This testcase has two partition col conditions  with OR +  non parition
     * col conditions
     */
    @Test
    public void testMixed5() throws Exception {
        String q = query + "b = filter a by " +
                "(srcid == 10 or mrkt == 'us') and name == 'foo' and " +
                "dstid == 30;" + "store b into 'out';";
        test(q, Arrays.asList("srcid", "dstid", "mrkt"),
                "(((srcid == 10) or (mrkt == 'us')) and (dstid == 30))",
                "(name == 'foo')");
    }

    /**
     * test case where filter has both conditions on partition cols and non
     * partition cols and the filter condition will be split to extract the
     * conditions on partition columns -
     * This testcase has two partition col conditions  with OR +  non parition
     * col conditions
     */
    @Test
    public void testMixed6() throws Exception {
        String q = query + "b = filter a by " +
                "dstid == 30 and (srcid == 10 or mrkt == 'us') and name == 'foo';" + "store b into 'out';";
        test(q, Arrays.asList("srcid", "dstid", "mrkt"),
                "((dstid == 30) and ((srcid == 10) or (mrkt == 'us')))",
                "(name == 'foo')");
    }

    /**
     * test case where filter has both conditions on partition cols and non
     * partition cols and the filter condition will be split to extract the
     * conditions on partition columns. This testcase also tests arithmetic
     * in partition column conditions
     */
    @Test
    public void testMixedArith() throws Exception {
        String q = query + "b = filter a by " +
                "mrkt == 'us' and srcid * 10 == 150 + 20 and age != 15;" + "store b into 'out';";
        test(q, Arrays.asList("srcid", "dstid", "mrkt"),
                "((mrkt == 'us') and ((srcid * 10) == (150 + 20)))",
                "(age != 15)");
    }

    /**
     * test case where there is a single expression on partition columns in the
     * filter expression along with an expression on non partition column of
     * type map
     * @throws Exception
     */
    @Test
    public void testMixedNonPartitionTypeMap() throws Exception {
        String q = query + "b = filter a by srcid == 10 and browser#'type' == 'IE';" +
                "store b into 'out';";
        test(q, Arrays.asList("srcid"), "(srcid == 10)", "(browser#'type' == IE)", true);

        q = query + "b = filter a by srcid == 10 and browser#'type' == 'IE' and " +
                "browser#'version'#'major' == '8.0';" + "store b into 'out';";
        test(q, Arrays.asList("srcid"), "(srcid == 10)", "((browser#'type' == IE) and " +
                "(browser#'version'#'major' == 8.0))", true);


    }

    @Test
    public void testMixedNonPartitionTypeMapComplex() throws Exception {
        TestLoader.partFilter = null;
        String q = "a = load 'foo' using "
                + TestLoader.class.getName() +
                "('srcid:int, mrkt:chararray, dstid:int, name:chararray, " +
                "age:int, browser:map[], location:tuple(country:chararray, zip:int)', " +
                "'srcid,mrkt');" +
                "b = filter a by srcid == 10 and mrkt > '1' and mrkt < '5';" +
                "c = filter b by browser#'type' == 'IE';" + "store c into 'out';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( q );

        Assert.assertEquals("checking partition filter:",
                "(((srcid == 10) and (mrkt > '1')) and (mrkt < '5'))",
                TestLoader.partFilter.toString());
        Operator op = newLogicalPlan.getSinks().get(0);
        LOFilter filter = (LOFilter)newLogicalPlan.getPredecessors(op).get(0);

        String actual =
                getTestExpression((LogicalExpression) filter.getFilterPlan().
                        getSources().get(0)).toString();
        Assert.assertEquals("checking trimmed filter expression:",
                "(browser#'type' == IE)", actual);
    }

    /**
     * test case where there is a single expression on partition columns in the
     * filter expression along with an expression on non partition column of
     * type tuple
     * @throws Exception
     */
    @Test
    public void testMixedNonPartitionTypeTuple() throws Exception {
        String q = query + "b = filter a by srcid == 10 and location.country == 'US';" +
                "store b into 'out';";
        test(q, Arrays.asList("srcid"), "(srcid == 10)", "(location.$0 == US)", true);
    }

    @Test
    public void testAndORConditionPartitionKeyCol() throws Exception {
        // Case of AND and OR
        String q = "b = filter a by (srcid == 10 and dstid == 5) " +
                "or (srcid == 11 and dstid == 6) or (srcid == 12 and dstid == 7);" +
                "store b into 'out';";
        test(query + q, Arrays.asList("srcid", "dstid"),
                "((((srcid == 10) and (dstid == 5)) " +
                        "or ((srcid == 11) and (dstid == 6))) or ((srcid == 12) and (dstid == 7)))",
                        null);
        // TODO fix following test after PIG-3465
        //testFull(query2+ q, "((((srcid == 10) and (dstid == 5)) " +
        //        "or ((srcid == 11) and (dstid == 6))) or ((srcid == 12) and (dstid == 7)))", null, false);

        // Additional filter on non-partition key column
        q = "b = filter a by ((srcid == 10 and dstid == 5) " +
                "or (srcid == 11 and dstid == 6) or (srcid == 12 and dstid == 7)) and mrkt == 'US';" +
                "store b into 'out';";
        test(query + q, Arrays.asList("srcid", "dstid"),
                "((((srcid == 10) and (dstid == 5)) " +
                        "or ((srcid == 11) and (dstid == 6))) or ((srcid == 12) and (dstid == 7)))",
                "(mrkt == 'US')");
        testFull(query2+q, "((((srcid == 10) and (dstid == 5)) " +
                "or ((srcid == 11) and (dstid == 6))) or ((srcid == 12) and (dstid == 7)))",
                "(mrkt == 'US')", false);

        // Additional filter on partition key column that cannot be pushed
        q = query +
                "b = filter a by ((srcid == 10 and dstid == 5) " +
                "or (srcid == 11 and dstid == 6) or (srcid == 12 and dstid == 7)) and srcid is null;" +
                "store b into 'out';";
        test(q, Arrays.asList("srcid", "dstid"),
                "((((srcid == 10) and (dstid == 5)) " +
                        "or ((srcid == 11) and (dstid == 6))) or ((srcid == 12) and (dstid == 7)))",
                        "(srcid is null)", true);

        // partition key col but null condition which should not become part of
        // the pushed down filter
        q = query + "b = filter a by (srcid is null and dstid == 5) " +
                "or (srcid == 11 and dstid == 6) or (srcid == 12 and dstid == 7);" +
                "store b into 'out';";
        test(q, Arrays.asList("srcid", "dstid"),
                "(((dstid == 5) or ((srcid == 11) and (dstid == 6))) or ((srcid == 12) and (dstid == 7)))",
                "(((srcid is null) or ((srcid == 11) and (dstid == 6))) or ((srcid == 12) and (dstid == 7)))", true);

        // Case of AND of ORs
        q = query +
                "b = filter a by (mrkt == 'US' or mrkt == 'UK') and (srcid == 11 or srcid == 10);" +
                "store b into 'out';";
        test(q, Arrays.asList("srcid", "mrkt"),
                "(((mrkt == 'US') or (mrkt == 'UK')) and ((srcid == 11) or (srcid == 10)))", null);
        test(q, Arrays.asList("srcid"),
                "((srcid == 11) or (srcid == 10))", "((mrkt == 'US') or (mrkt == 'UK'))");
    }

    @Test
    public void testAndORConditionMixedCol() throws Exception {
        // Case of AND and OR with partition key and non-partition key columns
        String q = "b = filter a by (srcid == 10 and dstid == 5) " +
                "or (srcid == 11 and dstid == 6) or (srcid == 12 and dstid == 7) " +
                "or (srcid == 13 and dstid == 8);" +
                "store b into 'out';";
        test(query + q, Arrays.asList("srcid"), "((((srcid == 10) or (srcid == 11)) or (srcid == 12)) or (srcid == 13))",
                "(((((srcid == 10) or (srcid == 11)) or (srcid == 12)) or (dstid == 8)) " +
                        "and ((((((srcid == 10) or (srcid == 11)) or (dstid == 7)) and " +
                        "(((((srcid == 10) or (dstid == 6)) and (((dstid == 5) or (srcid == 11)) " +
                        "and ((dstid == 5) or (dstid == 6)))) or " +
                        "(srcid == 12)) and ((((srcid == 10) or (dstid == 6)) " +
                        "and (((dstid == 5) or (srcid == 11)) and ((dstid == 5) or (dstid == 6)))) or (dstid == 7)))) or (srcid == 13)) " +
                        "and (((((srcid == 10) or (srcid == 11)) or (dstid == 7)) and (((((srcid == 10) or (dstid == 6)) " +
                        "and (((dstid == 5) or (srcid == 11)) and ((dstid == 5) or (dstid == 6)))) or (srcid == 12)) " +
                "and ((((srcid == 10) or (dstid == 6)) and (((dstid == 5) or (srcid == 11)) and ((dstid == 5) or (dstid == 6)))) or (dstid == 7)))) or (dstid == 8))))");
        //
        //testFull(query3 + q, "((((srcid == 10) or (srcid == 11)) or (srcid == 12)) or (srcid == 13))", "", false);

        // Additional filter on a partition key column
        q = query +
                "b = filter a by ((srcid == 10 and dstid == 5) or (srcid == 11 and dstid == 6) " +
                "or (srcid == 12 and dstid == 7)) and mrkt == 'US';" +
                "store b into 'out';";
        test(q, Arrays.asList("srcid", "mrkt"), "((((srcid == 10) or (srcid == 11)) or (srcid == 12)) and (mrkt == 'US'))",
                "((((srcid == 10) or (srcid == 11)) or (dstid == 7)) and (((((srcid == 10) or (dstid == 6)) and (((dstid == 5) or (srcid == 11)) " +
                        "and ((dstid == 5) or (dstid == 6)))) or (srcid == 12)) and ((((srcid == 10) or (dstid == 6)) and (((dstid == 5) or (srcid == 11)) " +
                "and ((dstid == 5) or (dstid == 6)))) or (dstid == 7))))");

        q = query + "b = filter a by (mrkt == 'US' or mrkt == 'UK') and " +
                "((srcid == 10 and dstid == 5) or (srcid == 11 and dstid == 6) " +
                "or (srcid == 12 and dstid == 7));" +
                "store b into 'out';";
        test(q, Arrays.asList("srcid", "mrkt"), "(((mrkt == 'US') or (mrkt == 'UK')) and (((srcid == 10) or (srcid == 11)) or (srcid == 12)))",
                "((((srcid == 10) or (srcid == 11)) or (dstid == 7)) and (((((srcid == 10) or (dstid == 6)) and (((dstid == 5) or (srcid == 11)) and ((dstid == 5) or (dstid == 6)))) or (srcid == 12)) and ((((srcid == 10) or (dstid == 6)) and (((dstid == 5) or (srcid == 11)) and ((dstid == 5) or (dstid == 6)))) or (dstid == 7))))");

        // Additional filter on a non-partition key column
        q = query +
                "b = filter a by ((srcid == 10 and dstid == 5) or (srcid == 11 and dstid == 6) " +
                "or (srcid == 12 and dstid == 7)) and mrkt == 'US';" +
                "store b into 'out';";
        test(q, Arrays.asList("srcid"), "(((srcid == 10) or (srcid == 11)) or (srcid == 12))",
                "(((((srcid == 10) or (srcid == 11)) or (dstid == 7)) " +
                        "and (((((srcid == 10) or (dstid == 6)) and " +
                        "(((dstid == 5) or (srcid == 11)) and ((dstid == 5) or (dstid == 6)))) or (srcid == 12)) " +
                        "and ((((srcid == 10) or (dstid == 6)) and (((dstid == 5) or (srcid == 11)) " +
                "and ((dstid == 5) or (dstid == 6)))) or (dstid == 7)))) and (mrkt == 'US'))");

        // Case of OR and AND
        q = query +
                "b = filter a by (mrkt == 'US' or mrkt == 'UK') and " +
                "(srcid == 11 or srcid == 10) and (dstid == 5 or dstid == 6);" +
                "store b into 'out';";
        test(q, Arrays.asList("srcid"),
                "((srcid == 11) or (srcid == 10))",
                "(((mrkt == 'US') or (mrkt == 'UK')) and ((dstid == 5) or (dstid == 6)))");
        test(q, Arrays.asList("mrkt"),
                "((mrkt == 'US') or (mrkt == 'UK'))",
                "(((srcid == 11) or (srcid == 10)) and ((dstid == 5) or (dstid == 6)))");
    }

    private LogicalPlan migrateAndOptimizePlan(String query) throws Exception {
        PigServer pigServer = new PigServer( pc );
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        PlanOptimizer optimizer = new MyPlanOptimizer( newLogicalPlan, 3 );
        optimizer.optimize();
        return newLogicalPlan;
    }

    private void testFull(String q, String partFilter, String filterExpr, boolean unsupportedExpr) throws Exception {
        TestLoader.partFilter = null;
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( q );

        if (partFilter != null) {
            Assert.assertEquals("checking partition filter:",
                    partFilter,
                    TestLoader.partFilter.toString());
        } else {
            Assert.assertTrue(TestLoader.partFilter == null);
        }

        if (filterExpr != null) {
            Operator op = newLogicalPlan.getSinks().get(0);
            LOFilter filter = (LOFilter)newLogicalPlan.getPredecessors(op).get(0);

            String actual =
                    FilterExtractor.getExpression((LogicalExpression) filter.getFilterPlan().
                            getSources().get(0)).toString();
            Assert.assertEquals("checking trimmed filter expression:",
                    filterExpr, actual);
        } else {
            Iterator<Operator> it = newLogicalPlan.getOperators();
            while( it.hasNext() ) {
                Assert.assertFalse("Checking that filter has been removed since it contained" +
                        " only conditions on partition cols:",
                        (it.next() instanceof LOFilter));
            }
        }

        // Test that the filtered plan can be translated to physical plan (PIG-3657)
        LogToPhyTranslationVisitor translator = new LogToPhyTranslationVisitor(newLogicalPlan);
        translator.visit();
    }

    /**
     * Test that pig sends correct partition column names in setPartitionFilter
     * when the user has a schema in the load statement which renames partition
     * columns
     * @throws Exception
     */
    @Test
    public void testColNameMapping1() throws Exception {
        String q = "a = load 'foo' using "
                + TestLoader.class.getName() +
                "('srcid:int, mrkt:chararray, dstid:int, name:chararray, age:int', " +
                "'srcid,mrkt') as (f1, f2, f3, f4, f5);" +
                "b = filter a by " +
                "(f5 >= 20 and f2 == 'us') and (f1 == 10 and f3 == 15);" +
                "store b into 'out';";

        testFull(q, "((mrkt == 'us') and (srcid == 10))", "((f5 >= 20) and (f3 == 15))", false);
    }

    /**
     * Test that pig sends correct partition column names in setPartitionFilter
     * when the user has a schema in the load statement which renames partition
     * columns - in this test case there is no condition on partition columns
     * - so setPartitionFilter() should not be called and the filter condition
     * should remain as is.
     * @throws Exception
     */
    @Test
    public void testColNameMapping2() throws Exception {
        String q = "a = load 'foo' using "
                + TestLoader.class.getName() +
                "('srcid:int, mrkt:chararray, dstid:int, name:chararray, age:int', " +
                "'srcid') as (f1, f2, f3, f4, f5);" +
                "b = filter a by " +
                "f5 >= 20 and f2 == 'us' and f3 == 15;" +
                "store b into 'out';";

        testFull(q, null, "(((f5 >= 20) and (f2 == 'us')) and (f3 == 15))", false);
    }

    /**
     * Test that pig sends correct partition column names in setPartitionFilter
     * when the user has a schema in the load statement which renames partition
     * columns - in this test case the filter only has conditions on partition
     * columns
     * @throws Exception
     */
    @Test
    public void testColNameMapping3() throws Exception {
        String query = "a = load 'foo' using "
                + TestLoader.class.getName() +
                "('srcid:int, mrkt:chararray, dstid:int, name:chararray, age:int', " +
                "'srcid,mrkt,dstid,age') as (f1, f2, f3, f4, f5);" +
                "b = filter a by " +
                "(f5 >= 20 or f2 == 'us') and (f1 == 10 and f3 == 15);" +
                "store b into 'out';";

        testFull(query, "(((age >= 20) or (mrkt == 'us')) and ((srcid == 10) and " +
                "(dstid == 15)))", null, false);

    }

    /**
     * Test that pig sends correct partition column names in setPartitionFilter
     * when the user has a schema in the load statement which renames partition
     * columns - in this test case the schema in load statement is a prefix
     * (with columns renamed) of the schema returned by
     * {@link LoadMetadata#getSchema(String, Configuration)}
     * @throws Exception
     */
    @Test
    public void testColNameMapping4() throws Exception {
        String q = "a = load 'foo' using "
                + TestLoader.class.getName() +
                "('srcid:int, mrkt:chararray, dstid:int, name:chararray, age:int', " +
                "'srcid,mrkt') as (f1:int, f2:chararray, f3:int, name:chararray, age:int);" +
                "b = filter a by " +
                "(age >= 20 and f2 == 'us') and (f1 == 10 and f3 == 15);" + "store b into 'out';";

        testFull(q, "((mrkt == 'us') and (srcid == 10))", "((age >= 20) and (f3 == 15))", false);
    }

    /**
     * Test PIG-1267
     * @throws Exception
     */
    @Test
    public void testColNameMapping5() throws Exception {
        TestLoader.partFilter = null;
        String q = "a = load 'foo' using "
                + TestLoader.class.getName() +
                "('mrkt:chararray, a1:chararray, a2:chararray, srcid:int, bcookie:chararray', " +
                "'srcid');" +
                "b = load 'bar' using "
                + TestLoader.class.getName() +
                "('dstid:int, b1:int, b2:int, srcid:int, bcookie:chararray, mrkt:chararray'," +
                "'srcid');" +
                "a1 = filter a by srcid == 10;" +
                "b1 = filter b by srcid == 20;"+
                "c = join a1 by bcookie, b1 by bcookie;" +
                "d = foreach c generate $4 as bcookie:chararray, " +
                "$5 as dstid:int, $0 as mrkt:chararray;" +
                "store d into 'out';";

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( q );

        String partFilter = TestLoader.partFilter.toString();
        Assert.assertTrue( "(srcid == 20)".equals( partFilter ) ||  "(srcid == 10)".equals( partFilter ) );

        int counter = 0;
        Iterator<Operator> iter = newLogicalPlan.getOperators();
        while (iter.hasNext()) {
            Assert.assertTrue(!(iter.next() instanceof LOFilter));
            counter++;
        }
        Assert.assertEquals(counter, 5);
    }

    /**
     * Test PIG-2778 Add matches operator to predicate pushdown
     * @throws Exception
     */
    @Test
    public void testMatchOpPushDown() throws Exception {
        // regexp condition on a partition col
        String q = query + "b = filter a by name matches 'foo*';" + "store b into 'out';";
        test(q, Arrays.asList("name"), "(name matches 'foo*')", null);

        // regexp condition on a non-partition col
        q = query + "b = filter a by name matches 'foo*';" + "store b into 'out';";
        test(q, Arrays.asList("srcid"), null, "(name matches 'foo*')");
    }

    /**
     * Test PIG-3395 Large filter expression makes Pig hang
     * @throws Exception
     */
    @Test
    public void testLargeAndOrCondition() throws Exception {
        String q = query + "b = filter a by " +
                "(srcid == 1 and mrkt == '2' and dstid == 3) " +
                "or (srcid == 4 and mrkt == '5' and dstid == 6) " +
                "or (srcid == 7 and mrkt == '8' and dstid == 9) " +
                "or (srcid == 10 and mrkt == '11' and dstid == 12) " +
                "or (srcid == 13 and mrkt == '14' and dstid == 15) " +
                "or (srcid == 16 and mrkt == '17' and dstid == 18) " +
                "or (srcid == 19 and mrkt == '20' and dstid == 21) " +
                "or (srcid == 22 and mrkt == '23' and dstid == 24) " +
                "or (srcid == 25 and mrkt == '26' and dstid == 27) " +
                "or (srcid == 28 and mrkt == '29' and dstid == 30) " +
                "or (srcid == 31 and mrkt == '32' and dstid == 33) " +
                "or (srcid == 34 and mrkt == '35' and dstid == 36) " +
                "or (srcid == 37 and mrkt == '38' and dstid == 39) " +
                "or (srcid == 40 and mrkt == '41' and dstid == 42) " +
                "or (srcid == 43 and mrkt == '44' and dstid == 45) " +
                "or (srcid == 46 and mrkt == '47' and dstid == 48) " +
                "or (srcid == 49 and mrkt == '50' and dstid == 51) " +
                "or (srcid == 52 and mrkt == '53' and dstid == 54) " +
                "or (srcid == 55 and mrkt == '56' and dstid == 57) " +
                "or (srcid == 58 and mrkt == '59' and dstid == 60) " +
                "or (srcid == 61 and mrkt == '62' and dstid == 63) " +
                "or (srcid == 64 and mrkt == '65' and dstid == 66) " +
                "or (srcid == 67 and mrkt == '68' and dstid == 69);" +
                "store b into 'out';";
        test(q, Arrays.asList("srcid", "mrkt", "dstid"),
                "(((((((((((((((((((((((((srcid == 1) and (mrkt == '2')) and (dstid == 3)) " +
                        "or (((srcid == 4) and (mrkt == '5')) and (dstid == 6))) " +
                        "or (((srcid == 7) and (mrkt == '8')) and (dstid == 9))) " +
                        "or (((srcid == 10) and (mrkt == '11')) and (dstid == 12))) " +
                        "or (((srcid == 13) and (mrkt == '14')) and (dstid == 15))) " +
                        "or (((srcid == 16) and (mrkt == '17')) and (dstid == 18))) " +
                        "or (((srcid == 19) and (mrkt == '20')) and (dstid == 21))) " +
                        "or (((srcid == 22) and (mrkt == '23')) and (dstid == 24))) " +
                        "or (((srcid == 25) and (mrkt == '26')) and (dstid == 27))) " +
                        "or (((srcid == 28) and (mrkt == '29')) and (dstid == 30))) " +
                        "or (((srcid == 31) and (mrkt == '32')) and (dstid == 33))) " +
                        "or (((srcid == 34) and (mrkt == '35')) and (dstid == 36))) " +
                        "or (((srcid == 37) and (mrkt == '38')) and (dstid == 39))) " +
                        "or (((srcid == 40) and (mrkt == '41')) and (dstid == 42))) " +
                        "or (((srcid == 43) and (mrkt == '44')) and (dstid == 45))) " +
                        "or (((srcid == 46) and (mrkt == '47')) and (dstid == 48))) " +
                        "or (((srcid == 49) and (mrkt == '50')) and (dstid == 51))) " +
                        "or (((srcid == 52) and (mrkt == '53')) and (dstid == 54))) " +
                        "or (((srcid == 55) and (mrkt == '56')) and (dstid == 57))) " +
                        "or (((srcid == 58) and (mrkt == '59')) and (dstid == 60))) " +
                        "or (((srcid == 61) and (mrkt == '62')) and (dstid == 63))) " +
                        "or (((srcid == 64) and (mrkt == '65')) and (dstid == 66))) " +
                        "or (((srcid == 67) and (mrkt == '68')) and (dstid == 69)))",
                        null);
    }

    // Or with non partition filter condition should not push projection
    @Test
    public void testOrWithNonPartitionCondition() throws Exception {
        String q = query + "b = filter a by " +
                "(srcid == 1 and mrkt == '2' and dstid == 3) " +
                "or (srcid == 4 and mrkt == '5' and dstid == 6) " +
                "or (srcid == 7 and mrkt == '8' and dstid == 9) " +
                "or (name is null);" +
                "store b into 'out';";
        negativeTest(q, Arrays.asList("srcid", "mrkt", "dstid"));
    }

    // PIG-3657
    @Test
    public void testFilteredPlanWithLogToPhyTranslator() throws Exception {
        String q = "a = load 'foo' using " + TestLoader.class.getName() +
                "('srcid:int, mrkt:chararray', 'srcid') as (f1, f2);" +
                "b = filter a by (f1 < 5 or (f1 == 10 and f2 == 'UK'));" +
                "store b into 'out';";
        testFull(q, "((srcid < 5) or (srcid == 10))", "((f1 < 5) or (f2 == 'UK'))", false);
    }

    //// helper methods ///////
    private FilterExtractor test(String query, List<String> partitionCols,
            String expPartFilterString, String expFilterString)
                    throws Exception {
        return test(query, partitionCols, expPartFilterString, expFilterString, false);
    }

    private FilterExtractor test(String query, List<String> partitionCols,
            String expPartFilterString, String expFilterString, boolean unsupportedExpression)
                    throws Exception {
        PigServer pigServer = new PigServer( pc );
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        Operator op = newLogicalPlan.getSinks().get(0);
        LOFilter filter = (LOFilter)newLogicalPlan.getPredecessors(op).get(0);
        FilterExtractor pColExtractor = new FilterExtractor(
                filter.getFilterPlan(), partitionCols);
        pColExtractor.visit();

        if(expPartFilterString == null) {
            Assert.assertEquals("Checking partition column filter:", null,
                    pColExtractor.getPColCondition());
        } else  {
            Assert.assertEquals("Checking partition column filter:",
                    expPartFilterString,
                    pColExtractor.getPColCondition().toString());
        }

        if (expFilterString == null) {
            Assert.assertTrue("Check that filter can be removed:",
                    pColExtractor.isFilterRemovable());
        } else {
            if (unsupportedExpression) {
                String actual = getTestExpression((LogicalExpression)pColExtractor.getFilteredPlan().getSources().get(0)).toString();
                Assert.assertEquals("checking trimmed filter expression:", expFilterString, actual);
            } else {
                String actual = FilterExtractor.getExpression((LogicalExpression)pColExtractor.getFilteredPlan().getSources().get(0)).toString();
                Assert.assertEquals("checking trimmed filter expression:", expFilterString, actual);
            }
        }
        return pColExtractor;
    }

    // The filter cannot be pushed down unless it meets certain conditions. In
    // that case, PColExtractor.getPColCondition() should return null.
    private void negativeTest(String query, List<String> partitionCols) throws Exception {
        PigServer pigServer = new PigServer( pc );
        LogicalPlan newLogicalPlan = Util.buildLp(pigServer, query);
        Operator op = newLogicalPlan.getSinks().get(0);
        LOFilter filter = (LOFilter)newLogicalPlan.getPredecessors(op).get(0);
        FilterExtractor extractor = new FilterExtractor(
                filter.getFilterPlan(), partitionCols);
        extractor.visit();
        Assert.assertFalse(extractor.canPushDown());
    }

    /**
     * this loader is only used to test that parition column filters are given
     * in the manner expected in terms of column names - hence it does not
     * implement many of the methods and only implements required ones.
     */
    public static class TestLoader extends LoadFunc implements LoadMetadata {

        Schema schema;
        String[] partCols;
        static Expression partFilter = null;

        public TestLoader(String schemaString, String commaSepPartitionCols)
                throws ParserException {
            schema = Utils.getSchemaFromString(schemaString);
            partCols = commaSepPartitionCols.split(",");
        }

        @Override
        public InputFormat getInputFormat() throws IOException {
            return null;
        }

        @Override
        public Tuple getNext() throws IOException {
            return null;
        }

        @Override
        public void prepareToRead(RecordReader reader, PigSplit split)
                throws IOException {
        }

        @Override
        public void setLocation(String location, Job job) throws IOException {
        }

        @Override
        public String[] getPartitionKeys(String location, Job job)
                throws IOException {
            return partCols;
        }

        @Override
        public ResourceSchema getSchema(String location, Job job)
                throws IOException {
            return new ResourceSchema(schema);
        }

        @Override
        public ResourceStatistics getStatistics(String location,
                Job job) throws IOException {
            return null;
        }

        @Override
        public void setPartitionFilter(Expression partitionFilter)
                throws IOException {
            partFilter = partitionFilter;
        }

    }

    public class MyPlanOptimizer extends LogicalPlanOptimizer {
        protected MyPlanOptimizer(OperatorPlan p,  int iterations) {
            super( p, iterations, new HashSet<String>() );
        }

        protected List<Set<Rule>> buildRuleSets() {
            List<Set<Rule>> ls = new ArrayList<Set<Rule>>();

            Set<Rule> s = new HashSet<Rule>();
            // add split filter rule
            Rule r = new PartitionFilterOptimizer("PartitionFilterPushDown");
            s = new HashSet<Rule>();
            s.add(r);
            ls.add(s);

            r = new LoadTypeCastInserter( "LoadTypeCastInserter" );
            s = new HashSet<Rule>();
            s.add(r);
            ls.add(s);

            // Logical expression simplifier
            // TODO enable this test after PIG-3465
            /*
            s = new HashSet<Rule>();
            // add logical expression simplification rule
            r = new LogicalExpressionSimplifier("FilterLogicExpressionSimplifier");
            s.add(r);
            ls.add(s);*/

            return ls;
        }
    }

    // Helper Functions
    public LogicalPlan buildPlan(PigServer pigServer, String query) throws Exception {
        try {
            return Util.buildLp(pigServer, query);
        } catch(Throwable t) {
            throw new AssertionFailedError(t.getMessage());
        }
    }

    private static String braketize(String input) {
        return "(" + input + ")";
    }

    private static String getTestExpression(LogicalExpression op) throws FrontendException {
        if(op == null) {
            return null;
        }
        if(op instanceof ConstantExpression) {
            ConstantExpression constExpr =(ConstantExpression)op ;
            return String.valueOf(constExpr.getValue());
        } else if (op instanceof ProjectExpression) {
            ProjectExpression projExpr = (ProjectExpression)op;
            String fieldName = projExpr.getFieldSchema().alias;
            return fieldName;
        } else {
            if(op instanceof BinaryExpression) {
                String lhs = getTestExpression(((BinaryExpression) op).getLhs());
                String rhs = getTestExpression(((BinaryExpression) op).getRhs());
                String opStr = null;
                if(op instanceof EqualExpression) {
                    opStr = " == ";
                } else if (op instanceof AndExpression) {
                    opStr = " and ";
                } else if (op instanceof OrExpression) {
                    opStr = " or ";
                } else {
                    opStr = op.getName();
                }
                return braketize(lhs + opStr + rhs);
            } else if (op instanceof CastExpression) {
                String expr = getTestExpression(((CastExpression) op).getExpression());
                return expr;
            } else if(op instanceof IsNullExpression) {
                String expr = getTestExpression(((IsNullExpression) op).getExpression());
                return braketize(expr + " is null");
            } else if(op instanceof MapLookupExpression) {
                String col = getTestExpression(((MapLookupExpression)op).getMap());
                String key = ((MapLookupExpression)op).getLookupKey();
                return col + "#'" + key + "'";
            } else if(op instanceof DereferenceExpression) {
                String alias = getTestExpression(((DereferenceExpression) op).getReferredExpression());
                int colind = ((DereferenceExpression) op).getBagColumns().get(0);
                String column = String.valueOf(colind);
                return alias + ".$" + column;
            } else {
                throw new FrontendException("Unsupported conversion of LogicalExpression to Expression: " + op.getName());
            }
        }
    }
}
