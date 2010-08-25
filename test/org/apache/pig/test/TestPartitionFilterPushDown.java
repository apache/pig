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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.ExecType;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.newplan.logical.LogicalPlanMigrationVistor;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.optimizer.LogicalPlanOptimizer;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.rules.PartitionFilterOptimizer;
import org.apache.pig.newplan.logical.rules.LoadTypeCastInserter;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PColFilterExtractor;
import org.apache.pig.newplan.optimizer.PlanOptimizer;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.PlanSetter;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.test.utils.LogicalPlanTester;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * unit tests to test extracting partition filter conditions out of the filter
 * condition in the filter following a load which talks to metadata system (.i.e.
 * implements {@link LoadMetadata})
 */
public class TestPartitionFilterPushDown {
    static PigContext pc = new PigContext(ExecType.LOCAL, new Properties());
    static LogicalPlanTester lpTester;
    
    @BeforeClass
    public static void setup() throws Exception {
        lpTester = new LogicalPlanTester(pc);
        lpTester.buildPlan("a = load 'foo' as (srcid, mrkt, dstid, name, age);");
    }

    @AfterClass
    public static void tearDown() {
    }

    /**
     * test case where there is a single expression on partition columns in 
     * the filter expression along with an expression on non partition column
     * @throws IOException 
     */
    @Test
    public void testSimpleMixed() throws IOException {
    	org.apache.pig.impl.logicalLayer.LogicalPlan lp = 
            lpTester.buildPlan("b = filter a by srcid == 10 and name == 'foo';");
        test(lp, Arrays.asList("srcid"), "(srcid == 10)", "(name == 'foo')");
    }
    
    /**
     * test case where filter does not contain any condition on partition cols
     * @throws Exception
     */
    @Test
    public void testNoPartFilter() throws Exception {
    	org.apache.pig.impl.logicalLayer.LogicalPlan lp = 
            lpTester.buildPlan("b = filter a by age == 20 and name == 'foo';");
        test(lp, Arrays.asList("srcid"), null, 
                "((age == 20) and (name == 'foo'))");
    }
    
    /**
     * test case where filter only contains condition on partition cols
     * @throws Exception
     */
    @Test
    public void testOnlyPartFilter1() throws Exception {
    	org.apache.pig.impl.logicalLayer.LogicalPlan lp = 
            lpTester.buildPlan("b = filter a by srcid > 20 and mrkt == 'us';");
        test(lp, Arrays.asList("srcid", "mrkt"), 
                    "((srcid > 20) and (mrkt == 'us'))", null);
        
    }
    
    /**
     * test case where filter only contains condition on partition cols
     * @throws Exception
     */
    @Test
    public void testOnlyPartFilter2() throws Exception {
    	org.apache.pig.impl.logicalLayer.LogicalPlan lp = 
            lpTester.buildPlan("b = filter a by mrkt == 'us';");
        test(lp, Arrays.asList("srcid", "mrkt"), 
                    "(mrkt == 'us')", null);
        
    }
    
    /**
     * test case where filter only contains condition on partition cols
     * @throws Exception
     */
    @Test
    public void testOnlyPartFilter3() throws Exception {
    	org.apache.pig.impl.logicalLayer.LogicalPlan lp = 
            lpTester.buildPlan("b = filter a by srcid == 20 or mrkt == 'us';");
        test(lp, Arrays.asList("srcid", "mrkt"), 
                    "((srcid == 20) or (mrkt == 'us'))", null);
        
    }
    
    /**
     * test case where filter has both conditions on partition cols and non
     * partition cols and the filter condition will be split to extract the
     * conditions on partition columns
     */
    @Test
    public void testMixed1() throws Exception {
    	org.apache.pig.impl.logicalLayer.LogicalPlan lp = 
            lpTester.buildPlan("b = filter a by " +
            		"(age < 20 and  mrkt == 'us') and (srcid == 10 and " +
            		"name == 'foo');");
        test(lp, Arrays.asList("srcid", "mrkt"), 
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
    	org.apache.pig.impl.logicalLayer.LogicalPlan lp = 
            lpTester.buildPlan("b = filter a by " +
                    "(age >= 20 and  mrkt == 'us') and (srcid == 10 and " +
                    "dstid == 15);");
        test(lp, Arrays.asList("srcid", "dstid", "mrkt"), 
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
    	org.apache.pig.impl.logicalLayer.LogicalPlan lp = 
            lpTester.buildPlan("b = filter a by " +
                    "age >= 20 and  mrkt == 'us' and srcid == 10;");
        test(lp, Arrays.asList("srcid", "dstid", "mrkt"), 
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
    	org.apache.pig.impl.logicalLayer.LogicalPlan lp = 
            lpTester.buildPlan("b = filter a by " +
                    "age >= 20 and  mrkt == 'us' and name == 'foo' and " +
                    "srcid == dstid;");
        test(lp, Arrays.asList("srcid", "dstid", "mrkt"), 
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
    	org.apache.pig.impl.logicalLayer.LogicalPlan lp = 
            lpTester.buildPlan("b = filter a by " +
                    "(srcid == 10 or mrkt == 'us') and name == 'foo' and " +
                    "dstid == 30;");
        test(lp, Arrays.asList("srcid", "dstid", "mrkt"), 
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
    	org.apache.pig.impl.logicalLayer.LogicalPlan lp = 
            lpTester.buildPlan("b = filter a by " +
                    "dstid == 30 and (srcid == 10 or mrkt == 'us') and name == 'foo';");
        test(lp, Arrays.asList("srcid", "dstid", "mrkt"), 
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
    	org.apache.pig.impl.logicalLayer.LogicalPlan lp = 
            lpTester.buildPlan("b = filter a by " +
                    "mrkt == 'us' and srcid * 10 == 150 + 20 and age != 15;");
        test(lp, Arrays.asList("srcid", "dstid", "mrkt"), 
                "((mrkt == 'us') and ((srcid * 10) == (150 + 20)))", 
                "(age != 15)");
    }
    
    @Test
    public void testNegPColConditionWithNonPCol() throws Exception {
        // use of partition column condition and non partition column in 
        // same condition should fail
    	org.apache.pig.impl.logicalLayer.LogicalPlan lp = lpTester.buildPlan("b = filter a by " +
                    "srcid > age;");
        negativeTest(lp, Arrays.asList("srcid"), 1111);
        lp =  lpTester.buildPlan("b = filter a by " +
                    "srcid + age == 20;");
        negativeTest(lp, Arrays.asList("srcid"), 1111);

        // OR of partition column condition and non partiton col condition 
        // should fail
        lp = lpTester.buildPlan("b = filter a by " +
                    "srcid > 10 or name == 'foo';");
        negativeTest(lp, Arrays.asList("srcid"), 1111);
    }
    
    @Test
    public void testNegPColInWrongPlaces() throws Exception {
        
        int expectedErrCode = 1112;
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = lpTester.buildPlan("b = filter a by " +
        "(srcid > 10 and name == 'foo') or dstid == 10;");
        negativeTest(lp, Arrays.asList("srcid", "dstid"), expectedErrCode); 
        
        expectedErrCode = 1110;
        lp = lpTester.buildPlan("b = filter a by " +
                "CONCAT(mrkt, '_10') == 'US_10' and age == 20;");
        negativeTest(lp, Arrays.asList("srcid", "dstid", "mrkt"), expectedErrCode);
        
        lp = lpTester.buildPlan("b = filter a by " +
                "mrkt matches '.*us.*' and age < 15;");
        negativeTest(lp, Arrays.asList("srcid", "dstid", "mrkt"), expectedErrCode);
        
        lp = lpTester.buildPlan("b = filter a by " +
                "(int)mrkt == 10 and name matches '.*foo.*';");
        negativeTest(lp, Arrays.asList("srcid", "dstid", "mrkt"),expectedErrCode);
        
        lp = lpTester.buildPlan("b = filter a by " +
            "(mrkt == 'us' ? age : age + 10) == 40 and name matches '.*foo.*';");
        negativeTest(lp, Arrays.asList("srcid", "dstid", "mrkt"), expectedErrCode);
        
        lp = lpTester.buildPlan("b = filter a by " +
            "(mrkt is null) and name matches '.*foo.*';");
        negativeTest(lp, Arrays.asList("srcid", "dstid", "mrkt"), expectedErrCode);
        
        lp = lpTester.buildPlan("b = filter a by " +
            "(mrkt is not null) and name matches '.*foo.*';");
        negativeTest(lp, Arrays.asList("srcid", "dstid", "mrkt"), expectedErrCode);
    }
    
    
    /**
     * Test that pig sends correct partition column names in setPartitionFilter
     * when the user has a schema in the load statement which renames partition
     * columns
     * @throws Exception
     */
    @Test
    public void testColNameMapping1() throws Exception {
        TestLoader.partFilter = null;
        lpTester.buildPlan("a = load 'foo' using "
            + TestLoader.class.getName() + 
            "('srcid:int, mrkt:chararray, dstid:int, name:chararray, age:int', " +
            "'srcid,mrkt') as (f1, f2, f3, f4, f5);");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = lpTester.buildPlan("b = filter a by " +
        		"(f5 >= 20 and f2 == 'us') and (f1 == 10 and f3 == 15);");
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Assert.assertEquals("checking partition filter:",             
                    "((mrkt == 'us') and (srcid == 10))",
                    TestLoader.partFilter.toString());
        LOFilter filter = (LOFilter)newLogicalPlan.getSinks().get(0);
        String actual = PColFilterExtractor.getExpression(
                (LogicalExpression)filter.getFilterPlan().getSources().get(0)).
                toString().toLowerCase();
        Assert.assertEquals("checking trimmed filter expression:", 
                "((f5 >= 20) and (f3 == 15))", actual);
    }
    
    private LogicalPlan migrateAndOptimizePlan(org.apache.pig.impl.logicalLayer.LogicalPlan plan) throws IOException {
        LogicalPlan newLogicalPlan = migratePlan( plan );
        PlanOptimizer optimizer = new MyPlanOptimizer( newLogicalPlan, 3 );
        optimizer.optimize();
        return newLogicalPlan;
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
        TestLoader.partFilter = null;
        lpTester.buildPlan("a = load 'foo' using "
            + TestLoader.class.getName() + 
            "('srcid:int, mrkt:chararray, dstid:int, name:chararray, age:int', " +
            "'srcid') as (f1, f2, f3, f4, f5);");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = lpTester.buildPlan("b = filter a by " +
                "f5 >= 20 and f2 == 'us' and f3 == 15;");

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );

        Assert.assertEquals("checking partition filter:",             
                    null,
                    TestLoader.partFilter);
        LOFilter filter = (LOFilter) newLogicalPlan.getSinks().get(0);
        String actual = PColFilterExtractor.getExpression(
                (LogicalExpression) filter.getFilterPlan().
                getSources().get(0)).
                toString().toLowerCase();
        Assert.assertEquals("checking trimmed filter expression:", 
                "(((f5 >= 20) and (f2 == 'us')) and (f3 == 15))", actual);
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
        TestLoader.partFilter = null;
        lpTester.buildPlan("a = load 'foo' using "
            + TestLoader.class.getName() + 
            "('srcid:int, mrkt:chararray, dstid:int, name:chararray, age:int', " +
            "'srcid,mrkt,dstid,age') as (f1, f2, f3, f4, f5);");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = lpTester.buildPlan("b = filter a by " +
                "(f5 >= 20 or f2 == 'us') and (f1 == 10 and f3 == 15);");

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );

        Assert.assertEquals("checking partition filter:",             
                    "(((age >= 20) or (mrkt == 'us')) and ((srcid == 10) and " +
                    "(dstid == 15)))",
                    TestLoader.partFilter.toString());
        Iterator<Operator> it = newLogicalPlan.getOperators();
        Assert.assertTrue("Checking that filter has been removed since it contained" +
        		" only conditions on partition cols:", 
        		(it.next() instanceof LOLoad));
        Assert.assertFalse("Checking that filter has been removed since it contained" +
                " only conditions on partition cols:", 
                it.hasNext());
        
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
        TestLoader.partFilter = null;
        lpTester.buildPlan("a = load 'foo' using "
            + TestLoader.class.getName() + 
            "('srcid:int, mrkt:chararray, dstid:int, name:chararray, age:int', " +
            "'srcid,mrkt') as (f1, f2, f3);");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = lpTester.buildPlan("b = filter a by " +
                "(age >= 20 and f2 == 'us') and (f1 == 10 and f3 == 15);");

        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        Assert.assertEquals("checking partition filter:",             
                    "((mrkt == 'us') and (srcid == 10))",
                    TestLoader.partFilter.toString());
        LOFilter filter = (LOFilter) newLogicalPlan.getSinks().get(0);
        String actual = PColFilterExtractor.getExpression(
                (LogicalExpression) filter.getFilterPlan().getSources().get(0)).
                toString().toLowerCase();
        Assert.assertEquals("checking trimmed filter expression:", 
                "((age >= 20) and (f3 == 15))", actual);
    }
    
    /**
     * Test PIG-1267
     * @throws Exception
     */
    @Test
    public void testColNameMapping5() throws Exception {
        TestLoader.partFilter = null;
        lpTester.buildPlan("a = load 'foo' using "
            + TestLoader.class.getName() + 
            "('mrkt:chararray, a1:chararray, a2:chararray, srcid:int, bcookie:chararray', " +
            "'srcid');");
        lpTester.buildPlan("b = load 'bar' using "
                + TestLoader.class.getName() + 
                "('dstid:int, b1:int, b2:int, srcid:int, bcookie:chararray, mrkt:chararray'," +
                "'srcid');");
        lpTester.buildPlan("a1 = filter a by srcid == 10;");
        lpTester.buildPlan("b1 = filter b by srcid == 20;");
        lpTester.buildPlan("c = join a1 by bcookie, b1 by bcookie;");
        org.apache.pig.impl.logicalLayer.LogicalPlan lp = lpTester
                .buildPlan("d = foreach c generate $4 as bcookie:chararray, " +
                		"$5 as dstid:int, $0 as mrkt:chararray;");
        
        new PlanSetter(lp).visit();
        
        LogicalPlan newLogicalPlan = migrateAndOptimizePlan( lp );
        
        String partFilter = TestLoader.partFilter.toString();
        Assert.assertTrue( "(srcid == 20)".equals( partFilter ) ||  "(srcid == 10)".equals( partFilter ) );
        
        int counter = 0;
        Iterator<Operator> iter = newLogicalPlan.getOperators();
        while (iter.hasNext()) {
        	 Assert.assertTrue(!(iter.next() instanceof LOFilter));
            counter++;
        }      
        Assert.assertEquals(counter, 4);
    }
    
    //// helper methods ///////
    
    private PColFilterExtractor test(org.apache.pig.impl.logicalLayer.LogicalPlan lp, List<String> partitionCols, 
            String expPartFilterString, String expFilterString) 
    throws IOException {
    	LogicalPlan newLogicalPlan = migratePlan( lp );
        LOFilter filter = (LOFilter)newLogicalPlan.getSinks().get(0);
        PColFilterExtractor pColExtractor = new PColFilterExtractor(
                filter.getFilterPlan(), partitionCols);
        pColExtractor.visit();
        
        if(expPartFilterString == null) {
        	 Assert.assertEquals("Checking partition column filter:", null, 
                    pColExtractor.getPColCondition());
        } else  {
        	 Assert.assertEquals("Checking partition column filter:", 
                    expPartFilterString.toLowerCase(), 
                    pColExtractor.getPColCondition().toString().toLowerCase());   
        }
        
        if(expFilterString == null) {
        	 Assert.assertTrue("Check that filter can be removed:", 
                    pColExtractor.isFilterRemovable());
        } else {
            String actual = PColFilterExtractor.getExpression(
                                (LogicalExpression)filter.getFilterPlan().getSources().get(0)).
                                toString().toLowerCase();
            Assert.assertEquals("checking trimmed filter expression:", expFilterString,
                    actual);
        }
        return pColExtractor;
    }
    
    private void negativeTest(org.apache.pig.impl.logicalLayer.LogicalPlan lp, List<String> partitionCols,
            int expectedErrorCode) throws VisitorException {
    	LogicalPlan newLogicalPlan = migratePlan( lp );
        LOFilter filter = (LOFilter)newLogicalPlan.getSinks().get(0);
        PColFilterExtractor pColExtractor = new PColFilterExtractor(
                filter.getFilterPlan(), partitionCols);
        try {
            pColExtractor.visit();
        } catch(Exception e) {
        	 Assert.assertEquals("Checking if exception has right error code", 
                    expectedErrorCode, LogUtils.getPigException(e).getErrorCode());
            return;
        }
        Assert.fail("Exception expected!");
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
        throws ParseException {
            schema = Util.getSchemaFromString(schemaString);
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
            return ls;
        }
    }    

    private LogicalPlan migratePlan(org.apache.pig.impl.logicalLayer.LogicalPlan lp) throws VisitorException{
        LogicalPlanMigrationVistor visitor = new LogicalPlanMigrationVistor(lp);        
        visitor.visit();
        LogicalPlan newPlan = visitor.getNewLogicalPlan();
        return newPlan;
    }
    
}
