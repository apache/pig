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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.SortColInfo;
import org.apache.pig.SortInfo;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.LogToPhyTranslationVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.LODefine;
import org.apache.pig.impl.logicalLayer.LOFilter;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOPrinter;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.test.utils.LogicalPlanTester;
import org.junit.Test;

/**
 * All new tests should be included at the end of the existing test cases. This is to ensure that 
 * nodeIdGenerator produces the same sequence as in the golden files for the previously existing test cases 
 * 
 * To generate golden files, use the following code :
 * ByteArrayOutputStream baos = new ByteArrayOutputStream();
 * pp.explain(baos);
 * FileOutputStream fos = new FileOutputStream("test/org/apache/pig/test/data/GoldenFiles/Union.gld");
 * fos.write(baos.toByteArray());
 *
 */
public class TestLogToPhyCompiler extends junit.framework.TestCase {

    private final Log log = LogFactory.getLog(getClass());
    
    File A;
    final int MAX_RANGE = 10;
    
    Random r = new Random();
    PigContext pc = new PigContext(ExecType.LOCAL, new Properties());

    private boolean generate = false;
    
    
    
    @Override
    protected void setUp() throws Exception {
        pc.connect();
    }
    
    @Test
    public void testComplexForeach() throws VisitorException, IOException {
        /*String query = "foreach (load 'a') {" +
                "B = FILTER $0 BY (($1 == $2) AND ('a' < 'b'));" +
                "generate B;" +
                "};";*/
    	String query = "foreach (load 'a') {" +
        "B = FILTER $0 BY ($1 == $2);" +
        "generate B;" +
        "};";
        LogicalPlan plan = buildPlan(query);
    	PhysicalPlan pp = buildPhysicalPlan(plan); 
    	int MAX_SIZE = 100000;
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pp.explain(baos);
        baos.write((int)'\n');
        String compiledPlan = baos.toString();
        compiledPlan = compiledPlan.replaceAll("Load(.*)","Load()");
        
        if(generate){
            FileOutputStream fos = new FileOutputStream("test/org/apache/pig/test/data/GoldenFiles/ComplexForeach.gld");
            fos.write(baos.toByteArray());
            return;
        }
        
    	FileInputStream fis = new FileInputStream("test/org/apache/pig/test/data/GoldenFiles/ComplexForeach.gld");
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        String goldenPlan = new String(b, 0, len);
        goldenPlan = goldenPlan.replaceAll("Load(.*)","Load()");

        System.out.println();
        System.out.println(compiledPlan);
        System.out.println("-------------");
        //System.out.println(compiledPlan.compareTo(goldenPlan)==0);
        assertEquals(compiledPlan, goldenPlan);
    }
        
    public void testSort() throws VisitorException, IOException {
    	String query = "order (load 'a') by $0;";
    	LogicalPlan plan = buildPlan(query);
    	PhysicalPlan pp = buildPhysicalPlan(plan);

    	int MAX_SIZE = 100000;
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pp.explain(baos);
        baos.write((int)'\n');
        String compiledPlan = baos.toString();
        compiledPlan = compiledPlan.replaceAll("Load(.*)","Load()");

        if(generate){
            FileOutputStream fos = new FileOutputStream("test/org/apache/pig/test/data/GoldenFiles/Sort.gld");
            fos.write(baos.toByteArray());
            return;
        }
        
    	FileInputStream fis = new FileInputStream("test/org/apache/pig/test/data/GoldenFiles/Sort.gld");
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        String goldenPlan = new String(b, 0, len);
        goldenPlan = goldenPlan.replaceAll("Load(.*)","Load()");

        System.out.println();
        System.out.println(compiledPlan);
        System.out.println("-------------");
        
        //System.out.println(compiledPlan.compareTo(goldenPlan)==0);
        assertEquals(compiledPlan, goldenPlan);
    }
        
    public void testDistinct() throws VisitorException, IOException {
    	String query = "distinct (load 'a');";
    	LogicalPlan plan = buildPlan(query);
    	PhysicalPlan pp = buildPhysicalPlan(plan);
    	
    	int MAX_SIZE = 100000;
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pp.explain(baos);
        baos.write((int)'\n');
        String compiledPlan = baos.toString();
        compiledPlan = compiledPlan.replaceAll("Load(.*)","Load()");
        
        if(generate){
            FileOutputStream fos = new FileOutputStream("test/org/apache/pig/test/data/GoldenFiles/Distinct.gld");
            fos.write(baos.toByteArray());
            return;
        }
        
    	FileInputStream fis = new FileInputStream("test/org/apache/pig/test/data/GoldenFiles/Distinct.gld");
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        String goldenPlan = new String(b, 0, len);
        goldenPlan = goldenPlan.replaceAll("Load(.*)","Load()");

        System.out.println();
        System.out.println(compiledPlan);
        System.out.println("-------------");
        //System.out.println(compiledPlan.compareTo(goldenPlan)==0);
        assertEquals(compiledPlan, goldenPlan);
    }
    
    public void testCogroup() throws VisitorException, IOException {
        System.out.println("testCogroup");
    	String query = "cogroup (load 'a') by ($0 + $1, $0 - $1), (load 'b') by ($0 + $1, $0 - $1);";
    	LogicalPlan plan = buildPlan(query);
    	PhysicalPlan pp = buildPhysicalPlan(plan);
    	
    	
    	int MAX_SIZE = 100000;
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pp.explain(baos);
        baos.write((int)'\n');
        String compiledPlan = baos.toString();
        compiledPlan = compiledPlan.replaceAll("Load(.*)","Load()");
        
        if(generate){
            FileOutputStream fos = new FileOutputStream("test/org/apache/pig/test/data/GoldenFiles/Cogroup.gld");
            fos.write(baos.toByteArray());
            return;
        }
        
    	FileInputStream fis = new FileInputStream("test/org/apache/pig/test/data/GoldenFiles/Cogroup.gld");
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        String goldenPlan = new String(b, 0, len);
        goldenPlan = goldenPlan.replaceAll("Load(.*)","Load()");

        System.out.println();
        System.out.println(compiledPlan);
        System.out.println("-------------");
        //System.out.println(compiledPlan.compareTo(goldenPlan)==0);
        assertEquals(compiledPlan, goldenPlan);
    }
    
    public void testArithmetic() throws VisitorException, IOException, ExecException {
    	
    	String query = "foreach (load 'A') generate $0 + $1 + '5', $0 - '5' - $1, 'hello';";
    	LogicalPlan lp = buildPlan(query);
    	
    	PhysicalPlan pp = buildPhysicalPlan(lp);
    	
        //Ensure that there is only 1 leaf node
    	assertEquals(1, pp.getLeaves().size());
    	
    	
    	int MAX_SIZE = 100000;
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pp.explain(baos);
        baos.write((int)'\n');
        String compiledPlan = baos.toString();
        compiledPlan = compiledPlan.replaceAll("Load(.*)","Load()");

        if(generate){
            FileOutputStream fos = new FileOutputStream("test/org/apache/pig/test/data/GoldenFiles/Arithmetic.gld");
            fos.write(baos.toByteArray());
            return;
        }
        
    	FileInputStream fis = new FileInputStream("test/org/apache/pig/test/data/GoldenFiles/Arithmetic.gld");
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        String goldenPlan = new String(b, 0, len);
        goldenPlan = goldenPlan.replaceAll("Load(.*)","Load()");

        System.out.println();
        System.out.println(compiledPlan);
        System.out.println("-------------");
        //System.out.println(compiledPlan.compareTo(goldenPlan)==0);
        assertEquals(compiledPlan, goldenPlan);
    }
    
    public void testComparison() throws VisitorException, IOException {
    	String query = "filter (load 'a' using " + PigStorage.class.getName() + "(':')) by $0 + $1 > ($0 - $1) * ('4' / '2');";
    	LogicalPlan lp = buildPlan(query);
    	PhysicalPlan pp = buildPhysicalPlan(lp);
    	
        //Ensure that there is only 1 leaf node
    	assertEquals(1, pp.getLeaves().size());
    	
    	int MAX_SIZE = 100000;
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pp.explain(baos);
        baos.write((int)'\n');
        String compiledPlan = baos.toString();
        compiledPlan = compiledPlan.replaceAll("Load(.*)","Load()");
        
        if(generate){
            FileOutputStream fos = new FileOutputStream("test/org/apache/pig/test/data/GoldenFiles/Comparison.gld");
            fos.write(baos.toByteArray());
            return;
        }
        
    	FileInputStream fis = new FileInputStream("test/org/apache/pig/test/data/GoldenFiles/Comparison.gld");
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        String goldenPlan = new String(b, 0, len);
        goldenPlan = goldenPlan.replaceAll("Load(.*)","Load()");

        System.out.println();
        System.out.println(compiledPlan);
        System.out.println("-------------");
        //System.out.println(compiledPlan.compareTo(goldenPlan)==0);
        assertEquals(compiledPlan, goldenPlan);
    }

    @Test
    public void testBinCond() throws VisitorException, IOException {
        String query = "foreach (load 'a') generate ($1 == '3'? $2 + $3 : $2 - $3) ;";
        LogicalPlan lp = buildPlan(query);

        PhysicalPlan pp = buildPhysicalPlan(lp);

        
        int MAX_SIZE = 100000;
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pp.explain(baos);
        baos.write((int)'\n');
        String compiledPlan = baos.toString();
        compiledPlan = compiledPlan.replaceAll("Load(.*)","Load()");

        if(generate){
            FileOutputStream fos = new FileOutputStream("test/org/apache/pig/test/data/GoldenFiles/BinCond.gld");
            fos.write(baos.toByteArray());
            return;
        }
        
    	FileInputStream fis = new FileInputStream("test/org/apache/pig/test/data/GoldenFiles/BinCond.gld");
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        String goldenPlan = new String(b, 0, len);
        goldenPlan = goldenPlan.replaceAll("Load(.*)","Load()");

        System.out.println();
        System.out.println(compiledPlan);
        System.out.println("-------------");
        //System.out.println(compiledPlan.compareTo(goldenPlan)==0);
        assertEquals(compiledPlan, goldenPlan);
    }
    
    
    @Test
    public void testGenerate() throws VisitorException, IOException {
        String query = "foreach (load 'a') generate ($1+$2), ($1-$2), ($1*$2), ($1/$2), ($1%$2), -($1) ;";
        LogicalPlan lp = buildPlan(query);

        PhysicalPlan pp = buildPhysicalPlan(lp);

        
        int MAX_SIZE = 100000;
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pp.explain(baos);
        baos.write((int)'\n');
        String compiledPlan = baos.toString();
        compiledPlan = compiledPlan.replaceAll("Load(.*)","Load()");

        if(generate){
            FileOutputStream fos = new FileOutputStream("test/org/apache/pig/test/data/GoldenFiles/Generate.gld");
            fos.write(baos.toByteArray());
            return;
        }
        
    	FileInputStream fis = new FileInputStream("test/org/apache/pig/test/data/GoldenFiles/Generate.gld");
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        String goldenPlan = new String(b, 0, len);
        goldenPlan = goldenPlan.replaceAll("Load(.*)","Load()");

        System.out.println();
        System.out.println(compiledPlan);
        System.out.println("-------------");
        
        assertEquals(compiledPlan, goldenPlan);
    }

    @Test
    public void testUnion() throws VisitorException, IOException {
    	String query = "union (load 'a'), (load 'b'), (load 'c');";
    	LogicalPlan lp = buildPlan(query);
    	PhysicalPlan pp = buildPhysicalPlan(lp);
    	
    	int MAX_SIZE = 100000;
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pp.explain(baos);
        baos.write((int)'\n');
        String compiledPlan = baos.toString();
        compiledPlan = compiledPlan.replaceAll("Load(.*)","Load()");

        if(generate){
            FileOutputStream fos = new FileOutputStream("test/org/apache/pig/test/data/GoldenFiles/Union.gld");
            fos.write(baos.toByteArray());
            return;
        }
        
    	FileInputStream fis = new FileInputStream("test/org/apache/pig/test/data/GoldenFiles/Union.gld");
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        String goldenPlan = new String(b, 0, len);
        goldenPlan = goldenPlan.replaceAll("Load(.*)","Load()");

        System.out.println();
        System.out.println(compiledPlan);
        System.out.println("-------------");
        
        assertEquals(compiledPlan, goldenPlan);
    }
    
    @Test
    public void testSplit() throws VisitorException, IOException {
    	String query = "split (load 'a') into x if $0 < '7', y if $0 > '7';";
    	LogicalPlan plan = buildPlan(query);
    	
    	PhysicalPlan pp = buildPhysicalPlan(plan);
    	
    	int MAX_SIZE = 100000;
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pp.explain(baos);
        baos.write((int)'\n');
        String compiledPlan = baos.toString();
        compiledPlan = compiledPlan.replaceAll("Load(.*)","Load()");

        if(generate){
            FileOutputStream fos = new FileOutputStream("test/org/apache/pig/test/data/GoldenFiles/Split1.gld");
            fos.write(baos.toByteArray());
            return;
        }
        
    	FileInputStream fis1 = new FileInputStream("test/org/apache/pig/test/data/GoldenFiles/Split1.gld");
    	FileInputStream fis2 = new FileInputStream("test/org/apache/pig/test/data/GoldenFiles/Split2.gld");
        byte[] b1 = new byte[MAX_SIZE];
        byte[] b2 = new byte[MAX_SIZE];
        int len = fis1.read(b1);
        int test = fis2.read(b2);
        //System.out.println("Length of first plan = " + len + " of second = " + test);
        String goldenPlan1 = new String(b1, 0, len);
        String goldenPlan2 = new String(b2, 0, len);
        goldenPlan1 = goldenPlan1.replaceAll("Load(.*)","Load()");
        goldenPlan2 = goldenPlan2.replaceAll("Load(.*)","Load()");

        System.out.println();
        System.out.println(compiledPlan);
        System.out.println("-------------");

        if(compiledPlan.compareTo(goldenPlan1) == 0 || compiledPlan.compareTo(goldenPlan2) == 0) {
            // good
        }
        else {
            System.out.println("Expected plan1=") ;
            System.out.println(goldenPlan1) ;
            System.out.println("Expected plan2=") ;
            System.out.println(goldenPlan1) ;
            System.out.println("Actual plan=") ;
            System.out.println(compiledPlan) ;
            System.out.println("**END**") ;
            fail("Plan not match") ;
        }
    	
    }
    
    @Test
    public void testIsNull() throws VisitorException, IOException {
        //TODO
        //PONot is not implemented. The query below translates to POIsNull istead
        //of PONOt(POIsNull)
    	String query = "split (load 'a') into x if $0 IS NULL, y if $0 IS NOT NULL;";
    	LogicalPlan plan = buildPlan(query);
    	
    	PhysicalPlan pp = buildPhysicalPlan(plan);
    	
    	int MAX_SIZE = 100000;
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pp.explain(baos);
        baos.write((int)'\n');
        String compiledPlan = baos.toString();
        compiledPlan = compiledPlan.replaceAll("Load(.*)","Load()");

        if(generate){
            FileOutputStream fos = new FileOutputStream("test/org/apache/pig/test/data/GoldenFiles/IsNull1.gld");
            fos.write(baos.toByteArray());
            return;
        }
        
    	FileInputStream fis1 = new FileInputStream("test/org/apache/pig/test/data/GoldenFiles/IsNull1.gld");
    	FileInputStream fis2 = new FileInputStream("test/org/apache/pig/test/data/GoldenFiles/IsNull2.gld");
        byte[] b1 = new byte[MAX_SIZE];
        byte[] b2 = new byte[MAX_SIZE];
        int len = fis1.read(b1);
        int test = fis2.read(b2);
        //System.out.println("Length of first plan = " + len + " of second = " + test + " Length of compiled plan = " + compiledPlan.length());
        String goldenPlan1 = new String(b1, 0, len);
        String goldenPlan2 = new String(b2, 0, len);
        goldenPlan1 = goldenPlan1.replaceAll("Load(.*)","Load()");
        goldenPlan2 = goldenPlan2.replaceAll("Load(.*)","Load()");

        System.out.println();
        System.out.println(compiledPlan);
        System.out.println("-------------");

        if(compiledPlan.compareTo(goldenPlan1) == 0 || compiledPlan.compareTo(goldenPlan2) == 0)  {
            // good
        }
        else {
            System.out.println("Expected plan1=") ;
            System.out.println(goldenPlan1) ;
            System.out.println("Expected plan2=") ;
            System.out.println(goldenPlan1) ;
            System.out.println("Actual plan=") ;
            System.out.println(compiledPlan) ;
            System.out.println("**END**") ;
            fail("Plan not match") ;
        }
    	
    }

    @Test
    public void testLimit() throws VisitorException, IOException {
        String query = "limit (load 'a') 5;";
        LogicalPlan plan = buildPlan(query);
        PhysicalPlan pp = buildPhysicalPlan(plan);

        int MAX_SIZE = 100000;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pp.explain(baos);
        baos.write((int)'\n');
        String compiledPlan = baos.toString();
        compiledPlan = compiledPlan.replaceAll("Load(.*)","Load()");

        if(generate){
            FileOutputStream fos = new FileOutputStream("test/org/apache/pig/test/data/GoldenFiles/Limit.gld");
            fos.write(baos.toByteArray());
            return;
        }
        
        FileInputStream fis = new FileInputStream("test/org/apache/pig/test/data/GoldenFiles/Limit.gld");
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        String goldenPlan = new String(b, 0, len);
        goldenPlan = goldenPlan.replaceAll("Load(.*)","Load()");

        System.out.println();
        System.out.println(compiledPlan);
        System.out.println("-------------");

        //System.out.println(compiledPlan.compareTo(goldenPlan)==0);
        assertEquals(compiledPlan, goldenPlan);
    }

    @Test
    public void testErrLimit() throws VisitorException, IOException {
        String query = "limit (load 'a') 5;";
        LogicalPlan plan = buildPlan(query);
        plan.remove(plan.getRoots().get(0));
        try {
            buildPhysicalPlan(plan);
            fail("Expected error.");
        } catch(VisitorException ve) {
            assertTrue(ve.getErrorCode() == 2051);
        }
    }

    @Test
    public void testErrFilter() throws VisitorException, IOException {
        String query = "filter (load 'a') by $0 > 5;";
        LogicalPlan plan = buildPlan(query);
        plan.remove(plan.getRoots().get(0));
        try {
            buildPhysicalPlan(plan);
            fail("Expected error.");
        } catch(VisitorException ve) {
            assertTrue(ve.getErrorCode() == 2051);
        }
    }

    @Test
    public void testErrSort() throws VisitorException, IOException {
        String query = "order (load 'a') by $0 desc;";
        LogicalPlan plan = buildPlan(query);
        plan.remove(plan.getRoots().get(0));
        try {
            buildPhysicalPlan(plan);
            fail("Expected error.");
        } catch(VisitorException ve) {
            assertTrue(ve.getErrorCode() == 2051);
        }
    }

    @Test
    public void testErrNull() throws VisitorException, IOException {
        String query = "filter (load 'a') by $0 is null;";
        LogicalPlan plan = buildPlan(query);
        LOFilter filter = (LOFilter)plan.getLeaves().get(0);
        LogicalPlan innerPlan = filter.getComparisonPlan();
        innerPlan.remove(innerPlan.getRoots().get(0));
        try {
            buildPhysicalPlan(plan);
            fail("Expected error.");
        } catch(VisitorException ve) {
            assertTrue(ve.getErrorCode() == 2051);
        }
    }

    /**
     * tests sortinfo for the case where order by is ascending on
     * all sort columns
     */
    @Test
    public void testSortInfoAsc() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'bla' as (i:int, n:chararray, d:double);");
        lpt.buildPlan("b = order a by i, d;");
        LogicalPlan lp = lpt.buildPlan("store b into 'foo';");
        PigServer.SortInfoSetter siSetter = new PigServer.SortInfoSetter(lp); 
        siSetter.visit();
        PhysicalPlan pp = buildPhysicalPlan(lp);
        SortInfo si = ((POStore)(pp.getLeaves().get(0))).getSortInfo();
        SortInfo expected = getSortInfo(
                Arrays.asList(new String[] {"i", "d"}),
                Arrays.asList(new Integer[] {0, 2}),
                Arrays.asList(new SortColInfo.Order[] {
                        SortColInfo.Order.ASCENDING, 
                        SortColInfo.Order.ASCENDING}));
        assertEquals(expected, si);
    }
    
    /**
     * tests sortInfo for mixed ascending and descending in order by
     * @throws Exception
     */
    @Test
    public void testSortInfoAscDesc() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'bla' as (i:int, n:chararray, d:double);");
        lpt.buildPlan("b = filter a by i > 10;");
        lpt.buildPlan("c = order b by i desc, d;");
        LogicalPlan lp = lpt.buildPlan("store c into 'foo';");
        PigServer.SortInfoSetter siSetter = new PigServer.SortInfoSetter(lp); 
        siSetter.visit();
        PhysicalPlan pp = buildPhysicalPlan(lp);
        SortInfo si = ((POStore)(pp.getLeaves().get(0))).getSortInfo();
        SortInfo expected = getSortInfo(
                Arrays.asList(new String[] {"i", "d"}), 
                Arrays.asList(new Integer[] {0, 2}),
                Arrays.asList(new SortColInfo.Order[] {
                        SortColInfo.Order.DESCENDING, 
                        SortColInfo.Order.ASCENDING}));
        assertEquals(expected, si);
    }
    
    /**
     * tests that sortInfo is null when there is no order by
     * before the store
     * @throws Exception
     */
    @Test
    public void testSortInfoNoOrderBy1() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'bla' as (i:int, n:chararray, d:double);");
        lpt.buildPlan("b = filter a by i > 10;");
        LogicalPlan lp = lpt.buildPlan("store b into 'foo';");
        PigServer.SortInfoSetter siSetter = new PigServer.SortInfoSetter(lp); 
        siSetter.visit();
        PhysicalPlan pp = buildPhysicalPlan(lp);
        SortInfo si = ((POStore)(pp.getLeaves().get(0))).getSortInfo();
        assertEquals(null, si);
    }
    
    /**
     * tests that sortInfo is null when there is an operator other than limit
     * between order by and the store
     * @throws Exception
     */
    @Test
    public void testSortInfoNoOrderBy2() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'bla' as (i:int, n:chararray, d:double);");
        lpt.buildPlan("b = order a by i, d;");
        lpt.buildPlan("c = filter b by i > 10;");
        LogicalPlan lp = lpt.buildPlan("store c into 'foo';");
        PhysicalPlan pp = buildPhysicalPlan(lp);
        PigServer.SortInfoSetter siSetter = new PigServer.SortInfoSetter(lp); 
        siSetter.visit();
        SortInfo si = ((POStore)(pp.getLeaves().get(0))).getSortInfo();
        assertEquals(null, si);
    }
    
    /**
     * tests that sortInfo is not null when there is a limit
     * between order by and the store
     * @throws Exception
     */
    @Test
    public void testSortInfoOrderByLimit() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'bla' as (i:int, n:chararray, d:double);");
        lpt.buildPlan("b = order a by i, d desc;");
        lpt.buildPlan("c = limit b 10;");
        LogicalPlan lp = lpt.buildPlan("store c into 'foo';");
        PigServer.SortInfoSetter siSetter = new PigServer.SortInfoSetter(lp); 
        siSetter.visit();
        LOPrinter lpr = new LOPrinter(System.err, lp);
        lpr.visit();
        PhysicalPlan pp = buildPhysicalPlan(lp);
        SortInfo si = ((POStore)(pp.getLeaves().get(0))).getSortInfo();
        SortInfo expected = getSortInfo(
                Arrays.asList(new String[] {"i", "d"}), 
                Arrays.asList(new Integer[] {0, 2}),
                Arrays.asList(new SortColInfo.Order[] {
                        SortColInfo.Order.ASCENDING, 
                        SortColInfo.Order.DESCENDING}));
        assertEquals(expected, si);
    }
    
    /**
     * tests that sortInfo is not null when there are multiple store
     * @throws Exception
     */
    @Test
    public void testSortInfoMultipleStore() throws Exception {
        PigServer myPig = new PigServer(ExecType.LOCAL);
        myPig.getPigContext().getProperties().setProperty("pig.usenewlogicalplan", "false");
        myPig.setBatchOn();
        myPig.registerQuery("a = load 'bla' as (i:int, n:chararray, d:double);");
        myPig.registerQuery("b = order a by i, d desc;");
        myPig.registerQuery("store b into '1';");
        myPig.registerQuery("store b into '2';");
        java.lang.reflect.Method compileLp = myPig.getClass()
            .getDeclaredMethod("compileLp",
            new Class[] { String.class });

        compileLp.setAccessible(true);

        LogicalPlan lp = (LogicalPlan) compileLp.invoke(myPig, new Object[] { null });
        LOPrinter lpr = new LOPrinter(System.err, lp);
        lpr.visit();
        
        PhysicalPlan pp = buildPhysicalPlan(lp);
        SortInfo si0 = ((POStore)(pp.getLeaves().get(0))).getSortInfo();
        SortInfo si1 = ((POStore)(pp.getLeaves().get(1))).getSortInfo();
        SortInfo expected = getSortInfo(
                Arrays.asList(new String[] {"i", "d"}), 
                Arrays.asList(new Integer[] {0, 2}),
                Arrays.asList(new SortColInfo.Order[] {
                        SortColInfo.Order.ASCENDING, 
                        SortColInfo.Order.DESCENDING}));
        assertEquals(expected, si0);
        assertEquals(expected, si1);
    }
    
    /**
     * tests that sortInfo is null when there is no schema for order by
     * before the store
     * @throws Exception
     */
    @Test
    public void testSortInfoNoOrderBySchema() throws Exception {
        LogicalPlanTester lpt = new LogicalPlanTester(pc);
        lpt.buildPlan("a = load 'bla' ;");
        lpt.buildPlan("b = order a by $0;");
        LogicalPlan lp = lpt.buildPlan("store b into 'foo';");
        PigServer.SortInfoSetter siSetter = new PigServer.SortInfoSetter(lp); 
        siSetter.visit();
        PhysicalPlan pp = buildPhysicalPlan(lp);
        SortInfo si = ((POStore)(pp.getLeaves().get(0))).getSortInfo();
        SortInfo expected = getSortInfo(Arrays.asList(new String[] {null}),
                Arrays.asList(new Integer[] {0}), 
                Arrays.asList(new SortColInfo.Order[] { 
                        SortColInfo.Order.ASCENDING}));
        assertEquals(expected, si);
    }
    
    /*@Test
    public void testUserFunc() throws VisitorException {
    	String query = "foreach (group (load 'file:ABCD') all) generate " + COUNT.class.getName() + "($1) ;";
    	LogicalPlan plan = buildPlan(query);
    	
    	PhysicalPlan pp = buildPhysicalPlan(plan);
    	
    	pp.explain(System.out);
    }*/
    
    /*@Test
    public void testQuery4() throws VisitorException {
        String query = "foreach (load 'a') generate AVG($1, $2) ;";
        LogicalPlan lp = buildPlan(query);
        PhysicalPlan pp = buildPhysicalPlan(lp);
        
        DependencyOrderWalker<PhysicalOperator, PhysicalPlan> walker = new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(pp);
    	PhyPlanPrinterVisitor visitor = new PhyPlanPrinterVisitor(pp, walker);
    	visitor.visit();
    	System.out.println(visitor.output);
    }*/
    
    // Helper Functions
    // =================
    
    private SortInfo getSortInfo(List<String> colNames, List<Integer> colIndices, 
            List<SortColInfo.Order> orderingTypes) {
        List<SortColInfo> sortColInfoList = new ArrayList<SortColInfo>();
        for(int i = 0; i < colNames.size(); i++) {
            sortColInfoList.add(new SortColInfo(colNames.get(i),
                    colIndices.get(i), orderingTypes.get(i)));
        }
        return new SortInfo(sortColInfoList);
    }
    
    public PhysicalPlan buildPhysicalPlan(LogicalPlan lp) throws VisitorException {
    	LogToPhyTranslationVisitor visitor = new LogToPhyTranslationVisitor(lp);
    	visitor.setPigContext(pc);
    	visitor.visit();
    	return visitor.getPhysicalPlan();
    }
    
    public LogicalPlan buildPlan(String query) {
        return buildPlan(query, LogicalPlanBuilder.class.getClassLoader());
    }

    public LogicalPlan buildPlan(String query, ClassLoader cldr) {
        LogicalPlanBuilder.classloader = cldr;
        PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());
        try {

            pigContext.connect();
            
            LogicalPlanBuilder builder = new LogicalPlanBuilder(pigContext); //


            LogicalPlan lp = builder.parse("Test-Plan-Builder",
                                           query,
                                           aliases,
                                           logicalOpTable,
                                           aliasOp,
                                           fileNameMap);
            List<LogicalOperator> roots = lp.getRoots();
            
            
            if(roots.size() > 0) {
                for(LogicalOperator op: roots) {
                    if (!(op instanceof LOLoad) && !(op instanceof LODefine)){
                        throw new Exception("Cannot have a root that is not the load or define operator. Found " + op.getClass().getName());
                    }
                }
            }
            
            System.err.println("Query: " + query);
            
            //Just the top level roots and their children
            //Need a recursive one to travel down the tree
            
            for(LogicalOperator op: lp.getRoots()) {
                System.err.println("Logical Plan Root: " + op.getClass().getName() + " object " + op);    

                List<LogicalOperator> listOp = lp.getSuccessors(op);
                
                if(null != listOp) {
                    Iterator<LogicalOperator> iter = listOp.iterator();
                    while(iter.hasNext()) {
                        LogicalOperator lop = iter.next();
                        System.err.println("Successor: " + lop.getClass().getName() + " object " + lop);
                    }
                }
            }
            
            assertTrue(lp != null);
            return lp;
        } catch (IOException e) {
            // log.error(e);
            //System.err.println("IOException Stack trace for query: " + query);
            //e.printStackTrace();
            fail("IOException: " + e.getMessage());
        } catch (Exception e) {
            log.error(e);
            //System.err.println("Exception Stack trace for query: " + query);
            //e.printStackTrace();
            fail(e.getClass().getName() + ": " + e.getMessage() + " -- " + query);
        }
        return null;
    }
    
    Map<LogicalOperator, LogicalPlan> aliases = new HashMap<LogicalOperator, LogicalPlan>();
    Map<OperatorKey, LogicalOperator> logicalOpTable = new HashMap<OperatorKey, LogicalOperator>();
    Map<String, LogicalOperator> aliasOp = new HashMap<String, LogicalOperator>();
    Map<String, String> fileNameMap = new HashMap<String, String>();
}
