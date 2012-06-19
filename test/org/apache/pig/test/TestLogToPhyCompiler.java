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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.lang.CharSet;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.SortColInfo;
import org.apache.pig.SortInfo;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LogToPhyTranslationVisitor;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

import org.junit.Test;

/**
 * 
 * To generate golden files, update generate to true
 * 
 */
public class TestLogToPhyCompiler extends junit.framework.TestCase {
    File A;
    final int MAX_RANGE = 10;
    
    Random r = new Random();
    PigContext pc = new PigContext(ExecType.LOCAL, new Properties());

    private boolean generate = false;
    
    PigServer pigServer = null;
    private static final int MAX_SIZE = 100000;;
   
    
    @Override
    protected void setUp() throws Exception {
    	pigServer = new PigServer( ExecType.LOCAL, new Properties() );
        pc.connect();
    }
    
    private String compilePlan(String query, String goldenFile, boolean checkOneLeaf)
            throws Exception, FrontendException, FileNotFoundException,
            IOException {
        LogicalPlan lp = buildPlan(query);
        PhysicalPlan pp = buildPhysicalPlan(lp);
        
        if (checkOneLeaf) {
          //Ensure that there is only 1 leaf node
            assertEquals(1, pp.getLeaves().size());
        }
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pp.explain(baos);
        baos.write((int)'\n');
        String compiledPlan = baos.toString();
        compiledPlan = removedUnaffectingChanges(compiledPlan);

        generateGolden(goldenFile, compiledPlan);
        return compiledPlan;
    }

    private void generateGolden(String goldenFile, String data)
            throws FileNotFoundException, IOException {
        if(generate){
            FileOutputStream fos = new FileOutputStream(goldenFile);
            fos.write(data.getBytes("UTF-8"));
            fos.close();
        }
    }

    private String readGolden(int MAX_SIZE, String goldenFile)
            throws FileNotFoundException, IOException {
        FileInputStream fis = new FileInputStream(goldenFile);
        byte[] b = new byte[MAX_SIZE];
        int len = fis.read(b);
        fis.close();
        String goldenPlan = new String(b, 0, len);
        goldenPlan = removedUnaffectingChanges(goldenPlan);
        return goldenPlan;
    }

    private String removedUnaffectingChanges(String compiledPlan) {
        return compiledPlan
                .replaceAll("test-[0-9]*: ","test-: ")
                .replaceAll("Load(.*)","Load()")
                .replaceAll("Store(.*)","Store()")
                .replaceAll("scope-[0-9]*\\n", "\n");
    }
    
    private void checkAgainstGolden(String query, String goldenFile,
            boolean checkOneLeaf, String testName) throws Exception, FrontendException,
            FileNotFoundException, IOException {
        goldenFile = "test/org/apache/pig/test/data/GoldenFiles/" + goldenFile;
        String compiledPlan = compilePlan(query, goldenFile, checkOneLeaf);
        String goldenPlan = readGolden(MAX_SIZE, goldenFile);
        System.out.println();
        System.out.println(compiledPlan);
        System.out.println("-------------" + testName);
        assertEquals(compiledPlan, goldenPlan);
    }
    
    @Test // Commented out due to PIG-2020
    public void testComplexForeach() throws Exception {
        checkAgainstGolden(
                "C = foreach (load 'a' as  (a:bag{} ) ) {" +
                "B = FILTER $0 BY ($1 == $2);" +
                "generate B;" +
                "};" + 
                "store C into 'output';", 
                "ComplexForeach.gld", 
                false, 
                "testComplexForeach");
    }
 
    public void testSort() throws Exception {
        checkAgainstGolden(
                "store (order (load 'a') by $0) into 'output';", 
                "Sort.gld", 
                false, 
                "testSort");
    }

        
    public void testDistinct() throws Exception {
        checkAgainstGolden(
                "store( distinct (load 'a') ) into 'output';", 
                "Distinct.gld", 
                false, 
                "testDistinct");
    }
    
    public void testCogroup() throws Exception {
        checkAgainstGolden(
              "A = cogroup (load 'a') by ($0 + $1, $0 - $1), (load 'b') by ($0 + $1, $0 - $1);" +
              "store A into 'output';", 
              "Cogroup.gld", 
              false, 
              "testCogroup");
    }
    
    public void testArithmetic() throws Exception {
        checkAgainstGolden(
                "A = foreach (load 'A') generate $0 + $1 + 5, $0 - 5 - $1, 'hello';" +
                "store A into 'output';", 
                "Arithmetic.gld", 
                true, 
                "testArithmetic");
    }

    public void testComparison() throws Exception {
        checkAgainstGolden(
                "A = filter (load 'a' using " + PigStorage.class.getName() + "(':')) by $0 + $1 > ($0 - $1) * (4 / 2);" +
                "store A into 'output';", 
                "Comparison.gld", 
                true, 
                "testComparison");
    }

    @Test
    public void testBinCond() throws Exception {
        checkAgainstGolden(
                "A = foreach (load 'a') generate ($1 == '3'? $2 + $3 : $2 - $3) ;" +
                "store A into 'output';", 
                "BinCond.gld", 
                false, 
                "testBinCond");
    }

    @Test
    public void testGenerate() throws Exception {
        checkAgainstGolden(
                "A = foreach (load 'a') generate ($1+$2), ($1-$2), ($1*$2), ($1/$2), ((int)$1%(int)$2), -($1) ;" +
                "store A into 'output';", 
                "Generate.gld", 
                false, 
                "testGenerate");
    }

    @Test
    public void testUnion() throws Exception {
        checkAgainstGolden(
                "A = union (load 'a'), (load 'b'), (load 'c');" +
                        "store A into 'output';", 
                        "Union.gld", 
                        false, 
                "testUnion");
    }
    
    @Test
    public void testSplit() throws Exception {
    	String query = "split (load 'a') into x if $0 < '7', y if $0 > '7';"  +
    	"store x into 'output';";
    	String goldenFile = "test/org/apache/pig/test/data/GoldenFiles/Split1.gld";
    	
    	String compiledPlan = compilePlan(query, goldenFile, false);
        
    	FileInputStream fis1 = new FileInputStream(goldenFile);
    	FileInputStream fis2 = new FileInputStream("test/org/apache/pig/test/data/GoldenFiles/Split2.gld");
        byte[] b1 = new byte[MAX_SIZE];
        byte[] b2 = new byte[MAX_SIZE];
        int len = fis1.read(b1);
        int test = fis2.read(b2);
        //System.out.println("Length of first plan = " + len + " of second = " + test);
        String goldenPlan1 = new String(b1, 0, len);
        String goldenPlan2 = new String(b2, 0, len);
        goldenPlan1 = removedUnaffectingChanges(goldenPlan1);
        goldenPlan2 = removedUnaffectingChanges(goldenPlan2);

        System.out.println();
        System.out.println(compiledPlan);
        System.out.println("-------------testSplit");

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
    public void testIsNull() throws Exception {
        //TODO
        //PONot is not implemented. The query below translates to POIsNull istead
        //of PONOt(POIsNull)
    	String query = "split (load 'a') into x if $0 IS NULL, y if $0 IS NOT NULL;" +
    	"store x into 'output';";
    	LogicalPlan plan = buildPlan(query);
    	
    	PhysicalPlan pp = buildPhysicalPlan(plan);
    	
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pp.explain(baos);
        baos.write((int)'\n');
        String compiledPlan = baos.toString();
        compiledPlan = removedUnaffectingChanges(compiledPlan);

        if(generate){
            FileOutputStream fos = new FileOutputStream("test/org/apache/pig/test/data/GoldenFiles/IsNull1.gld");
            fos.write(baos.toByteArray());
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
        goldenPlan1 = removedUnaffectingChanges(goldenPlan1);
        goldenPlan2 = removedUnaffectingChanges(goldenPlan2);

        System.out.println();
        System.out.println(compiledPlan);
        System.out.println("-------------testIsNull");

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
    public void testLimit() throws Exception {
        checkAgainstGolden(
                "store( limit (load 'a') 5 ) into 'output';", 
                "Limit.gld", 
                false, 
                "testLimit");
    }

    /**
     * tests sortinfo for the case where order by is ascending on
     * all sort columns
     */
    @Test
    public void testSortInfoAsc() throws Exception {
        String query = "a = load 'bla' as (i:int, n:chararray, d:double);" +
                       "b = order a by i, d;" +
                       "store b into 'foo';";
        PhysicalPlan pp = Util.buildPp( pigServer, query );
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
        String query = "a = load 'bla' as (i:int, n:chararray, d:double);" +
                       "b = filter a by i > 10;" +
                       "c = order b by i desc, d;" + 
                       "store c into 'foo';";
        PhysicalPlan pp = Util.buildPp( pigServer, query );
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
        String query = "a = load 'bla' as (i:int, n:chararray, d:double);" + 
                       "b = filter a by i > 10;" +
                       "store b into 'foo';";
        PhysicalPlan pp = Util.buildPp( pigServer, query );
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
        String query = "a = load 'bla' as (i:int, n:chararray, d:double);" +
                       "b = order a by i, d;" +
                       "c = distinct b;" +
                       "store c into 'foo';";
        PhysicalPlan pp = Util.buildPp( pigServer, query );
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
        String query = "a = load 'bla' as (i:int, n:chararray, d:double);" + 
                       "b = order a by i, d desc;" +
                       "c = limit b 10;" +
                       "store c into 'foo';";
        PhysicalPlan pp = Util.buildPp( pigServer, query );
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
        String query = "a = load 'bla' as (i:int, n:chararray, d:double);" +
                       "b = order a by i, d desc;" +
                       "store b into '1';" + 
                       "store b into '2';";
        
        PhysicalPlan pp = Util.buildPp(pigServer, query);
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
        String query = "a = load 'bla' ;" +
                       "b = order a by $0;" +
                       "store b into 'foo';";
        PhysicalPlan pp = Util.buildPp( pigServer, query );
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
    
    public PhysicalPlan buildPhysicalPlan(LogicalPlan lp) throws FrontendException {
    	LogToPhyTranslationVisitor visitor = new LogToPhyTranslationVisitor(lp);
    	visitor.setPigContext(pc);
    	visitor.visit();
    	return visitor.getPhysicalPlan();
    }
    
    public LogicalPlan buildPlan(String query) throws Exception {
    	try {
    		return Util.parse(query, pc);
    	} catch(Throwable t) {
    		throw new Exception("Catch exception: " + t.toString() );
    	}
    }

}
