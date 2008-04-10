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
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import junit.framework.AssertionFailedError;

import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.impl.builtin.ShellBagEvalFunc;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOEval;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.impl.logicalLayer.parser.ParseException;


public class TestLogicalPlanBuilder extends junit.framework.TestCase {

    private final Log log = LogFactory.getLog(getClass());
    
    @Test
    public void testQuery1() {
        String query = "foreach (load 'a') generate $1,$2;";
        buildPlan(query);
    }

    @Test
    public void testQuery2() {
        String query = "foreach (load 'a' using " + PigStorage.class.getName() + "(':')) generate $1, 'aoeuaoeu' ;";
        buildPlan(query);
    }

    @Test
    public void testQuery3() {
        String query = "foreach (cogroup (load 'a') by $1, (load 'b') by $1) generate org.apache.pig.builtin.AVG($1) ;";
        buildPlan(query);
    }

    @Test
    public void testQuery4() {
        String query = "foreach (load 'a') generate AVG($1, $2) ;";
        buildPlan(query);
    }

    @Test
    public void testQuery5() {
        String query = "foreach (group (load 'a') ALL) generate $1 ;";
        buildPlan(query);
    }

    @Test
    public void testQuery6() {
        String query = "foreach (group (load 'a') by $1) generate group, '1' ;";
        buildPlan(query);
    }

    @Test
    public void testQuery7() {
        String query = "foreach (load 'a' using " + PigStorage.class.getName() + "()) generate $1 ;";
        buildPlan(query);
    }

    @Test
    public void testQuery10() {
        String query = "foreach (cogroup (load 'a') by ($1), (load 'b') by ($1)) generate $1.$1, $2.$1 ;";
        buildPlan(query);
    }

    @Test
    public void testQuery11() {
        String query = " foreach (group (load 'a') by $1, (load 'b') by $2) generate group, AVG($1) ;";
        buildPlan(query);
    }
    
    @Test
    public void testQuery12() {
        String query = "foreach (load 'a' using " + PigStorage.class.getName() + "()) generate AVG($1) ;";
        buildPlan(query);
    }

    @Test
    public void testQuery13() {
        String query = "foreach (cogroup (load 'a') ALL) generate group ;";
        buildPlan(query);
    }

    @Test
    public void testQuery14() {
        String query = "foreach (group (load 'a') by ($6, $7)) generate flatten(group) ;";
        buildPlan(query);
    }

    @Test
    public void testQuery15() {
        String query = " foreach (load 'a') generate $1, 'hello', $3 ;";
        buildPlan(query);
    }
    

    @Test
    public void testQueryFail1() {
        String query = " foreach (group (A = load 'a') by $1) generate A.'1' ;";
        try {
            buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }

    @Test
    public void testQueryFail2() {
        String query = "foreach group (load 'a') by $1 generate $1.* ;";
        try {
            buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }
    

    @Test
    public void testQueryFail3() {
        String query = "generate DISTINCT foreach (load 'a');";
        try {
            buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }
    
    @Test
    public void testQueryFail4() {
        String query = "generate [ORDER BY $0][$3, $4] foreach (load 'a');";
        try {
            buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }

    @Test
    public void testQueryFail5() {
        String query = "generate " + TestApplyFunc.class.getName() + "($2.*) foreach (load 'a');";
        try {
            buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }

    
    
    /**
     * User generate functions must be in default package Bug 831620 - fixed
     */
    @Test
    public void testQuery17() {
        String query =  "foreach (load 'A')" + "generate " + TestApplyFunc.class.getName() + "($1);";
        buildPlan(query);
    }

    static public class TestApplyFunc extends org.apache.pig.EvalFunc<Tuple> {
        @Override
        public void exec(Tuple input, Tuple output) throws IOException {
            output.appendTuple(input);
        }
    }

    /**
     * Validate that parallel is parsed correctly Bug 831714 - fixed
     */
    @Test
    public void testQuery18() {
        String query = "FOREACH (group (load 'a') ALL PARALLEL 16) generate group;";
        LogicalPlan lp = buildPlan(query);
        Map<OperatorKey, LogicalOperator> logicalOpTable = lp.getOpTable();
        OperatorKey logicalKey = lp.getRoot();
        LogicalOperator lo = logicalOpTable.get(logicalOpTable.get(logicalKey).getInputs().get(0));
        
        if (lo instanceof LOCogroup) {
            assertTrue(((LOCogroup) lo).getRequestedParallelism() == 16);
        } else {
            fail("Error: Unexpected Parse Tree output");
        }
    }
    
    
    @Test
    public void testQuery19() {
        buildPlan("a = load 'a';");
    	buildPlan("a = filter a by $1 == '3';");
    }
    
    @Test
    public void testQuery20() {
        String query = "foreach (load 'a') generate ($1 == '3'? $2 : $3) ;";
        buildPlan(query);
    }
    
    @Test
    public void testQuery21() {
    	buildPlan("A = load 'a';");
    	buildPlan("B = load 'b';");
        buildPlan("foreach (cogroup A by ($1), B by ($1)) generate A, flatten(B.($1, $2, $3));");
    }
    
    @Test
    public void testQuery22() {
    	buildPlan("A = load 'a';");
    	buildPlan("B = load 'b';");
    	buildPlan("C = cogroup A by ($1), B by ($1);");
        String query = "foreach C { " +
        		"B = order B by $0; " +
        		"generate FLATTEN(A), B.($1, $2, $3) ;" +
        		"};" ;
        buildPlan(query);
    }
    
    @Test
    public void testQuery23() {
    	buildPlan("A = load 'a';");
    	buildPlan("B = load 'b';");
    	
    	buildPlan("C = cogroup A by ($1), B by ($1);");
        
    	String query = "foreach C { " +
		"A = Distinct A; " +
		"B = FILTER A BY $1 < 'z'; " +
		"C = FILTER A BY $2 == $3;" +
		"B = ARRANGE B BY $1;" +
		"GENERATE A, FLATTEN(B.$0);" +
		"};";
        buildPlan(query);
    }
    
    @Test
    public void testQuery24() {
    	buildPlan("a = load 'a';");
    	
    	String query = "foreach a generate (($0 == $1) ? 'a' : $2), $4 ;";
        buildPlan(query);
    }
    
    @Test
    public void testQuery25() {
        String query = "foreach (load 'a') {" +
        		"B = FILTER $0 BY (($1 == $2) AND ('a' < 'b'));" +
        		"generate B;" +
        		"};";
        buildPlan(query);
    }
    
    @Test
    public void testQuery26() {
        String query = "foreach (load 'a') generate  ((NOT (($1 == $2) OR ('a' < 'b'))) ? 'a' : $2), 'x' ;";
        buildPlan(query);
    }
    
    @Test
    public void testQuery27() {
        String query =  "foreach (load 'a'){" +
        		"A = DISTINCT $3.$1;" +
        		" generate " + TestApplyFunc.class.getName() + "($2, $1.($1, $4));" +
        				"};";
        buildPlan(query);
    }
    
    @Test
    public void testQuery28() {
        String query = "foreach (load 'a') generate " + TestApplyFunc.class.getName() + "($2, " + TestApplyFunc.class.getName() + "($2.$3));";
        buildPlan(query);
    }
    
    @Test
    public void testQuery29() {
        String query = "load 'myfile' using " + TestStorageFunc.class.getName() + "() as (col1);";
        buildPlan(query);
    }


    @Test
    public void testQuery30() {
        String query = "load 'myfile' using " + TestStorageFunc.class.getName() + "() as (col1, col2);";
        buildPlan(query);
    }
 
    public static class TestStorageFunc implements LoadFunc{
    	public void bindTo(String fileName, BufferedPositionedInputStream is, long offset, long end) throws IOException {
    		
    	}
    	
    	public Tuple getNext() throws IOException {
    		return null;
    	}
    }

    @Test
    public void testQuery31() {
        String query = "load 'myfile' as (col1, col2);";
        buildPlan(query);
    }
    
    @Test
    public void testQuery32() {
        String query = "foreach (load 'myfile' as (col1, col2 : (sub1, sub2), col3 : (bag1))) generate col1 ;";
        buildPlan(query);
    }
    
    @Test
    public void testQuery33() {
    	buildPlan("A = load 'a' as (aCol1, aCol2);");
    	buildPlan("B = load 'b' as (bCol1, bCol2);");
    	buildPlan("C = cogroup A by (aCol1), B by bCol1;");
    	String query = "foreach C generate group, A.aCol1;";
        buildPlan(query);
    }
    
    
    @Test
    //TODO: Nested schemas don't work now. Probably a bug in the new parser.
    public void testQuery34() {
    	buildPlan("A = load 'a' as (aCol1, aCol2 : (subCol1, subCol2));");
    	buildPlan("A = filter A by aCol2 == '1';");
    	buildPlan("B = load 'b' as (bCol1, bCol2);");
    	String query = "foreach (cogroup A by (aCol1), B by bCol1 ) generate A.aCol2, B.bCol2 ;";
        buildPlan(query);
    }
    
    
    
    @Test
    public void testQuery35() {
        String query = "foreach (load 'a' as (col1, col2)) generate col1, col2 ;";
        buildPlan(query);
    }
    
    @Test
    public void testQuery36() {
        String query = "foreach (cogroup ( load 'a' as (col1, col2)) by col1) generate $1.(col2, col1);";
        buildPlan(query);
    }
    
    @Test
    public void testQueryFail37() {
        String query = "A = load 'a'; asdasdas";
        try{
        	buildPlan(query);
        }catch(AssertionFailedError e){
        	assertTrue(e.getMessage().contains("Exception"));
        }
    }
    
    @Test
    public void testQuery38(){
    	String query = "c = cross (load 'a'), (load 'b');";
    	buildPlan(query);
    }
    
    
    // @Test
    // TODO: Schemas don't quite work yet
    public void testQuery39(){
    	buildPlan("a = load 'a' as (url, host, rank);");
    	buildPlan("b = group a by (url,host); ");
    	LogicalPlan lp = buildPlan("c = foreach b generate flatten(group.url), SUM(a.rank) as totalRank;");
    	buildPlan("d = filter c by totalRank > '10';");
    	buildPlan("e = foreach d generate url;");
    	
    }
    
    @Test
    public void testQuery40() {
        buildPlan("a = FILTER (load 'a') BY IsEmpty($2);");
        buildPlan("a = FILTER (load 'a') BY (IsEmpty($2) AND ($3 == $2));");
    }
    
    @Test
    public void testQuery41() {
    	buildPlan("a = load 'a';");
    	buildPlan("b = a as (host,url);");
    	buildPlan("foreach b generate host;");
    }
    
    @Test
    public void testQuery42() {
    	buildPlan("a = load 'a';");
    	buildPlan("b = foreach a generate $0 as url, $1 as rank;");
    	buildPlan("foreach b generate url;");
    }

    @Test
    public void testQuery43() {
    	buildPlan("a = load 'a' as (url,hitCount);");
    	buildPlan("b = load 'a' as (url,rank);");
    	buildPlan("c = cogroup a by url, b by url;");
    	buildPlan("d = foreach c generate group,flatten(a),flatten(b);");
    	buildPlan("e = foreach d generate group, a::url, b::url, b::rank, rank;");
    }

    @Test
    public void testQuery44() {
    	buildPlan("a = load 'a' as (url, pagerank);");
    	buildPlan("b = load 'b' as (url, query, rank);");
    	buildPlan("c = cogroup a by (pagerank#'nonspam', url) , b by (rank/'2', url) ;");
    	buildPlan("foreach c generate group.url;");
    }

    @Test
    public void testQueryFail44() throws Throwable {
        PigServer pig = null;
        try {
            pig = new PigServer("local");
        } catch (IOException e) {
            assertTrue(false);  // pig server failed for some reason
        }
	    pig.registerFunction("myTr",ShellBagEvalFunc.class.getName() + "('tr o 0')");
		try{
			pig.registerQuery("b = foreach (load 'a') generate myTr(myTr(*));");
        }catch(Exception e){
        	return;
        }
        assertTrue(false);
    }

    @Test
    public void testRegressionPig100NoSuchAlias() throws Throwable {
        PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());
        LogicalPlanBuilder builder = new LogicalPlanBuilder(pigContext);

        boolean caughtIt = false;
        try {
            LogicalPlan lp = builder.parse("test",
                "b = filter c by $0 > '5';", aliases, logicalOpTable);
        } catch (ParseException e) {
            caughtIt = true;
			assertEquals("Unable to find alias:",
				e.getMessage().substring(0, 21));
        }
        assertTrue(caughtIt);
    }

    @Test
    public void testRegressionPig100NoAliases() throws Throwable {
        PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());
        LogicalPlanBuilder builder = new LogicalPlanBuilder(pigContext);

        boolean caughtIt = false;
        try {
            LogicalPlan lp = builder.parse("test",
				"b = filter c by $0 > '5';", null, logicalOpTable);
        } catch (RuntimeException e) {
            caughtIt = true;
			assertEquals("aliases var is not initialize.", e.getMessage());
        }
        assertTrue(caughtIt);
    }

	/*
    // Select
    public void testQuery45() {
    	buildPlan("A = load 'a' as (url,hitCount);");
    	buildPlan("B = select url, hitCount from A;");
    	buildPlan("C = select url, hitCount from B;");
    }

    //Select + Join
    public void testQuery46() {
    	buildPlan("A = load 'a' as (url,hitCount);");
    	buildPlan("B = load 'b' as (url,pageRank);");
    	buildPlan("C = select A.url, A.hitCount, B.pageRank from A join B on A.url == B.url;");    	
    }

    // Mutliple Joins
    public void testQuery47() {
    	buildPlan("A = load 'a' as (url,hitCount);");
    	buildPlan("B = load 'b' as (url,pageRank);");
    	buildPlan("C = load 'c' as (pageRank, position);");
    	buildPlan("B = select A.url, A.hitCount, B.pageRank from (A join B on A.url == B.url) join C on B.pageRank == C.pageRank;");
    }

    // Group
    public void testQuery48() {
    	buildPlan("A = load 'a' as (url,hitCount);");    	
    	buildPlan("C = select A.url, AVG(A.hitCount) from A group by url;");
    }

    // Join + Group
    public void testQuery49() {
    	buildPlan("A = load 'a' as (url,hitCount);");
    	buildPlan("B = load 'b' as (url,pageRank);");
    	buildPlan("C = select A.url, AVG(B.pageRank), SUM(A.hitCount) from A join B on A.url == B.url group by A.url;");
    }

    // Group + Having
    public void testQuery50() {
    	buildPlan("A = load 'a' as (url,hitCount);");    	
    	buildPlan("C = select A.url, AVG(A.hitCount) from A group by url having AVG(A.hitCount) > '6';");
    }

 // Group + Having + Order
    public void testQuery51() {
    	buildPlan("A = load 'a' as (url,hitCount);");    	
    	buildPlan("C = select A.url, AVG(A.hitCount) from A group by url order by A.url;");
    }
    
    // Group + Having + Order
    public void testQuery52() {
    	buildPlan("A = load 'a' as (url,hitCount);");    	
    	buildPlan("C = select A.url, AVG(A.hitCount) from A group by url having AVG(A.hitCount) > '6' order by A.url;");
    }

    // Group + Having + Order 2
    public void testQuery53() {
    	buildPlan("A = load 'a' as (url,hitCount);");
    	buildPlan("C = select A.url, AVG(A.hitCount) from A group by url having AVG(A.hitCount) > '6' order by AVG(A.hitCount);");
    }

    // Group + Having + Order 2
    public void testQuery54() {
    	buildPlan("A = load 'a' as (url,hitCount, size);");
    	buildPlan("C = select A.url, AVG(A.hitCount) from A group by url having AVG(A.size) > '6' order by AVG(A.hitCount);");
    }

    // Group + Having + Order 2
    public void testQuery55() {
    	buildPlan("A = load 'a' as (url,hitCount, size);");
    	buildPlan("C = select A.url, AVG(A.hitCount), SUM(A.size) from A group by url having AVG(A.size) > '6' order by AVG(A.hitCount);");
    }

    // Group + Having + Order 2
    public void testQuery56() {
    	buildPlan("A = load 'a' as (url,hitCount, date);");
    	buildPlan("C = select A.url, A.date, SUM(A.hitCount) from A group by url, date having AVG(A.hitCount) > '6' order by A.date;");
    }
	*/

    // Helper Functions
    // =================
    public LogicalPlan buildPlan(String query) {
        return buildPlan(query, LogicalPlanBuilder.class.getClassLoader());
    }

    public LogicalPlan buildPlan(String query, ClassLoader cldr) {
        LogicalPlanBuilder.classloader = cldr;
        PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());
        LogicalPlanBuilder builder = new LogicalPlanBuilder(pigContext); //

        try {
            LogicalPlan lp = builder.parse("Test-Plan-Builder",
                                           query,
                                           aliases,
                                           logicalOpTable);
            if (logicalOpTable.get(lp.getRoot()) instanceof LOEval){
            	System.out.println(query);
            	System.out.println(((LOEval)logicalOpTable.get(lp.getRoot())).getSpec());
            }
            if (lp.getAlias()!=null){
            	aliases.put(lp.getAlias(), lp);
            }
            
            assertTrue(lp != null);
            return lp;
        } catch (IOException e) {
            // log.error(e);
            fail("IOException: " + e.getMessage());
        } catch (Exception e) {
            log.error(e);
            fail(e.getClass().getName() + ": " + e.getMessage() + " -- " + query);
        }
        return null;
    }
    
    Map<String, LogicalPlan> aliases = new HashMap<String, LogicalPlan>();
    Map<OperatorKey, LogicalOperator> logicalOpTable = new HashMap<OperatorKey, LogicalOperator>();    
}
