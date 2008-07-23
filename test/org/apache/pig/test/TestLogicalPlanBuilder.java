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
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.net.URL;
import java.util.List;
import java.util.Set;

import junit.framework.AssertionFailedError;

import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.LoadFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.ExecType;
//import org.apache.pig.impl.builtin.ShellBagEvalFunc;
import org.apache.pig.impl.builtin.GFAny;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOLoad;
//import org.apache.pig.impl.logicalLayer.LOEval;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.logicalLayer.ExpressionOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.impl.logicalLayer.LOPrinter;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;


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

    // TODO FIX Query3 and Query4
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

    // TODO FIX Query11 and Query12
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
 
    // TODO FIX Query17
    @Test
    public void testQuery17() {
        String query =  "foreach (load 'A')" + "generate " + TestApplyFunc.class.getName() + "($1);";
        buildPlan(query);
    }


    static public class TestApplyFunc extends org.apache.pig.EvalFunc<Tuple> {
        @Override
        public Tuple exec(Tuple input) throws IOException {
            Tuple output = TupleFactory.getInstance().newTuple(input.getAll());
            return output;
        }
    }
    
    
    /**
     * Validate that parallel is parsed correctly Bug 831714 - fixed
     */
    
    @Test
    public void testQuery18() {
        String query = "FOREACH (group (load 'a') ALL PARALLEL 16) generate group;";
        LogicalPlan lp = buildPlan(query);
        LogicalOperator root = lp.getRoots().get(0);   
        
        List<LogicalOperator> listOp = lp.getSuccessors(root);
        
        LogicalOperator lo = listOp.get(0);
        
        if (lo instanceof LOCogroup) {
            assertTrue(((LOCogroup) lo).getRequestedParallelism() == 16);
        } else {
            fail("Error: Unexpected Parse Tree output");
        }  
    }
    
    
    
    
    @Test
    public void testQuery19() {
        buildPlan("a = load 'a';");
        buildPlan("b = filter a by $1 == '3';");
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
        //TODO
        //A sequence of filters within a foreach translates to
        //a split statement. Currently it breaks as adding an
        //additional output to the filter fails as filter supports
        //single output
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
    
    // TODO FIX Query27 and Query28
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
        
        public Schema determineSchema(URL filename) {
            return null;
        }
        
        public void fieldsToRead(Schema schema) {
            
        }
        
        public DataBag bytesToBag(byte[] b) throws IOException {
            return null;
        }

        public Boolean bytesToBoolean(byte[] b) throws IOException {
            return null;
        }
        
        public String bytesToCharArray(byte[] b) throws IOException {
            return null;
        }
        
        public Double bytesToDouble(byte[] b) throws IOException {
            return null;
        }
        
        public Float bytesToFloat(byte[] b) throws IOException {
            return null;
        }
        
        public Integer bytesToInteger(byte[] b) throws IOException {
            return null;
        }

        public Long bytesToLong(byte[] b) throws IOException {
            return null;
        }

        public Map<Object, Object> bytesToMap(byte[] b) throws IOException {
            return null;
        }

        public Tuple bytesToTuple(byte[] b) throws IOException {
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
        String query = "foreach (load 'myfile' as (col1, col2 : tuple(sub1, sub2), col3 : tuple(bag1))) generate col1 ;";
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
        buildPlan("A = load 'a' as (aCol1, aCol2 : tuple(subCol1, subCol2));");
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
    
    
    // TODO FIX Query39 and Query40
    @Test
    public void testQuery39(){
        buildPlan("a = load 'a' as (url, host, rank);");
        buildPlan("b = group a by (url,host); ");
        LogicalPlan lp = buildPlan("c = foreach b generate flatten(group.url), SUM(a.rank) as totalRank;");
        buildPlan("d = filter c by totalRank > '10';");
        buildPlan("e = foreach d generate totalRank;");
    }
    
    @Test
    public void testQueryFail39(){
        buildPlan("a = load 'a' as (url, host, rank);");
        buildPlan("b = group a by (url,host); ");
        LogicalPlan lp = buildPlan("c = foreach b generate flatten(group.url), SUM(a.rank) as totalRank;");
        buildPlan("d = filter c by totalRank > '10';");
        try {
            buildPlan("e = foreach d generate url;");//url has been falttened and hence the failure
        } catch(AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
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
    public void testQueryFail43() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = load 'b' as (name, height);");
        try {
            String query = "c = cogroup a by (name, age), b by (height);";
            buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    } 


    @Test
    public void testQuery44() {
        buildPlan("a = load 'a' as (url, pagerank);");
        buildPlan("b = load 'b' as (url, query, rank);");
        buildPlan("c = cogroup a by (pagerank#'nonspam', url) , b by (rank/'2', url) ;");
        buildPlan("foreach c generate group.url;");
    }

//TODO
//Commented out testQueryFail44 as I am not able to include org.apache.pig.PigServer;
    @Test
    public void testQueryFail44() throws Throwable {
        PigServer pig = null;
        try {
            pig = new PigServer("local");
        } catch (IOException e) {
            assertTrue(false);  // pig server failed for some reason
        }
        pig.registerFunction("myTr",
            new FuncSpec(GFAny.class.getName() + "('tr o 0')"));
        try{
            pig.registerQuery("b = foreach (load 'a') generate myTr(myTr(*));");
        }catch(Exception e){
            return;
        }
        assertTrue(false);
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

    @Test
    public void testQuery57() {
        String query = "foreach (load 'a') generate ($1+$2), ($1-$2), ($1*$2), ($1/$2), ($1%$2), -($1) ;";
        buildPlan(query);
    }

    
    @Test
    public void testQuery58() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = group a by name;");
        String query = "foreach b {d = a.name; generate group, d;};";
        buildPlan(query);
    } 

	@Test
    public void testQueryFail58(){
        buildPlan("a = load 'a' as (url, host, rank);");
        buildPlan("b = group a by url; ");
        try {
        	LogicalPlan lp = buildPlan("c = foreach b generate group.url;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }

    @Test
    public void testQuery59() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = load 'b' as (name, height);");
        String query = "c = join a by name, b by name;";
        buildPlan(query);
    } 
    
    @Test
    public void testQuery60() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = load 'b' as (name, height);");
        String query = "c = cross a,b;";
        buildPlan(query);
    } 

    @Test
    public void testQuery61() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = load 'b' as (name, height);");
        String query = "c = union a,b;";
        buildPlan(query);
    }

    @Test
    public void testQuery62() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = load 'b' as (name, height);");
        String query = "c = cross a,b;";
        buildPlan(query);
        buildPlan("d = order c by b::name, height, a::gpa;");
        buildPlan("e = order a by name, age, gpa desc;");
        buildPlan("f = order a by $0 asc, age, gpa desc;");
        buildPlan("g = order a by * asc;");
        buildPlan("h = cogroup a by name, b by name;");
        buildPlan("i = foreach h {i1 = order a by *; generate i1;};");
    }

    @Test
    public void testQueryFail62() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = load 'b' as (name, height);");
        String query = "c = cross a,b;";
        buildPlan(query);
        try {
        	buildPlan("d = order c by name, b::name, height, a::gpa;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }

    @Test
    public void testQuery63() {
        buildPlan("a = load 'a' as (name, details: tuple(age, gpa));");
        buildPlan("b = group a by details;");
        String query = "d = foreach b generate group.age;";
        buildPlan(query);
        buildPlan("e = foreach a generate name, details;");
    }

    @Test
    public void testQueryFail63() {
        String query = "foreach (load 'myfile' as (col1, col2 : (sub1, sub2), col3 : (bag1))) generate col1 ;";
        try {
        	buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }
    
    @Test
    public void testQuery64() {
        buildPlan("a = load 'a' as (name: chararray, details: tuple(age, gpa), mymap: map[]);");
        buildPlan("c = load 'a' as (name, details: bag{mytuple: tuple(age: int, gpa)});");
        buildPlan("b = group a by details;");
        String query = "d = foreach b generate group.age;";
        buildPlan(query);
		buildPlan("e = foreach a generate name, details;");
		buildPlan("f = LOAD 'myfile' AS (garage: bag{tuple1: tuple(num_tools: int)}, links: bag{tuple2: tuple(websites: chararray)}, page: bag{something_stupid: tuple(yeah_double: double)}, coordinates: bag{another_tuple: tuple(ok_float: float, bite_the_array: bytearray, bag_of_unknown: bag{})});");
    }

    @Test
    public void testQueryFail64() {
        String query = "foreach (load 'myfile' as (col1, col2 : bag{age: int})) generate col1 ;";
        try {
        	buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }
    
    @Test
    public void testQuery65() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = load 'b' as (name, height);");
		buildPlan("c = cogroup a by (name, age), b by (name, height);");
		buildPlan("d = foreach c generate group.name, a.name as aName, b.name as b::name;");
	}

    @Test
    public void testQueryFail65() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = load 'b' as (name, height);");
		buildPlan("c = cogroup a by (name, age), b by (name, height);");
        try {
			buildPlan("d = foreach c generate group.name, a.name, b.height as age, a.age;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
	}

    @Test
    public void testQuery67() {
        buildPlan(" a = load 'input1' as (name, age, gpa);");
        buildPlan(" b = foreach a generate age, age * 10L, gpa/0.2f, {(16, 4.0e-2, 'hello')};");
    }

    @Test
    public void testQuery68() {
        buildPlan(" a = load 'input1';");
        buildPlan(" b = foreach a generate 10, {(16, 4.0e-2, 'hello'), (0.5f, 'another tuple', 12l, {()})};");
    }

    @Test
    public void testQuery69() {
        buildPlan(" a = load 'input1';");
        buildPlan(" b = foreach a generate {(16, 4.0e-2, 'hello'), (0.5f, 'another tuple', 12L, (1), ())};");
    }

    @Test
    public void testQuery70() {
        buildPlan(" a = load 'input1';");
        buildPlan(" b = foreach a generate [10L#'hello', 4.0e-2#10L, 0.5f#(1), 'world'#42, 42#{('guide')}] as mymap;");
        buildPlan(" c = foreach b generate mymap#10L;");
    }

    @Test
    public void testQueryFail67() {
        buildPlan(" a = load 'input1' as (name, age, gpa);");
        try {
            buildPlan(" b = foreach a generate age, age * 10L, gpa/0.2f, {16, 4.0e-2, 'hello'};");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }
    
    @Test
    public void testQueryFail68() {
        buildPlan(" a = load 'input1' as (name, age, gpa);");
        try {
            buildPlan(" b = foreach a generate {(16 L, 4.0e-2, 'hello'), (0.5f, 'another tuple', 12L, {()})};");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }
    
    @Test
    public void testQuery71() {
        buildPlan("split (load 'a') into x if $0 > '7', y if $0 < '7';");
        buildPlan("b = foreach x generate $0;");
        buildPlan("c = foreach y generate $1;");
    }

    @Test
    public void testQuery72() {
        buildPlan("split (load 'a') into x if $0 > '7', y if $0 < '7';");
        buildPlan("b = foreach x generate (int)$0;");
        buildPlan("c = foreach y generate (bag{})$1;");
        buildPlan("d = foreach y generate (int)($1/2);");
        buildPlan("e = foreach y generate (bag{tuple(int, float)})($1/2);");
        buildPlan("f = foreach x generate (tuple(int, float))($1/2);");
        buildPlan("g = foreach x generate (tuple())($1/2);");
        buildPlan("h = foreach x generate (chararray)($1/2);");
        buildPlan("i = foreach x generate (bytearray)($1/2);");
    }

    @Test
    public void testQueryFail72() {
        buildPlan("split (load 'a') into x if $0 > '7', y if $0 < '7';");
        try {
            buildPlan("c = foreach y generate (bag)$1;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
        try {
            buildPlan("c = foreach y generate (bag{int, float})$1;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
        try {
            buildPlan("c = foreach y generate (tuple)$1;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }

    @Test
    public void testQuery73() {
        buildPlan("split (load 'a') into x if $0 > '7', y if $0 < '7';");
        buildPlan("b = filter x by $0 matches '^fred.*';");
        buildPlan("c = foreach y generate $0, ($0 matches 'yuri.*' ? $1 - 10 : $1);");
    }

    @Test
    public void testQuery74() {
        buildPlan("a = load 'a' as (field1: int, field2: long);");
        buildPlan("b = load 'a' as (field1: bytearray, field2: double);");
        buildPlan("c = group a by field1, b by field1;");
        buildPlan("d = cogroup a by ((field1+field2)*field1), b by field1;");
    }

    @Test
    public void testQuery75() {
        buildPlan("a = union (load 'a'), (load 'b'), (load 'c');");
        buildPlan("b = foreach a {generate $0;} parallel 10;");
    }
    
    @Test
    public void testQuery76() {
        buildPlan("split (load 'a') into x if $0 > '7', y if $0 < '7';");
        buildPlan("b = filter x by $0 IS NULL;");
        buildPlan("c = filter y by $0 IS NOT NULL;");
        buildPlan("d = foreach b generate $0, ($1 IS NULL ? 0 : $1 - 7);");
        buildPlan("e = foreach c generate $0, ($1 IS NOT NULL ? $1 - 5 : 0);");
    }

    @Test 
    public void testQuery80() {
        buildPlan("a = load 'input1' as (name, age, gpa);");
        buildPlan("b = filter a by age < '20';");
        buildPlan("c = group b by age;");
        String query = "d = foreach c {" 
            + "cf = filter b by gpa < '3.0';"
            + "cp = cf.gpa;"
            + "cd = distinct cp;"
            + "co = order cd by gpa;"
            + "generate group, flatten(co);"
            //+ "generate group, flatten(cd);"
            + "};";
        buildPlan(query);
    }

    @Test
    public void testQuery81() {
        buildPlan("a = load 'input1' using PigStorage() as (name, age, gpa);");
        buildPlan("split a into b if name lt 'f', c if (name gte 'f' and name lte 'h'), d if name gt 'h';");
    }

    @Test
    public void testQueryFail81() {
        buildPlan("a = load 'input1' using PigStorage() as (name, age, gpa);");
        try {
            buildPlan("split a into b if name lt 'f', c if (name ge 'f' and name le 'h'), d if name gt 'h';");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }
    
    @Test
    public void testQuery82() {
        buildPlan("a = load 'myfile';");
        buildPlan("b = group a by $0;"); 
        String query = "c = foreach b {"
            + "c1 = order $1 by *;" 
            + "c2 = $1.$0;" 
            + "generate flatten(c1), c2;"
            + "};";
        buildPlan(query);
    }

    @Test
    public void testQueryFail82() {
        buildPlan("a = load 'myfile';");
        buildPlan("b = group a by $0;"); 
        String query = "c = foreach b {"
            + "c1 = order $1 by *;" 
            + "c2 = $1;" 
            + "generate flatten(c1), c2;"
            + "};";
        try {
        buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }

    @Test
    public void testQuery83() {
        buildPlan("a = load 'input1' as (name, age, gpa);");
        buildPlan("b = filter a by age < '20';");
        buildPlan("c = group b by (name,age);");
        String query = "d = foreach c {" 
            + "cf = filter b by gpa < '3.0';"
            + "cp = cf.gpa;"
            + "cd = distinct cp;"
            + "co = order cd by gpa;"
            + "generate group, flatten(co);"
            + "};";
        buildPlan(query);
    }

    @Test
    public void testQuery84() {
        buildPlan("a = load 'input1' as (name, age, gpa);");
        buildPlan("b = filter a by age < '20';");
        buildPlan("c = group b by (name,age);");
        String query = "d = foreach c {"
            + "cf = filter b by gpa < '3.0';"
            + "cp = cf.$2;"
            + "cd = distinct cp;"
            + "co = order cd by gpa;"
            + "generate group, flatten(co);"
            + "};";
        buildPlan(query);
    }
    
    @Test
    public void testQuery85() throws FrontendException {
        LogicalPlan lp;
        buildPlan("a = load 'myfile' as (name, age, gpa);");
        lp = buildPlan("b = group a by (name, age);");
        LOCogroup cogroup = (LOCogroup) lp.getLeaves().get(0);

        Schema.FieldSchema nameFs = new Schema.FieldSchema("name", DataType.BYTEARRAY);
        Schema.FieldSchema ageFs = new Schema.FieldSchema("age", DataType.BYTEARRAY);
        Schema.FieldSchema gpaFs = new Schema.FieldSchema("gpa", DataType.BYTEARRAY);
        
        Schema groupSchema = new Schema(nameFs);
        groupSchema.add(ageFs);
        Schema.FieldSchema groupFs = new Schema.FieldSchema("group", groupSchema, DataType.TUPLE);
        
        Schema loadSchema = new Schema(nameFs);
        loadSchema.add(ageFs);
        loadSchema.add(gpaFs);

        Schema.FieldSchema bagFs = new Schema.FieldSchema("a", loadSchema, DataType.BAG);
        
        Schema cogroupExpectedSchema = new Schema(groupFs);
        cogroupExpectedSchema.add(bagFs);

        assertTrue(cogroup.getSchema().equals(cogroupExpectedSchema));

        lp = buildPlan("c = foreach b generate group.name, group.age, COUNT(a.gpa);");
        LOForEach foreach  = (LOForEach) lp.getLeaves().get(0);

        Schema foreachExpectedSchema = new Schema(nameFs);
        foreachExpectedSchema.add(ageFs);
        foreachExpectedSchema.add(new Schema.FieldSchema(null, DataType.LONG));

        assertTrue(foreach.getSchema().equals(foreachExpectedSchema));
    }

    @Test
    public void testQuery86() throws FrontendException {
        LogicalPlan lp;
        buildPlan("a = load 'myfile' as (name:Chararray, age:Int, gpa:Float);");
        lp = buildPlan("b = group a by (name, age);");
        LOCogroup cogroup = (LOCogroup) lp.getLeaves().get(0);

        Schema.FieldSchema nameFs = new Schema.FieldSchema("name", DataType.CHARARRAY);
        Schema.FieldSchema ageFs = new Schema.FieldSchema("age", DataType.INTEGER);
        Schema.FieldSchema gpaFs = new Schema.FieldSchema("gpa", DataType.FLOAT);

        Schema groupSchema = new Schema(nameFs);
        groupSchema.add(ageFs);
        Schema.FieldSchema groupFs = new Schema.FieldSchema("group", groupSchema, DataType.TUPLE);

        Schema loadSchema = new Schema(nameFs);
        loadSchema.add(ageFs);
        loadSchema.add(gpaFs);

        Schema.FieldSchema bagFs = new Schema.FieldSchema("a", loadSchema, DataType.BAG);

        Schema cogroupExpectedSchema = new Schema(groupFs);
        cogroupExpectedSchema.add(bagFs);

        assertTrue(cogroup.getSchema().equals(cogroupExpectedSchema));

    }

    private void printPlan(LogicalPlan lp) {
        LOPrinter graphPrinter = new LOPrinter(System.err, lp);
        System.err.println("Printing the logical plan");
        try {
            graphPrinter.visit();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        System.err.println();
    }
    
    // Helper Functions
    
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
                                           logicalOpTable,
                                           aliasOp);
            List<LogicalOperator> roots = lp.getRoots();
            
            if(roots.size() > 0) {
                for(LogicalOperator op: roots) {
                    if (!(op instanceof LOLoad)){
                        throw new Exception("Cannot have a root that is not the load operator LOLoad. Found " + op.getClass().getName());
                    }
                }
            }
            
            //System.err.println("Query: " + query);
            
            //Just the top level roots and their children
            //Need a recursive one to travel down the tree
            /*
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
            */

            assertNotNull(lp != null);
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
}
