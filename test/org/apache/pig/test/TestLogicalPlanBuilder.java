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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.AssertionFailedError;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.builtin.GFAny;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalPlanData;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.test.utils.Identity;
import org.junit.Before;
import org.junit.Test;

public class TestLogicalPlanBuilder {
    PigContext pigContext = null;
    private PigServer pigServer = null;

    @Before
    public void setUp() throws Exception {
        pigContext = new PigContext(Util.getLocalTestMode(), new Properties());
    	pigServer = new PigServer( pigContext );
        pigServer.setValidateEachStatement(true);
    	pigContext.connect();
    }

    @Test
    public void testQuery1() throws Exception {
        String query = "foreach (load 'a') generate $1,$2;";
        buildPlan(query);
    }

    @Test
    public void testQuery2() throws Exception {
        String query = "foreach (load 'a' using " + PigStorage.class.getName() + "(':')) generate $1, 'aoeuaoeu' ;";
        buildPlan(query);
    }

    // TODO FIX Query3 and Query4
    @Test
    public void testQuery3() throws Exception {
        String query = "foreach (cogroup (load 'a' as (u:int)) by $0, (load 'b' as (v:int) ) by $0) generate org.apache.pig.builtin.AVG($1);";
        buildPlan(query);
    }

    @Test
    public void testQuery4() throws Exception {
        String query = "foreach (load 'a' as (u:int, v:bag{T:tuple(t:double)})) generate AVG($1) ;";
        buildPlan(query);
    }

    @Test
    public void testQuery5() throws Exception {
        String query = "foreach (group (load 'a') ALL) generate $1 ;";
        buildPlan(query);
    }


    @Test
    public void testQuery6() throws Exception {
        String query = "foreach (group (load 'a') by $1) generate group, '1' ;";
        buildPlan(query);
    }


    @Test
    public void testQuery7() throws Exception {
        String query = "foreach (load 'a' using " + PigStorage.class.getName() + "()) generate $1 ;";
        buildPlan(query);
    }


    @Test
    public void testQuery10() throws Exception {
        String query = "foreach (cogroup (load 'a') by ($1), (load 'b') by ($1)) generate $1.$1, $2.$1 ;";
        buildPlan(query);
    }

    // TODO FIX Query11 and Query12
    @Test
    public void testQuery11() throws Exception {
        String query = " foreach (group (load 'a' as (u:int)) by $0, (load 'b' as (v:long)) by $0) generate group, AVG($1) ;";
        buildPlan(query);
    }

    @Test
    public void testQuery12() throws Exception {
        String query = "foreach (load 'a' using " + PigStorage.class.getName() + "() as (v: long, u:bag{T:tuple(t:double)} ) ) generate AVG($1) ;";
        buildPlan(query);
    }

    @Test
    public void testQuery13() throws Exception {
        String query = "foreach (cogroup (load 'a') ALL) generate group ;";
        buildPlan(query);
    }

    @Test
    public void testQuery14() throws Exception {
        String query = "foreach (group (load 'a') by ($6, $7)) generate flatten(group) ;";
        buildPlan(query);
    }

    @Test
    public void testQuery15() throws Exception {
        String query = " foreach (load 'a') generate $1, 'hello', $3 ;";
        buildPlan(query);
    }

    @Test
    public void testQuery100() throws Exception {
        // test define syntax
        String query = "define FUNC ARITY();";
        buildPlan(query);
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail1() throws Exception {
        String query = " foreach (group (A = load 'a') by $1) generate A.'1' ;";
        buildPlan(query);
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail2() throws Exception {
        String query = "foreach group (load 'a') by $1 generate $1.* ;";
        buildPlan(query);
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail3() throws Exception {
        String query = "A = generate DISTINCT foreach (load 'a');";
        LogicalPlan lp = buildPlan(query);
        System.out.println( lp.toString() );
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail4() throws Exception {
        String query = "A = generate [ORDER BY $0][$3, $4] foreach (load 'a');";
        buildPlan(query);
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail5() throws Exception {
        String query = "A = generate " + TestApplyFunc.class.getName() + "($2.*) foreach (load 'a');";
        buildPlan(query);
    }

    /**
     * User generate functions must be in default package Bug 831620 - fixed
     */

    // TODO FIX Query17
    @Test
    public void testQuery17() throws Exception {
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
    @Test //PIG-1996
    public void testQuery18() throws Exception {
        String query = "store (FOREACH (group (load 'a') ALL PARALLEL 16) generate group ) into 'y';";
        LogicalPlan lp = buildPlan(query);
        Operator root = lp.getSources().get(0);
        List<Operator> listOp = lp.getSuccessors(root);
        Operator lo = listOp.get(0);

        assertTrue(lo instanceof LOCogroup);
        assertEquals( 16, ((LOCogroup) lo).getRequestedParallelism() );//Local mode, paraallel = 1

    }

    @Test
    public void testQuery19() throws Exception {
        String query = "a = load 'a';" +
                       "b = filter a by $1 == '3';";
        buildPlan( query );
    }

    @Test
    public void testQuery20() throws Exception {
        String query = "foreach (load 'a') generate ($1 == '3'? $2 : $3) ;";
        buildPlan(query);
    }

    @Test
    public void testQuery21() throws Exception {
        String query = "A = load 'a';" +
                       "B = load 'b';" +
                       "foreach (cogroup A by ($1), B by ($1)) generate A, flatten(B.($1, $2, $3));";
        buildPlan( query );
    }

    @Test
    public void testQuery22() throws Exception {
        String query = "A = load 'a';" +
                       "B = load 'b';" +
                       "C = cogroup A by ($1), B by ($1);" +
                       "foreach C { " +
                       "B = order B by $0; " +
                       "generate FLATTEN(A), B.($1, $2, $3) ;" + "};" ;
        buildPlan(query);
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail22() throws Exception {
        String query = "A = load 'a';" +
                       "B = group A by (*, $0);";
        try {
            buildPlan(query);
        } catch (Exception e) {
            assertTrue(e.getCause().getMessage().contains("Grouping attributes can either be star (*"));
            throw e;
        }
    }

    @Test
    public void testQuery23() throws Exception {
        String query = "A = load 'a';" +
                       "B = load 'b';" +

                       "C = cogroup A by ($1), B by ($1);" +

                       "foreach C { " +
                       "A = Distinct A; " +
                       "B = FILTER A BY $1 < 'z'; " +
        //TODO
        //A sequence of filters within a foreach translates to
        //a split statement. Currently it breaks as adding an
        //additional output to the filter fails as filter supports
        //single output
                       "C = FILTER A BY $2 == $3;" +
                       "B = ORDER B BY $1;" +
                       "GENERATE A, FLATTEN(B.$0);" +
                       "};";
        buildPlan(query);
    }

    @Test(expected = FrontendException.class)
    public void testQuery23Fail() throws Exception {
        String query = "A = load 'a' as (a: int, b:double);" +
                       "B = load 'b';" +
                       "C = cogroup A by (*, $0), B by ($0, $1);";
        try {
            buildPlan(query);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("The arity of cogroup/group by columns " +
                        "do not match"));
            throw e;
        }
    }

    @Test(expected = FrontendException.class)
    public void testQuery23Fail2() throws Exception {
        String query = "A = load 'a';" +
                       "B = load 'b';" +
                       "C = cogroup A by (*, $0), B by ($0, $1);";
        buildPlan(query);
    }

    @Test(expected = FrontendException.class)
    public void testQuery23Fail3() throws Exception {
        String query = "A = load 'a' as (a: int, b:double);" +
                       "B = load 'b' as (a:int);" +
                       "C = cogroup A by *, B by *;";
        try {
            buildPlan(query);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("The arity of cogroup/group by columns " +
                        "do not match"));
            throw e;
        }
    }

    @Test
    public void testQuery24() throws Exception {
        String query = "a = load 'a';" + "foreach a generate (($0 == $1) ? 'a' : $2), $4 ;";
        buildPlan(query);
    }

    @Test
    public void testQuery25() throws Exception {
        String query = "foreach (load 'a' as (u:bag{}, v, w) ) {" +
                "B = FILTER $0 BY (($1 == $2) AND ('a' < 'b'));" +
                "generate B;" +
                "};";
        buildPlan(query);
    }

    @Test
    public void testQuery26() throws Exception {
        String query = "foreach (load 'a') generate  ((NOT (($1 == $2) OR ('a' < 'b'))) ? 'a' : $2), 'x' ;";
        buildPlan(query);
    }

    // TODO FIX Query27 and Query28
    @Test
    public void testQuery27() throws Exception {
        String query =  "foreach (load 'a' as (u, v:bag{}, w, x:bag{}, y) ){" +
                "A = DISTINCT $3.$1;" +
                " generate " + TestApplyFunc.class.getName() + "($2, $1.($1, $4));" +
                        "};";
        buildPlan(query);
    }

    @Test
    public void testQuery28() throws Exception {
        String query = "foreach (load 'a') generate " + TestApplyFunc.class.getName() + "($2, " + TestApplyFunc.class.getName() + "($2.$3));";
        buildPlan(query);
    }

    @Test
    public void testQuery29() throws Exception {
        String query = "load 'myfile' using " + TestStorageFunc.class.getName() + "() as (col1);";
        buildPlan(query);
    }

    @Test
    public void testQuery30() throws Exception {
        String query = "load 'myfile' using " + TestStorageFunc.class.getName() + "() as (col1, col2);";
        buildPlan(query);
    }

    public static class TestStorageFunc extends LoadFunc{

        public Tuple getNext() throws IOException {
            return null;
        }

        @Override
        public InputFormat getInputFormat() throws IOException {
            return null;
        }

        @Override
        public LoadCaster getLoadCaster() throws IOException {
            return null;
        }

        @Override
        public void prepareToRead(RecordReader reader, PigSplit split)
                throws IOException {

        }

        @Override
        public String relativeToAbsolutePath(String location, Path curDir)
                throws IOException {
            return null;
        }

        @Override
        public void setLocation(String location, Job job) throws IOException {

        }
    }

    @Test
    public void testQuery31() throws Exception {
        String query = "load 'myfile' as (col1, col2);";
        buildPlan(query);
    }

    @Test
    public void testQuery32() throws Exception {
        String query = "foreach (load 'myfile' as (col1, col2 : tuple(sub1, sub2), col3 : tuple(bag1))) generate col1 ;";
        buildPlan(query);
    }

    @Test
    public void testQuery33() throws Exception {
        String query = "A = load 'a' as (aCol1, aCol2);" +
                       "B = load 'b' as (bCol1, bCol2);" +
                       "C = cogroup A by (aCol1), B by bCol1;" +
                       "foreach C generate group, A.aCol1;";
        buildPlan(query);
    }

    @Test
    //TODO: Nested schemas don't work now. Probably a bug in the new parser.
    public void testQuery34() throws Exception {
        String query = "A = load 'a' as (aCol1, aCol2 : tuple(subCol1, subCol2));" +
        "A = filter A by aCol1 == '1';" +
        "B = load 'b' as (bCol1, bCol2);" +
        "foreach (cogroup A by (aCol1), B by bCol1 ) generate A.aCol2, B.bCol2 ;";
        buildPlan(query);
    }

    @Test
    public void testQuery35() throws Exception {
        String query = "foreach (load 'a' as (col1, col2)) generate col1, col2 ;";
        buildPlan(query);
    }

    @Test
    public void testQuery36() throws Exception {
        String query = "foreach (cogroup ( load 'a' as (col1, col2)) by col1) generate $1.(col2, col1);";
        buildPlan(query);
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail37() throws Exception {
        String query = "A = load 'a'; asdasdas";
        buildPlan(query);
    }

    @Test
    public void testQuery38() throws Exception {
        String query = "c = cross (load 'a'), (load 'b');";
        buildPlan(query);
    }

    // TODO FIX Query39 and Query40
    @Test
    public void testQuery39() throws Exception{
        String query = "a = load 'a' as (url, host, ranking:double);" +
                       "b = group a by (url,host); " +
        "c = foreach b generate flatten(group.url), SUM(a.ranking) as totalRank;";
        buildPlan(query);
        query += "d = filter c by totalRank > 10;" +
                 "e = foreach d generate totalRank;";
        buildPlan( query );
    }

    @Test(expected = FrontendException.class)
    public void testQuery39Fail() throws Exception{
        String query = "a = load 'a' as (url, host, ranking);" +
                       "b = group a by (url,host); " +
        "c = foreach b generate flatten(group.url), SUM(a.ranking) as totalRank;" +
                       "d = filter c by totalRank > '10';" +
                       "e = foreach d generate url;";
        buildPlan(query);
    }

    @Test
    public void testQuery40() throws Exception {
        String query = "a = FILTER (load 'a') BY IsEmpty($2);";
        buildPlan( query +"a = FILTER (load 'a') BY (IsEmpty($2) AND ($3 == $2));" );
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail41() throws Exception {
        buildPlan("a = load 'a';" + "b = a as (host,url);");
        // TODO
        // the following statement was earlier present
        // eventually when we do allow assignments of the form
        // above, we should test with the line below
        // uncommented
        //buildPlan("foreach b generate host;");
    }

    @Test
    public void testQuery42() throws Exception {
        String q = "a = load 'a';" +
        "b = foreach a generate $0 as url, $1 as ranking;" +
        "foreach b generate url;";
        buildPlan( q );
    }

    @Test
    public void testQuery43() throws Exception {
        String q = "a = load 'a' as (url,hitCount);" +
        "b = load 'a' as (url,ranking);" +
        "c = cogroup a by url, b by url;" +
        "d = foreach c generate group,flatten(a),flatten(b);" +
        "e = foreach d generate group, a::url, b::url, b::ranking;";
        buildPlan( q );
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail43() throws Exception {
        String q = "a = load 'a' as (name, age, gpa);" +
        "b = load 'b' as (name, height);";
        String query = q + "c = cogroup a by (name, age), b by (height);";
        buildPlan(query);
    }

    @Test
    public void testQuery44() throws Exception {
        String q = "a = load 'a' as (url, pagerank);" +
        "b = load 'b' as (url, query, ranking);" +
        "c = cogroup a by (pagerank#'nonspam', url) , b by (ranking, url) ;" +
        "foreach c generate group.url;";
        buildPlan( q );
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail44() throws Throwable {
        PigServer pig = null;
        pig = new PigServer(Util.getLocalTestMode());
        pig.registerFunction("myTr",
            new FuncSpec(GFAny.class.getName() + "('tr o 0')"));
        pig.registerQuery("b = foreach (load 'a') generate myTr(myTr(*));");
    }

    @Test
    public void testQuery57() throws Exception {
        String query = "foreach (load 'a' as (u:int, v:long, w:int)) generate ($1+$2), ($1-$2), ($1*$2), ($1/$2), ($1%$2), -($1) ;";
        buildPlan(query);
    }

    @Test
    public void testQuery58() throws Exception {
        String query = "a = load 'a' as (name, age, gpa);" +
        "b = group a by name;" +
        "foreach b {d = a.name; generate group, d;};";
        buildPlan(query);
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail58() throws Exception{
        String query = "a = load 'a' as (url, host, ranking);" +
        "b = group a by url; ";
        buildPlan(query + "c = foreach b generate group.url;");
    }

    @Test
    public void testQuery59() throws Exception {
    	String query = "a = load 'a' as (name, age, gpa);" +
        "b = load 'b' as (name, height);" +
        "c = join a by name, b by name;";
        buildPlan(query);
    }

    @Test
    public void testQuery60() throws Exception {
         String query = "a = load 'a' as (name, age, gpa);" +
        "b = load 'b' as (name, height);" +
       "c = cross a,b;";
        buildPlan(query);
    }

    @Test
    public void testQuery61() throws Exception {
        String query = "a = load 'a' as (name, age, gpa);" +
        "b = load 'b' as (name, height);" +
        "c = union a,b;";
        buildPlan(query);
    }

    @Test
    public void testQuery62() throws Exception {
        String query = "a = load 'a' as (name, age, gpa);" +
        "b = load 'b' as (name, height);" +
        "c = cross a,b;" +
        "d = order c by b::name, height, a::gpa;" +
        "e = order a by name, age, gpa desc;" +
        "f = order a by $0 asc, age, gpa desc;" +
        "g = order a by * asc;" +
        "h = cogroup a by name, b by name;" +
        "i = foreach h {i1 = order a by *; generate i1;};";
        buildPlan(query);
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail62() throws Exception {
        String query = "a = load 'a' as (name, age, gpa);" +
        "b = load 'b' as (name, height);" +
        "c = cross a,b;" +
        "d = order c by name, b::name, height, a::gpa;";
        buildPlan(query);
    }

    @Test
    public void testQuery63() throws Exception {
        String query = "a = load 'a' as (name, details: tuple(age, gpa));" +
        "b = group a by details;" +
        "d = foreach b generate group.age;" +
        "e = foreach a generate name, details;";
        buildPlan(query);
    }

    @Test
    public void testQuery63a() throws Exception {
        String query = "foreach (load 'myfile' as (col1, col2 : (sub1, sub2), col3 : (bag1))) generate col1 ;";
        buildPlan(query);
    }

    @Test
    public void testQuery64() throws Exception {
        String query = "a = load 'a' as (name: chararray, details: tuple(age, gpa), mymap: map[]);" +
        "c = load 'a' as (name, details: bag{mytuple: tuple(age: int, gpa)});" +
        "b = group a by details;" +
        "d = foreach b generate group.age;" +
		"e = foreach a generate name, details;" +
		"f = LOAD 'myfile' AS (garage: bag{tuple1: tuple(num_tools: int)}, links: bag{tuple2: tuple(websites: chararray)}, page: bag{something_stupid: tuple(yeah_double: double)}, coordinates: bag{another_tuple: tuple(ok_float: float, bite_the_array: bytearray, bag_of_unknown: bag{})});";
        buildPlan(query);
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail64() throws Exception {
        String query = "foreach (load 'myfile' as (col1, col2 : bag{age: int})) generate col1 ;";
        buildPlan(query);
    }

    @Test
    public void testQuery65() throws Exception {
        String q = "a = load 'a' as (name, age, gpa);" +
        "b = load 'b' as (name, height);" +
		"c = cogroup a by (name, age), b by (name, height);" +
		"d = foreach c generate group.name, a.name as aName, b.name as bname;";
        buildPlan( q );
	}

    @Test
    public void testQuery66() throws Exception {
        String q = "a = load 'a' as (name, age, gpa);" +
        "b = load 'b' as (name, height);" +
        "c = cogroup a by (name, age), b by (name, height);" +
        "d = foreach c generate group.name, a.name, b.height as age, a.age;";
        buildPlan( q );
	}

    @Test
    public void testQuery67() throws Exception {
        String q = " a = load 'input1' as (name, age, gpa);" +
        " b = foreach a generate age, age * 10L, gpa/0.2f, {(16, 4.0e-2, 'hello')};";
        buildPlan( q );
    }

    @Test
    public void testQuery68() throws Exception {
    	String q = " a = load 'input1';" +
        " b = foreach a generate 10, {(16, 4.0e-2, 'hello'), (0.5f, 12l, 'another tuple')};";
        buildPlan( q );
    }

    @Test
    public void testQuery69() throws Exception {
    	String q = " a = load 'input1';" +
        " b = foreach a generate {(16, 4.0e-2, 'hello'), (0.5f, 'another tuple', 12L, (1))};";
        buildPlan( q );
    }

    @Test
    public void testQuery70() throws Exception {
    	String q = " a = load 'input1';" +
        " b = foreach a generate ['10'#'hello', '4.0e-2'#10L, '0.5f'#(1), 'world'#42, '42'#{('guide')}] as mymap:map[];" +
        " c = foreach b generate mymap#'10';";
        buildPlan( q );
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail67() throws Exception {
        String q = " a = load 'input1' as (name, age, gpa);" +
        " b = foreach a generate age, age * 10L, gpa/0.2f, {16, 4.0e-2, 'hello'};";
        try {
            buildPlan(q);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Pig script failed to parse: MismatchedTokenException"));
            throw e;
        }
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail68() throws Exception {
        String q = " a = load 'input1' as (name, age, gpa);";
        buildPlan( q +
          " b = foreach a generate {(16 L, 4.0e-2, 'hello'), (0.5f, 'another tuple', 12L, {()})};");
    }

    @Test
    public void testQuery71() throws Exception {
        String q = "split (load 'a') into x if $0 > '7', y if $0 < '7';" +
        "b = foreach x generate $0;" +
        "c = foreach y generate $1;";
        buildPlan( q );
    }

    @Test
    public void testQuery72() throws Exception {
        String q = "split (load 'a') into x if $0 > 7, y if $0 < 7;" +
        "b = foreach x generate (int)$0;" +
        "c = foreach y generate (bag{})$1;" +
        "d = foreach y generate (int)($2/2);" +
        "e = foreach y generate (bag{tuple(int, float)})($2);" +
        "f = foreach x generate (tuple(int, float))($3);" +
        "g = foreach x generate (tuple())($4);" +
        "h = foreach x generate (chararray)($5);";
        buildPlan( q );
    }

    @Test
    public void testQueryFail72() throws Exception {
    	boolean catchEx = false;
        String q = "split (load 'a') into x if $0 > '7', y if $0 < '7';";
        try {
            buildPlan( q + "c = foreach y generate (bag)$1;");
        } catch (FrontendException e) {
        	catchEx = true;
        }
        assertTrue( catchEx );
        catchEx = false;
        try {
        	buildPlan( q + "c = foreach y generate (bag{int, float})$1;");
        } catch (FrontendException e) {
        	catchEx = true;
        }
        assertTrue( catchEx );
        catchEx = false;
        try {
        	buildPlan( q + "c = foreach y generate (tuple)$1;");
        } catch (FrontendException e) {
        	catchEx = true;
        }
        assertTrue( catchEx );
    }

    @Test
    public void testQuery73() throws Exception {
    	String q = "split (load 'a') into x if $0 > '7', y if $0 < '7';" +
        "b = filter x by $0 matches '^fred.*';" +
        "c = foreach y generate $0, ($0 matches 'yuri.*' ? $1 - 10 : $1);";
        buildPlan( q );
    }

    @Test
    public void testQuery74() throws Exception {
        String q = "a = load 'a' as (field1: int, field2: long);" +
        "b = load 'a' as (field1: bytearray, field2: double);" +
        "c = group a by field1, b by field1;" +
        "d = cogroup a by ((field1+field2)*field1), b by field1;";
        buildPlan( q );
    }

    @Test
    public void testQuery77() throws Exception {
        buildPlan("limit (load 'a') 100;");
    }

    @Test
    public void testLimitWithLong() throws Exception {
        buildPlan("limit (load 'a') 100L;");
    }

    @Test
    public void testQuery75() throws Exception {
        String q = "a = union (load 'a'), (load 'b'), (load 'c');";
        buildPlan( q + "b = foreach a {generate $0;};");
    }

    @Test
    public void testQuery76() throws Exception {
    	String q = "split (load 'a') into x if $0 > '7', y if $0 < '7';" +
        "b = filter x by $0 IS NULL;" +
        "c = filter y by $0 IS NOT NULL;" +
        "d = foreach b generate $0, ($1 IS NULL ? 0 : $1 - 7);" +
        "e = foreach c generate $0, ($1 IS NOT NULL ? $1 - 5 : 0);";
    	buildPlan( q );
    }

    @Test
    public void testQuery80() throws Exception {
    	String q = "a = load 'input1' as (name, age, gpa);" +
        "b = filter a by age < '20';" +
        "c = group b by age;" +
        "d = foreach c {"
            + "cf = filter b by gpa < '3.0';"
            + "cp = cf.gpa;"
            + "cd = distinct cp;"
            + "co = order cd by gpa;"
            + "generate group, flatten(co);"
            //+ "generate group, flatten(cd);"
            + "};";
        buildPlan(q);
    }

    @Test
    public void testQuery81() throws Exception {
    	String q = "a = load 'input1' using PigStorage() as (name, age, gpa);" +
        "split a into b if name lt 'f', c if (name gte 'f' and name lte 'h'), d if name gt 'h';";
    	buildPlan( q );
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail81() throws Exception {
        String q = "a = load 'input1' using PigStorage() as (name, age, gpa);";
        buildPlan(q + "split a into b if name lt 'f', c if (name ge 'f' and name le 'h'), d if name gt 'h';");
    }

    @Test
    public void testQuery82() throws Exception {
        String q = "a = load 'myfile';" +
        "b = group a by $0;" +
        "c = foreach b {"
        + "c1 = order $1 by *;"
        + "c2 = $1.$0;"
            + "generate flatten(c1), c2;"
            + "};";
        buildPlan(q);
    }

    @Test
    public void testQuery82a() throws Exception {
    	String q = "a = load 'myfile';" +
        "b = group a by $0;" +
        "c = foreach b {"
            + "c1 = order $1 by *;"
            + "c2 = $1;"
            + "generate flatten(c1), c2;"
            + "};";
        buildPlan(q);
    }

    @Test
    public void testQuery83() throws Exception {
    	String q = "a = load 'input1' as (name, age, gpa);" +
        "b = filter a by age < '20';" +
        "c = group b by (name,age);" +
        "d = foreach c {"
            + "cf = filter b by gpa < '3.0';"
            + "cp = cf.gpa;"
            + "cd = distinct cp;"
            + "co = order cd by gpa;"
            + "generate group, flatten(co);"
            + "};";
        buildPlan(q);
    }

    @Test
    public void testQuery84() throws Exception {
    	String q = "a = load 'input1' as (name, age, gpa);" +
        "b = filter a by age < '20';" +
        "c = group b by (name,age);" +
        "d = foreach c {"
            + "cf = filter b by gpa < '3.0';"
            + "cp = cf.$2;"
            + "cd = distinct cp;"
            + "co = order cd by gpa;"
            + "generate group, flatten(co);"
            + "};";
        buildPlan(q);
    }

    @Test
    public void testQuery85() throws Exception {
        LogicalPlan lp;
        String query = "a = load 'myfile' as (name, age, gpa);" +
		               "b = group a by (name, age);";
        lp = buildPlan( query + "store b into 'output';");
        Operator store = lp.getSinks().get(0);
        LOCogroup cogroup = (LOCogroup) lp.getPredecessors(store).get(0);

        LogicalSchema actual = cogroup.getSchema();
        System.out.println( actual.toString( false ) );

        assertEquals("group:tuple(name:bytearray,age:bytearray),a:bag{:tuple(name:bytearray,age:bytearray,gpa:bytearray)}", actual.toString(false));

        lp = buildPlan(query +
        		       "c = foreach b generate group.name, group.age, COUNT(a.gpa);" +
        		       "store c into 'output';");
        store = lp.getSinks().get(0);
        LOForEach foreach  = (LOForEach) lp.getPredecessors(store).get(0);


        assertEquals("name:bytearray,age:bytearray,:long", foreach.getSchema().toString(false));
    }

    @Test
    public void testQuery86() throws Exception {
        LogicalPlan lp;
        String query = "a = load 'myfile' as (name:Chararray, age:Int, gpa:Float);" +
                       "b = group a by (name, age);" +
                       "store b into 'output';";
        lp = buildPlan( query );
        Operator store = lp.getSinks().get(0);
        LOCogroup cogroup = (LOCogroup) lp.getPredecessors(store).get(0);

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
        assertEquals("group:tuple(name:chararray,age:int),a:bag{:tuple(name:chararray,age:int,gpa:float)}", cogroup.getSchema().toString(false));
    }

    @Test
    public void testQuery87() throws Exception {
        String query = "a = load 'myfile';" +
                       "b = group a by $0;" +
                       "c = foreach b {c1 = order $1 by $1; generate flatten(c1); };" +
                       "store c into 'output';";
        LogicalPlan lp = buildPlan( query );
        Operator store = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach) lp.getPredecessors(store).get(0);
        LogicalPlan nestedPlan = foreach.getInnerPlan();
        LOGenerate gen = (LOGenerate) nestedPlan.getSinks().get(0);
        LOSort nestedSort = (LOSort)nestedPlan.getPredecessors(gen).get(0);
        LogicalExpressionPlan sortPlan = nestedSort.getSortColPlans().get(0);
        assertEquals(1, sortPlan.getSinks().size());
    }

    @Test
    public void testQuery88() throws Exception {
        String query = "a = load 'myfile';" +
                       "b = group a by $0;" +
                       "c = order b by $1 ;" +
                       "store c into 'output';";
        LogicalPlan lp = buildPlan( query );
        Operator store = lp.getSinks().get(0);
        LOSort sort = (LOSort) lp.getPredecessors(store).get(0);
        //        LOProject project1 = (LOProject) sort.getSortColPlans().get(0).getSinks().get(0) ;
        //        LOCogroup cogroup = (LOCogroup) lp.getPredecessors(sort).get(0) ;
        //        assertEquals(project1.getExpression(), cogroup) ;
    }

    @Test
    public void testQuery89() throws Exception {
        String query = "a = load 'myfile';" +
                       "b = foreach a generate $0, $100;" +
                       "c = load 'myfile' as (i: int);" +
                       "d = foreach c generate $0 as zero, i;";
        buildPlan( query );
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail89() throws Exception {
        String q = "c = load 'myfile' as (i: int);";
        try {
            buildPlan(q + "d = foreach c generate $0, $5;");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Out of bound access"));
            throw e;
        }
    }

    @Test
    public void testQuery90() throws Exception {
        LogicalPlan lp;
        LOForEach foreach;

        String query = "a = load 'myfile' as (name:Chararray, age:Int, gpa:Float);" +
                       "b = group a by (name, age);";
        //the first and second elements in group, i.e., name and age are renamed as myname and myage
        lp = buildPlan(query +
        		"c = foreach b generate flatten(group) as (myname, myage), COUNT(a) as mycount;" +
        		"store c into 'output';");
        Operator store = lp.getSinks().get(0);
        foreach = (LOForEach)lp.getPredecessors(store).get(0);
        assertTrue(foreach.getSchema().isEqual(Utils.parseSchema("myname: chararray, age: int, mycount: long")));

        //the schema of group is unchanged
        lp = buildPlan( query +
        		"c = foreach b generate flatten(group), COUNT(a) as mycount;" +
        		"store c into 'output';" );
        store = lp.getSinks().get(0);
        foreach = (LOForEach)lp.getPredecessors(store).get(0);
        assertEquals("group::name:chararray,group::age:int,mycount:long", foreach.getSchema().toString(false));

        //group is renamed as mygroup
        lp = buildPlan(query +
        		"c = foreach b generate group as mygroup, COUNT(a) as mycount;" +
        		"store c into 'output';");
        store = lp.getSinks().get(0);
        foreach = (LOForEach)lp.getPredecessors(store).get(0);
        assertEquals("mygroup:tuple(name:chararray,age:int),mycount:long", foreach.getSchema().toString(false));

        //group is renamed as mygroup and the elements are renamed as myname and myage
        lp = buildPlan(query +
        		"c = foreach b generate group as mygroup:(myname, myage), COUNT(a) as mycount;" +
        	    "store c into 'output';");
        store = lp.getSinks().get(0);
        foreach = (LOForEach)lp.getPredecessors(store).get(0);
        assertEquals("mygroup:tuple(myname:chararray,myage:int),mycount:long",foreach.getSchema().toString(false));
        //setting the schema of flattened bag that has no schema with the user defined schema
        String q = "a = load 'myfile' as (name:Chararray, age:Int, gpa:Float);" +
                   "c = load 'another_file';" +
                   "d = cogroup a by $0, c by $0;";
        lp = buildPlan( q + "e = foreach d generate flatten(DIFF(a, c)) as (x, y, z), COUNT(a) as mycount;" + "store e into 'output';" );
        store = lp.getSinks().get(0);
        foreach = (LOForEach)lp.getPredecessors(store).get(0);
        assertEquals("x:bytearray,y:bytearray,z:bytearray,mycount:long",foreach.getSchema().toString(false));

        //setting the schema of flattened bag that has no schema with the user defined schema
        q = query +
                  "c = load 'another_file';" +
                  "d = cogroup a by $0, c by $0;" +
                  "e = foreach d generate flatten(DIFF(a, c)) as (x: int, y: float, z), COUNT(a) as mycount;" +
                  "store e into 'output';";
        lp = buildPlan(q);
        store = lp.getSinks().get(0);
        foreach = (LOForEach)lp.getPredecessors(store).get(0);
        assertEquals("x:int,y:float,z:bytearray,mycount:long",foreach.getSchema().toString(false));

        //setting the schema of flattened bag that has no schema with the user defined schema
        q = query +
            "c = load 'another_file';" +
            "d = cogroup a by $0, c by $0;" +
            "e = foreach d generate flatten(DIFF(a, c)) as x, COUNT(a) as mycount;" +
            "store e into 'output';";
        lp = buildPlan(q);
        store = lp.getSinks().get(0);
        foreach = (LOForEach)lp.getPredecessors(store).get(0);
        assertEquals("x:bytearray,mycount:long",foreach.getSchema().toString(false));

        //setting the schema of flattened bag that has no schema with the user defined schema
        q = query +
            "c = load 'another_file';" +
            "d = cogroup a by $0, c by $0;" +
            "e = foreach d generate flatten(DIFF(a, c)) as x: int, COUNT(a) as mycount;" +
            "store e into 'output';";
        lp = buildPlan(q);
        store = lp.getSinks().get(0);
        foreach = (LOForEach)lp.getPredecessors(store).get(0);
        assertEquals("x:int,mycount:long",foreach.getSchema().toString(false));
    }

    /**
     * Test for {@link org.apache.pig.newplan.logical.visitor.ForEachUserSchemaVisitor} visit() for inserting Cast operator into plan correctly.
     * @throws Exception
     */
    @Test
    public void testQuery90a() throws Exception {
        LogicalPlan lp = null;
        LOForEach foreach = null;
        Operator store = null;

        String query = "a = load 'myfile' as (name:Chararray, age:Int, gpa:Float);" +
                "b = group a by (name, age);";

        // Simply should work renaming
        lp = buildPlan( query + "c = foreach b generate group as mygroup:(myname, myage), COUNT(a) as mycount; store c into 'output';");
        store = lp.getSinks().get(0);
        foreach = (LOForEach)lp.getPredecessors(store).get(0);
        assertEquals("mygroup:tuple(myname:chararray,myage:int),mycount:long",foreach.getSchema().toString(false));

        // Casting should work regardless of type mismatch
        lp = buildPlan( query + "c = foreach b generate group as mygroup:(myname: int, myage), COUNT(a) as mycount; store c into 'output';");
        store = lp.getSinks().get(0);
        foreach = (LOForEach)lp.getPredecessors(store).get(0);
        assertEquals("mygroup:tuple(myname:int,myage:int),mycount:long",foreach.getSchema().toString(false));

        // Casting in final phase should work too (no succeeding operators after cast in final foreach)
        buildPlan( query + "c = foreach b generate group as mygroup:(myname, myage: chararray), COUNT(a) as mycount;");
    }

    @Test
    public void testQueryFail90() throws Exception {
        String query = "a = load 'myfile' as (name:Chararray, age:Int, gpa:Float);" +
                       "b = group a by (name, age);";

        try {
            buildPlan( query + "c = foreach b generate group as mygroup:{t: (myname, myage)}, COUNT(a) as mycount;");
            fail("Should have thrown error");
        } catch (FrontendException e) {
            assertTrue(e.getMessage().contains("Incompatible field schema"));
        }

        try {
            buildPlan( query + "c = foreach b generate flatten(group) as (myname, myage, mygpa), COUNT(a) as mycount;");
            fail("Should have thrown error");
        } catch (FrontendException e) {
            assertTrue(e.getMessage().contains("Incompatible schema"));
        }
    }

    @Test
    public void testQuery91() throws Exception {
        String query = "a = load 'myfile' as (name:Chararray, age:Int, gpa:Float);" +
                       "b = group a by name;";
        buildPlan(query + "c = foreach b generate SUM(a.age) + SUM(a.gpa);");
    }

    @Test
    public void testQuery92() throws Exception {
        String query = "a = load 'myfile' as (name, age, gpa);" +
                       "b = group a by name;" +
                       "c = foreach b { "
        + " alias = name#'alias'; "
        + " af = alias#'first'; "
        + " al = alias#'last'; "
        + " generate SUM(a.age) + SUM(a.gpa); "
        + "};";
        buildPlan( query );
    }

    @Test
    public void testQuery93() throws Exception {
        String query = "a = load 'one' as (name, age, gpa);" +
                       "b = group a by name;" +
                       "c = foreach b generate flatten(a);" +
                       "d = foreach c generate name;" +
        // test that we can refer to "name" field and not a::name
                       "e = foreach d generate name;";
        buildPlan( query );
    }

    @Test
    public void testQuery93_a() throws Exception {
        String query = "a = load 'one' as (name, age, gpa);" +
        "b = group a by name;"+
        "c = foreach b generate flatten(a);"+
        "d = foreach c generate name;"+
        // test that we can refer to "name" field and a::name
        "e = foreach d generate a::name;";
        buildPlan( query );
    }

    @Test
    public void testQuery94() throws Exception {
        String query = "a = load 'one' as (name, age, gpa);" +
        "b = load 'two' as (name, age, somethingelse);"+
        "c = cogroup a by name, b by name;"+
        "d = foreach c generate flatten(a), flatten(b);"+
        // test that we can refer to "a::name" field and not name
        // test that we can refer to "b::name" field and not name
        "e = foreach d generate a::name, b::name;"+
        // test that we can refer to gpa and somethingelse
        "f = foreach d generate gpa, somethingelse;";
        buildPlan( query );
    }

    @Test(expected = FrontendException.class)
    public void testQueryFail94() throws Exception {
        String query = "a = load 'one' as (name, age, gpa);" +
        "b = load 'two' as (name, age, somethingelse);"+
        "c = cogroup a by name, b by name;"+
        "d = foreach c generate flatten(a), flatten(b);"+
        "e = foreach d generate name;";
        // test that we can refer to "a::name" field and not name
        try {
            buildPlan(query);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Invalid field projection. Projected field [name] does not exist"));
            throw e;
        }
    }

    @Test
    public void testQuery95() throws Exception {
        String query = "a = load 'myfile' as (name, age, gpa);" +
                       "b = group a by name;" +
                       "c = foreach b {d = order a by $1; generate flatten(d), MAX(a.age) as max_age;};" +
                       "store c into 'output';";
        LogicalPlan lp = buildPlan(query);
        Operator store = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach)lp.getPredecessors(store).get(0);
        LOCogroup cogroup = (LOCogroup) lp.getPredecessors(foreach).get(0);
        String s = cogroup.getSchema().toString(false);
        assertEquals("group:bytearray,a:bag{:tuple(name:bytearray,age:bytearray,gpa:bytearray)}", s);
        s = foreach.getSchema().toString(false);
        assertEquals("d::name:bytearray,d::age:bytearray,d::gpa:bytearray,max_age:double", s);
    }

    @Test
    public void testQuery96() throws Exception {
        String query = "a = load 'input' as (name, age, gpa);" +
                       "b = filter a by age < 20;" +
                       "c = group b by age;" +
                       "d = foreach c {"
        + "cf = filter b by gpa < 3.0;"
        + "cd = distinct cf.gpa;"
        + "co = order cd by $0;"
        + "generate group, flatten(co);"
        + "};" +
        "store d into 'output';";
        LogicalPlan lp = buildPlan(query);
        Operator store = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach)lp.getPredecessors(store).get(0);
        LogicalPlan foreachPlans = foreach.getInnerPlan();
        //        LogicalPlan flattenPlan = foreachPlans.get(1);
        //        LogicalOperator project = flattenPlan.getLeaves().get(0);
        //        assertTrue(project instanceof LOProject);
        //        LogicalOperator sort = flattenPlan.getPredecessors(project).get(0);
        //        assertTrue(sort instanceof LOSort);
        //        LogicalOperator distinct = flattenPlan.getPredecessors(sort).get(0);
        //        assertTrue(distinct instanceof LODistinct);
        //
        //        //testing the presence of the nested foreach
        //        LogicalOperator nestedForeach = flattenPlan.getPredecessors(distinct).get(0);
        //        assertTrue(nestedForeach instanceof LOForEach);
        //        LogicalPlan nestedForeachPlan = ((LOForEach)nestedForeach).getForEachPlans().get(0);
        //        LogicalOperator nestedProject = nestedForeachPlan.getRoots().get(0);
        //        assertTrue(nestedProject instanceof LOProject);
        //        assertTrue(((LOProject)nestedProject).getCol() == 2);
        //
        //        //testing the filter inner plan for the absence of the project connected to project
        //        LogicalOperator filter = flattenPlan.getPredecessors(nestedForeach).get(0);
        //        assertTrue(filter instanceof LOFilter);
        //        LogicalPlan comparisonPlan = ((LOFilter)filter).getComparisonPlan();
        //        LOLesserThan lessThan = (LOLesserThan)comparisonPlan.getLeaves().get(0);
        //        LOProject filterProject = (LOProject)lessThan.getLhsOperand();
        //        assertTrue(null == comparisonPlan.getPredecessors(filterProject));
    }
    /*
    @Test
    public void testQuery97() throws FrontendException, ParseException {
        LogicalPlan lp;
        LOForEach foreach;

        String query = "a = load 'one' as (name, age, gpa);";
        String store = "store b into 'output';";

        lp = buildPlan(query + "b = foreach a generate 1;" + store);
        foreach = (LOForEach)lp.getPredecessors(op);
//        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("x: int"), false, true));

        lp = buildPlan(query + "b = foreach a generate 1L;" + store);
        op = lp.getSinks().get(0);
        Operator op = lp.getSinks().get(0);
        foreach = (LOForEach)lp.getPredecessors(op);
//        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("x: long"), false, true));

        lp = buildPlan(query + "b = foreach a generate 1.0;" + store);
        op = lp.getSinks().get(0);
        foreach = (LOForEach)lp.getPredecessors(op);
//        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("x: double"), false, true));

        lp = buildPlan(query + "b = foreach a generate 1.0f;" + store);
        op = lp.getSinks().get(0);
        foreach = (LOForEach)lp.getPredecessors(op);
//        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("x: float"), false, true));

        lp = buildPlan(query + "b = foreach a generate 'hello';" + store);
        op = lp.getSinks().get(0);
        foreach = (LOForEach)lp.getPredecessors(op);
//        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("x: chararray"), false, true));
    }

    @Test
    public void testQuery98() throws FrontendException, ParseException {
        LogicalPlan lp;
        LOForEach foreach;

        buildPlan("a = load 'one' as (name, age, gpa);");

        lp = buildPlan("b = foreach a generate (1);");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("t:(x: int)"), false, true));

        lp = buildPlan("b = foreach a generate (1L);");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("t:(x: long)"), false, true));

        lp = buildPlan("b = foreach a generate (1.0);");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("t:(x: double)"), false, true));

        lp = buildPlan("b = foreach a generate (1.0f);");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("t:(x: float)"), false, true));

        lp = buildPlan("b = foreach a generate ('hello');");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("t:(x: chararray)"), false, true));

        lp = buildPlan("b = foreach a generate ('hello', 1, 1L, 1.0f, 1.0);");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("t:(x: chararray, y: int, z: long, a: float, b: double)"), false, true));

        lp = buildPlan("b = foreach a generate ('hello', {(1), (1.0)});");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("t:(x: chararray, ib:{it:(d: double)})"), false, true));

    }

    @Test
    public void testQuery99() throws FrontendException, ParseException {
        LogicalPlan lp;
        LOForEach foreach;

        buildPlan("a = load 'one' as (name, age, gpa);");

        lp = buildPlan("b = foreach a generate {(1, 'hello'), (2, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:(x: int, y: chararray)}"), false, true));

        lp = buildPlan("b = foreach a generate {(1, 'hello'), (1L, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:(x: long, y: chararray)}"), false, true));

        lp = buildPlan("b = foreach a generate {(1, 'hello'), (1.0f, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:(x: float, y: chararray)}"), false, true));

        lp = buildPlan("b = foreach a generate {(1, 'hello'), (1.0, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:(x: double, y: chararray)}"), false, true));

        lp = buildPlan("b = foreach a generate {(1L, 'hello'), (1.0f, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:(x: float, y: chararray)}"), false, true));

        lp = buildPlan("b = foreach a generate {(1L, 'hello'), (1.0, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:(x: double, y: chararray)}"), false, true));

        lp = buildPlan("b = foreach a generate {(1.0f, 'hello'), (1.0, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:(x: double, y: chararray)}"), false, true));

        lp = buildPlan("b = foreach a generate {(1.0, 'hello'), (1.0f, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:(x: double, y: chararray)}"), false, true));

        lp = buildPlan("b = foreach a generate {(1.0, 'hello', 3.14), (1.0f, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:()}"), false, true));

    }

    @Test
    public void testQuery101() throws Exception {
        // test usage of an alias from define
        String query = "define FUNC ARITY();";
        buildPlan(query);

        query = "foreach (load 'data') generate FUNC($0);";
        buildPlan(query);
    }
     */
    @Test
    public void testQuery102() throws Exception {
        // test basic store
        buildPlan( "a = load 'a';" + "store a into 'out';" );
    }

    @Test
    public void testQuery103() throws Exception {
        // test store with store function
        buildPlan("a = load 'a';" + "store a into 'out' using PigStorage();");
    }

    //    @Test // Commented out due to PIG-2037
    //    public void testQuery104() throws Exception {
    //        // check that a field alias can be referenced
    //        // by unambiguous free form alias, fully qualified alias
    //        // and partially qualified unambiguous alias
    //        String query = "a = load 'st10k' as (name, age, gpa);\n"  +
    //         "b = group a by name;\n"  +
    //        "c = foreach b generate flatten(a);\n"  +
    //        "d = filter c by name != 'fred';\n"  +
    //        "e = group d by name;\n"  +
    //        "f = foreach e generate flatten(d);\n"  +
    //        "g = foreach f generate name, d::a::name, a::name;\n" +
    //        "store g into 'output';";
    //        buildPlan( query );
    //    }

    @Test
    public void testQuery105() throws Exception {
        // test that the alias "group" can be used
        // after a flatten(group)
        String query = "a = load 'st10k' as (name, age, gpa);" +
        "b = group a by name;" +
        "c = foreach b generate flatten(group), COUNT(a) as cnt;" +
        "d = foreach c generate group;" +
        "store d into 'output';";
        buildPlan( query );
    }

    @Test
    public void testQuery106()  throws Exception {
        String query = "a = load 'one' as (name, age, gpa);" +
        "b = foreach a generate *;" + "store b into 'output';";

        LogicalPlan lp = buildPlan(query);
        Operator store = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach)lp.getPredecessors(store).get(0);
        String s = foreach.getSchema().toString(false);
        assertEquals("name:bytearray,age:bytearray,gpa:bytearray", s);
    }

    @Test
    public void testQuery107()  throws Exception {
        String query = "a = load 'one';" + "b = foreach a generate *;" + "store b into 'output';";
        LogicalPlan lp = buildPlan( query );
        Operator store = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach)lp.getPredecessors(store).get(0);
        LOGenerate gen = (LOGenerate) foreach.getInnerPlan().getSinks().get(0);
        LogicalExpressionPlan foreachPlan = gen.getOutputPlans().get(0);
        assertTrue(checkPlanForProjectStar(foreachPlan));
    }

    @Test
    public void testQuery108()  throws Exception {
        String query = "a = load 'one' as (name, age, gpa);" +
        "b = group a by *;" +
        "store b into 'output';";
        LogicalPlan lp = buildPlan(query);
        Operator store = lp.getSinks().get(0);
        LOCogroup cogroup = (LOCogroup)lp.getPredecessors(store).get(0);
        String s = cogroup.getSchema().toString(false);
        assertEquals("group:tuple(name:bytearray,age:bytearray,gpa:bytearray),a:bag{:tuple(name:bytearray,age:bytearray,gpa:bytearray)}", s);
    }

    @Test
    public void testQuery109()  throws Exception {
        String query = "a = load 'one' as (name, age, gpa);" +
                       "b = load 'two' as (first_name, enrol_age, high_school_gpa);" +
                       "c = group a by *, b by *;" +
                       "store c into 'output';";
        LogicalPlan lp = buildPlan(query);
        Operator store = lp.getSinks().get(0);
        LOCogroup cogroup = (LOCogroup)lp.getPredecessors(store).get(0);
        String s = cogroup.getSchema().toString(false);
        assertEquals("group:tuple(name:bytearray,age:bytearray,gpa:bytearray),a:bag{:tuple(name:bytearray,age:bytearray,gpa:bytearray)},b:bag{:tuple(first_name:bytearray,enrol_age:bytearray,high_school_gpa:bytearray)}", s);
    }

    @Test(expected = FrontendException.class)
    public void testQuery110Fail()  throws Exception {
    	String query = "a = load 'one' as (name, age, gpa);" +
    	"b = load 'two';" + "c = cogroup a by $0, b by *;";

        try {
            buildPlan( query );
        } catch(Exception e) {
            assertTrue(e.getMessage().contains("Cogroup/Group by '*' or 'x..' (range of columns to the end) is only allowed " +
            		"if the input has a schema" ) );
            throw e;
        }
    }

    @Test
    public void testQuery111()  throws Exception {
        String query = "a = load 'one' as (name, age, gpa);" +
        "b = order a by *;" + "store b into 'y';";

        LogicalPlan lp = buildPlan(query);
        Operator store = lp.getSinks().get(0);
        LOSort sort = (LOSort)lp.getPredecessors(store).get(0);

        for(LogicalExpressionPlan sortPlan: sort.getSortColPlans() ) {
            assertFalse(checkPlanForProjectStar(sortPlan));
        }
    }

    @Test
    public void testQuery112()  throws Exception {
        String query = "a = load 'one' as (name, age, gpa);" +
        "b = group a by *;" +
        "c = foreach b {a1 = order a by *; generate a1;};" +
        "store c into 'y';";
        LogicalPlan lp = buildPlan(query);
        Operator store = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach)lp.getPredecessors(store).get(0);
        LOGenerate gen = (LOGenerate) foreach.getInnerPlan().getSinks().get(0);
        for(LogicalExpressionPlan foreachPlan: gen.getOutputPlans()) {
            assertTrue(checkPlanForProjectStar(foreachPlan));
        }

        LogicalPlan foreachPlan = foreach.getInnerPlan();
        LOSort sort = (LOSort)foreachPlan.getPredecessors(gen).get(0);

        // project (*) operator here is translated to a list of projection
        // operators
        for(LogicalExpressionPlan sortPlan: sort.getSortColPlans()) {
            assertFalse(checkPlanForProjectStar(sortPlan));
        }
    }

    @Test
    public void testQuery114()  throws Exception {
        String query = "a = load 'one' as (name, age, gpa);" +
        "b = foreach a generate " + Identity.class.getName() + "(name, age);" +
        "store b into 'y';";

        LogicalPlan lp = buildPlan(query);
        Operator store = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach)lp.getPredecessors(store).get(0);
        String s = foreach.getSchema().toString(false);
        assertEquals(":tuple(name:bytearray,age:bytearray)", s);
    }

    @Test
    public void testQuery115()  throws Exception {
        String query = "a = load 'one' as (name, age, gpa);" +
        "b = foreach a generate " + Identity.class.getName() + "(*);" +
        "store b into 'y';";

        LogicalPlan lp = buildPlan(query);
        Operator store = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach)lp.getPredecessors(store).get(0);
        String s = foreach.getSchema().toString(false);
        assertEquals(":tuple(name:bytearray,age:bytearray,gpa:bytearray)", s);
    }

    @Test
    public void testQuery116()  throws Exception {
        String query = "a = load 'one';" +
                       "b = foreach a generate " + Identity.class.getName() + "($0, $1);" +
                       "store b into 'y';";

        LogicalPlan lp = buildPlan(query);
        Operator store = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach)lp.getPredecessors(store).get(0);
        String s = foreach.getSchema().toString(false);
        assertEquals(":tuple(:bytearray,:bytearray)", s);
    }

    @Test
    public void testQuery117()  throws Exception {
        String query = "a = load 'one';" +
        "b = foreach a generate " + Identity.class.getName() + "(*);" +
        "store b into 'y';";
        LogicalPlan lp = buildPlan(query);
        Operator store = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach)lp.getPredecessors(store).get(0);
        String s = foreach.getSchema().toString(false);
        assertTrue(s.equals(":tuple()"));
    }

    @Test
    public void testNullConsArithExprs() throws Exception {
        String query = "a = load 'a' as (x:int, y:double);" +
        "b = foreach a generate x + null, x * null, x / null, x - null, null % x, " +
                "y + null, y * null, y / null, y - null;" +
        "store b into 'output';";
        buildPlan( query );
    }

    @Test
    public void testNullConsBincond1() throws Exception {
    	String query = "a = load 'a' as (x:int, y:double);" +
        "b = foreach a generate (2 > 1? null : 1), ( 2 < 1 ? null : 1), " +
                "(2 > 1 ? 1 : null), ( 2 < 1 ? 1 : null);"
        + "store b into 'output';";
    	buildPlan( query );
    }

    @Test
    public void testNullConsBincond2() throws Exception {
    	String query = "a = load 'a' as (x:int, y:double);" +
        "b = foreach a generate (null is null ? 1 : 2), ( null is not null ? 2 : 1);" +
        "store b into 'output';";
        buildPlan( query );
    }

    @Test
    public void testNullConsForEachGenerate() throws Exception {
    	String query = "a = load 'a' as (x:int, y:double);" +
        "b = foreach a generate x, null, y, null;" +
        "store b into 'output';";
        buildPlan( query );
    }

    @Test
    public void testNullConsOuterJoin() throws Exception {
    	String query = "a = load 'a' as (x:int, y:chararray);" +
        "b = load 'b' as (u:int, v:chararray);" +
        "c = cogroup a by x, b by u;" +
        "d = foreach c generate flatten((SIZE(a) == 0 ? null : a)), " +
                "flatten((SIZE(b) == 0 ? null : b));" +
        "store d into 'output';";
        buildPlan(query);
    }

    @Test
    public void testNullConsConcatSize() throws Exception {
    	String query = "a = load 'a' as (x:int, y:double, str:chararray);" +
        "b = foreach a generate SIZE(null), CONCAT(str, null), " +
                "CONCAT(null, str);" +
                "store b into 'output';";
        buildPlan(query);
    }

    @Test
    public void testFilterUdfDefine() throws Exception {
    	String query = "define isempty IsEmpty();" +
        "a = load 'a' as (x:int, y:double, str:chararray);" +
        "b = filter a by isempty(*);" + "store b into 'output';";
    	buildPlan(query);
    }

    @Test
    public void testLoadUdfDefine() throws Exception {
    	String query = "define PS PigStorage();" +
        "a = load 'a' using PS as (x:int, y:double, str:chararray);"  +
        "b = filter a by IsEmpty(*);" +
        " store b into 'x' using PS;";
    	buildPlan(query);
    }

    @Test
    public void testLoadUdfConstructorArgDefine() throws Exception {
    	String query = "define PS PigStorage(':');" +
        "a = load 'a' using PS as (x:int, y:double, str:chararray);"  +
        "b = filter a by IsEmpty(*);" +
        " store b into 'x' using PS;";
    	buildPlan(query);
    }

    @Test
    public void testStoreUdfDefine() throws Exception {
    	String query =  "define PS PigStorage();" +
        "a = load 'a' using PS as (x:int, y:double, str:chararray);"  +
        "b = filter a by IsEmpty(*);"  +
        "store b into 'x' using PS;";
    	buildPlan(query);
    }

    @Test
    public void testStoreUdfConstructorArgDefine() throws Exception {
    	String query =  "define PS PigStorage(':');" +
        " a = load 'a' using PS as (x:int, y:double, str:chararray);"  +
        " b = filter a by IsEmpty(*);"  +
        " store b into 'x' using PS;";
    	buildPlan(query);
    }

    @Test
    public void testCastAlias() throws Exception {
    	String query = "a = load 'one.txt' as (x,y); " +
        "b =  foreach a generate (int)x, (double)y;" +
        "c = group b by x;" +
        "store c into 'output';";
    	buildPlan(query);
    }

    @Test
    public void testCast() throws Exception {
    	String query = "a = load 'one.txt' as (x,y); " +
        "b = foreach a generate (int)$0, (double)$1;" +
        "c = group b by $0;"+
        "store c into 'output';";
    	buildPlan(query);
    }

    @Test
    public void testReservedWordsInFunctionNames() throws Exception {
        // test that define can contain reserved words are later parts of
        // fully qualified function name
       	String[] keywords = {
			"define",
			"load",
			"filter",
			"foreach",
			"matches",
			"order",
			"arrange",
			"distinct",
			"cogroup",
			"join",
			"cross",
			"union",
			"split",
			"into",
			"if",
			"all",
			"any",
			"as",
			"by",
			"using",
			"inner",
			"outer",
			"parallel",
			"partition",
			"group",
			"and",
			"or",
			"not",
			"generate",
			"flatten",
			"eval",
			"asc",
			"desc",
			"int",
			"long",
			"float",
			"double",
			"chararray",
			"bytearray",
			"bag",
			"tuple",
			"map",
			"is",
			"null",
			"stream",
			"through",
			"store",
			"ship",
			"cache",
			"input",
			"output",
			"stderr",
			"stdin",
			"stdout",
			"limit",
			"sample",
			"left",
			"right",
			"full",
			"eq",
			"gt",
			"lt",
			"gte",
			"lte",
			"neq"
		};

		for(String keyword: keywords) {
			String query = "define FUNC org.apache."+keyword+"();";
        	LogicalPlan lp = buildPlan( query );
		}
    }

    @Test
    public void testTokenizeSchema()  throws Exception {
        String query = "a = load 'one' as (f1: chararray);" +
        "b = foreach a generate TOKENIZE(f1);" +
        "store b into 'output';";
        LogicalPlan lp = buildPlan(query);
        Operator store = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach) lp.getPredecessors(store).get(0);
        String s = foreach.getSchema().toString(false);
        assertEquals("bag_of_tokenTuples_from_f1:bag{tuple_of_tokens:tuple(token:chararray)}", s);
    }

    @Test
    public void testTokenizeSchema2()  throws Exception {
        String query = "a = load 'one' as (f1: chararray, f2: chararray);" +
        "b = foreach a generate TOKENIZE(f1), TOKENIZE(f2);" +
        "store b into 'output';";
        LogicalPlan lp = buildPlan(query);
        Operator store = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach) lp.getPredecessors(store).get(0);
        String s = foreach.getSchema().toString(false);
        assertEquals("bag_of_tokenTuples_from_f1:bag{tuple_of_tokens:tuple(token:chararray)}"
                +",bag_of_tokenTuples_from_f2:bag{tuple_of_tokens:tuple(token:chararray)}", s);
    }

    @Test
    public void testEmptyTupleConst() throws Exception{
        String query = "a = foreach (load 'b') generate ();" + "store a into 'output';";
        LogicalPlan lp = buildPlan( query );
        Operator store = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach) lp.getPredecessors(store).get(0);
        LOGenerate gen = (LOGenerate)foreach.getInnerPlan().getSinks().get(0);
        LogicalExpressionPlan exprPlan = gen.getOutputPlans().get(0);
        Operator logOp = exprPlan.getSources().get(0);
        assertTrue(logOp instanceof ConstantExpression);

        ConstantExpression loConst = (ConstantExpression)logOp;
        assertEquals(DataType.TUPLE, loConst.getType());
        assertTrue(loConst.getValue() instanceof Tuple);
        assertEquals(TupleFactory.getInstance().newTuple(), loConst.getValue());

        String s = foreach.getSchema().toString(false);
        assertEquals(":tuple()", s);
    }

    @Test
    public void testEmptyMapConst() throws Exception{
        String query = "a = foreach (load 'b') generate [];" + "store a into 'output';";
        LogicalPlan lp = buildPlan(query);
        Operator store = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach) lp.getPredecessors(store).get(0);
        LOGenerate gen = (LOGenerate)foreach.getInnerPlan().getSinks().get(0);
        LogicalExpressionPlan exprPlan = gen.getOutputPlans().get(0);
        Operator logOp = exprPlan.getSources().get(0);
        assertTrue(logOp instanceof ConstantExpression);

        ConstantExpression loConst = (ConstantExpression)logOp;
        assertEquals(DataType.MAP, loConst.getType());
        assertTrue(loConst.getValue() instanceof Map);
        assertEquals(new HashMap<String,Object>(), loConst.getValue());

        String s = foreach.getSchema().toString(false);
        assertTrue( s.equals(":map"));
    }

    @Test
    public void testEmptyBagConst() throws Exception{
        String query = "a = foreach (load 'b') generate {};" +
                       "store a into 'output';";
        LogicalPlan lp = buildPlan(query);
        Operator store = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach) lp.getPredecessors(store).get(0);
        LOGenerate gen = (LOGenerate)foreach.getInnerPlan().getSinks().get(0);
        LogicalExpressionPlan exprPlan = gen.getOutputPlans().get(0);
        Operator logOp = exprPlan.getSources().get(0);
        assertTrue(logOp instanceof ConstantExpression);

        ConstantExpression loConst = (ConstantExpression)logOp;
        assertEquals(DataType.BAG, loConst.getType());
        assertTrue(loConst.getValue() instanceof DataBag);
        assertEquals(BagFactory.getInstance().newDefaultBag(), loConst.getValue());

        assertEquals(":bag{}", foreach.getSchema().toString(false));
    }

    @Test
    public void testEmptyTupConstRecursive1() throws Exception{
        String query = "a = foreach (load 'b') generate (());" +
                      "store a into 'output';";
        LogicalPlan lp = buildPlan(query);
        Operator store = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach) lp.getPredecessors(store).get(0);

        assertEquals(":tuple(:tuple())", foreach.getSchema().toString(false));
    }

    @Test
    public void testEmptyTupConstRecursive2() throws Exception{
        String query = "a = foreach (load 'b') generate ([]);" +
                       "store a into 'output';";
        LogicalPlan lp = buildPlan( query );
        Operator store = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach) lp.getPredecessors(store).get(0);

        assertEquals(":tuple(:map)", foreach.getSchema().toString(false));
    }

    @Test
    public void testEmptyTupConstRecursive3() throws Exception{
        String query = "a = foreach (load 'b') generate ({});" +
    	"store a into 'output';";
        LogicalPlan lp = buildPlan(query);
        Operator op = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach)lp.getPredecessors(op).get(0);

        assertEquals(":tuple(:bag{})", foreach.getSchema().toString(false));
    }

    @Test
    public void testEmptyBagConstRecursive() throws Exception{
    	String query = "a = foreach (load 'b') generate {()};" +
    	               "store a into 'output';";
        LogicalPlan lp = buildPlan(query);
        Operator op = lp.getSinks().get(0);
        LOForEach foreach = (LOForEach)lp.getPredecessors(op).get(0);

        assertEquals(":bag{:tuple()}", foreach.getSchema().toString(false));
    }

    @Test
    public void testRandomEmptyConst() throws Exception{
        // Various random scripts to test recursive nature of parser with empty constants.

        buildPlan("a = foreach (load 'b') generate {({})}; store a into 'output';");
        buildPlan("a = foreach (load 'b') generate ({()}); store a into 'output';");
        buildPlan("a = foreach (load 'b') generate {(),()}; store a into 'output';");
        buildPlan("a = foreach (load 'b') generate ({},{}); store a into 'output';");
        buildPlan("a = foreach (load 'b') generate ((),()); store a into 'output';");
        buildPlan("a = foreach (load 'b') generate ([],[]); store a into 'output';");
        buildPlan("a = foreach (load 'b') generate {({},{})}; store a into 'output';");
        buildPlan("a = foreach (load 'b') generate {([],[])}; store a into 'output';");
        buildPlan("a = foreach (load 'b') generate (({},{})); store a into 'output';");
        buildPlan("a = foreach (load 'b') generate (([],[])); store a into 'output';");
    }

    @Test
    // See PIG-1024, shall not throw exception
    public void testLimitMultipleOutput() throws Exception {
        String query = " a = load '1.txt' as (a0:int, a1:int, a2:int);" +
                       " b = group a by a0;" +
                       " c = foreach b { c1 = limit a 10; c2 =  distinct c1.a1; c3 = distinct c1.a2; generate c2, c3;};" +
                       " store c into 'output';";
        buildPlan( query );
    }

    @Test(expected = FrontendException.class)
    public void testCogroupByStarFailure1() throws Exception {
        try {
            String query = " a = load '1.txt' as (a0:int, a1:int);" +
            " b = load '2.txt'; " +
            "c = cogroup a by *, b by *;" +
            "store c into 'output';";
            buildPlan(query);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Cogroup/Group by '*' or 'x..' (range of columns to the end) is only" +
            		" allowed if the input has a schema"));
            throw e;
        }
    }

    @Test(expected = FrontendException.class)
    public void testCogroupByStarFailure2() throws Exception {
        try {
            String query = " a = load '1.txt' ;" +
            " b = load '2.txt' as (b0:int, b1:int); " +
            "c = cogroup a by *, b by *;" +
            "store c into 'output';";
            buildPlan( query );
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Cogroup/Group by '*' or 'x..' (range of columns to the end) is only allowed if the input has a schema"));
            throw e;
        }
    }

    @Test(expected = FrontendException.class)
    public void testMissingSemicolon() throws Exception {
        try {
            String query = "A = load '1.txt' \n" +
                           "B = load '2.txt' as (b0:int, b1:int);\n" +
                           "C = union A, B;\n" +
                           "store C into 'output';";
            buildPlan( query );
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("mismatched input 'B' expecting SEMI_COLON"));
            throw e;
        }
    }

    @Test(expected = FrontendException.class)
    public void testCogroupByIncompatibleSchemaFailure() throws Exception {
        try {
            String query = " a = load '1.txt' as (a0:int, a1:int);" +
            " b = load '2.txt' as (a0:int, a1:chararray); " +
            "c = cogroup a by (a0,a1), b by (a0,a1);";
            buildPlan( query );
        } catch (Exception e) {
            String msg =
                "group column no. 2 in relation no. 2 of  group statement" +
                " has datatype chararray which is incompatible with type of" +
                " corresponding column in earlier relation(s) in the statement";
            assertTrue(e.getCause().getMessage().contains(msg));
            throw e;
        }
    }

    @Test
    public void testLoaderSignature() throws Exception {
    	String query = "a = load '1.txt' using org.apache.pig.test.PigStorageWithSchema() as (a0:int, a1:int);" +
    	               "store a into 'output';";
        LogicalPlan plan = buildPlan( query );
        Operator op = plan.getSinks().get(0);
        LOLoad load = (LOLoad)plan.getPredecessors(op).get(0);
        // the signature is now a unique string of the format "{alias}_{scope id}-{id}" example: "a_12-0"
        String udfContextSignature = ((PigStorageWithSchema)(load).getLoadFunc()).getUDFContextSignature();
        assertTrue(udfContextSignature, udfContextSignature.matches("a_[0-9]*-[0-9]*"));

        query = " b = load '1.txt' using org.apache.pig.test.PigStorageWithSchema();" +
                "store b into 'output';";
        plan = buildPlan(query);
        op = plan.getSinks().get(0);
        load = (LOLoad)plan.getPredecessors(op).get(0);
        udfContextSignature = ((PigStorageWithSchema)(load).getLoadFunc()).getUDFContextSignature();
        assertTrue(udfContextSignature, udfContextSignature.matches("b_[0-9]*-[0-9]*"));
    }

    @Test
    public void testLastAlias() throws Exception {
        String query = "B = load '2.txt' as (b0:int, b1:int);\n" +
                "C = ORDER B by b0;";
        buildPlan(query);
        assertEquals("C", pigServer.getPigContext().getLastAlias());
    }
    
    @Test
    public void testBuildLoadOpWithDefaultFunc() throws Exception {
        String query = "a = load '1.txt';" +
                "store a into 'output';";
        LogicalPlan lp = buildPlan(query);
        FuncSpec funcSpec = getFirstLoadFuncSpec(lp);
        assertEquals("org.apache.pig.builtin.PigStorage", funcSpec.getClassName());
        
        // set default load func in config
        pigServer.getPigContext().getProperties().setProperty(PigConfiguration.PIG_DEFAULT_LOAD_FUNC, "org.apache.pig.test.PigStorageWithSchema");
        query = "a = load '1.txt';" +
                "store a into 'output';";
        lp = buildPlan(query);
        funcSpec = getFirstLoadFuncSpec(lp);
        assertEquals("org.apache.pig.test.PigStorageWithSchema", funcSpec.getClassName());    
        
        // unset default load func
        pigServer.getPigContext().getProperties().remove(PigConfiguration.PIG_DEFAULT_LOAD_FUNC);      
    }
    
    @Test
    public void testBuildStoreOpWithDefaultFunc() throws Exception {
        String query = "a = load '1.txt';" +
                "store a into 'output';";
        LogicalPlan lp = buildPlan(query);
        FuncSpec funcSpec = getFirstStoreFuncSpec(lp);
        assertEquals("org.apache.pig.builtin.PigStorage", funcSpec.getClassName());
        
        // set default load func in config
        pigServer.getPigContext().getProperties().setProperty(PigConfiguration.PIG_DEFAULT_STORE_FUNC, "org.apache.pig.test.PigStorageWithSchema");
        query = "a = load '1.txt';" +
                "store a into 'output';";
        lp = buildPlan(query);
        funcSpec = getFirstStoreFuncSpec(lp);
        assertEquals("org.apache.pig.test.PigStorageWithSchema", funcSpec.getClassName());    
        
        // unset default load func
        pigServer.getPigContext().getProperties().remove(PigConfiguration.PIG_DEFAULT_STORE_FUNC);      
    }
    
    @Test
    public void testLogicalPlanData() throws Exception {
        String query = "a = load 'input.txt'; b = load 'anotherinput.txt'; c = join a by $0, b by $1;" +
                "store c into 'output' using org.apache.pig.test.PigStorageWithSchema();";
        // Set batch on so the query is not executed
        pigServer.setBatchOn();
        pigServer.registerQuery(query);
        LogicalPlanData lData = pigServer.getLogicalPlanData();
        assertEquals("LoadFunc must be PigStorage", "org.apache.pig.builtin.PigStorage", lData.getLoadFuncs().get(0));
        assertEquals("StoreFunc must be PigStorageWithSchema", "org.apache.pig.test.PigStorageWithSchema", lData.getStoreFuncs().get(0));
        assertEquals("Number of sources must be 2", lData.getNumSources(), 2);
        assertEquals("Number of sinks must be 1", lData.getNumSinks(), 1);
        assertTrue("Source must end with input.txt", lData.getSources().get(0).endsWith("input.txt"));
        assertTrue("Sink must end with output", lData.getSinks().get(0).endsWith("output"));
        assertEquals("Number of logical relational operators must be 4", lData.getNumLogicalRelationOperators(), 4);
    }

    @Test
    public void testFlattenMap() throws Exception {
       String query = "A = LOAD 'input.txt' as (rowId:int, dataMap:map[int]);" +
               "B = FOREACH A GENERATE rowId, FLATTEN(dataMap);";

        pigServer.registerQuery(query);
        Schema schema = pigServer.dumpSchema("B");

        assertEquals(3, schema.size());

        assertEquals(DataType.INTEGER, schema.getField(0).type);
        assertEquals("rowId", schema.getField(0).alias);

        assertEquals(DataType.CHARARRAY, schema.getField(1).type);
        assertEquals("dataMap::key", schema.getField(1).alias);
        assertEquals(DataType.INTEGER, schema.getField(2).type);
        assertEquals("dataMap::value", schema.getField(2).alias);
    }
    /**
     * This method is not generic. Expects logical plan to have atleast
     * 1 source and returns the corresponding FuncSpec.
     * Specific to {@link #testBuildLoadOpWithDefaultFunc()}.
     * 
     * @param lp LogicalPlan
     * @return FuncSpec associated with 1st source
     */
    private FuncSpec getFirstLoadFuncSpec(LogicalPlan lp) {
        List<Operator> sources = lp.getSources();
        return ((LOLoad)sources.get(0)).getFileSpec().getFuncSpec();
    }
    
    /**
     * This method is not generic. Expects logical plan to have atleast
     * 1 sink and returns the corresponding FuncSpec
     * Specific to {@link #testBuildStoreOpWithDefaultFunc()}.
     * 
     * @param lp LogicalPlan
     * @return FuncSpec associated with 1st sink
     */
    private FuncSpec getFirstStoreFuncSpec(LogicalPlan lp) {
        List<Operator> sinks = lp.getSinks();
        return ((LOStore)sinks.get(0)).getFileSpec().getFuncSpec();
    }

    private boolean checkPlanForProjectStar(LogicalExpressionPlan lp) {
        List<Operator> leaves = lp.getSinks();

        for(Operator op: leaves) {
            if(op instanceof ProjectExpression) {
                if(((ProjectExpression) op).isProjectStar()) {
                    return true;
                }
            }
        }
        return false;
    }

    // Helper Functions
    public LogicalPlan buildPlan(String query) throws Exception {
        return Util.buildLp(pigServer, query);
    }
}
