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

package org.apache.pig.parser;

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.antlr.runtime.MismatchedTokenException;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.test.TestEvalPipeline.MapUDF;
import org.apache.pig.test.Util;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestLogicalPlanGenerator {
    static File command;

    private PigServer pigServer;

    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(Util.getLocalTestMode());
    }

    @BeforeClass
    public static void oneTimeSetup() throws IOException, Exception {
        // Perl script
        String[] script =
            new String[] {
                          "#!/usr/bin/perl",
                          "open(INFILE,  $ARGV[0]) or die \"Can't open \".$ARGV[0].\"!: $!\";",
                          "while (<INFILE>) {",
                          "  chomp $_;",
                          "  print STDOUT \"$_\n\";",
                          "  print STDERR \"STDERR: $_\n\";",
                          "}",
                         };
        command = Util.createInputFile("script", "pl", script);
    }

    @Test
    public void test1() throws Exception {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " +
                       "B = limit A 100; " +
                       "C = filter B by 2 > 1; " +
                       "D = load 'y' as (d1, d2); " +
                       "E = join C by ( $0, $1 ), D by ( d1, d2 ) using 'replicated' parallel 16; " +
                       "F = store E into 'output';";
        generateLogicalPlan( query );

    }

    @Test
    public void test2() throws Exception {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " +
                       "B = distinct A partition by org.apache.pig.Identity; " +
                       "C = sample B 0.49; " +
                       "D = order C by $0, $1; " +
                       "E = load 'y' as (d1, d2); " +
                       "F = union onschema D, E; " +
                       "G = load 'z' as (g1:int, g2:tuple(g21, g22)); " +
                       "H = cross F, G; " +
                       "split H into I if 10 > 5, J if 'world' eq 'hello', K if 77 <= 200; " +
                       "L = store J into 'output';";
        generateLogicalPlan( query );
    }

    @Test
    public void test3() throws Exception {
        String query = "a = load '1.txt'  as (name, age, gpa);" +
                       "b = group a by name PARTITION BY org.apache.pig.test.utils.SimpleCustomPartitioner2;" +
                       "c = foreach b generate group, COUNT(a.age);" +
                       "store c into 'y';";
        generateLogicalPlan( query );
    }

    private void generateLogicalPlan(String query) throws Exception {
        ParserTestingUtils.generateLogicalPlan( query );
    }

    @Test
    public void test4() throws Exception {
        String query = "A = load 'x'; " +
                       "B = mapreduce '" + "myjar.jar" + "' " +
                           "Store A into 'table_testNativeMRJobSimple_input' "+
                           "Load 'table_testNativeMRJobSimple_output' "+
                           "`org.apache.pig.test.utils.WordCount -files " + "file " +
                           "table_testNativeMRJobSimple_input table_testNativeMRJobSimple_output " +
                           "stopworld.file" + "`;" +
                        "C = Store B into 'output';";
        generateLogicalPlan( query );
    }

    // Test define function.
    @Test
    public void test5() throws Exception {
        String query = "define myudf org.apache.pig.builtin.PigStorage( ',' );" +
                       "A = load 'x' using myudf;" +
                       "store A into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test6() throws Exception {
        String query = "A = load 'x' as ( a : int, b, c : chararray );" +
                       "B = group A by ( a, $2 );" +
                       "store B into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test7() throws Exception {
        String query = "A = load 'x' as ( a : int, b, c : chararray );" +
                       "B = foreach A generate a, $2;" +
                       "store B into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test8() throws Exception {
        String query = "A = load 'x' as ( a : int, b, c : chararray );" +
                       "B = group A by a;" +
                       "C = foreach B { S = A.b; generate S; };" +
                       "store C into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test9() throws Exception {
        String query = "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );" +
                       "B = foreach A { R = a; S = R.u; T = limit S 100; generate S, T, c + d/5; };" +
                       "store B into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test10() throws Exception {
        String query = "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );" +
                       "B = foreach A { S = a; T = limit S 100; generate T; };" +
                       "store B into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test11() throws Exception {
        String query = "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );" +
                       "B = foreach A { T = limit a 100; generate T; };" +
                       "store B into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test12() throws Exception {
        String query = "define CMD `perl GroupBy.pl '\t' 0 1` ship('"+Util.encodeEscape(command.toString())+"');" +
                       "A = load 'x';" +
                       "B = group A by $0;" +
                       "C = foreach B {" +
                       "   D = order A by $1; " +
                       "   generate flatten(D);" +
                       "};" +
                       "E = stream C through CMD;" +
                       "store E into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test13() throws Exception {
        String query = "define CMD `perl PigStreaming.pl` ship('"+Util.encodeEscape(command.toString())+"') stderr('CMD');" +
                       "A = load 'x';" +
                       "C = stream A through CMD;" +
                       "store C into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test14() throws Exception {
        String query = "a = load 'x1' using PigStorage() as (name, age:int, gpa);" +
                       "b = load 'x2' as (name, age, registration, contributions);" +
                       "e = cogroup a by name, b by name parallel 8;" +
                       "f = foreach e generate group,  SUM(a.age) as s;" +
                       "g = filter f by s>0;" +
                       "store g into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test15() throws Exception {
        String query = "a = load 'x1' using PigStorage() as (name, age, gpa);" +
                       "b = group a all;" +
                       "c = foreach b generate AVG(a.age) as avg; " +
                       "d = load 'x2' using PigStorage() as (name, age, registration, contributions);" +
                       "e = group d all;" +
                       "f = foreach e generate AVG(d.age) as avg;" +
                       "y = foreach a generate age/c.avg, age/f.avg;" +
                       "store y into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test16() throws Exception {
        String query = "AA = load 'x';" +
                       "A = foreach (group (filter AA by $0 > 0) all) generate flatten($1);" +
                       "store A into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test17() throws Exception {
        String query = "store ( load 'x' ) into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test18() throws Exception {
        String query = "A = load 'x';\n" +
                       "C = group (foreach A generate $0 parallel 5) all;";
        generateLogicalPlan( query );
    }

    @Test
    public void test19() throws Exception {
        String query = "A = load 'x' as (u:map[], v);\n" +
                       "B = foreach A { T = (chararray)u#'hello'#'world'; generate T; };";
        generateLogicalPlan( query );
    }

    @Test
    public void test20() throws Exception {
        String query = "A = load 'x' using PigStorage() as (a:int,b:chararray);\n" +
                       "B = foreach A { C = TOMAP()#'key1'; generate C as C; };";
        generateLogicalPlan( query );
    }

    @Test
    public void test21() throws Exception {
        String query = "A = load 'x' as (u, v);\n" +
                       "B = foreach A { S = u; T = org.apache.pig.builtin.TOMAP(); generate S, T;};";
        generateLogicalPlan( query );
    }

    @Test
    public void test22() throws Exception {
        String query = "A = (load 'x' as (u, v));\n" +
                       "B = (group (foreach A generate $0 parallel 5) all);";
        generateLogicalPlan( query );
    }

    @Test
    public void test23() throws Exception {
        String query = "a = (load 'x1' using PigStorage() as (name, age, gpa));" +
                       "b = (group a all);" +
                       "c = (foreach b generate AVG(a.age) as avg); " +
                       "d = (load 'x2' using PigStorage() as (name, age, registration, contributions));" +
                       "e = (group d all);" +
                       "f = (foreach e generate AVG(d.age) as avg);" +
                       "y = (foreach a generate age/c.avg, age/f.avg);" +
                       "store y into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test24() throws Exception {
        String query = "a = (load 'x1' using PigStorage() as (name, age:int, gpa));" +
                       "b = (load 'x2' as (name, age, registration, contributions));" +
                       "e = (cogroup a by name, b by name parallel 8);" +
                       "f = (foreach e generate group,  SUM(a.age) as s);" +
                       "g = (filter f by s>0);" +
                       "(store g into 'y');";
        generateLogicalPlan( query );
    }

    @Test
    public void test25() throws Exception {
        String query = "A = (load 'x' as ( u:int, v:long, w:bytearray)); " +
                       "B = (distinct A partition by org.apache.pig.Identity); " +
                       "C = (sample B 0.49); " +
                       "D = (order C by $0, $1); " +
                       "E = (load 'y' as (d1, d2)); " +
                       "F = (union onschema D, E); " +
                       "G = (load 'z' as (g1:int, g2:tuple(g21, g22))); " +
                       "H = (cross F, G); " +
                       "split H into I if 10 > 5, J if 'world' eq 'hello', K if 77 <= 200; " +
                       "L = (store J into 'output');";
        generateLogicalPlan( query );
    }

    @Test
    public void testCubeBasic() throws Exception {
      	String query = "a = load 'input' as (x:chararray,y:chararray,z:long);"
      	        + "b = cube a by cube(x,y);"
      	        + "c = foreach b generate flatten(group) as (x,y), COUNT(cube) as count, SUM(cube.z) as total;"
      	        + "store c into 'output';";
      	generateLogicalPlan(query);
    }

    @Test
    public void testCubeMultipleIAlias() throws Exception {
      	String query = "a = load 'input' as (x:chararray,y:chararray,z:long);"
      	        + "a = load 'input' as (x,y:chararray,z:long);"
      	        + "a = load 'input' as (x:chararray,y:chararray,z:long);"
      	        + "b = cube a by rollup(x,y);"
      	        + "c = foreach b generate flatten(group) as (x,y), COUNT(cube) as count, SUM(cube.z) as total;"
      	        + "store c into 'c';";
      	generateLogicalPlan(query);
    }

    @Test
    public void testCubeAfterForeach() throws Exception {
      	String query = "a = load 'input' as (x:chararray,y:chararray,z:long);"
      	        + "b = foreach a generate x as type,y as location,z as number;"
      	        + "c = cube b by cube(type,location);"
      	        + "d = foreach c generate flatten(group) as (type,location), COUNT(cube) as count, SUM(cube.number) as total;"
      	        + "store d into 'd';";
      	generateLogicalPlan(query);
    }

    @Test
    public void testFilter1() throws Exception {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " +
                       "B = filter A by 2 > 1;\n" +
                       "store B into 'y';";
        generateLogicalPlan( query );
    }
    
    @Test
    public void testFilter2() throws Exception {
        generateLogicalPlan(
                "A = load 'x' as ( u:int, v:long, w:bytearray); " +
                "B = filter A by u is null;\n");
    }
    
    @Test
    public void testFilter3() throws Exception {
        generateLogicalPlan(
                "A = load 'x' as ( u:int, v:long, w:bytearray); " +
                "B = filter A by u is not null;\n");
    }

    @Test
    public void testFilter4() throws Exception {
        generateLogicalPlan("b = filter (load 'd.txt' as (id:int, v1, v2)) by (id > 3) AND (v1 is null);");
    }
    
    @Test
    public void testScopedAlias() throws Exception {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray);" +
                       "B = load 'y' as ( u:int, x:int, y:chararray);" +
                       "C = join A by u, B by u;" +
                       "D = foreach C generate A::u, B::u, v, x;" +
                       "store D into 'z';";
        generateLogicalPlan ( query );
    }

    @Test
    public void testConstantWithNegativeValue() throws Exception {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray);" +
                       "B = foreach A generate u, { ( -1, -15L, -3.5, -4.03F, -2.3e3 ) };" +
                       "store B into 'y';";
        generateLogicalPlan ( query );
    }

    @Test(expected = NonProjectExpressionException.class)
    public void testNegative1() throws Exception {
        String query = "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );" +
                       "B = foreach A { S = c * 2; T = limit S 100; generate T; };" +
                       "store B into 'y';";
        ParserTestingUtils.generateLogicalPlan( query );
    }

    @Test(expected = MismatchedTokenException.class)
    public void testNegative2() throws Exception {
      	String query = "ship = load 'x';";
      	try {
            ParserTestingUtils.generateLogicalPlan( query );
        } catch(Exception ex) {
            MismatchedTokenException mex = (MismatchedTokenException)ex;
            assertTrue( mex.token.getText().equals("ship") );
            throw ex;
        }
    }

    @Test(expected = MismatchedTokenException.class)
    public void testNegative3() throws Exception {
    	String query = "A = load 'y'; all = load 'x';";
    	try {
            ParserTestingUtils.generateLogicalPlan( query );
        } catch(Exception ex) {
            MismatchedTokenException mex = (MismatchedTokenException)ex;
            assertTrue( mex.token.getText().equals("all") );
            throw ex;
        }
    }

    @Test
    public void testMultilineFunctionArgument() throws Exception {
        String query = "LOAD 'testIn' \n" +
            "USING PigStorage ('\n');";
        generateLogicalPlan( query );
    }

    @Test
    // See PIG-2320
    public void testInlineOpInGroup() throws Exception {
        String query = "a = load 'data1' as (x:int); \n" +
            "a_1 = filter (group a by x) by COUNT(a) > 0;";
        generateLogicalPlan( query );
    }

    @Test
    public void testRank01() throws Exception {
        String query = "A = LOAD 'data4' AS (name:chararray,surname:chararray,sales:double,code:int);"
            + "B = rank A by sales;" + "store B into 'rank01_test';";
        generateLogicalPlan(query);
    }

    @Test
    public void testRank02() throws Exception {
        String query = "A = LOAD 'data4' AS (name:chararray,surname:chararray,sales:double,code:int);"
            + "C = rank A by sales DENSE;" + "store C into 'rank02_test';";
        generateLogicalPlan(query);
    }

    @Test
    public void testRank03() throws Exception {
        String query = "A = load 'test02' using PigStorage(',') as (firstname:chararray,lastname:chararray,rownumberPrev:int,rankPrev:int,denserankPrev:int,quartilePrev:int,sales:double,postalcode:int);"
            + "B = rank A;" + "store B into 'rank03_test';";
        generateLogicalPlan(query);
    }

    @Test
    public void testRank04() throws Exception {
        String query = "A = load 'test02' using PigStorage(',') as (firstname:chararray,lastname:chararray,rownumberPrev:int,rankPrev:int,denserankPrev:int,quartilePrev:int,sales:double,postalcode:int);"
            + "C = rank A by postalcode DESC;"
            + "store C into 'rank04_test';";
        generateLogicalPlan(query);
    }

    @Test
    public void testRank05() throws Exception {
        String query = "A = load 'test02' using PigStorage(',') as (firstname:chararray,lastname:chararray,rownumberPrev:int,rankPrev:int,denserankPrev:int,quartilePrev:int,sales:double,postalcode:int);"
            + "D = rank A by postalcode DENSE;"
            + "store D into 'rank05_test';";
        generateLogicalPlan(query);
    }

    @Test
    public void testRank06() throws Exception {
        String query = "A = load 'data' as (x:int,y:chararray,z:int,rz:chararray);"
            + "C = rank A by x..rz;";
        generateLogicalPlan(query);
    }

    @Test
    public void testRank07() throws Exception {
        String query = "A = load 'data' as (x:int,y:chararray,z:int,rz:chararray);"
            + "C = rank A by x ASC, y DESC;";
        generateLogicalPlan(query);
    }

    @Test
    public void testRank08() throws Exception {
        String query = "A = load 'data' as (x:int,y:chararray,z:int,rz:chararray);"
            + "C = rank A;";
        generateLogicalPlan(query);
    }
    
    @Test
    public void testCast1() throws Exception {
        String query = "data = LOAD 'data.txt' AS (num:CHARARRAY);" +
            "numbers = FOREACH data GENERATE (INT) num;";
        generateLogicalPlan(query);
    }

    @Test
    public void testCast2() throws Exception {
        generateLogicalPlan(
                "sds = LOAD '/my/data/location' AS (simpleFields:map[], mapFields:map[], listMapFields:map[]); " +
                "queries_rand = FOREACH sds GENERATE (CHARARRAY) (mapFields#'page_params'#'query') AS query_string;");
    }
    
    @Test
    public void testBoolean1() throws Exception {
        generateLogicalPlan(
            "A = load 'INPUT_FILE' as (id:int, fruit);" +
            "B = group A by id;" +
            "C = foreach B generate group,  " +
                "((org.apache.pig.test.utils.AccumulatorBagCount(A)>1 and " +
                "org.apache.pig.test.utils.AccumulatorBagCount(A)<3)?0:1);");
    }

    @Test
    public void testBoolean2() throws Exception {
        generateLogicalPlan(
            "A = load 'INPUT_FILE' as (id:int, fruit);" +
            "B = group A by id;" +
            "C = foreach B generate group,  " +
                "((org.apache.pig.test.utils.AccumulatorBagCount(A)>3 or " +
                "org.apache.pig.test.utils.AccumulatorBagCount(A)<2)?0:1);");
    }
    
    @Test
    public void testBoolean3() throws Exception {
        generateLogicalPlan(
                "A = load 'INPUT_FILE' as (id:int, fruit);" +
                "B = filter A by id < 5 and ( fruit neq 'cabbage' or id == 17 );");
    }
    
    @Test
    public void testBoolean4() throws Exception {
        generateLogicalPlan(
                "a = load '1.txt' as (a0, a1);" +
                "b = foreach a generate (a0 is not null ? 0 : 1);");
    }
    
    @Test
    public void testBoolean5() throws Exception {
        generateLogicalPlan(
                "a = load '1.txt' as (a0, a1);" +
                "b = foreach a generate (a0 is null ? 0 : 2);");
    }
    
    @Test
    public void testAccumWithRegexp() throws Exception {
        generateLogicalPlan(
                "A = load 'AccumulatorInput.txt' as (id:int, fruit);" +
                "B = group A by id;" +
                "C = foreach B generate group,  (((chararray)org.apache.pig.test.utils.AccumulatorBagCount(A)) matches '1*' ?0:1);");
    }

    @Test
    public void testMapsideGroupByMultipleColumns() throws Exception {
        generateLogicalPlan(
                "A = LOAD 'MapSideGroupInput.txt' using org.apache.pig.test.TestCollectedGroup$DummyCollectableLoader() as (id, name, grade);" +
                "B = group A by (id, name) using 'collected';");
    }
    
    @Test
    public void testMapUDF() throws Exception {
        generateLogicalPlan(
                "A = LOAD 'someData';" +
                "B = foreach A generate " + MapUDF.class.getName() + "($0) as mymap;" +
                "C = foreach B {" +
                "generate (double)mymap#'double' as d, " +
                "(long)mymap#'long' + (float)mymap#'float' as float_sum, " +
                "CONCAT((chararray) mymap#'string', ' World!'), " +
                "mymap#'int' * 10, " +
                "(bag{tuple()}) mymap#'bag' as mybag, " +
                "(tuple()) mymap#'tuple' as mytuple, " +
                "(map[])mymap#'map' as mapInMap, " +
                "mymap#'dba' as dba;" +
                "};");
    }

    @Test
    public void testSimpleMapCast() throws Exception {
        generateLogicalPlan(
                "a = load 'testSimpleMapCast' as (m);" + 
                "b = foreach a generate ([int])m;");
    }
    
    @Test
    public void testComplexCast() throws Exception {
        generateLogicalPlan(
                "a = load 'testComplexCast' as (m);" +
                "b = foreach a generate ([{(i:int,j:int)}])m;");
    }
    
    @Test
    public void testNullConstant() throws Exception {
        generateLogicalPlan(
                "a = load 'foo' as (x:int, y:double, str:chararray);" +
                "b = foreach a generate {(null)}, ['2'#null];");
    }
    
    @Test
    public void testEmptyTupConst() throws Exception {
        generateLogicalPlan( "a = foreach (load 'b') generate ({});");
    }
    
    @Test
    public void testJoin1() throws Exception {
        generateLogicalPlan(
                "A = load 'hat' as (m:map[]);" +
                "B = filter A by m#'cond'==1;" +
                "C = filter B by m#'key1'==1;" +
                "D = filter B by m#'key2'==2;" +
                "E = join C by m#'key1', D by m#'key1';");
    }
    
    // See: PIG-2937
    @Test
    public void testRelationAliasInNestedForeachWhereUnspecified() throws Exception {
        Data data = resetData(pigServer);
        List<Tuple> values = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
          values.add(tuple(i % 3 == 0 ? null : "a", "b"));
        }
        data.set("foo", values);
        pigServer.registerQuery("raw_data = load 'foo' using mock.Storage() as (field_a:chararray, field_b:chararray);");
        pigServer.registerQuery("records = foreach raw_data {" +
            "  generated_field = (field_a is null ? '-' : field_b);"+
            "  GENERATE" +
            "    field_a," +
            "    field_b," +
            "    generated_field; }");
        pigServer.registerQuery("use_records = foreach records generate generated_field, CONCAT(generated_field,generated_field);");
        Schema expectedSchema = Utils.getSchemaFromString("field_a:chararray, field_b:chararray, generated_field:chararray");
        assertEquals(expectedSchema, pigServer.dumpSchema("records"));
        for (Iterator<Tuple> it = pigServer.openIterator("records"); it.hasNext();) {
            Tuple t = it.next();
            String a = (String)t.get(0);
            String b = (String)t.get(1);
            assertEquals("b", b);
            if (a == null) {
                assertEquals("-", t.get(2));
            } else {
                assertEquals("a", a);
                assertEquals(b, t.get(2));
            }
        }
        for (Iterator<Tuple> it = pigServer.openIterator("use_records"); it.hasNext();) {
            Tuple t = it.next();
            String x = (String)t.get(0);
            assertEquals(x+x, t.get(1));
        }
    }
}
