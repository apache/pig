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

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.antlr.runtime.MismatchedTokenException;
import org.antlr.runtime.NoViableAltException;
import org.apache.pig.test.Util;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLogicalPlanGenerator {
    static File command;
    
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
    public void test1() {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " + 
                       "B = limit A 100; " +
                       "C = filter B by 2 > 1; " +
                       "D = load 'y' as (d1, d2); " +
                       "E = join C by ( $0, $1 ), D by ( d1, d2 ) using 'replicated' parallel 16; " +
                       "F = store E into 'output';";
        generateLogicalPlan( query );

    }

    @Test
    public void test2() {
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
    public void test3() {
        String query = "a = load '1.txt'  as (name, age, gpa);" + 
                       "b = group a by name PARTITION BY org.apache.pig.test.utils.SimpleCustomPartitioner2;" +
                       "c = foreach b generate group, COUNT(a.age);" +
                       "store c into 'y';";
        generateLogicalPlan( query );
    }
    
    private void generateLogicalPlan(String query) {
        try {
            ParserTestingUtils.generateLogicalPlan( query );
        } catch(Exception ex) {
            Assert.fail( "Failed to generate logical plan for query [" + query + "] due to exception: " + ex );
        }
    }

    @Test
    public void test4() {
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
    public void test5() {
        String query = "define myudf org.apache.pig.builtin.PigStorage( ',' );" +
                       "A = load 'x' using myudf;" +
                       "store A into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test6() {
        String query = "A = load 'x' as ( a : int, b, c : chararray );" +
                       "B = group A by ( a, $2 );" +
                       "store B into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test7() {
        String query = "A = load 'x' as ( a : int, b, c : chararray );" +
                       "B = foreach A generate a, $2;" +
                       "store B into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test8() {
        String query = "A = load 'x' as ( a : int, b, c : chararray );" +
                       "B = group A by a;" +
                       "C = foreach B { S = A.b; generate S; };" +
                       "store C into 'y';";
        generateLogicalPlan( query );
    }
    
    @Test
    public void test9() {
        String query = "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );" +
                       "B = foreach A { R = a; S = R.u; T = limit S 100; generate S, T, c + d/5; };" +
                       "store B into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test10() {
        String query = "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );" +
                       "B = foreach A { S = a; T = limit S 100; generate T; };" +
                       "store B into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test11() {
        String query = "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );" +
                       "B = foreach A { T = limit a 100; generate T; };" +
                       "store B into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test12() {
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
    public void test13() {
        String query = "define CMD `perl PigStreaming.pl` ship('"+Util.encodeEscape(command.toString())+"') stderr('CMD');" +
                       "A = load 'x';" +
                       "C = stream A through CMD;" +
                       "store C into 'y';";
        generateLogicalPlan( query );
    }
    
    @Test
    public void test14() {
        String query = "a = load 'x1' using PigStorage() as (name, age:int, gpa);" +
                       "b = load 'x2' as (name, age, registration, contributions);" +
                       "e = cogroup a by name, b by name parallel 8;" +
                       "f = foreach e generate group,  SUM(a.age) as s;" +
                       "g = filter f by s>0;" +
                       "store g into 'y';";
        generateLogicalPlan( query );
    }
    
    @Test
    public void test15() {
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
    public void test16() {
        String query = "AA = load 'x';" +
                       "A = foreach (group (filter AA by $0 > 0) all) generate flatten($1);" +
                       "store A into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test17() {
        String query = "store ( load 'x' ) into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void test18() {
        String query = "A = load 'x';\n" +
                       "C = group (foreach A generate $0 parallel 5) all;";
        generateLogicalPlan( query );
    }

    @Test
    public void test19() {
        String query = "A = load 'x' as (u:map[], v);\n" +
                       "B = foreach A { T = (chararray)u#'hello'#'world'; generate T; };";
        generateLogicalPlan( query );
    }
    
    @Test
    public void test20() {
        String query = "A = load 'x' using PigStorage() as (a:int,b:chararray);\n" +
                       "B = foreach A { C = TOMAP()#'key1'; generate C as C; };";
        generateLogicalPlan( query );
    }
    
    @Test
    public void test21() {
        String query = "A = load 'x' as (u, v);\n" +
                       "B = foreach A { S = u; T = org.apache.pig.builtin.TOMAP(); generate S, T;};";
        generateLogicalPlan( query );
    }
    
    @Test
    public void test22() {
        String query = "A = (load 'x' as (u, v));\n" +
                       "B = (group (foreach A generate $0 parallel 5) all);";
        generateLogicalPlan( query );
    }
    
    @Test
    public void test23() {
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
    public void test24() {
        String query = "a = (load 'x1' using PigStorage() as (name, age:int, gpa));" +
                       "b = (load 'x2' as (name, age, registration, contributions));" +
                       "e = (cogroup a by name, b by name parallel 8);" +
                       "f = (foreach e generate group,  SUM(a.age) as s);" +
                       "g = (filter f by s>0);" +
                       "(store g into 'y');";
        generateLogicalPlan( query );
    }
    
    @Test
    public void test25() {
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
    public void testCubeBasic() {
	String query = "a = load 'input' as (x:chararray,y:chararray,z:long);"
	        + "b = cube a by cube(x,y);"
	        + "c = foreach b generate flatten(group) as (x,y), COUNT(cube) as count, SUM(cube.z) as total;"
	        + "store c into 'output';";
	generateLogicalPlan(query);
    }
    
    @Test
    public void testCubeMultipleIAlias() {
	String query = "a = load 'input' as (x:chararray,y:chararray,z:long);"
	        + "a = load 'input' as (x,y:chararray,z:long);"
	        + "a = load 'input' as (x:chararray,y:chararray,z:long);"
	        + "b = cube a by rollup(x,y);"
	        + "c = foreach b generate flatten(group) as (x,y), COUNT(cube) as count, SUM(cube.z) as total;"
	        + "store c into 'c';";
	generateLogicalPlan(query);
    }
    
    @Test
    public void testCubeAfterForeach() {
	String query = "a = load 'input' as (x:chararray,y:chararray,z:long);"
	        + "b = foreach a generate x as type,y as location,z as number;"
	        + "c = cube b by cube(type,location);"
	        + "d = foreach c generate flatten(group) as (type,location), COUNT(cube) as count, SUM(cube.number) as total;"
	        + "store d into 'd';";
	generateLogicalPlan(query);
    }
    
    @Test
    public void testFilter() {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray); " + 
                       "B = filter A by 2 > 1;\n" +
                       "store B into 'y';";
        generateLogicalPlan( query );
    }

    @Test
    public void testScopedAlias() {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray);" + 
                       "B = load 'y' as ( u:int, x:int, y:chararray);" +
                       "C = join A by u, B by u;" +
                       "D = foreach C generate A::u, B::u, v, x;" +
                       "store D into 'z';";
        generateLogicalPlan ( query );
    }

    @Test
    public void testConstantWithNegativeValue() {
        String query = "A = load 'x' as ( u:int, v:long, w:bytearray);" + 
                       "B = foreach A generate u, { ( -1, -15L, -3.5, -4.03F, -2.3e3 ) };" +
                       "store B into 'y';";
        generateLogicalPlan ( query );
    }

    @Test
    public void testNegative1() {
        String query = "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );" +
                       "B = foreach A { S = c * 2; T = limit S 100; generate T; };" +
                       "store B into 'y';";
        try {
            ParserTestingUtils.generateLogicalPlan( query );
        } catch(Exception ex) {
            Assert.assertTrue( ex instanceof NonProjectExpressionException );
            return;
        }
        Assert.fail( "Query is supposed to be failing." );
    }
    
    @Test
    public void testNegative2() {
    	String query = "ship = load 'x';";
    	try {
            ParserTestingUtils.generateLogicalPlan( query );
        } catch(Exception ex) {
            Assert.assertTrue( ex instanceof MismatchedTokenException );
            MismatchedTokenException mex = (MismatchedTokenException)ex;
            Assert.assertTrue( mex.token.getText().equals("ship") );
            return;
        }
        Assert.fail( "Query is supposed to be failing." );
    }

    @Test
    public void testNegative3() {
    	String query = "A = load 'y'; all = load 'x';";
    	try {
            ParserTestingUtils.generateLogicalPlan( query );
        } catch(Exception ex) {
            Assert.assertTrue( ex instanceof MismatchedTokenException );
            MismatchedTokenException mex = (MismatchedTokenException)ex;
            Assert.assertTrue( mex.token.getText().equals("all") );
            return;
        }
        Assert.fail( "Query is supposed to be failing." );
    }

    @Test
    public void testMultilineFunctionArgument() {
        String query = "LOAD 'testIn' \n" +
            "USING PigStorage ('\n');";
        generateLogicalPlan( query );
    }
    
    @Test
    // See PIG-2320
    public void testInlineOpInGroup() {
        String query = "a = load 'data1' as (x:int); \n" +
            "a_1 = filter (group a by x) by COUNT(a) > 0;";
        generateLogicalPlan( query );
    }

}
