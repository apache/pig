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
import java.io.FileWriter;
import java.io.IOException;

import junit.framework.Assert;

import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import org.apache.pig.ExecType;
import org.apache.pig.PigRunner;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.test.Util;
import org.apache.pig.tools.pigstats.PigStats;
import org.junit.Test;

public class TestQueryParser {

    @Test
    public void test() throws IOException, RecognitionException  {
        CharStream input = new QueryParserFileStream( "test/org/apache/pig/parser/TestParser.pig" );
        QueryLexer lexer = new QueryLexer(input);
        CommonTokenStream tokens = new  CommonTokenStream(lexer);

        QueryParser parser = new QueryParser(tokens);
        QueryParser.query_return result = parser.query();

        Tree ast = (Tree)result.getTree();

        System.out.println( ast.toStringTree() );
        TreePrinter.printTree( (CommonTree)ast, 0 );
        Assert.assertEquals( 0, lexer.getNumberOfSyntaxErrors() );
        Assert.assertEquals( 0, parser.getNumberOfSyntaxErrors() );
    }

    @Test
    // After PIG-438, realias statement is valid
    public void testNegative1() throws IOException, RecognitionException {
        shouldPass("A = load 'x'; B=A;");
    }
    
    @Test
    public void testNegative2() throws IOException, RecognitionException {
        shouldFail("A = load 'x'; B=(A);");
    }

    @Test
    public void testNegative3() throws IOException, RecognitionException {
        shouldFail("A = load 'x';B = (A) as (a:int, b:long);");
    }

    @Test
    public void testNegative4() throws IOException, RecognitionException {
        shouldFail("A = load 'x'; B = ( filter A by $0 == 0 ) as (a:bytearray, b:long);");
    }
    
    @Test
    public void testNegative5() throws IOException, RecognitionException {
        shouldFail("A = load 'x'; D = group A by $0:long;");
    }
    
    @Test
    public void testNegative6() throws IOException, RecognitionException {
        shouldFail("A = load '/Users/gates/test/data/studenttab10'; B = foreach A generate $0, 3.0e10.1;");
    }
    
    @Test // test error message with file name
    public void testNagative7() throws IOException {
        File f1 = new File("myscript.pig");
        f1.deleteOnExit();
        
        FileWriter fw1 = new FileWriter(f1);
        fw1.append("A = loadd '1.txt';");
        fw1.close();
        
        String[] args = { "-x", "local", "-c", "myscript.pig" };
        PigStats stats = PigRunner.run(args, null);
       
        Assert.assertFalse(stats.isSuccessful());
        
        String expected = "<file myscript.pig, line 1, column 0>";
        String msg = stats.getErrorMessage();
        
        Assert.assertFalse(msg == null);
        Assert.assertTrue(msg.startsWith(expected));
    }
    
    // See PIG-2238
    @Test
    public void testDependentNullAlias() throws IOException, RecognitionException {
        PigServer pigServer = new PigServer(ExecType.LOCAL);
        try {
            pigServer.registerQuery( "F = limit F 20;store F into 'out';" );
        } catch(Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("Unrecognized alias F"));
            return;
        }
        Assert.fail();
    }
    
    @Test
    public void test2() throws IOException, RecognitionException {
        shouldPass("A = load '/Users/gates/test/data/studenttab10'; B = foreach A generate ( $0 == 0 ? 1 : 0 );");
    }

    @Test
    public void test3() throws IOException, RecognitionException {
        String query = "a = load '1.txt' as (a0);" +
                       "b = foreach a generate flatten( (bag{tuple(map[])})a0 ) as b0:map[];" +
                       "c = foreach b generate (long)b0#'key1';";
        shouldPass( query );
    }

    @Test
    public void test4() throws IOException, RecognitionException {
        String query = "a = load '1.txt'  as (name, age, gpa); b = group a by name;" +
                       "c = foreach b generate group, COUNT(a.age);" +
                       "store c into 'y';";
        shouldPass( query );
    }
    
    @Test
    public void test5() throws IOException, RecognitionException {
        String query = "a = load 'x' as (name, age, gpa);" +
            "b = foreach a generate name, age +  2L, 3.125F, 3.4e2;" +
            " store b into 'y'; ";
        shouldPass( query );
    }
    
    @Test
    public void test6() throws IOException, RecognitionException {
        String query = "a = load '/user/pig/tests/data/singlefile/studentnulltab10k' as (name:chararray, age:int, gpa:double);" +
                       "b = foreach a generate (int)((int)gpa/((int)gpa - 1)) as norm_gpa:int;" + 
                       "c = foreach b generate (norm_gpa is not null? norm_gpa: 0);" +
                       "store c into '/user/pig/out/jianyong.1297229709/Types_37.out';";
        shouldPass( query );
    }
    
    @Test
    public void test7() throws IOException, RecognitionException {
        String query = "a = load '/user/pig/tests/data/singlefile/studenttab10k';" +
                       "b = group a by $0;" +
                       "c = foreach b {c1 = order $1 by * using org.apache.pig.test.udf.orderby.OrdDesc; generate flatten(c1); };" +
                       "store c into '/user/pig/out/jianyong.1297305352/Order_15.out';";
        shouldPass( query );
    }
    
    @Test
    public void test8() throws IOException, RecognitionException {
        String query = "a = load '/user/pig/tests/data/singlefile/studenttab10k';" +
                       "b = group a by $0;" +
                       "c = foreach b {c1 = order $1 by $1; generate flatten(c1), MAX($1.$1); };" +
                       "store c into '/user/pig/out/jianyong.1297305352/Order_17.out';";
        shouldPass( query );
    }
    
    @Test
    public void testCubeNegative1() throws IOException, RecognitionException {
	// cube keyword used as alias
    	String query = "x = load 'cubedata' as (a, b, c, d); " +
    				   "cube = cube x by (a, b, c);";
    	shouldFail( query );
    }
    
    @Test
    public void testCubeNegative2() throws IOException, RecognitionException {
	// syntax error - brackets missing
    	String query = "x = load 'cubedata' as (a, b, c, d); " +
    				   "y = cube x by a, b, c;";
    	shouldFail( query );
    }
    
    @Test
    public void testCubeNegative3() throws IOException, RecognitionException {
	// syntax error - BY missing
    	String query = "x = load 'cubedata' as (a, b, c, d); " +
    				   "y = cube x (a, b, c);";
    	shouldFail( query );
    }
    
    @Test
    public void testCubeNegative4() throws IOException, RecognitionException {
	// syntax error - UDF at the end 
    	String query = "x = load 'cubedata' as (a, b, c, d); " +
    				   "y = cube x by (a, b, c), UDF(c);";
    	shouldFail( query );
    }
    
    @Test
    public void testCubePositive1() throws IOException, RecognitionException {
	// syntactically correct
    	String query = "x = load 'cubedata' as (a, b, c, d);" + 
    				   "y = cube x by (a, b, c);" +
    				   "z = foreach y generate flatten(group) as (a, b, c), COUNT(x) as count;" +
    				   "store z into 'cube_output';";
    	shouldPass( query );
    }
    
    @Test
    public void testCubePositive2() throws IOException, RecognitionException {
	// all columns using *
    	String query = "x = load 'cubedata' as (a, b, c, d);" + 
    				   "y = cube x by (*);" +
    				   "z = foreach y generate flatten(group) as (a, b, c, d), COUNT(x) as count;" +
    				   "store z into 'cube_output';";
    	shouldPass( query );
    }
    
    
    @Test
    public void testCubePositive3() throws IOException, RecognitionException {
	// range projection
    	String query = "x = load 'cubedata' as (a, b, c, d);" + 
    				   "y = cube x by ($0, $1);" +
    				   "z = foreach y generate flatten(group) as (a, b), COUNT(x) as count;" +
    				   "store z into 'cube_output';";
    	shouldPass( query );
    }
    
    @Test
    public void test9() throws IOException, RecognitionException {
        String query = "a = load 'x' as (u,v);" +
                       "b = load 'y' as (u,w);" +
                       "c = join a by u, b by u;" +
                       "d = foreach c generate a::u, b::u, w;";
        shouldPass( query );
    }

    @Test
    public void test10() throws IOException, RecognitionException {
        String query = "a = load 'x' as (name, age, gpa);" +
            "b = FOREACH C GENERATE group, flatten( ( 1 == 2 ? 2 : 3 ) );" +
            " store b into 'y'; ";
        shouldPass( query );
    }

    // 'repl' and such, shouldn't be treated as a constant. So, the following should pass.
    @Test
    public void test11() throws IOException, RecognitionException {
        String query = "a = load 'repl' as (name, age, gpa);" +
            "b = FOREACH C GENERATE group, flatten( ( 1 == 2 ? 2 : 3 ) );" +
            " store b into 'skewed'; ";
        shouldPass( query );
    }

    @Test
    public void testBagType() throws IOException, RecognitionException {
        String query = "a = load '1.txt' as ( u : bag{}, v : bag{tuple(x, y)} );" +
            "b = load '2.x' as ( t : {}, u : {(r,s)}, v : bag{ T : tuple( x, y ) }, w : bag{(z1, z2)} );" +
            "c = load '3.x' as p : int;";
        int errorCount = parse( query );
        Assert.assertTrue( errorCount == 0 );
    }

    @Test
    public void testFlatten() throws IOException, RecognitionException {
        String query = "a = load '1.txt' as ( u, v, w : int );" +
            "b = foreach a generate * as ( x, y, z ), flatten( u ) as ( r, s ), flatten( v ) as d, w + 5 as e:int;";
        int errorCount = parse( query );
        Assert.assertTrue( errorCount == 0 );
    }
    
    @Test //PIG-2083
    public void testNullInBinCondNoSpace() throws IOException{
        String query = "a = load '1.txt' as (a0, a1);" +
        "b = foreach a generate (a0==0?null:2);"; //no space around the null keyword
        PigServer pig = new PigServer(ExecType.LOCAL);
        Util.registerMultiLineQuery(pig, query);
        pig.explain("b", System.out);
    }

    @Test
    public void testAST() throws IOException, RecognitionException  {
        CharStream input = new QueryParserFileStream( "test/org/apache/pig/parser/TestAST.pig" );
        QueryLexer lexer = new QueryLexer(input);
        CommonTokenStream tokens = new  CommonTokenStream(lexer);

        QueryParser parser = new QueryParser(tokens);
        QueryParser.query_return result = parser.query();

        Tree ast = (Tree)result.getTree();

        System.out.println( ast.toStringTree() );
        TreePrinter.printTree( (CommonTree)ast, 0 );
        Assert.assertEquals( 0, lexer.getNumberOfSyntaxErrors() );
        Assert.assertEquals( 0, parser.getNumberOfSyntaxErrors() );
   
        Assert.assertEquals( "QUERY", ast.getText() );
        Assert.assertEquals( 5, ast.getChildCount() );
        
        for( int i = 0; i < ast.getChildCount(); i++ ) {
            Tree c = ast.getChild( i );
            Assert.assertEquals( "STATEMENT", c.getText() );
        }
        
        Tree stmt = ast.getChild( 0 );
        Assert.assertEquals( "A", stmt.getChild( 0 ).getText() ); // alias
        Assert.assertTrue( "LOAD".equalsIgnoreCase( stmt.getChild( 1 ).getText() ) );
        
        stmt = ast.getChild( 1 );
        Assert.assertEquals( "B", stmt.getChild( 0 ).getText() ); // alias
        Assert.assertTrue( "FOREACH".equalsIgnoreCase( stmt.getChild( 1 ).getText() ) );
        
        stmt = ast.getChild( 2 );
        Assert.assertEquals( "C", stmt.getChild( 0 ).getText() ); // alias
        Assert.assertTrue( "FILTER".equalsIgnoreCase( stmt.getChild( 1 ).getText() ) );

        stmt = ast.getChild( 3 );
        Assert.assertEquals( "D", stmt.getChild( 0 ).getText() ); // alias
        Assert.assertTrue( "LIMIT".equalsIgnoreCase( stmt.getChild( 1 ).getText() ) );

        stmt = ast.getChild( 4 );
        Assert.assertTrue( "STORE".equalsIgnoreCase( stmt.getChild( 0 ).getText() ) );
    }

    @Test
    public void testMultilineFunctionArguments() throws RecognitionException, IOException {
        final String pre = "STORE data INTO 'testOut' \n" +
                           "USING PigStorage (\n";

        String lotsOfNewLines = "'{\"debug\": 5,\n" +
                                "  \"data\": \"/user/lguo/testOut/ComponentActTracking4/part-m-00000.avro\",\n" +
                                "  \"field0\": \"int\",\n" +
                                "  \"field1\": \"def:browser_id\",\n" +
                                "  \"field3\": \"def:act_content\" }\n '\n";

        String [] queries = { lotsOfNewLines,
                            "'notsplitatall'",
                            "'see you\nnext line'",
                            "'surrounded \n by spaces'",
                            "'\nleading newline'",
                            "'trailing newline\n'",
                            "'\n'",
                            "'repeated\n\n\n\n\n\n\n\n\nnewlines'",
                            "'also\ris\rsupported\r'"};

        final String post = ");";

        for(String q : queries) {
            shouldPass(pre + q + post);
        }
    }

    private void shouldPass(String query) throws RecognitionException, IOException {
        System.out.println("Testing: " + query);
        Assert.assertEquals(query + " should have passed", 0, parse(query));
    }

    private void shouldFail(String query) throws RecognitionException, IOException {
        System.out.println("Testing: " + query);
        try {
            parse( query );
        } catch(Exception ex) {
            return;
        }
        Assert.fail( query + " should have failed" );
    }
    
    private int parse(String query) throws IOException, RecognitionException  {
        CharStream input = new QueryParserStringStream( query, null );
        QueryLexer lexer = new QueryLexer(input);
        CommonTokenStream tokens = new  CommonTokenStream(lexer);

        QueryParser parser = QueryParserUtils.createParser(tokens);
        QueryParser.query_return result = parser.query();

        Tree ast = (Tree)result.getTree();

        System.out.println( ast.toStringTree() );
        TreePrinter.printTree((CommonTree) ast, 0);
        Assert.assertEquals(0, lexer.getNumberOfSyntaxErrors());
        return parser.getNumberOfSyntaxErrors();
    }

    //PIG-2267
    public void testThatColNameIsGeneratedProperly() throws IOException {
        String query = "a = load '1.txt' as (int,(long,[]),{([])});"
                     + "b = foreach a generate val_0, tuple_0, bag_0;"
                     + "c = foreach b generate val_0, flatten(tuple_0), flatten(bag_0);"
                     + "d = foreach c generate val_0, tuple_0::val_0, bag_0::map_0;";
        PigServer pig = new PigServer(ExecType.LOCAL);
        Util.registerMultiLineQuery(pig, query);
    }
}
