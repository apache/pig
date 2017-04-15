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

import java.io.IOException;
import java.util.Properties;

import org.junit.Assert;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.validators.TypeCheckerException;
import org.junit.Before;
import org.junit.Test;

public class TestErrorHandling {
    private PigServer pig = null;

    @Before
    public void setUp() throws Exception{
        pig = new PigServer(ExecType.LOCAL, new Properties());
        pig.setValidateEachStatement(true);
    }

    @Test // Error from SchemaAliasVisitor
    public void tesNegative1() {
        String query = "A = load 'x' as ( u:int, v:long, u:chararray, w:bytearray );";
        try {
            pig.registerQuery( query );
        } catch(Exception ex) {
            System.out.println( ex.getMessage() );
            Assert.assertTrue( ex.getMessage().contains( "line 1, column 33" ) );
            return;
        }
        Assert.fail( "Testcase should fail" );
    }

    @Test // Error from ColumnAliasConversionVisitor
    public void tesNegative2() {
        String query = "A = load 'x' as ( u:int, v:long, w );\n" +
                       "B = foreach A generate $5;";
        try {
            pig.registerQuery( query );
        } catch(Exception ex) {
            System.out.println( ex.getMessage() );
            Assert.assertTrue( ex.getMessage().contains( "line 2, column 23" ) );
            return;
        }
        Assert.fail( "Testcase should fail" );
    }

    @Test // Error from ASTValidator
    public void tesNegative3() throws IOException {
        String query = "A = load 'x';\n" +
                       "C = limit B 100;";
        try {
            pig.registerQuery( query );
        } catch(Exception ex) {
            System.out.println( ex.getMessage() );
            Assert.assertTrue( ex.getMessage().contains( "line 2, column 10" ) );
            return;
        }
        Assert.fail( "Testcase should fail" );
    }
    
    @Test // Type checking error
    public void tesNegative4() throws IOException {
        String query = "A = load 'x' as ( u:int, v:chararray );\n" +
                       "C = foreach A generate u + v;";
        try {
            pig.registerQuery( query );
        } catch(TypeCheckerException ex) {
            System.out.println( ex.getCause().getMessage() );
            Assert.assertTrue( ex.getCause().getMessage().contains( "line 2, column 25" ) );
            return;
        }
        Assert.fail( "Testcase should fail" );
    }
    
    @Test // Type checking lineage error message
    public void tesNegative5() throws IOException {
        String query = "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );\n" +
                       "b = load 'b' using PigStorage('b') as (field4, field5, field6: chararray );\n" +
                       "c = cogroup a by *, b by * ;\n" +
                       "d = foreach c generate group, flatten($1), flatten($2);\n" +
                       "e = foreach d generate group + 1, field1 + 1, field4 + 2.0;";
        try {
            pig.registerQuery( query );
        } catch(TypeCheckerException ex) {
            System.out.println( ex.getCause().getMessage() );
            Assert.assertTrue( ex.getCause().getMessage().contains( "line 5, column 29" ) );
            return;
        }
        Assert.fail( "Testcase should fail" );
    }
    
    @Test // Error from ScalarAliasVisitor
    public void tesNegative6() {
        String query = "A = load 'x';\n" + 
                       "B = load 'y' as ( u : int, v : chararray );\n" +
                       "C = foreach A generate B.w, $0;\n" +
                       "D = store C into 'output';\n";
        try {
            pig.registerQuery( query );
        } catch(Exception ex) {
            System.out.println( ex.getMessage() );
            Assert.assertTrue( ex.getMessage().contains( "line 3, column 23" ) );
            return;
        }
        Assert.fail( "Testcase should fail" );
    }
    
    @Test // Error from InputOutputFileVisitor
    public void tesNegative7() throws IOException {
        String query = "A = load 'x';\n" + 
                       "store A into 'fs2you://output';\n";
        try {
            pig.registerQuery( query );
        } catch(FrontendException ex) {
            System.out.println( ex.getCause().getMessage() );
            Assert.assertTrue( ex.getCause().getMessage().contains( "line 2, column 0" ) );
            Assert.assertTrue( ex.getCause().getMessage().contains( "No FileSystem for scheme: fs2you" ) );
            return;
        }
        Assert.fail( "Testcase should fail" );
    }

    @Test // Error from InputOutputFileVisitor
    public void tesNegative8() throws IOException {
        String query = "A = load 'x' as ( a : bag{ T:tuple(u, v) }, c : int, d : long );\n" +
                       "B = foreach A { S = c * 2; T = limit S 100; generate T; };\n" +
                       "store B into 'y';";
        try {
            pig.registerQuery( query );
        } catch(FrontendException ex) {
            System.out.println( ex.getMessage() );
            Assert.assertTrue( ex.getMessage().contains( "line 2, column 37" ) );
            return;
        }
        Assert.fail( "Testcase should fail" );
    }

    @Test // PIG-1956, 1957
    public void tesNegative9() throws IOException {
		pig.registerQuery("a = load 'temp' as (a0:int, a1:int);\n");
		pig.registerQuery("b = group a by a0;\n");
		try {
			pig.registerQuery("c = foreach b { " +
					" c1 = foreach a { " +
					" c11 = filter a by a1 > 0; " +
					" generate c11; " +
					" } " +
					" generate c1; " +
			" }\n");
		} catch (FrontendException ex) {
			String msg = ex.getMessage();
			Assert.assertTrue( msg.contains( "line 5, column 32" ) );
			Assert.assertTrue( msg.contains( "mismatched input '{' expecting GENERATE"));
			return;
		}
        Assert.fail( "Testcase should fail" );
    }

    @Test
    public void tesNegative10() throws IOException {
        String query = "A = load 'x' as (u :int, v: chararray);\n" +
                       "B = load 'y';\n" +
                       "C = union onschema A, B;";
        try {
            pig.registerQuery( query );
        } catch(FrontendException ex) {
            System.out.println( ex.getMessage() );
            Assert.assertTrue( ex.getMessage().contains( "line 3, column 4" ) );
            Assert.assertTrue( ex.getMessage().contains( 
            		"UNION ONSCHEMA cannot be used with relations that have null schema" ) );
            return;
        }
        Assert.fail( "Testcase should fail" );
    }

    @Test // PIG-1961
    public void tesNegative11() throws IOException {
        String query = "A = load ^ 'x' as (name, age, gpa);\n";
        try {
            pig.registerQuery( query );
        } catch(FrontendException ex) {
        	String msg = ex.getMessage();
            System.out.println( msg );
            Assert.assertFalse( msg.contains( "file null" ) );
            Assert.assertTrue( msg.contains( "Unexpected character" ) );
            return;
        }
        Assert.fail( "Testcase should fail" );
    }
    
    @Test // PIG-1961
    public void tesNegative12() throws IOException {
        String query = "A = loadd 'x' as (name, age, gpa);\n";
        try {
            pig.registerQuery( query );
        } catch(FrontendException ex) {
        	String msg = ex.getMessage();
            System.out.println( msg );
            Assert.assertFalse( msg.contains( "file null" ) );
            Assert.assertTrue( msg.contains( "Syntax error, unexpected symbol at or near 'A'" ) );
            return;
        }
        Assert.fail( "Testcase should fail" );
    }

    @Test // PIG-1921
    public void tesNegative13() throws IOException {
        String query = "A = load 'x' as (u:int, v);\n" +
                       "B = load 'y';\n" +
                       "C = join A by u, B by w;";
        try {
            pig.registerQuery( query );
        } catch(FrontendException ex) {
        	String msg = ex.getMessage();
            System.out.println( msg );
            Assert.assertTrue( !msg.contains( "null") );
            Assert.assertTrue( msg.contains( "Projected field [w] does not exist.") );
            return;
        }
        Assert.fail( "Testcase should fail" );
    }
    
    
    @Test //pig-2606
    public void testNegative14() throws IOException {
        String query = "A = load 'x'; \n" +
                       "B = union A, A;";
        try {
            pig.registerQuery( query );
        } catch(FrontendException ex) {
            String msg = ex.getMessage();
            System.out.println( msg );
            Assert.assertTrue( msg.contains( "Pig does not accept same alias as input for") );
            Assert.assertTrue( msg.contains( "UNION") );
            return;
        }
        Assert.fail( "Testcase should fail" );
    }
    
    @Test //pig-2606
    public void testNegative15() throws IOException {
        String query = "A = load 'x' as (a0, a1); \n" +
                       "B = join A by a0, A by a1;";
        try {
            pig.registerQuery( query );
        } catch(FrontendException ex) {
            String msg = ex.getMessage();
            System.out.println( msg );
            Assert.assertTrue( msg.contains( "Pig does not accept same alias as input for") );
            Assert.assertTrue( msg.contains( "JOIN") );
            return;
        }
        Assert.fail( "Testcase should fail" );
    }    

    @Test //pig-2267
    public void testAutomaticallyGivenSchemaName1() throws IOException{
        String query = "a = load 'x' as (int,val_0);";
        String failMsg = "Duplicated alias in schema";
        shouldFailWithMessage(query,failMsg);
    }

    @Test //pig-2267
    public void testAutomaticallyGivenSchemaName2() throws IOException{
        String query = "a = load 'x' as ((int,int),tuple_0:tuple(int,int));";
        String failMsg = "Duplicated alias in schema";
        shouldFailWithMessage(query,failMsg);
    }

    @Test //pig-2267
    public void testAutomaticallyGivenSchemaName3() throws IOException{
        String query = "a = load 'x' as (tuple_0:tuple(int,int),(int,int));";
        String failMsg = "Duplicated alias in schema";
        shouldFailWithMessage(query,failMsg);
    }

    @Test //pig-2267
    public void testAutomaticallyGivenSchemaName4() throws IOException{
        String query = "a = load 'x' as (bag_0:bag{},{});";
        String failMsg = "Duplicated alias in schema";
        shouldFailWithMessage(query,failMsg);
    }

    @Test //pig-2267
    public void testAutomaticallyGivenSchemaName5() throws IOException{
        String query = "a = load 'x' as ([],map_0:map[]);";
        String failMsg = "Duplicated alias in schema";
        shouldFailWithMessage(query,failMsg);
    }

    @Test //pig-2267
    public void testAutomaticallyGivenSchemaName6() throws IOException{
        String query = "a = load 'x' as (int,int,int,val_3,val_2);";
        String failMsg = "Duplicated alias in schema";
        shouldFailWithMessage(query,failMsg);
    }

    public void shouldFailWithMessage(String query, String... messages) throws IOException {
        try {
            pig.registerQuery(query);
        } catch (Exception e) {
            String msg = e.getMessage();
            System.out.println(msg);
            for (String message : messages) {
                Assert.assertTrue(msg.contains(message));
            }
            return;
        }
        Assert.fail("Testcase should fail");
    }

}
