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

import junit.framework.Assert;

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
        String query = "A = load 'x' as (name, age, gpa);\n" +
                       "B = group A by name;\n" +
                       "C = foreach B { ba = filter A by age < '25'; bb = foreach ba generate gpa; generate group, flatten(bb);}";
        try {
            pig.registerQuery( query );
        } catch(FrontendException ex) {
        	String msg = ex.getMessage();
            System.out.println( msg );
            Assert.assertEquals( 1200, ex.getErrorCode() );
            Assert.assertTrue( msg.contains( "line 3, column 58" ) );
            Assert.assertTrue( msg.contains( "mismatched input 'ba' expecting LEFT_PAREN" ) );
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
            Assert.assertTrue( msg.contains( "mismatched input ''x'' expecting LEFT_PAREN" ) );
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

}
