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

import junit.framework.Assert;

import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
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
        printTree( (CommonTree)ast, 0 );
        Assert.assertEquals( 0, lexer.getNumberOfSyntaxErrors() );
        Assert.assertEquals( 0, parser.getNumberOfSyntaxErrors() );
    }

    @Test
    public void testNegative1() throws IOException, RecognitionException {
        int errorCount = parse( "A = load 'x'; B=A;" );
        Assert.assertTrue( errorCount > 0 );
    }
    
    @Test
    public void testNegative2() throws IOException, RecognitionException {
        int errorCount = parse( "A = load 'x'; B=(A);" );
        Assert.assertTrue( errorCount > 0 );
    }

    @Test
    public void testNegative3() throws IOException, RecognitionException {
        int errorCount = parse( "A = load 'x';B = (A) as (a:int, b:long);" );
        Assert.assertTrue( errorCount > 0 );
    }

    @Test
    public void testNegative4() throws IOException, RecognitionException {
        int errorCount = parse( "A = load 'x'; B = ( filter A by $0 == 0 ) as (a:bytearray, b:long);" );
        Assert.assertTrue( errorCount > 0 );
    }
    
    @Test
    public void testNegative5() throws IOException, RecognitionException {
        int errorCount = parse( "A = load 'x'; D = group A by $0:long;" );
        Assert.assertTrue( errorCount > 0 );
    }
    
    @Test
    public void testNegative6() throws IOException, RecognitionException {
        int errorCount = parse( "A = load '/Users/gates/test/data/studenttab10'; B = foreach A generate $0, 3.0e10.1;" );
        Assert.assertTrue( errorCount > 0 );
    }

    private int parse(String query) throws IOException, RecognitionException  {
        CharStream input = new QueryParserStringStream( query );
        QueryLexer lexer = new QueryLexer(input);
        CommonTokenStream tokens = new  CommonTokenStream(lexer);

        QueryParser parser = new QueryParser(tokens);
        QueryParser.query_return result = parser.query();

        Tree ast = (Tree)result.getTree();

        System.out.println( ast.toStringTree() );
        printTree( (CommonTree)ast, 0 );
        Assert.assertEquals( 0, lexer.getNumberOfSyntaxErrors() );
        return parser.getNumberOfSyntaxErrors();
    }

    public static void printTree(CommonTree tree, int indent) {
        if( tree != null ) {
            StringBuilder sb = new StringBuilder();
            for ( int i = 0; i < indent; i++ )
                sb = sb.append( "   " );

            System.out.println( sb + tree.getText() );

            for ( int i = 0; i < tree.getChildCount(); i++ ) {
                printTree( (CommonTree)tree.getChild( i ), indent + 1 );
            }
        }
    }

}
