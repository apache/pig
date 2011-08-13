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
import org.antlr.runtime.tree.CommonTreeNodeStream;
import org.antlr.runtime.tree.Tree;
import org.junit.Test;

public class TestAstValidator {

    /**
     * Verify if default data type is inserted.
     */
    @Test
    public void testDefaultDataTypeInsertion() throws IOException, RecognitionException  {
        CharStream input = new QueryParserFileStream( "test/org/apache/pig/parser/TestDefaultDataTypeInserter.pig" );
        QueryLexer lex = new QueryLexer(input);
        CommonTokenStream tokens = new  CommonTokenStream(lex);

        QueryParser parser = QueryParserUtils.createParser(tokens);
        QueryParser.query_return result = parser.query();

        Tree ast = (Tree)result.getTree();

        System.out.println( ast.toStringTree() );
        TreePrinter.printTree( (CommonTree)ast, 0 );

        CommonTreeNodeStream nodes = new CommonTreeNodeStream( ast );
        AstValidator walker = new AstValidator( nodes );
        AstValidator.query_return newResult = walker.query();
        
        Assert.assertEquals( 0, walker.getNumberOfSyntaxErrors() );

        ast = (Tree)newResult.getTree();
        validateDataTypePresent( (CommonTree)ast );
        
        TreePrinter.printTree( (CommonTree)ast, 0 );
    }
    
    private void validateDataTypePresent(CommonTree tree) {
        if( tree != null ) {
            if( tree.getText().equals( "TUPLE_DEF" ) ) {
                for ( int i = 0; i < tree.getChildCount(); i++ ) {
                    CommonTree child = (CommonTree)tree.getChild( i ); // FIELD node
                    Assert.assertTrue( "FIELD_DEF".equals( child.getText() ) );
                    CommonTree datatype = (CommonTree)child.getChild( 1 );
                    Assert.assertTrue( datatype != null );
                    String typeName = datatype.getText();
                    Assert.assertTrue( !typeName.isEmpty() );
                    validateDataTypePresent( child );
                }
            } else {
                for ( int i = 0; i < tree.getChildCount(); i++ ) {
                    validateDataTypePresent( (CommonTree)tree.getChild( i ) );
                }
            }
        }
    }

    /**
     * Validate if alias name duplication is caught.
     */
    @Test
    public void tesNegative1() throws RecognitionException, IOException {
        try {
            ParserTestingUtils.validateAst( "A = load 'x' as ( u:int, v:long, u:chararray, w:bytearray );" );
        } catch(Exception ex) {
            Assert.assertTrue( ex instanceof DuplicatedSchemaAliasException );
            return;
        }
        Assert.fail( "Testcase should fail" );
    }

    /**
     * Validate if alias name duplication is caught. Slightly more complicated than above test case.
     */
    @Test
    public void tesNegative2() throws RecognitionException, IOException {
        try {
            ParserTestingUtils.validateAst( "A = load 'x' as ( u:int, v:long, w:tuple( w:long, u:chararray, w:bytearray) );" );
        } catch(Exception ex) {
            Assert.assertTrue( ex instanceof DuplicatedSchemaAliasException );
            return;
        }
        Assert.fail( "Testcase should fail" );
    }

    @Test
    public void tesNegative3() throws RecognitionException, IOException {
        try {
            ParserTestingUtils.validateAst( "A = load 'x'; C = limit B 100;" );
        } catch(Exception ex) {
            Assert.assertTrue( ex instanceof UndefinedAliasException );
            return;
        }
        Assert.fail( "Testcase should fail" );
    }
    
    // TODO: need a test similar to above but for foreach inner plan.

    @Test
    public void testMultilineFunctionArgument() throws RecognitionException, ParsingFailureException, IOException {
        String query = "LOAD 'testIn' \n" +
            "USING PigStorage ('\n');";
        ParserTestingUtils.validateAst(query);
    }

}
