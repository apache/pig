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

import org.junit.Assert;

import org.antlr.runtime.CharStream;

import org.antlr.runtime.Token;
import org.junit.Test;

public class TestQueryLexer {

    @Test
    public void TestLexer() throws IOException {
        CharStream input = new QueryParserFileStream( "test/org/apache/pig/parser/TestLexer.pig" );
        QueryLexer lexer = new QueryLexer( input );
        int tokenCount = 0;
        Token token;
        while( ( token = lexer.nextToken() ).getType() != Token.EOF ) {
            if( token.getChannel() == Token.HIDDEN_CHANNEL )
                continue;
            tokenCount++;
            if( token.getText().equals( ";" ) ) {
                System.out.println( token.getText() );
            } else {
                System.out.print( token.getText() + "(" + token.getType() + ") " );
            }
        }

        // While we can check more conditions, such as type of each token, for now I think the following
        // is enough. If the token type is wrong, it will be most likely caught by the parser.
        Assert.assertEquals( 455, tokenCount );
        Assert.assertEquals( 0, lexer.getNumberOfSyntaxErrors() );
    }

    @Test
    public void test2() throws IOException {
        String query = "A = load 'input' using PigStorage(';');" +
                       "B = foreach ^ A generate string.concatsep( ';', $1, $2 );";
        CharStream input = new QueryParserStringStream( query, null );
        QueryLexer lexer = new QueryLexer( input );
        Token token;
        try {
            while( ( token = lexer.nextToken() ).getType() != Token.EOF ) {
                if( token.getChannel() == Token.HIDDEN_CHANNEL )
                    continue;
                if( token.getText().equals( ";" ) ) {
                    System.out.println( token.getText() );
                } else {
                    System.out.print( token.getText() + "(" + token.getType() + ") " );
                }
            }
        } catch(Exception ex) {
            Assert.assertTrue( ex.getMessage().contains( "Unexpected character" ) );
            return;
        }
        Assert.fail( "Query should fail." );
    }
}
