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

import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeNodeStream;
import org.antlr.runtime.tree.Tree;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

public class ParserTestingUtils {
    public static CommonTokenStream tokenize(String query) throws IOException, ParsingFailureException {
        CharStream input = new QueryParserStringStream( query, null );
        QueryLexer lexer = new QueryLexer( input );
        CommonTokenStream tokens = new CommonTokenStream( lexer );
        if( 0 < lexer.getNumberOfSyntaxErrors() )
            throw new ParsingFailureException( QueryLexer.class );
        return tokens;
    }

    public static Tree parse(String query) throws IOException, RecognitionException, ParsingFailureException  {
        CommonTokenStream tokens = tokenize( query );
        QueryParser parser = QueryParserUtils.createParser( tokens );
        QueryParser.query_return result = parser.query();
        Tree ast = (Tree)result.getTree();
        TreePrinter.printTree( (CommonTree)ast, 0 );
        
        if( 0 < parser.getNumberOfSyntaxErrors() )
            throw new ParsingFailureException( QueryParser.class );
        
        return ast;
    }
    
    public static Tree validateAst(String query)
    throws RecognitionException, ParsingFailureException, IOException {
        Tree ast = parse( query );
        CommonTreeNodeStream nodes = new CommonTreeNodeStream( ast );
        AstValidator walker = new AstValidator( nodes );
        AstValidator.query_return newResult = walker.query();
        Tree newAst = (Tree)newResult.getTree();
        TreePrinter.printTree( (CommonTree)newAst, 0 );
        
        if( 0 < walker.getNumberOfSyntaxErrors() ) 
            throw new ParsingFailureException( AstValidator.class );
        
        return newAst;
    }
    
    public static LogicalPlan generateLogicalPlan(String query)
    throws RecognitionException, ParsingFailureException, IOException {
        Tree ast = validateAst( query );
        
        CommonTreeNodeStream input = new CommonTreeNodeStream( ast );
        LogicalPlanBuilder builder = new LogicalPlanBuilder( input );
        LogicalPlanGenerator walker = new LogicalPlanGenerator( input, builder );
        walker.query();
        
        if( 0 < walker.getNumberOfSyntaxErrors() ) 
            throw new ParsingFailureException( LogicalPlanGenerator.class );
        
        LogicalPlan plan = walker.getLogicalPlan();
        System.out.println( "Generated logical plan: " + plan.toString() );
        
        return plan;
    }

}
