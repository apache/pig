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
import java.util.Map;

import org.antlr.runtime.BaseRecognizer;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTreeNodeStream;
import org.antlr.runtime.tree.Tree;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LogicalPlan;

public class QueryParserDriver {
    public static ClassLoader classloader = QueryParserDriver.class.getClassLoader();
    
    private PigContext pigContext;
    private String scope;
    private Map<String, String>fileNameMap;
    private Map<String, Operator> operators;
    
    public QueryParserDriver(PigContext pigContext, String scope, Map<String, String> fileNameMap) {
        this.pigContext = pigContext;
        this.scope = scope;
        this.fileNameMap = fileNameMap;
    }

    public LogicalPlan parse(String query) throws IOException, ParserException {
        LogicalPlan plan = null;
        
        try {
            CommonTokenStream tokenStream = tokenize( query );
            
            Tree ast = parse( tokenStream );
            
            ast = validateAst( ast );
            
            LogicalPlanGenerator planGenerator = 
                new LogicalPlanGenerator( new CommonTreeNodeStream( ast ), pigContext, scope, fileNameMap );
            planGenerator.query();
            
            checkError( planGenerator );
            
            plan = planGenerator.getLogicalPlan();
            operators = planGenerator.getOperators();
        } catch(RecognitionException ex) {
            throw new ParserException( ex );
        }
        
        return plan;
    }
    
    public Map<String, Operator> getOperators() {
        return operators;
    }

    private static CommonTokenStream tokenize(String query) throws IOException, ParserException {
        CharStream input = new QueryParserStringStream( query );
        QueryLexer lexer = new QueryLexer( input );
        CommonTokenStream tokens = new CommonTokenStream( lexer );
        checkError( lexer );
        return tokens;
    }
    
    private static void checkError(BaseRecognizer recognizer) throws ParserException {
        int errorCount = recognizer.getNumberOfSyntaxErrors();
        if( 0 < errorCount )
            throw new ParserException( "Encountered " + errorCount + " parsing errors in the query" );
    }

    private static Tree parse(CommonTokenStream tokens) throws IOException, RecognitionException, ParserException  {
        QueryParser parser = new QueryParser( tokens );
        QueryParser.query_return result = parser.query();
        Tree ast = (Tree)result.getTree();
        
        checkError( parser );
        
        return ast;
    }
    
    private static Tree validateAst(Tree ast) throws IOException, RecognitionException, ParserException  {
        CommonTreeNodeStream nodes = new CommonTreeNodeStream( ast );
        AstValidator walker = new AstValidator( nodes );
        AstValidator.query_return newResult = walker.query();
        Tree newAst = (Tree)newResult.getTree();
        
        checkError( walker );
        
        return newAst;
    }

}
