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
package org.apache.pig.newplan.logical.optimizer;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.pig.newplan.DepthFirstMemoryWalker;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.AddExpression;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.BinCondExpression;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.DereferenceExpression;
import org.apache.pig.newplan.logical.expression.DivideExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanExpression;
import org.apache.pig.newplan.logical.expression.IsNullExpression;
import org.apache.pig.newplan.logical.expression.LessThanEqualExpression;
import org.apache.pig.newplan.logical.expression.LessThanExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.MapLookupExpression;
import org.apache.pig.newplan.logical.expression.ModExpression;
import org.apache.pig.newplan.logical.expression.MultiplyExpression;
import org.apache.pig.newplan.logical.expression.NegativeExpression;
import org.apache.pig.newplan.logical.expression.NotEqualExpression;
import org.apache.pig.newplan.logical.expression.NotExpression;
import org.apache.pig.newplan.logical.expression.OrExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.RegexExpression;
import org.apache.pig.newplan.logical.expression.SubtractExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;

public class ExprPrinter extends LogicalExpressionVisitor {

    protected PrintStream stream = null;
    
    public ExprPrinter(OperatorPlan plan, int startingLevel, PrintStream ps) {
        super(plan, new DepthFirstMemoryWalker(plan, startingLevel));
        stream = ps;
    }
    
    public ExprPrinter(OperatorPlan plan, PrintStream ps) {
        super(plan, new DepthFirstMemoryWalker(plan, 0));
        stream = ps;
    }
    
    private void simplevisit(LogicalExpression exp) {
        stream.print( ((DepthFirstMemoryWalker)currentWalker).getPrefix() );
        stream.println( exp.toString() );
    }

    @Override
    public void visit(AndExpression exp) throws IOException {
        simplevisit(exp);
    }

    @Override
    public void visit(OrExpression exp) throws IOException {
        simplevisit(exp);
    }

    @Override
    public void visit(EqualExpression exp) throws IOException {
        simplevisit(exp);
    }

    @Override
    public void visit(ProjectExpression exp) throws IOException {
        simplevisit(exp);
    }
    
    @Override
    public void visit(MapLookupExpression exp) throws IOException {
        simplevisit(exp);
    }

    @Override
    public void visit(ConstantExpression exp) throws IOException {
        simplevisit(exp);
    }

    @Override
    public void visit(CastExpression exp) throws IOException {
        simplevisit(exp);
    }

    @Override
    public void visit(GreaterThanExpression exp) throws IOException {
        simplevisit(exp);
    }

    @Override
    public void visit(GreaterThanEqualExpression exp) throws IOException {
        simplevisit(exp);
    }

    @Override
    public void visit(LessThanExpression exp) throws IOException {
        simplevisit(exp);
    }

    @Override
    public void visit(LessThanEqualExpression exp) throws IOException {
        simplevisit(exp);
    }
    
    @Override
    public void visit(NotEqualExpression exp) throws IOException { 
        simplevisit(exp);
    }

    @Override
    public void visit(NotExpression exp ) throws IOException {
        simplevisit(exp);
    }

    @Override
    public void visit(IsNullExpression exp) throws IOException {
        simplevisit(exp);
    }
    
    @Override
    public void visit(NegativeExpression exp) throws IOException {
        simplevisit(exp);
    }
    
    @Override
    public void visit(AddExpression exp) throws IOException {
        simplevisit(exp);
    }
    
    @Override
    public void visit(SubtractExpression exp) throws IOException {
        simplevisit(exp);
    }
    
    @Override
    public void visit(MultiplyExpression exp) throws IOException {
        simplevisit(exp);
    }
    
    @Override
    public void visit(ModExpression exp) throws IOException {
        simplevisit(exp);
    }
    
    @Override
    public void visit(DivideExpression exp) throws IOException {
        simplevisit(exp);
    }
    
    @Override
    public void visit(BinCondExpression exp ) throws IOException {
        simplevisit(exp);
    }
    
    @Override
    public void visit(UserFuncExpression exp) throws IOException {
        simplevisit(exp);
    }
    
    @Override
    public void visit(DereferenceExpression exp) throws IOException {
        simplevisit(exp);
    }
    
    @Override
    public void visit(RegexExpression op) throws IOException {
        simplevisit(op);
    }
}
