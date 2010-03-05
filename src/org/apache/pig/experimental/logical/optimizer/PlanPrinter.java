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
package org.apache.pig.experimental.logical.optimizer;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.pig.experimental.logical.expression.AddExpression;
import org.apache.pig.experimental.logical.expression.AndExpression;
import org.apache.pig.experimental.logical.expression.CastExpression;
import org.apache.pig.experimental.logical.expression.ConstantExpression;
import org.apache.pig.experimental.logical.expression.DivideExpression;
import org.apache.pig.experimental.logical.expression.EqualExpression;
import org.apache.pig.experimental.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.experimental.logical.expression.GreaterThanExpression;
import org.apache.pig.experimental.logical.expression.IsNullExpression;
import org.apache.pig.experimental.logical.expression.LessThanEqualExpression;
import org.apache.pig.experimental.logical.expression.LessThanExpression;
import org.apache.pig.experimental.logical.expression.LogicalExpression;
import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.experimental.logical.expression.MapLookupExpression;
import org.apache.pig.experimental.logical.expression.ModExpression;
import org.apache.pig.experimental.logical.expression.MultiplyExpression;
import org.apache.pig.experimental.logical.expression.NegativeExpression;
import org.apache.pig.experimental.logical.expression.NotEqualExpression;
import org.apache.pig.experimental.logical.expression.NotExpression;
import org.apache.pig.experimental.logical.expression.OrExpression;
import org.apache.pig.experimental.logical.expression.ProjectExpression;
import org.apache.pig.experimental.logical.expression.SubtractExpression;
import org.apache.pig.experimental.logical.relational.LOFilter;
import org.apache.pig.experimental.logical.relational.LOForEach;
import org.apache.pig.experimental.logical.relational.LOGenerate;
import org.apache.pig.experimental.logical.relational.LOInnerLoad;
import org.apache.pig.experimental.logical.relational.LOLoad;
import org.apache.pig.experimental.logical.relational.LOStore;
import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.plan.DepthFirstWalker;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanVisitor;
import org.apache.pig.experimental.plan.PlanWalker;
import org.apache.pig.experimental.plan.ReverseDependencyOrderWalker;
import org.apache.pig.impl.plan.optimizer.Rule.WalkerAlgo;

public class PlanPrinter extends AllExpressionVisitor {

    protected PrintStream stream = null;
    protected int level = 0;
    
//    private String TAB1 = "    ";
//    private String TABMore = "|   ";
//    private String LSep = "|\n|---";
//    private String USep = "|   |\n|   ";
//    private int levelCntr = -1;
    
    public class DepthFirstMemoryWalker extends DepthFirstWalker {
        
        private int level = 0;
        private int startingLevel = 0;
        private Stack<String> prefixStack;
        private String currentPrefix = "";
        
        public DepthFirstMemoryWalker(OperatorPlan plan, int startingLevel) {
            super(plan);
            level = startingLevel;
            this.startingLevel = startingLevel;
            prefixStack = new Stack<String>();
        }

        @Override
        public PlanWalker spawnChildWalker(OperatorPlan plan) {
            return new DepthFirstMemoryWalker(plan, level);
        }

        /**
         * Begin traversing the graph.
         * @param visitor Visitor this walker is being used by.
         * @throws IOException if an error is encountered while walking.
         */
        @Override
        public void walk(PlanVisitor visitor) throws IOException {
            List<Operator> roots = plan.getSources();
            Set<Operator> seen = new HashSet<Operator>();

            depthFirst(null, roots, seen, visitor);
        }
        
        public String getPrefix() {
            return currentPrefix;
        }

        private void depthFirst(Operator node,
                                Collection<Operator> successors,
                                Set<Operator> seen,
                                PlanVisitor visitor) throws IOException {
            if (successors == null) return;
            
            StringBuilder strb = new StringBuilder(); 
            for(int i = 0; i < startingLevel; i++ ) {
                strb.append("|\t");
            }
            if( ((level-1) - startingLevel ) >= 0 )
                strb.append("\t");
            for(int i = 0; i < ((level-1) - startingLevel ); i++ ) {
                strb.append("|\t");
            }
            strb.append( "|\n" );
            for(int i = 0; i < startingLevel; i++ ) {
                strb.append("|\t");
            }
            if( ((level-1) - startingLevel ) >= 0 )
                strb.append("\t");
            for(int i = 0; i < ((level-1) - startingLevel ); i++ ) {
                strb.append("|\t");
            }
            strb.append("|---");
            currentPrefix = strb.toString();

            for (Operator suc : successors) {
                if (seen.add(suc)) {
                    suc.accept(visitor);
                    Collection<Operator> newSuccessors = plan.getSuccessors(suc);
                    level++;
                    prefixStack.push(currentPrefix);
                    depthFirst(suc, newSuccessors, seen, visitor);
                    level--;
                    currentPrefix = prefixStack.pop();
                }
            }
        }
    }
    
    public PlanPrinter(OperatorPlan plan, PrintStream ps) {
        super(plan, new ReverseDependencyOrderWalker(plan));
        stream = ps;
    }

    @Override
    protected LogicalExpressionVisitor getVisitor(LogicalExpressionPlan expr) {
        return new ExprPrinter(expr, level+1);
    }

    class ExprPrinter extends LogicalExpressionVisitor {

        protected ExprPrinter(OperatorPlan plan, int startingLevel) {
            super(plan, new DepthFirstMemoryWalker(plan, startingLevel));
        }
        
        private void simplevisit(LogicalExpression exp) {
            stream.print( ((DepthFirstMemoryWalker)currentWalker).getPrefix() );
            stream.println( exp.toString() );
        }

        @Override
        public void visitAnd(AndExpression exp) throws IOException {
            simplevisit(exp);
        }

        @Override
        public void visitOr(OrExpression exp) throws IOException {
            simplevisit(exp);
        }

        @Override
        public void visitEqual(EqualExpression exp) throws IOException {
            simplevisit(exp);
        }

        @Override
        public void visitProject(ProjectExpression exp) throws IOException {
            simplevisit(exp);
        }
        
        @Override
        public void visitMapLookup(MapLookupExpression exp) throws IOException {
            simplevisit(exp);
        }

        @Override
        public void visitConstant(ConstantExpression exp) throws IOException {
            simplevisit(exp);
        }

        @Override
        public void visitCast(CastExpression exp) throws IOException {
            simplevisit(exp);
        }

        @Override
        public void visitGreaterThan(GreaterThanExpression exp) throws IOException {
            simplevisit(exp);
        }

        @Override
        public void visitGreaterThanEqual(GreaterThanEqualExpression exp) throws IOException {
            simplevisit(exp);
        }

        @Override
        public void visitLessThan(LessThanExpression exp) throws IOException {
            simplevisit(exp);
        }

        @Override
        public void visitLessThanEqual(LessThanEqualExpression exp) throws IOException {
            simplevisit(exp);
        }
        
        @Override
        public void visitNotEqual(NotEqualExpression exp) throws IOException { 
            simplevisit(exp);
        }

        @Override
        public void visitNot(NotExpression exp ) throws IOException {
            simplevisit(exp);
        }

        @Override
        public void visitIsNull(IsNullExpression exp) throws IOException {
            simplevisit(exp);
        }
        
        @Override
        public void visitNegative(NegativeExpression exp) throws IOException {
            simplevisit(exp);
        }
        
        @Override
        public void visitAdd(AddExpression exp) throws IOException {
            simplevisit(exp);
        }
        
        @Override
        public void visitSubtract(SubtractExpression exp) throws IOException {
            simplevisit(exp);
        }
        
        @Override
        public void visitMultiply(MultiplyExpression exp) throws IOException {
            simplevisit(exp);
        }
        
        @Override
        public void visitMod(ModExpression exp) throws IOException {
            simplevisit(exp);
        }
        
        @Override
        public void visitDivide(DivideExpression exp) throws IOException {
            simplevisit(exp);
        }
    }

    @Override
    public void visitLOLoad(LOLoad op) throws IOException {
        printLevel();
        stream.println( op.toString() );
    }

    @Override
    public void visitLOStore(LOStore op) throws IOException {
        printLevel();
        stream.println( op.toString() );
    }

    @Override
    public void visitLOForEach(LOForEach op) throws IOException {
        printLevel();
        stream.println( op.toString() );
        level++;
        OperatorPlan innerPlan = op.getInnerPlan();
        PlanWalker newWalker = currentWalker.spawnChildWalker(innerPlan);
        pushWalker(newWalker);
        currentWalker.walk(this);
        popWalker();
        level--;
    }

    @Override
    public void visitLOFilter(LOFilter op) throws IOException {
        printLevel();
        stream.println( op.toString() );
        LogicalExpressionVisitor v = getVisitor(op.getFilterPlan());
        level++;
        v.visit();
        level--;
    }

    @Override
    public void visitLOGenerate(LOGenerate op) throws IOException {
        printLevel();        
        stream.println( op.toString() );
        List<LogicalExpressionPlan> plans = op.getOutputPlans();
        LogicalExpressionVisitor v = null;
        level++;
        for( LogicalExpressionPlan plan : plans ) {
            v = getVisitor(plan);
            v.visit();
        }
        level--;
    }

    @Override
    public void visitLOInnerLoad(LOInnerLoad op) throws IOException {
        printLevel();
        stream.println( op.toString() );
    }

    public String toString() {
        return stream.toString();
    }   
    
    private void printLevel() {
        for(int i =0; i < level; i++ ) {
            stream.print("|\t");
        }
        stream.println("|");
        for(int i =0; i < level; i++ ) {
            stream.print("|\t");
        }
        stream.print("|---");
    }
}
