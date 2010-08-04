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
import java.util.Collection;
import java.util.List;

import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;

public class LogicalPlanPrinter extends LogicalRelationalNodesVisitor {

    protected PrintStream stream = null;
    protected int level = 0;
    
//    private String TAB1 = "    ";
//    private String TABMore = "|   ";
//    private String LSep = "|\n|---";
//    private String USep = "|   |\n|   ";
//    private int levelCntr = -1;
    
    public LogicalPlanPrinter(OperatorPlan plan, PrintStream ps) {
        super(plan, new ReverseDependencyOrderWalker(plan));
        stream = ps;
    }

    protected LogicalExpressionVisitor getVisitor(LogicalExpressionPlan expr) {
        return new ExprPrinter(expr, level+1, stream);
    }

    @Override
    public void visit(LOLoad op) throws IOException {
        printLevel();
        stream.println( op.toString() );
    }

    @Override
    public void visit(LOStore op) throws IOException {
        printLevel();
        stream.println( op.toString() );
    }

    @Override
    public void visit(LOForEach op) throws IOException {
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
    public void visit(LOFilter op) throws IOException {
        printLevel();
        stream.println( op.toString() );
        LogicalExpressionVisitor v = getVisitor(op.getFilterPlan());
        level++;
        v.visit();
        level--;
    }
    
    @Override
    public void visit(LOJoin op) throws IOException {
        printLevel();
        stream.println( op.toString() );
        
        LogicalExpressionVisitor v = null;
        level++;
        for (LogicalExpressionPlan plan : op.getExpressionPlans()) {
            v = getVisitor(plan);
            v.visit();
        }
        level--;
    }

    @Override
    public void visit(LOGenerate op) throws IOException {
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
    public void visit(LOInnerLoad op) throws IOException {
        printLevel();
        stream.println( op.toString() );
    }
    
    @Override
    public void visit(LOCogroup op) throws IOException {
        printLevel();
        stream.println( op.toString() );
        MultiMap<Integer,LogicalExpressionPlan> exprPlans = op.getExpressionPlans();
        for( Integer key : exprPlans.keySet() ) {
            Collection<LogicalExpressionPlan> plans = exprPlans.get(key);
            LogicalExpressionVisitor v = null;
            level++;
            for( LogicalExpressionPlan plan : plans ) {
                v = getVisitor(plan);
                v.visit();
            }
            level--;
        }
    }
    
    @Override
    public void visit(LOSplitOutput op) throws IOException {
        printLevel();
        stream.println( op.toString() );
        LogicalExpressionVisitor v = getVisitor(op.getFilterPlan());
        level++;
        v.visit();
        level--;
    }
    
    @Override
    public void visit(LOSplit op) throws IOException {
        printLevel();
        stream.println( op.toString() );
        level++;
    }
    
    @Override
    public void visit(LOUnion op) throws IOException {
        printLevel();
        stream.println( op.toString() );
        level++;
    }
    
    @Override
    public void visit(LOCross op) throws IOException {
        printLevel();
        stream.println( op.toString() );
        level++;
    }
    
    @Override
    public void visit(LOSort op) throws IOException {
        printLevel();        
        stream.println( op.toString() );
        List<LogicalExpressionPlan> plans = op.getSortColPlans();
        LogicalExpressionVisitor v = null;
        level++;
        for( LogicalExpressionPlan plan : plans ) {
            v = getVisitor(plan);
            v.visit();
        }
        level--;
    }
    
    @Override
    public void visit(LODistinct op) throws IOException {
        printLevel();
        stream.println( op.toString() );
    }
    
    @Override
    public void visit(LOLimit op) throws IOException {
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
