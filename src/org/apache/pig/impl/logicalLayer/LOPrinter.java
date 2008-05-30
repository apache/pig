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
package org.apache.pig.impl.logicalLayer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.io.PrintStream;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.MultiMap;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor mechanism printing out the logical plan.
 */
public class LOPrinter extends LOVisitor {

    private PrintStream mStream = null;
    private int mIndent = 0;

    /**
     * @param ps PrintStream to output plan information to
     * @param plan Logical plan to print
     */
    public LOPrinter(PrintStream ps, LogicalPlan plan) {
        super(plan, new DepthFirstWalker(plan));
    }

    public void visit(LOAdd a) throws VisitorException {
        visitBinary(a, "+");
    }

    public void visit(LOAnd a) throws VisitorException {
        visitBinary(a, "AND");
    }
    
    public void visit(LOBinCond bc) throws VisitorException {
        print(bc);
        mStream.print(" COND: (");
        bc.getCond().visit(this);
        mStream.print(") TRUE: (");
        bc.getLhsOp().visit(this);
        mStream.print(") FALSE (");
        bc.getRhsOp().visit(this);
        mStream.print(")");
    }

    public void visit(LOCogroup g) throws VisitorException {
        print(g);
        mStream.print("GROUP BY PLANS:");
        MultiMap<LogicalOperator, LogicalPlan> plans = g.getGroupByPlans();
        for (LogicalOperator lo : plans.keySet()) {
            // Visit the associated plans
            for (LogicalPlan plan : plans.get(lo)) {
                mIndent++;
                pushWalker(new DepthFirstWalker(plan));
                visit();
                popWalker();
                mIndent--;
            }
            mStream.println();
        }
        // Visit input operators
        for (LogicalOperator lo : plans.keySet()) {
            // Visit the operator
            lo.visit(this);
        }
    }
        
    public void visit(LOConst c) throws VisitorException {
        print(c);
        mStream.print(" VALUE (" + c.getValue() + ")");
    }

    public void visit(LOCross c) throws VisitorException {
        print(c);
        mStream.println();
        super.visit(c);
    }

    public void visit(LODistinct d) throws VisitorException {
        print(d);
        mStream.println();
        super.visit(d);
    }

    public void visit(LODivide d) throws VisitorException {
        visitBinary(d, "/");
    }

    public void visit(LOEqual e) throws VisitorException {
        visitBinary(e, "==");
    }

    public void visit(LOFilter f) throws VisitorException {
        print(f);
        mStream.print(" COMP: ");
        mIndent++;
        pushWalker(new DepthFirstWalker(f.getComparisonPlan()));
        visit();
        mIndent--;
        mStream.println();
        f.getInput().visit(this);
    }

     public void visit(LOForEach f) throws VisitorException {
        print(f);
        mStream.print(" PLAN: ");
        mIndent++;
        pushWalker(new DepthFirstWalker(f.getForEachPlan()));
        visit();
        mIndent--;
        mStream.println();
        // Visit our input
        mPlan.getPredecessors((LogicalOperator)f).get(0).visit(this);
    }
 
    public void visit(LOGreaterThan gt) throws VisitorException {
        visitBinary(gt, ">");
    }

    public void visit(LOGreaterThanEqual gte) throws VisitorException {
        visitBinary(gte, ">=");
    }

    public void visit(LOLesserThan lt) throws VisitorException {
        visitBinary(lt, "<");
    }

    public void visit(LOLesserThanEqual lte) throws VisitorException {
        visitBinary(lte, "<=");
    }

    public void visit(LOLoad load) throws VisitorException {
        print(load);
        mStream.print(" FILE: " + load.getInputFile().getFileName());
        mStream.print(" FUNC: " + load.getLoadFunc().getClass().getName());
        mStream.println();
    }

    public void visit(LOMapLookup mlu) throws VisitorException {
        print(mlu);
        mStream.print("(");
        mlu.getMap().visit(this);
        mStream.print(")# " + mlu.getOperatorKey());
    }

    public void visit(LOMod m) throws VisitorException {
        visitBinary(m, "MOD");
    }

    public void visit(LOMultiply m) throws VisitorException {
        visitBinary(m, "*");
    }

    public void visit(LONegative n) throws VisitorException {
        visitUnary(n, "-");
    }

    public void visit(LONot n) throws VisitorException {
        visitUnary(n, "NOT");
    }

    public void visit(LONotEqual ne) throws VisitorException {
        visitBinary(ne, "!=");
    }

    public void visit(LOOr or) throws VisitorException {
        visitBinary(or, "OR");
    }

    public void visit(LOProject p) throws VisitorException {
        print(p);
        if (p.isStar()) {
            mStream.print(" ALL ");
        } else {
            List<Integer> cols = p.getProjection();
            mStream.print(" COL");
            if (cols.size() > 1) mStream.print("S");
            mStream.print(" (");
            for (int i = 0; i < cols.size(); i++) {
                if (i > 0) mStream.print(", ");
                mStream.print(cols.get(i));
            }
            mStream.print(")");
        }
        mStream.print(" FROM ");
        if (p.getSentinel()) {
            // This project is connected to some other relation, don't follow
            // that path or we'll cycle in the graph.
            p.getExpression().name();
        } else {
            mIndent++;
            p.getExpression().visit(this);
            mIndent--;
        }
    }

    public void visit(LORegexp r) throws VisitorException {
        print(r);
        mStream.print(" REGEX (" + r.getRegexp() + ") LOOKING IN (");
        r.getOperand().visit(this);
        mStream.print(")");
    }

    /**
     * Only LOUnion.visit() should ever call this method.
     */
    /*
    @Override
    public void visitUnion(LOUnion u) {
        print(u, u.name());
        super.visitUnion(u);
    }
        
    /**
     * Only LOSort.visit() should ever call this method.
     */
    /*
    @Override
    public void visitSort(LOSort s) {
        List<EvalSpec> ls = new ArrayList<EvalSpec>();
        ls.add(s.getSpec());
        print(s, s.name());
        super.visitSort(s);
    }
        
    /**
     * Only LOSplit.visit() should ever call this method.
     */
    /*
    @Override
    public void visitSplit(LOSplit s) {
        print(s, s.name());
        super.visitSplit(s);
    }
        
    /**
     * Only LOStore.visit() should ever call this method.
     */
    /*
    @Override
    public void visitStore(LOStore s) {
        print(s, s.name());
        super.visitStore(s);
    }

    private void print(LogicalOperator lo, String name) {
        List<EvalSpec> empty = new ArrayList<EvalSpec>();
        print(lo, name, empty);
    }
    */

    private void visitBinary(
            BinaryExpressionOperator b,
            String op) throws VisitorException {
        print(b);
        mStream.print(" (");
        b.getLhsOperand().visit(this);
        mStream.print(") " + op + " (");
        b.getRhsOperand().visit(this);
        mStream.print(") ");
    }

    private void visitUnary(
            UnaryExpressionOperator e,
            String op) throws VisitorException {
        print(e);
        mStream.print(op + " (");
        e.getOperand().visit(this);
        mStream.print(") ");
    }

    private void print(LogicalOperator lo) {
        for (int i = 0; i < mIndent; i++) mStream.print("    ");

        printName(lo);

        if (!(lo instanceof ExpressionOperator)) {
            mStream.print("Inputs: ");
            for (LogicalOperator predecessor : mPlan.getPredecessors(lo)) {
                printName(predecessor);
            }
            mStream.print("Schema: ");
            try {
                printSchema(lo.getSchema());
            } catch (FrontendException fe) {
                // ignore it, nothing we can do
                mStream.print("()");
            }
        }
        mStream.print(" : ");
    }

    private void printName(LogicalOperator lo) {
        mStream.println(lo.name() + " key(" + lo.getOperatorKey().scope + 
            ", " + lo.getOperatorKey().id + ") ");
    }

    private void printSchema(Schema schema) {
        mStream.print("(");
        for (Schema.FieldSchema fs : schema.getFields()) {
            if (fs.alias != null) mStream.print(fs.alias + ": ");
            mStream.print(DataType.findTypeName(fs.type));
            if (fs.schema != null) {
                if (fs.type == DataType.BAG) mStream.print("{");
                printSchema(fs.schema);
                if (fs.type == DataType.BAG) mStream.print("}");
            }
        }
        mStream.print(")");
    }
}

        
