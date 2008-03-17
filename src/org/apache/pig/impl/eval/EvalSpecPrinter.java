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
package org.apache.pig.impl.eval;

import java.util.List;
import java.util.Iterator;
import java.io.PrintStream;

import org.apache.pig.impl.eval.cond.*;

public class EvalSpecPrinter extends EvalSpecVisitor {

    private PrintStream mStream = null;
    private int mIndent = 1;

    public EvalSpecPrinter(PrintStream ps) {
        mStream = ps;
    }

    /**
     * Only FilterSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitFilter(FilterSpec f) {
        indent();
        mStream.print("Filter: ");
        if (f.cond instanceof AndCond) mStream.println("AND");
        else if (f.cond instanceof CompCond) mStream.println("COMP");
        else if (f.cond instanceof FalseCond) mStream.println("FALSE");
        else if (f.cond instanceof FuncCond) mStream.println("FUNC");
        else if (f.cond instanceof NotCond) mStream.println("NOT");
        else if (f.cond instanceof OrCond) mStream.println("OR");
        else if (f.cond instanceof RegexpCond) mStream.println("REGEXP");
        else if (f.cond instanceof TrueCond) mStream.println("TRUE");
        else throw new AssertionError("Unknown Cond");
        mStream.println();
    }
        
    /**
     * Only SortDistinctSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitSortDistinct(SortDistinctSpec sd) {
        indent();
        mStream.println("SortDistinct: distinct = " + sd.distinct() + 
            " Key spec = ");
        mIndent++;
        sd.getSortSpec().visit(this);
        mIndent--;
    }

     /**
     * Only GenerateSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitGenerate(GenerateSpec g) {
        indent();
        StringBuilder sb = new StringBuilder();
        sb.append("Generate: has ");
        sb.append(g.getSpecs().size());
        sb.append(" children");
        mStream.println(sb.toString());
        mIndent++;
        super.visitGenerate(g);
        mIndent--;
    }
        
    /**
     * Only MapLookupSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitMapLookup(MapLookupSpec ml) {
        indent();
        mStream.println("MapLookup: key = " + ml.key());
    }
        
    /**
     * Only ConstSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitConst(ConstSpec c) {
        indent();
        mStream.println("Const: val = " + c.value());
    }
        
    /**
     * Only ProjectSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitProject(ProjectSpec p) {
        indent();
        mStream.print("Project: (");
        Iterator<Integer> i = p.getCols().iterator();
        while (i.hasNext()) {
            mStream.print(i.next().intValue());
            if (i.hasNext()) mStream.print(", ");
        }
        mStream.println(")");
    }
        
    /**
     * Only StarSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitStar(StarSpec s) {
        indent();
        mStream.println("Star");
    }
        
    /**
     * Only FuncEvalSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitFuncEval(FuncEvalSpec fe) {
        indent();
        StringBuilder sb = new StringBuilder();
        sb.append("FuncEval: name: ");
        sb.append(fe.getFuncName());
        sb.append(" args:");
        mStream.println(sb.toString());
        mIndent++;
        fe.getArgs().visit(this);
        mIndent--;
    }
        
    /**
     * Only CompositeEvalSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitCompositeEval(CompositeEvalSpec ce) {
        indent();
        StringBuilder sb = new StringBuilder();
        sb.append("Composite: has ");
        sb.append(ce.getSpecs().size());
        sb.append(" children");
        mStream.println(sb.toString());
        mIndent++;
        super.visitCompositeEval(ce);
        mIndent--;
    }

    /**
     * Only BinCondSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitBinCond(BinCondSpec bc) {
        indent();
        mStream.println("BinCond: True:");
        mIndent++;
        bc.ifTrue().visit(this);
        mStream.println("False:");
        bc.ifFalse().visit(this);
        mIndent--;
    }

    private void indent() {
        for (int i = 0; i < mIndent; i++) mStream.print("\t");
    }

}

        
