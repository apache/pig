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

import org.apache.pig.impl.eval.window.TimeWindowSpec;
import org.apache.pig.impl.eval.window.TupleWindowSpec;

/**
 * A visitor mechanism for navigating and operating on a tree of EvalSpecs.
 * This class contains the logic to navigate thre tree, but does
 * not do anything with or to the tree.  In order to operate on or extract
 * information from the tree, extend this class.  You only need to implement
 * the methods dealing with the eval specs you are concerned
 * with.  For example, if you wish to find every FilterSpec 
 * and perform some operation on it, your visitor would look like:
 * class MyEvalSpecVisitor extends EvalSpecVisitor {
 *     public void visitFilter(FilterSpec f) { you're logic here }
 * }
 * Any specs that you do not implement the visitX method for will then
 * be navigated through by this class.
 *
 * *NOTE* When envoking a visitor, you should never call one of the
 * methods in this class.  You should pass your visitor as an argument to
 * visit() on the object you want to visit.  So:
 * RIGHT:  FilterSpec myFilter; MyVisitor v; myFilter.visit(v);
 * WRONG:  FilterSpec myFilter; MyVisitor v; v.visitFilter(myFilter);
 * These methods are only public to make them accessible to the EvalSpec
 * objects.
 */
abstract public class EvalSpecVisitor {

    /**
     * Only FilterSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitFilter(FilterSpec f) {
    }
        
    /**
     * Only SortDistinctSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitSortDistinct(SortDistinctSpec sd) {
        if (sd.getSortSpec() != null) sd.getSortSpec().visit(this);
    }

     /**
     * Only GenerateSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitGenerate(GenerateSpec g) {
        Iterator<EvalSpec> i = g.getSpecs().iterator();
        while (i.hasNext()) {
            i.next().visit(this);
        }
    }
        
    /**
     * Only MapLookupSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitMapLookup(MapLookupSpec ml) {
    }
        
    /**
     * Only ConstSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitConst(ConstSpec c) {
    }
        
    /**
     * Only ProjectSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitProject(ProjectSpec p) {
    }
        
    /**
     * Only StarSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitStar(StarSpec s) {
    }
        
    /**
     * Only FuncEvalSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitFuncEval(FuncEvalSpec fe) {
        fe.getArgs().visit(this);
    }
        
    /**
     * Only CompositeEvalSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitCompositeEval(CompositeEvalSpec ce) {
        Iterator<EvalSpec> i = ce.getSpecs().iterator();
        while (i.hasNext()) {
            i.next().visit(this);
        }
    }

    /**
     * Only BinCondSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitBinCond(BinCondSpec bc) {
        bc.ifTrue().visit(this);
        bc.ifFalse().visit(this);
    }

    /**
     * Only TimeWindowSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitTimeWindow(TimeWindowSpec tw) {
    }

    /**
     * Only TupleWindowSpec.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitTupleWindow(TupleWindowSpec tw) {
    }

    /**
     * Only StreamSpec.visit() and subclass implementations of this function 
     * should ever call this method. 
     */
    public void visitStream(StreamSpec stream) {
    }

}

        
