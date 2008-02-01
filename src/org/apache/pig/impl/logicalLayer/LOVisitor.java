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

import java.util.List;
import java.util.Iterator;


/**
 * A visitor mechanism for navigating and operating on a tree of Logical
 * Operators.  This class contains the logic to navigate thre tree, but does
 * not do anything with or to the tree.  In order to operate on or extract
 * information from the tree, extend this class.  You only need to implement
 * the methods dealing with the logical operators you are concerned
 * with.  For example, if you wish to find every LOEval in a logical plan
 * and perform some operation on it, your visitor would look like:
 * class MyLOVisitor extends LOVisitor {
 *     public void visitEval(LOEval e) { you're logic here }
 * }
 * Any operators that you do not implement the visitX method for will then
 * be navigated through by this class.
 *
 * *NOTE* When envoking a visitor, you should never call one of the
 * methods in this class.  You should pass your visitor as an argument to
 * visit() on the object you want to visit.  So:
 * RIGHT:  LOEval myEval; MyVisitor v; myEval.visit(v);
 * WRONG:  LOEval myEval; MyVisitor v; v.visitEval(myEval);
 * These methods are only public to make them accessible to the LO* objects.
 */
abstract public class LOVisitor {

    /**
     * Only LOCogroup.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitCogroup(LOCogroup g) {
        basicVisit(g);
    }
        
    /**
     * Only LOEval.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitEval(LOEval e) {
        basicVisit(e);
    }
        
    /**
     * Only LOUnion.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitUnion(LOUnion u) {
        basicVisit(u);
    }
        
        
    /**
     * Only LOLoad.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitLoad(LOLoad load) {
        basicVisit(load);
    }
        
    /**
     * Only LOSort.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitSort(LOSort s) {
        basicVisit(s);
    }
        
    /**
     * Only LOSplit.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitSplit(LOSplit s) {
        basicVisit(s);
    }
    
    public void visitSplitOutput(LOSplitOutput s) {
        basicVisit(s);
    }
    
        
    /**
     * Only LOStore.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitStore(LOStore s) {
        basicVisit(s);
    }

    private void basicVisit(LogicalOperator lo) {
        List<OperatorKey> inputs = lo.getInputs();
        Iterator<OperatorKey> i = inputs.iterator();
        
        while (i.hasNext()) {
            LogicalOperator input = lo.getOpTable().get(i.next());
            input.visit(this);
        }
    }
}

        
