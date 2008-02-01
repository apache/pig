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
package org.apache.pig.backend.local.executionengine;

import java.io.PrintStream;

import org.apache.pig.impl.physicalLayer.PhysicalOperator;


/**
 * A visitor mechanism for navigating and operating on a tree of Physical
 * Operators.  This class contains the logic to navigate thre tree, but does
 * not do anything with or to the tree.  In order to operate on or extract
 * information from the tree, extend this class.  You only need to implement
 * the methods dealing with the physical operators you are concerned
 * with.  For example, if you wish to find every POMapreduce in a physical plan
 * and perform some operation on it, your visitor would look like:
 * class MyPOVisitor extends POVisitor  {
 *     public void visitMapreduce(POMapreduce mr) { you're logic here }
 * }
 * Any operators that you do not implement the visitX method for will then
 * be navigated through by this class.
 *
 * *NOTE* When envoking a visitor, you should never call one of the
 * methods in this class.  You should pass your visitor as an argument to
 * visit() on the object you want to visit.  So:
 * RIGHT:  POEval myEval; MyVisitor v; myEval.visit(v);
 * WRONG:  POEval myEval; MyVisitor v; v.visitEval(myEval);
 * These methods are only public to make them accessible to the PO* objects.
 */
public class POVisitor {

    private PrintStream printStream = null;

    public POVisitor(PrintStream ps) {
        printStream = ps;
    }

    public void visit(PhysicalOperator po, String prefix) {
        basicVisit(po, prefix);
    }

    public PrintStream getPrintStream() {
        return printStream;
    }
    
    private void basicVisit(PhysicalOperator po, String prefix) {
        po.visit(this, prefix);
        
        if (po.inputs != null) {
            for (int i = 0; i < po.inputs.length; i++) {
                prefix += "\t";
                
                ((PhysicalOperator)po.opTable.get(po.inputs[i])).visit(this, prefix);
            }
        }
    }
}

        
