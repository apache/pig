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
package org.apache.pig.impl.physicalLayer;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.eval.*;

public class POPrinter extends POVisitor {

    private PrintStream mStream = null;

    public POPrinter(PrintStream ps) {
        mStream = ps;
    }

    /**
     * Only POMapreduce.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitMapreduce(POMapreduce mr) {
        mStream.println("MAPREDUCE");
        printHeader(mr);

        mStream.println("Map: ");
        visitSpecs(mr.toMap);

        if (mr.toCombine != null) {
            mStream.println("Combine: ");
            //visitSpecs(mr.toCombine);
            mr.toCombine.visit(new EvalSpecPrinter(mStream));
        }

        if (mr.toReduce != null) {
            mStream.println("Reduce: ");
            mr.toReduce.visit(new EvalSpecPrinter(mStream));
        }
        
        if (mr.groupFuncs != null) {
            mStream.println("Grouping Funcs: ");
            visitSpecs(mr.groupFuncs);
        }

        mStream.print("Input Files: ");
        Iterator<FileSpec> i = mr.inputFileSpecs.iterator();
        while (i.hasNext()) {
            mStream.print(i.next().getFileName());
            if (i.hasNext()) mStream.print(", ");
        }
        mStream.println();

        if (mr.outputFileSpec != null) {
            mStream.println("Output File: " + mr.outputFileSpec.getFileName());
        }

        if (mr.partitionFunction != null) {
            mStream.println("Partition Function: " +
                mr.partitionFunction.getName());
        }

        super.visitMapreduce(mr);
    }
        
    /**
     * Only PORead.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitRead(PORead r) {
        mStream.println("READ");
        printHeader(r);
        super.visitRead(r);
    }
        
    /**
     * Only POLoad.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitLoad(POLoad load) {
        mStream.println("LOAD");
        printHeader(load);
        super.visitLoad(load);
    }
        
    /**
     * Only POSort.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitSort(POSort s) {
        mStream.println("SORT");
        printHeader(s);
        super.visitSort(s);
    }
        
    /**
     * Only POStore.visit() and subclass implementations of this function
     * should ever call this method.
     */
    public void visitStore(POStore s) {
        mStream.println("STORE");
        printHeader(s);
        super.visitStore(s);
    }

    private void visitSpecs(List<EvalSpec> specs) {
        Iterator<EvalSpec> j = specs.iterator();
        while (j.hasNext()) {
            j.next().visit(new EvalSpecPrinter(mStream));
        }
    }

    private void printHeader(PhysicalOperator po) {
        mStream.println("Object id: " + po.hashCode());
        mStream.print("Inputs: ");
        for (int i = 0; i < po.inputs.length; i++) {
            if (i != 0) mStream.print(", ");
            mStream.print(po.inputs[i].hashCode());
        }
        mStream.println();
    }

}

        
