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
import java.util.List;
import java.util.Iterator;
import java.io.PrintStream;

import org.apache.pig.impl.eval.*;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;
import org.apache.pig.impl.logicalLayer.schema.AtomSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * A visitor mechanism printing out the logical plan.
 */
public class LOPrinter extends LOVisitor {

    private PrintStream mStream = null;

    public LOPrinter(PrintStream ps) {
        mStream = ps;
    }

    /**
     * Only LOCogroup.visit() should ever call this method.
     */
    @Override
    public void visitCogroup(LOCogroup g) {
        print(g, g.name(), g.getSpecs());
        super.visitCogroup(g);
    }
        
    /**
     * Only LOEval.visit() should ever call this method.
     */
    @Override
    public void visitEval(LOEval e) {
        List<EvalSpec> ls = new ArrayList<EvalSpec>();
        ls.add(e.getSpec());
        print(e, e.name(), ls);
        super.visitEval(e);
    }
        
    /**
     * Only LOUnion.visit() should ever call this method.
     */
    @Override
    public void visitUnion(LOUnion u) {
        print(u, u.name());
        super.visitUnion(u);
    }
        
    /**
     * Only LOLoad.visit() should ever call this method.
     */
    @Override
    public void visitLoad(LOLoad l) {
        print(l, l.name());
        super.visitLoad(l);
    }
        
    /**
     * Only LOSort.visit() should ever call this method.
     */
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
    @Override
    public void visitSplit(LOSplit s) {
        print(s, s.name());
        super.visitSplit(s);
    }
        
    /**
     * Only LOStore.visit() should ever call this method.
     */
    @Override
    public void visitStore(LOStore s) {
        print(s, s.name());
        super.visitStore(s);
    }

    private void print(LogicalOperator lo, String name) {
        List<EvalSpec> empty = new ArrayList<EvalSpec>();
        print(lo, name, empty);
    }

    private void print(
            LogicalOperator lo,
            String name,
            List<EvalSpec> specs) {
        mStream.println(name);
        mStream.println("Object id: " + lo.hashCode());
        mStream.print("Inputs: ");
        List<OperatorKey> inputs = lo.getInputs();
        Iterator<OperatorKey> i = inputs.iterator();
        while (i.hasNext()) {
            LogicalOperator input = lo.getOpTable().get(i.next());
            mStream.print(input.hashCode() + " ");
        }
        mStream.println();

        mStream.print("Schema: ");
        printSchema(lo.outputSchema(), 0);
        mStream.println();

        mStream.println("EvalSpecs:");
        //printSpecs(specs);
        Iterator<EvalSpec> j = specs.iterator();
        while (j.hasNext()) {
            j.next().visit(new EvalSpecPrinter(mStream));
        }
    }

    private void printSchema(Schema schema, int pos) {
        if (schema instanceof AtomSchema) {
            String a = schema.getAlias();
            if (a == null) mStream.print("$" + pos);
            else mStream.print(a);
        } else if (schema instanceof TupleSchema) {
            mStream.print("(");
            TupleSchema ts = (TupleSchema)schema;
            int sz = ts.numFields();
            for (int j = 0; j < sz; j++) {
                if (j != 0) mStream.print(", ");
                Schema s = ts.schemaFor(j);
                if (s == null) mStream.print("$" + j);
                else printSchema(s, j);
            }
            mStream.print(")");
        } else {
            throw new AssertionError("Unknown schema type.");
        }
    }
}

        
