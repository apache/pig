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
import org.apache.pig.impl.physicalLayer.IntermedResult;

/**
 * A visitor mechanism printing out the logical plan.
 */
public class LOPrinter extends LOVisitor {

    private PrintStream mStream = null;
    private Schema mInputSchema = null;

    public LOPrinter(PrintStream ps) {
        mStream = ps;
    }

    /**
     * Only LOCogroup.visit() should ever call this method.
     */
    @Override
    public void visitCogroup(LOCogroup g) {
        print(g, "COGROUP", g.getSpecs());
        super.visitCogroup(g);
    }
        
    /**
     * Only LOEval.visit() should ever call this method.
     */
    @Override
    public void visitEval(LOEval e) {
        List<EvalSpec> ls = new ArrayList<EvalSpec>();
        ls.add(e.getSpec());
        print(e, "EVAL", ls);
        super.visitEval(e);
    }
        
    /**
     * Only LOUnion.visit() should ever call this method.
     */
    @Override
    public void visitUnion(LOUnion u) {
        print(u, "UNION");
        super.visitUnion(u);
    }
        
    /**
     * Only LORead.visit() should ever call this method.
     */
    @Override
    public void visitRead(LORead r) {
        print(r, "READ");
        super.visitRead(r);
        IntermedResult ir = r.getReadFrom();
        if (ir != null) {
            mInputSchema = ir.lp.getRoot().outputSchema();
        }
    }
        
    /**
     * Only LOLoad.visit() should ever call this method.
     */
    @Override
    public void visitLoad(LOLoad load) {
        print(load, "LOAD");
        super.visitLoad(load);
    }
        
    /**
     * Only LOSort.visit() should ever call this method.
     */
    @Override
    public void visitSort(LOSort s) {
        List<EvalSpec> ls = new ArrayList<EvalSpec>();
        ls.add(s.getSpec());
        print(s, "SORT");
        super.visitSort(s);
    }
        
    /**
     * Only LOSplit.visit() should ever call this method.
     */
    @Override
    public void visitSplit(LOSplit s) {
        print(s, "SPLIT");
        super.visitSplit(s);
    }
        
    /**
     * Only LOStore.visit() should ever call this method.
     */
    @Override
    public void visitStore(LOStore s) {
        print(s, "STORE");
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
        List<LogicalOperator> inputs = lo.getInputs();
        Iterator<LogicalOperator> i = inputs.iterator();
        while (i.hasNext()) {
            LogicalOperator input = i.next();
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
        // Setup the input schema for the next operator.
        mInputSchema = lo.outputSchema();
    }

    /*
    private void printSpecs(List<EvalSpec> specs) {
        Iterator<EvalSpec> j = specs.iterator();
        while (j.hasNext()) {
            EvalSpec spec = j.next();
            if (spec instanceof FilterSpec) {
                mStream.print("filter");
            } else if (spec instanceof SortDistinctSpec) {
                mStream.print("sort/distinct");
            } else if (spec instanceof GenerateSpec) {
                mStream.print("generate: (");
                printSpecs(((GenerateSpec)(spec)).getSpecs());
                mStream.print(")");
            } else if (spec instanceof BinCondSpec) {
                mStream.print("bincond");
            } else if (spec instanceof MapLookupSpec) {
                mStream.print("maplookup");
            } else if (spec instanceof ConstSpec) {
                mStream.print("const");
            } else if (spec instanceof ProjectSpec) {
                mStream.print("project");
            } else if (spec instanceof StarSpec) {
                mStream.print("star");
            } else if (spec instanceof FuncEvalSpec) {
                mStream.print("funceval");
            } else if (spec instanceof CompositeEvalSpec) {
                mStream.print("composite: (");
                printSpecs(((CompositeEvalSpec)(spec)).getSpecs());
                mStream.print(")");
            } else {
                throw new AssertionError("Unknown EvalSpec type");
            }

            if (mInputSchema != null) {
                mStream.print(" [Schema: ");
                printSchema(spec.getOutputSchemaForPipe(mInputSchema), 0);
                mStream.print("]");
            }
            if (j.hasNext()) mStream.print(", ");
        }
    }
    */

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

        
