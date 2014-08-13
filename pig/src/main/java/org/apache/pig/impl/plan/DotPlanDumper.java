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
package org.apache.pig.impl.plan;

import java.io.PrintStream;
import java.util.LinkedList;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;

import org.apache.pig.impl.util.MultiMap;

/**
 * This class puts everything that is needed to dump a plan in a
 * format readable by graphviz's dot algorithm. Out of the box it does
 * not print any nested plans.
 */
public class DotPlanDumper<E extends Operator, P extends OperatorPlan<E>, 
                           N extends Operator, S extends OperatorPlan<N>> 
    extends PlanDumper<E, P, S> {

    protected Set<Operator> mSubgraphs;
    protected Set<Operator> mMultiInputSubgraphs;    
    protected Set<Operator> mMultiOutputSubgraphs;
    private boolean isSubGraph = false;
  
    public DotPlanDumper(P plan, PrintStream ps) {
        this(plan, ps, false, new HashSet<Operator>(), new HashSet<Operator>(),
             new HashSet<Operator>());
    }

    protected DotPlanDumper(P plan, PrintStream ps, boolean isSubGraph, 
                            Set<Operator> mSubgraphs, 
                            Set<Operator> mMultiInputSubgraphs,
                            Set<Operator> mMultiOutputSubgraphs) {
        super(plan, ps);
        this.isSubGraph = isSubGraph;
        this.mSubgraphs = mSubgraphs;
        this.mMultiInputSubgraphs = mMultiInputSubgraphs;
        this.mMultiOutputSubgraphs = mMultiOutputSubgraphs;
    }

    @Override
    public void dump() {
        if (!isSubGraph) {
            ps.println("digraph plan {");
            ps.println("compound=true;");
            ps.println("node [shape=rect];");
        }
        super.dump();
        if (!isSubGraph) {
            ps.println("}");
        }
    }

    @Override
    protected void dumpMultiInputNestedOperator(E op, MultiMap<E, S> plans) {
        dumpInvisibleOutput(op);

        ps.print("subgraph ");
        ps.print(getClusterID(op));
        ps.println(" {");
        join("; ", getAttributes(op));
        ps.println("labelloc=b;");
        
        mMultiInputSubgraphs.add(op);

        for (E o: plans.keySet()) {
            ps.print("subgraph ");
            ps.print(getClusterID(op, o));
            ps.println(" {");
            ps.println("label=\"\";");
            dumpInvisibleInput(op, o);
            for (S plan : plans.get(o)) {
                PlanDumper dumper = makeDumper(plan, ps);
                dumper.dump();
                connectInvisibleInput(op, o, plan);
            }
            ps.println("};");
        }
        ps.println("};");
        
        for (E o: plans.keySet()) {
            for (S plan: plans.get(o)) {
                connectInvisibleOutput(op, plan);
            }
        }
    }

    @Override 
    protected void dumpMultiOutputNestedOperator(E op, Collection<S> plans) {
        super.dumpMultiOutputNestedOperator(op, plans);

        mMultiOutputSubgraphs.add(op);
        
        dumpInvisibleOutput(op);
        for (S plan: plans) {
            connectInvisibleOutput(op, plan);
        }
    }

    @Override
    protected void dumpNestedOperator(E op, Collection<S> plans) {
        dumpInvisibleOperators(op);
        ps.print("subgraph ");
        ps.print(getClusterID(op));
        ps.println(" {");
        join("; ", getAttributes(op));
        ps.println("labelloc=b;");

        mSubgraphs.add(op);
        
        for (S plan: plans) {
            PlanDumper dumper = makeDumper(plan, ps);
            dumper.dump();
            connectInvisibleInput(op, plan);
        }
        ps.println("};");

        for (S plan: plans) {
            connectInvisibleOutput(op, plan);
        }
    }

    @Override
    protected void dumpOperator(E op) {
        ps.print(getID(op));
        ps.print(" [");
        join(", ", getAttributes(op));
        ps.println("];");
    }

    @Override
    protected void dumpEdge(Operator op, Operator suc) {
        String in = getID(op);
        String out = getID(suc);
        String attributes = "";

        if (mMultiInputSubgraphs.contains(op) 
            || mSubgraphs.contains(op) 
            || mMultiOutputSubgraphs.contains(op)) {
            in = getSubgraphID(op, false);
        }

        ps.print(in);

        if (mMultiInputSubgraphs.contains(suc)) {
            out = getSubgraphID(suc, op, true);
            attributes = " [lhead="+getClusterID(suc,op)+"]";
        }

        if (mSubgraphs.contains(suc)) {
            out = getSubgraphID(suc, true);
            attributes = " [lhead="+getClusterID(suc)+"]";
        }
        
        ps.print(" -> ");
        ps.print(out);
        ps.println(attributes);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected PlanDumper makeDumper(S plan, PrintStream ps) {
        return new DotPlanDumper(plan, ps, true, 
                                 mSubgraphs, mMultiInputSubgraphs, 
                                 mMultiOutputSubgraphs);
    }

    /**
     * Used to generate the label for an operator.
     * @param op operator to dump
     */
    protected String getName(E op) {
        return op.name();
    }
    
    /**
     * Used to generate the the attributes of a node
     * @param op operator
     */
    protected String[] getAttributes(E op) {
        String[] attributes = new String[1];
        attributes[0] =  "label=\""+getName(op)+"\"";
        return attributes;
    }


    private void connectInvisibleInput(E op1, E op2, S plan) {
        String in = getSubgraphID(op1, op2, true);
        
        for (N l: plan.getRoots()) {
            dumpInvisibleEdge(in, getID(l));
        }
    }

    private void connectInvisibleInput(E op, S plan) {
        String in = getSubgraphID(op, true);

        for (N l: plan.getRoots()) {
            String out;
            if (mSubgraphs.contains(l) || mMultiInputSubgraphs.contains(l)) {
                out = getSubgraphID(l, true);
            } else {
                out = getID(l);
            }

            dumpInvisibleEdge(in, out);
        }
    }

    private void connectInvisibleOutput(E op, 
                                        OperatorPlan<? extends Operator> plan) {
        String out = getSubgraphID(op, false);

        for (Operator l: plan.getLeaves()) {
            String in;
            if (mSubgraphs.contains(l) 
                || mMultiInputSubgraphs.contains(l)
                || mMultiOutputSubgraphs.contains(l)) {
                in = getSubgraphID(l, false);
            } else {
                in = getID(l);
            }

            dumpInvisibleEdge(in, out);
        }
    }

    private void connectInvisible(E op, S plan) {
        connectInvisibleInput(op, plan);
        connectInvisibleOutput(op, plan);
    }        

    private void dumpInvisibleInput(E op1, E op2) {
        ps.print(getSubgraphID(op1, op2, true));
        ps.print(" ");
        ps.print(getInvisibleAttributes(op1));
        ps.println(";");
    }
    
    private void dumpInvisibleInput(E op) {
        ps.print(getSubgraphID(op, true));
        ps.print(" ");
        ps.print(getInvisibleAttributes(op));
        ps.println(";");
    }

    private void dumpInvisibleOutput(E op) {
        ps.print(getSubgraphID(op, false));
        ps.print(" ");
        ps.print(getInvisibleAttributes(op));
        ps.println(";");
    }

    protected void dumpInvisibleOperators(E op) {
        dumpInvisibleInput(op);
        dumpInvisibleOutput(op);
    }

    private String getClusterID(Operator op1, Operator op2) {
        return getClusterID(op1)+"_"+getID(op2);
    }

    private String getClusterID(Operator op) {
        return "cluster_"+getID(op);
    }

    private String getSubgraphID(Operator op1, Operator op2, boolean in) {
        String id = "s"+getID(op1)+"_"+getID(op2);
        if (in) {
            id += "_in";
        }
        else {
            id += "_out";
        }
        return id;
    }

    private String getSubgraphID(Operator op, boolean in) {
        String id =  "s"+getID(op);
        if (in) {
            id += "_in";
        }
        else {
            id += "_out";
        }
        return id;
    }

    private String getID(Operator op) {
        return ""+Math.abs(op.hashCode());
    }

    private String getInvisibleAttributes(Operator op) {
        return "[label=\"\", style=invis, height=0, width=0]";
    }
    
    private void dumpInvisibleEdge(String op, String suc) {
        ps.print(op);
        ps.print(" -> ");
        ps.print(suc);
        ps.println(" [style=invis];");
    }
}
