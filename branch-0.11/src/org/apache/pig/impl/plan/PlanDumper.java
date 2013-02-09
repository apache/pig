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
import org.apache.pig.impl.util.MultiMap;

/**
 * This class dumps a nested plan to a print stream. It does not walk
 * the graph in any particular fashion it merely iterates over all
 * operators and edges and calls a corresponding dump function. If a
 * node of the plan has nested plans this will be dumped when the
 * node is handled.
 */
public class PlanDumper<E extends Operator, 
                        P extends OperatorPlan<E>, 
                        S extends OperatorPlan<? extends Operator>> {
    
    protected PrintStream ps;
    protected P plan;
    protected boolean isVerbose = true;
  
    public PlanDumper(P plan, PrintStream ps) {
        this.plan = plan;
        this.ps = ps;
    }

    public void setVerbose(boolean verbose) {
        this.isVerbose = verbose;
    }

    public boolean isVerbose() {
        return isVerbose;
    }

    /**
     * This is the public interface. Dump writes the plan and nested
     * plans to the stream.
     */
    public void dump() {
        for (E op: plan) {
            MultiMap<E,S> map = getMultiInputNestedPlans(op);
            if (isVerbose && !map.isEmpty()) {
                dumpMultiInputNestedOperator(op, map);
                continue;
            }

            Collection<S> plans = getMultiOutputNestedPlans(op);
            if (plans.size() > 0) {
                dumpMultiOutputNestedOperator(op, plans);
                continue;
            }
            
            plans = getNestedPlans(op);
            if (isVerbose && plans.size() > 0) {
                dumpNestedOperator(op, plans);
                continue;
            }

            dumpOperator(op);
        }

        for(E op: plan) {
            Collection<E> successors = plan.getSuccessors(op);
            if (successors != null) {
                for (E suc: successors) {
                    dumpEdge(op, suc);
                }
            }
        }
    }

    /**
     * makeDumper is a factory method. Used by subclasses to specify
     * what dumper should handle the nested plan.
     * @param plan Plan that the new dumper should handle
     * @return the dumper for plan
     */
    @SuppressWarnings("unchecked")
    protected PlanDumper makeDumper(S plan, PrintStream ps) {
        return new PlanDumper(plan, ps);
    }

    /**
     * Will be called to dump a simple operator
     * @param op the operator to be dumped
     */
    protected void dumpOperator(E op) {
        ps.println(op.name().replace(" ","_"));
    }

    /**
     * Will be called when an operator has nested plans, which are
     * connected to one of the multiple inputs.
     * @param op the nested operator
     * @param plans a map of input operator to connected nested plan
     */
    protected void dumpMultiInputNestedOperator(E op, MultiMap<E,S> plans) {
        dumpOperator(op);
        for (E aop: plans.keySet()) {
            for (S plan: plans.get(aop)) {
                PlanDumper dumper = makeDumper(plan, ps);
                dumper.dump();
            }
        }
    }

    /**
     * Will be called for nested operators, where the plans represent
     * how the output of the operator is processed. 
     * @param op the nested operator
     * @param plans a collection of sub plans.
     */
    protected void dumpMultiOutputNestedOperator(E op, Collection<S> plans) {
        dumpOperator(op);
        for (S plan: plans) {
            PlanDumper  dumper = makeDumper(plan, ps);
            dumper.dump();
            for (Operator p: plan.getRoots()) {
                dumpEdge(op, p);
            }
        }
    }

    /**
     * Will be called for nested operators. The operators are not
     * specifically connected to any input or output operators of E
     * @param op the nested operator
     * @param plans a collection of sub plans.
     */
    protected void dumpNestedOperator(E op, Collection<S> plans) {
        dumpOperator(op);
        for (S plan: plans) {
            PlanDumper  dumper = makeDumper(plan, ps);
            dumper.dump();
        }
    }

    /**
     * Will be called to dump the edges of the plan. Each edge results
     * in one call.
     * @param op tail of the edge
     * @param suc head of the edge
     */
    protected void dumpEdge(Operator op, Operator suc) {
        ps.println(op.name()+" -> "+suc.name());
    }

    /**
     * Used to determine if an operator has nested plans, which are
     * connected to specific input operators.
     * @param op operator
     * @return Map describing the input to nested plan relationship.
     */
    protected MultiMap<E, S> getMultiInputNestedPlans(E op) {
        return new MultiMap<E, S>();
    }

    /**
     * Used to determine if an operator has nested output plans
     *
     * @param op operator
     * @return Map describing the input to nested plan relationship.
     */
    protected Collection<S> getMultiOutputNestedPlans(E op) {
        return new LinkedList<S>();
    }

    /**
     * Used to determine if an operator has nested plans (without
     * connections to in- or output operators.
     * @param op operator
     * @return Collection of nested plans.
     */
    protected Collection<S> getNestedPlans(E op) {
        return new LinkedList<S>();
    }

    /**
     * Helper function to print a string array.
     * @param sep Separator
     * @param strings Array to print
     */
    protected void join(String sep, String[] strings) {
        if (strings == null) {
            return;
        }
        
        for (int i = 0; i < strings.length; ++i) {
            if (i != 0) {
                ps.print(sep);
            }
            ps.print(strings[i]);
        }
    }
}
