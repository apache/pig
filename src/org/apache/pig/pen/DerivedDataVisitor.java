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

package org.apache.pig.pen;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PORead;
import org.apache.pig.backend.local.executionengine.physicalLayer.LocalLogToPhyTranslationVisitor;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOCross;
import org.apache.pig.impl.logicalLayer.LODistinct;
import org.apache.pig.impl.logicalLayer.LOFilter;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LOLimit;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOUnion;
import org.apache.pig.impl.logicalLayer.LOVisitor;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.PlanSetter;
import org.apache.pig.impl.logicalLayer.validators.LogicalPlanValidationExecutor;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.pen.util.DependencyOrderLimitedWalker;
import org.apache.pig.pen.util.LineageTracer;


//This class is used to pass data through the entire plan and save the intermediates results.
public class DerivedDataVisitor extends LOVisitor {

    Map<LogicalOperator, DataBag> derivedData = new HashMap<LogicalOperator, DataBag>();
    PigContext pc;
    PhysicalPlan physPlan = null;
    Map<LOLoad, DataBag> baseData = null;

    Map<LogicalOperator, PhysicalOperator> LogToPhyMap = null;
    Log log = LogFactory.getLog(getClass());

    Map<LogicalOperator, Collection<IdentityHashSet<Tuple>>> OpToEqClasses = null;
    Collection<IdentityHashSet<Tuple>> EqClasses = null;

    LineageTracer lineage = new LineageTracer();

    public DerivedDataVisitor(LogicalPlan plan, PigContext pc,
            Map<LOLoad, DataBag> baseData,
            Map<LogicalOperator, PhysicalOperator> logToPhyMap,
            PhysicalPlan physPlan) {
        super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(
                plan));
        this.pc = pc;
        this.baseData = baseData;

        OpToEqClasses = new HashMap<LogicalOperator, Collection<IdentityHashSet<Tuple>>>();
        EqClasses = new LinkedList<IdentityHashSet<Tuple>>();

        LogToPhyMap = logToPhyMap;
        this.physPlan = physPlan;
        // if(logToPhyMap == null)
        // compilePlan(plan);
        // else
        // LogToPhyMap = logToPhyMap;

    }

    public DerivedDataVisitor(LogicalOperator op, PigContext pc,
            Map<LOLoad, DataBag> baseData,
            Map<LogicalOperator, PhysicalOperator> logToPhyMap,
            PhysicalPlan physPlan) {
        super(op.getPlan(),
                new DependencyOrderLimitedWalker<LogicalOperator, LogicalPlan>(
                        op, op.getPlan()));
        this.pc = pc;
        this.baseData = baseData;

        OpToEqClasses = new HashMap<LogicalOperator, Collection<IdentityHashSet<Tuple>>>();
        EqClasses = new LinkedList<IdentityHashSet<Tuple>>();

        LogToPhyMap = logToPhyMap;
        this.physPlan = physPlan;
        // if(logToPhyMap == null)
        // compilePlan(op.getPlan());
        // else
        // LogToPhyMap = logToPhyMap;
    }

    public void setOperatorToEvaluate(LogicalOperator op) {
        mCurrentWalker = new DependencyOrderLimitedWalker<LogicalOperator, LogicalPlan>(
                op, op.getPlan());
    }

    @Override
    protected void visit(LOCogroup cg) throws VisitorException {
        // evaluateOperator(cg);
        // there is a slightly different code path for cogroup because of the
        // local rearranges
        PhysicalOperator physOp = LogToPhyMap.get(cg);
        Random r = new Random();
        // get the list of original inputs

        // List<PhysicalOperator> inputs = physOp.getInputs();
        List<PhysicalOperator> inputs = new ArrayList<PhysicalOperator>();
        PhysicalPlan phy = new PhysicalPlan();
        phy.add(physOp);

        // for(PhysicalOperator input : physOp.getInputs()) {
        for (PhysicalOperator input : physPlan.getPredecessors(physOp)) {
            inputs.add(input.getInputs().get(0));
            // input.setInputs(null);
            phy.add(input);
            try {
                phy.connect(input, physOp);
            } catch (PlanException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                log.error("Error connecting " + input.name() + " to "
                        + physOp.name());
            }
        }

        physOp.setLineageTracer(lineage);

        // replace the original inputs by POReads
        for (int i = 0; i < inputs.size(); i++) {
            DataBag bag = derivedData.get(cg.getInputs().get(i));
            PORead por = new PORead(new OperatorKey("", r.nextLong()), bag);
            phy.add(por);
            try {
                phy.connect(por, physOp.getInputs().get(i));
            } catch (PlanException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                log.error("Error connecting " + por.name() + " to "
                        + physOp.name());
            }
        }

        DataBag output = BagFactory.getInstance().newDefaultBag();
        Tuple t = null;
        try {
            for (Result res = physOp.getNext(t); res.returnStatus != POStatus.STATUS_EOP; res = physOp
                    .getNext(t)) {
                output.add((Tuple) res.result);
            }
        } catch (ExecException e) {
            log.error("Error evaluating operator : " + physOp.name());
        }
        derivedData.put(cg, output);

        try {
            Collection<IdentityHashSet<Tuple>> eq = EquivalenceClasses
                    .GetEquivalenceClasses(cg, derivedData);
            EqClasses.addAll(eq);
            OpToEqClasses.put(cg, eq);
        } catch (ExecException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            log
                    .error("Error updating equivalence classes while evaluating operators. \n"
                            + e.getMessage());
        }

        // re-attach the original operators
        // for(int i = 0; i < inputs.size(); i++) {
        // try {
        // physPlan.connect(inputs.get(i), physOp.getInputs().get(i));
        //		
        // } catch (PlanException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // log.error("Error connecting " + inputs.get(i).name() + " to " +
        // physOp.getInputs().get(i).name());
        // }
        // }
        physOp.setLineageTracer(null);
    }

    @Override
    protected void visit(LOCross cs) throws VisitorException {
        evaluateOperator(cs);
    }

    @Override
    protected void visit(LODistinct dt) throws VisitorException {
        evaluateOperator(dt);
    }

    @Override
    protected void visit(LOFilter filter) throws VisitorException {
        evaluateOperator(filter);
    }

    @Override
    protected void visit(LOForEach forEach) throws VisitorException {
        evaluateOperator(forEach);
    }

    @Override
    protected void visit(LOLoad load) throws VisitorException {
        derivedData.put(load, baseData.get(load));

        Collection<IdentityHashSet<Tuple>> eq = EquivalenceClasses
                .GetEquivalenceClasses(load, derivedData);
        EqClasses.addAll(eq);
        OpToEqClasses.put(load, eq);

        for (Iterator<Tuple> it = derivedData.get(load).iterator(); it
                .hasNext();) {
            lineage.insert(it.next());
        }

    }

    @Override
    protected void visit(LOSplit split) throws VisitorException {
        evaluateOperator(split);
    }

    @Override
    protected void visit(LOStore store) throws VisitorException {
        derivedData.put(store, derivedData.get(store.getPlan().getPredecessors(
                store).get(0)));
    }

    @Override
    protected void visit(LOUnion u) throws VisitorException {
        evaluateOperator(u);
    }

    @Override
    protected void visit(LOLimit l) throws VisitorException {
        evaluateOperator(l);
    }
    
    @Override
    protected void visit(LOSort sort) throws VisitorException {
        evaluateOperator(sort);
    }

    // private void compilePlan(LogicalPlan plan) {
    //	
    // plan = refineLogicalPlan(plan);
    //	
    // LocalLogToPhyTranslationVisitor visitor = new
    // LocalLogToPhyTranslationVisitor(plan);
    // visitor.setPigContext(pc);
    // try {
    // visitor.visit();
    // } catch (VisitorException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // log.error("Error visiting the logical plan in ExampleGenerator");
    // }
    // physPlan = visitor.getPhysicalPlan();
    // LogToPhyMap = visitor.getLogToPhyMap();
    // }
    //    
    // private LogicalPlan refineLogicalPlan(LogicalPlan plan) {
    // PlanSetter ps = new PlanSetter(plan);
    // try {
    // ps.visit();
    //	    
    // } catch (VisitorException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    //        
    // // run through validator
    // CompilationMessageCollector collector = new CompilationMessageCollector()
    // ;
    // FrontendException caught = null;
    // try {
    // LogicalPlanValidationExecutor validator =
    // new LogicalPlanValidationExecutor(plan, pc);
    // validator.validate(plan, collector);
    // } catch (FrontendException fe) {
    // // Need to go through and see what the collector has in it. But
    // // remember what we've caught so we can wrap it into what we
    // // throw.
    // caught = fe;
    // }
    //        
    //        
    // return plan;
    //
    // }

    private void evaluateOperator(LogicalOperator op) {
        PhysicalOperator physOp = LogToPhyMap.get(op);
        Random r = new Random();
        // get the list of original inputs

        List<PhysicalOperator> inputs = physOp.getInputs();
        physOp.setInputs(null);
        physOp.setLineageTracer(lineage);
        PhysicalPlan phy = new PhysicalPlan();
        phy.add(physOp);

        // replace the original inputs by POReads
        for (LogicalOperator l : op.getPlan().getPredecessors(op)) {
            DataBag bag = derivedData.get(l);
            PORead por = new PORead(new OperatorKey("", r.nextLong()), bag);
            phy.add(por);
            try {
                phy.connect(por, physOp);
            } catch (PlanException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                log.error("Error connecting " + por.name() + " to "
                        + physOp.name());
            }
        }

        DataBag output = BagFactory.getInstance().newDefaultBag();
        Tuple t = null;
        try {
            for (Result res = physOp.getNext(t); res.returnStatus != POStatus.STATUS_EOP; res = physOp
                    .getNext(t)) {
                output.add((Tuple) res.result);
            }
        } catch (ExecException e) {
            log.error("Error evaluating operator : " + physOp.name());
        }
        derivedData.put(op, output);

        try {
            Collection<IdentityHashSet<Tuple>> eq = EquivalenceClasses
                    .GetEquivalenceClasses(op, derivedData);
            EqClasses.addAll(eq);
            OpToEqClasses.put(op, eq);
        } catch (ExecException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            log
                    .error("Error updating equivalence classes while evaluating operators. \n"
                            + e.getMessage());
        }

        // re-attach the original operators
        physOp.setInputs(inputs);
        physOp.setLineageTracer(null);
    }

    public DataBag evaluateIsolatedOperator(LOCogroup op,
            List<DataBag> inputBags) {
        if (op.getPlan().getPredecessors(op).size() > inputBags.size())
            return null;

        int count = 0;
        for (LogicalOperator inputs : op.getPlan().getPredecessors(op)) {
            derivedData.put(inputs, inputBags.get(count++));
        }

        return evaluateIsolatedOperator(op);

    }

    public DataBag evaluateIsolatedOperator(LOCogroup op) {
        // return null if the inputs are not already evaluated
        for (LogicalOperator in : op.getPlan().getPredecessors(op)) {
            if (derivedData.get(in) == null)
                return null;
        }

        LineageTracer oldLineage = this.lineage;
        this.lineage = new LineageTracer();

        PhysicalOperator physOp = LogToPhyMap.get(op);
        Random r = new Random();
        // get the list of original inputs
        // List<PhysicalOperator> inputs = physOp.getInputs();
        List<PhysicalOperator> inputs = new ArrayList<PhysicalOperator>();
        PhysicalPlan phy = new PhysicalPlan();
        phy.add(physOp);

        for (PhysicalOperator input : physOp.getInputs()) {
            inputs.add(input.getInputs().get(0));
            input.setInputs(null);
            phy.add(input);
            try {
                phy.connect(input, physOp);
            } catch (PlanException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                log.error("Error connecting " + input.name() + " to "
                        + physOp.name());
            }
        }
        physOp.setLineageTracer(lineage);

        physOp.setLineageTracer(null);

        // replace the original inputs by POReads
        for (int i = 0; i < inputs.size(); i++) {
            DataBag bag = derivedData.get(((LOCogroup) op).getInputs().get(i));
            PORead por = new PORead(new OperatorKey("", r.nextLong()), bag);
            phy.add(por);
            try {
                phy.connect(por, physOp.getInputs().get(i));
            } catch (PlanException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                log.error("Error connecting " + por.name() + " to "
                        + physOp.name());
            }
        }

        // replace the original inputs by POReads
        // for(LogicalOperator l : op.getPlan().getPredecessors(op)) {
        // DataBag bag = derivedData.get(l);
        // PORead por = new PORead(new OperatorKey("", r.nextLong()), bag);
        // phy.add(por);
        // try {
        // phy.connect(por, physOp);
        // } catch (PlanException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // log.error("Error connecting " + por.name() + " to " + physOp.name());
        // }
        // }

        DataBag output = BagFactory.getInstance().newDefaultBag();
        Tuple t = null;
        try {
            for (Result res = physOp.getNext(t); res.returnStatus != POStatus.STATUS_EOP; res = physOp
                    .getNext(t)) {
                output.add((Tuple) res.result);
            }
        } catch (ExecException e) {
            log.error("Error evaluating operator : " + physOp.name());
        }

        this.lineage = oldLineage;

        physOp.setInputs(inputs);
        physOp.setLineageTracer(null);

        return output;
    }

}
