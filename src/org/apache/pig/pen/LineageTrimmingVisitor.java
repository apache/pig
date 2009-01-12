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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOCross;
import org.apache.pig.impl.logicalLayer.LODistinct;
import org.apache.pig.impl.logicalLayer.LOFilter;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LOLimit;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOUnion;
import org.apache.pig.impl.logicalLayer.LOVisitor;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.pen.util.LineageTracer;
import org.apache.pig.pen.util.MetricEvaluation;
import org.apache.pig.pen.util.PreOrderDepthFirstWalker;

public class LineageTrimmingVisitor extends LOVisitor {

    LogicalPlan plan = null;
    Map<LOLoad, DataBag> baseData = new HashMap<LOLoad, DataBag>();
    Map<LogicalOperator, PhysicalOperator> LogToPhyMap = null;
    PhysicalPlan physPlan = null;
    double completeness = 100.0;
    Log log = LogFactory.getLog(getClass());

    Map<LogicalOperator, Map<IdentityHashSet<Tuple>, Integer>> AffinityGroups = new HashMap<LogicalOperator, Map<IdentityHashSet<Tuple>, Integer>>();
    Map<LogicalOperator, LineageTracer> Lineage = new HashMap<LogicalOperator, LineageTracer>();

    boolean continueTrimming;
    PigContext pc;

    public LineageTrimmingVisitor(LogicalPlan plan,
            Map<LOLoad, DataBag> baseData,
            Map<LogicalOperator, PhysicalOperator> LogToPhyMap,
            PhysicalPlan physPlan, PigContext pc) {
        super(plan, new PreOrderDepthFirstWalker<LogicalOperator, LogicalPlan>(
                plan));
        // this.baseData.putAll(baseData);
        this.baseData = baseData;
        this.plan = plan;
        this.LogToPhyMap = LogToPhyMap;
        this.pc = pc;
        this.physPlan = physPlan;
        init();
    }

    public void init() {

        DerivedDataVisitor visitor = new DerivedDataVisitor(plan, pc, baseData,
                LogToPhyMap, physPlan);
        try {
            visitor.visit();
        } catch (VisitorException e) {
            log.error(e.getMessage());
        }

        LineageTracer lineage = visitor.lineage;
        Lineage.put(plan.getLeaves().get(0), lineage);
        Map<LogicalOperator, Collection<IdentityHashSet<Tuple>>> OpToEqClasses = visitor.OpToEqClasses;
        Collection<IdentityHashSet<Tuple>> EqClasses = visitor.EqClasses;
        Map<IdentityHashSet<Tuple>, Integer> affinityGroup = new HashMap<IdentityHashSet<Tuple>, Integer>();
        for (IdentityHashSet<Tuple> set : EqClasses) {
            affinityGroup.put(set, 1);
        }
        AffinityGroups.put(plan.getLeaves().get(0), affinityGroup);
        completeness = MetricEvaluation.getCompleteness(null,
                visitor.derivedData, OpToEqClasses, true);
        LogToPhyMap = visitor.LogToPhyMap;
        continueTrimming = true;

    }

    @Override
    protected void visit(LOCogroup cg) throws VisitorException {
        if (continueTrimming) {
            Map<IdentityHashSet<Tuple>, Integer> affinityGroups = null;

            continueTrimming = checkCompleteness(cg);
            
            DerivedDataVisitor visitor = null;
            LineageTracer lineage = null;
            // create affinity groups
            if (cg.getInputs().size() == 1) {
                affinityGroups = new HashMap<IdentityHashSet<Tuple>, Integer>();
                LogicalOperator childOp = cg.getInputs().get(0);
                visitor = new DerivedDataVisitor(childOp, null, baseData,
                        LogToPhyMap, physPlan);
                try {
                    visitor.visit();
                } catch (VisitorException e) {
                    log.error(e.getMessage());
                }

                lineage = visitor.lineage;

                DataBag bag = visitor.evaluateIsolatedOperator(cg);
                for (Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
                    DataBag field;
                    try {
                        field = (DataBag) it.next().get(1);
                    } catch (ExecException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        log.error(e.getMessage());
                        throw new VisitorException(
                                "Error trimming operator COGROUP operator "
                                        + cg.getAlias()
                                        + "in example generator");
                    }
                    IdentityHashSet<Tuple> set = new IdentityHashSet<Tuple>();
                    affinityGroups.put(set, 2);
                    for (Iterator<Tuple> it1 = field.iterator(); it1.hasNext();) {
                        set.add(it1.next());
                    }
                }

                // add the equivalence classes obtained from derived data
                // creation
                for (IdentityHashSet<Tuple> set : visitor.EqClasses) {
                    affinityGroups.put(set, 1);
                }
                AffinityGroups.put(cg.getInputs().get(0), affinityGroups);
                Lineage.put(cg.getInputs().get(0), lineage);

            } else {
                List<DataBag> inputs = new LinkedList<DataBag>();
                visitor = new DerivedDataVisitor(cg, null, baseData,
                        LogToPhyMap, physPlan);
                affinityGroups = new HashMap<IdentityHashSet<Tuple>, Integer>();
                for (int i = 0; i < cg.getInputs().size(); i++) {
                    // affinityGroups = new HashMap<IdentityHashSet<Tuple>,
                    // Integer>();
                    LogicalOperator childOp = cg.getInputs().get(i);
                    // visitor = new DerivedDataVisitor(cg.getInputs().get(i),
                    // null, baseData, LogToPhyMap, physPlan);
                    visitor.setOperatorToEvaluate(childOp);
                    try {
                        visitor.visit();
                    } catch (VisitorException e) {
                        log.error(e.getMessage());
                    }
                    // Lineage.put(childOp, visitor.lineage);
                    inputs.add(visitor.derivedData.get(childOp));

                    for (IdentityHashSet<Tuple> set : visitor.EqClasses)
                        affinityGroups.put(set, 1);

                    // AffinityGroups.put(cg.getInputs().get(i),
                    // affinityGroups);
                }
                for (LogicalOperator input : cg.getInputs()) {
                    Lineage.put(input, visitor.lineage);
                    AffinityGroups.put(input, affinityGroups);
                }

                visitor = new DerivedDataVisitor(cg, null, baseData,
                        LogToPhyMap, physPlan);
                DataBag output = visitor.evaluateIsolatedOperator(cg, inputs);

                for (int i = 1; i <= cg.getInputs().size(); i++) {
                    affinityGroups = new HashMap<IdentityHashSet<Tuple>, Integer>();
                    for (Iterator<Tuple> it = output.iterator(); it.hasNext();) {
                        DataBag bag = null;
                        try {
                            bag = (DataBag) it.next().get(i);
                        } catch (ExecException e) {
                            // TODO Auto-generated catch block
                            log.error(e.getMessage());
                        }
                        IdentityHashSet<Tuple> set = new IdentityHashSet<Tuple>();
                        affinityGroups.put(set, 1);
                        for (Iterator<Tuple> it1 = bag.iterator(); it1
                                .hasNext();) {
                            set.add(it1.next());
                        }
                    }
                    AffinityGroups.get(cg.getInputs().get(i - 1)).putAll(
                            affinityGroups);

                }
                AffinityGroups = AffinityGroups;
            }
        }
    }

    @Override
    protected void visit(LOCross cs) throws VisitorException {
        if(continueTrimming)
            processOperator(cs);

    }

    @Override
    protected void visit(LODistinct dt) throws VisitorException {
        if(continueTrimming)
            processOperator(dt);

    }

    @Override
    protected void visit(LOFilter filter) throws VisitorException {
        if (continueTrimming)
            processOperator(filter);
    }

    @Override
    protected void visit(LOForEach forEach) throws VisitorException {
        if (continueTrimming)
            processOperator(forEach);
    }

    @Override
    protected void visit(LOLimit limOp) throws VisitorException {
        if(continueTrimming)
            processOperator(limOp);

    }

    @Override
    protected void visit(LOLoad load) throws VisitorException {
        if (continueTrimming)
            processOperator(load);
    }

    @Override
    protected void visit(LOSort s) throws VisitorException {
        if(continueTrimming)
            processOperator(s);

    }

    @Override
    protected void visit(LOSplit split) throws VisitorException {
        if(continueTrimming)
            processOperator(split);

    }

    @Override
    protected void visit(LOUnion u) throws VisitorException {
        if(continueTrimming)
            processOperator(u);

    }

    private Map<LOLoad, DataBag> PruneBaseDataConstrainedCoverage(
            Map<LOLoad, DataBag> baseData, DataBag rootOutput,
            LineageTracer lineage,
            Map<IdentityHashSet<Tuple>, Integer> equivalenceClasses) {

        IdentityHashMap<Tuple, Collection<Tuple>> membershipMap = lineage
                .getMembershipMap();
        IdentityHashMap<Tuple, Double> lineageGroupWeights = lineage
                .getWeightedCounts(2f, 1);

        // compute a mapping from lineage group to the set of equivalence
        // classes covered by it
        // IdentityHashMap<Tuple, Set<Integer>> lineageGroupToEquivClasses = new
        // IdentityHashMap<Tuple, Set<Integer>>();
        IdentityHashMap<Tuple, Set<IdentityHashSet<Tuple>>> lineageGroupToEquivClasses = new IdentityHashMap<Tuple, Set<IdentityHashSet<Tuple>>>();
        int equivClassId = 0;
        for (IdentityHashSet<Tuple> equivClass : equivalenceClasses.keySet()) {
            for (Tuple t : equivClass) {
                Tuple lineageGroup = lineage.getRepresentative(t);
                // Set<Integer> entry =
                // lineageGroupToEquivClasses.get(lineageGroup);
                Set<IdentityHashSet<Tuple>> entry = lineageGroupToEquivClasses
                        .get(lineageGroup);
                if (entry == null) {
                    // entry = new HashSet<Integer>();
                    entry = new HashSet<IdentityHashSet<Tuple>>();
                    lineageGroupToEquivClasses.put(lineageGroup, entry);
                }
                // entry.add(equivClassId);
                entry.add(equivClass);
            }

            equivClassId++;
        }

        // select lineage groups such that we cover all equivalence classes
        IdentityHashSet<Tuple> selectedLineageGroups = new IdentityHashSet<Tuple>();
        while (!lineageGroupToEquivClasses.isEmpty()) {
            // greedily find the lineage group with the best "score", where
            // score = # equiv classes covered / group weight
            double bestScore = -1;
            Tuple bestLineageGroup = null;
            Set<IdentityHashSet<Tuple>> bestEquivClassesCovered = null;
            for (Tuple lineageGroup : lineageGroupToEquivClasses.keySet()) {
                double weight = lineageGroupWeights.get(lineageGroup);

                Set<IdentityHashSet<Tuple>> equivClassesCovered = lineageGroupToEquivClasses
                        .get(lineageGroup);
                int numEquivClassesCovered = equivClassesCovered.size();
                double score = ((double) numEquivClassesCovered)
                        / ((double) weight);

                if (score > bestScore) {

                    if (selectedLineageGroups.contains(lineageGroup)) {
                        bestLineageGroup = lineageGroup;
                        bestEquivClassesCovered = equivClassesCovered;
                        continue;
                    }

                    bestScore = score;
                    bestLineageGroup = lineageGroup;
                    bestEquivClassesCovered = equivClassesCovered;
                }
            }
            // add the best-scoring lineage group to the set of ones we plan to
            // retain
            selectedLineageGroups.add(bestLineageGroup);

            // make copy of bestEquivClassesCovered (or else the code that
            // follows won't work correctly, because removing from the reference
            // set)
            Set<IdentityHashSet<Tuple>> toCopy = bestEquivClassesCovered;
            bestEquivClassesCovered = new HashSet<IdentityHashSet<Tuple>>();
            bestEquivClassesCovered.addAll(toCopy);

            // remove the classes we've now covered
            Collection<Tuple> toRemove = new LinkedList<Tuple>();
            for (Tuple lineageGroup : lineageGroupToEquivClasses.keySet()) {

                Set<IdentityHashSet<Tuple>> equivClasses = lineageGroupToEquivClasses
                        .get(lineageGroup);

                for (Iterator<IdentityHashSet<Tuple>> it = equivClasses
                        .iterator(); it.hasNext();) {
                    IdentityHashSet<Tuple> equivClass = it.next();
                    if (bestEquivClassesCovered.contains(equivClass)) {
                        if ((equivalenceClasses.get(equivClass) - 1) <= 0) {
                            // equivClasses.remove(equivClass);
                            it.remove();
                        }
                    }
                }
                if (equivClasses.size() == 0)
                    toRemove.add(lineageGroup);

            }
            for (Tuple removeMe : toRemove)
                lineageGroupToEquivClasses.remove(removeMe);

            for (IdentityHashSet<Tuple> equivClass : bestEquivClassesCovered) {
                equivalenceClasses.put(equivClass, equivalenceClasses
                        .get(equivClass) - 1);
            }
        }

        // revise baseData to only contain the tuples that are part of
        // selectedLineageGroups
        IdentityHashSet<Tuple> tuplesToRetain = new IdentityHashSet<Tuple>();
        for (Tuple lineageGroup : selectedLineageGroups) {
            Collection<Tuple> members = membershipMap.get(lineageGroup);
            for (Tuple t : members)
                tuplesToRetain.add(t);
        }
        Map<LOLoad, DataBag> newBaseData = new HashMap<LOLoad, DataBag>();
        for (LOLoad loadOp : baseData.keySet()) {
            DataBag data = baseData.get(loadOp);
            // DataBag newData = new DataBag();
            DataBag newData = BagFactory.getInstance().newDefaultBag();
            for (Iterator<Tuple> it = data.iterator(); it.hasNext();) {
                Tuple t = it.next();
                if (tuplesToRetain.contains(t))
                    newData.add(t);
            }
            newBaseData.put(loadOp, newData);
        }

        return newBaseData;
    }

    private void processOperator(LogicalOperator op) {
        continueTrimming = checkCompleteness(op);

        if (op instanceof LOLoad || continueTrimming == false)
            return;

        LogicalOperator childOp = plan.getPredecessors(op).get(0);

        DerivedDataVisitor visitor = new DerivedDataVisitor(childOp, null,
                baseData, LogToPhyMap, physPlan);
        try {
            visitor.visit();
        } catch (VisitorException e) {
            log.error(e.getMessage());
        }

        DataBag bag = visitor.derivedData.get(childOp);
        Map<IdentityHashSet<Tuple>, Integer> affinityGroups = new HashMap<IdentityHashSet<Tuple>, Integer>();

        for (Iterator<Tuple> it = bag.iterator(); it.hasNext();) {
            IdentityHashSet<Tuple> set = new IdentityHashSet<Tuple>();
            affinityGroups.put(set, 1);
            set.add(it.next());
        }

        for (IdentityHashSet<Tuple> set : visitor.EqClasses) {
            // newEquivalenceClasses.put(set, 1);
            affinityGroups.put(set, 1);
        }

        AffinityGroups.put(childOp, affinityGroups);
        Lineage.put(childOp, visitor.lineage);

    }

    private boolean checkCompleteness(LogicalOperator op) {
        LineageTracer lineage = Lineage.get(op);
        Lineage.remove(op);

        Map<IdentityHashSet<Tuple>, Integer> affinityGroups = AffinityGroups
                .get(op);
        AffinityGroups.remove(op);

        Map<LOLoad, DataBag> newBaseData = PruneBaseDataConstrainedCoverage(
                baseData, null, lineage, affinityGroups);

        // obtain the derived data
        DerivedDataVisitor visitor = new DerivedDataVisitor(plan, null,
                newBaseData, LogToPhyMap, physPlan);
        try {
            visitor.visit();
        } catch (VisitorException e) {
            log.error(e.getMessage());
        }

        double newCompleteness = MetricEvaluation.getCompleteness(null,
                visitor.derivedData, visitor.OpToEqClasses, true);

        if (newCompleteness >= completeness) {
            completeness = newCompleteness;
            baseData.putAll(newBaseData);
        } else {
            continueTrimming = false;
        }

        return continueTrimming;
    }
}
