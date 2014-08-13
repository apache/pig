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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Comparator;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.Operator;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.pen.util.LineageTracer;
import org.apache.pig.pen.util.MetricEvaluation;
import org.apache.pig.pen.util.PreOrderDepthFirstWalker;
import org.apache.pig.pen.util.ExampleTuple;

public class LineageTrimmingVisitor extends LogicalRelationalNodesVisitor {

    LogicalPlan plan = null;
    Map<LOLoad, DataBag> baseData;
    Map<FileSpec, DataBag> inputToDataMap;
    Map<Operator, PhysicalOperator> LogToPhyMap = null;
    PhysicalPlan physPlan = null;
    double completeness = 100.0;
    Log log = LogFactory.getLog(getClass());

    Map<Operator, Collection<IdentityHashSet<Tuple>>> AffinityGroups = new HashMap<Operator, Collection<IdentityHashSet<Tuple>>>();
    Map<Operator, LineageTracer> Lineage = new HashMap<Operator, LineageTracer>();

    boolean continueTrimming;
    PigContext pc;
    private ExampleGenerator eg;

    public LineageTrimmingVisitor(LogicalPlan plan,
            Map<LOLoad, DataBag> baseData,
            ExampleGenerator eg,
            Map<Operator, PhysicalOperator> LogToPhyMap,
            PhysicalPlan physPlan, PigContext pc) throws IOException, InterruptedException {
        super(plan, new PreOrderDepthFirstWalker(plan));
        // this.baseData.putAll(baseData);
        this.baseData = baseData;
        this.plan = plan;
        this.LogToPhyMap = LogToPhyMap;
        this.pc = pc;
        this.physPlan = physPlan;
        this.eg = eg;
        this.inputToDataMap = new HashMap<FileSpec, DataBag>();
        init();
    }

    public void init() throws IOException, InterruptedException {

        Map<Operator, DataBag> data = eg.getData();

        LineageTracer lineage = eg.getLineage();
        Map<LogicalRelationalOperator, Collection<IdentityHashSet<Tuple>>> OpToEqClasses = eg.getLoToEqClassMap();
        for (Operator leaf : plan.getSinks()) {
            Lineage.put(leaf, lineage);
            AffinityGroups.put(leaf, eg.getEqClasses());
        }
        completeness = MetricEvaluation.getCompleteness(null,
                data, OpToEqClasses, true);
        LogToPhyMap = eg.getLogToPhyMap();
        continueTrimming = true;

    }

    @Override
    public void visit(LOCogroup cg) throws FrontendException {
        // can't separate CoGroup from succeeding ForEach
        if (plan.getSuccessors(cg) != null && plan.getSuccessors(cg).get(0) instanceof LOForEach)
            return;
        
        if (continueTrimming) {
            try {

                continueTrimming = checkCompleteness(cg);
                
                LineageTracer lineage = null;
                // create affinity groups
                if (cg.getInputs(plan).size() == 1) {
                    lineage = eg.getLineage();
                    AffinityGroups.put(cg.getInputs(plan).get(0), eg.getEqClasses());
                    Lineage.put(cg.getInputs(plan).get(0), lineage);

                } else {
                    for (Operator input : cg.getInputs(plan)) {
                        Lineage.put(input, eg.getLineage());
                        AffinityGroups.put(input, eg.getEqClasses());
                    }
                }
            } catch (Exception e) {
                throw new FrontendException("Exception : "+e.getMessage());
            }
        }
    }

    @Override
    public void visit(LOJoin join) throws FrontendException {
        if (continueTrimming) {
          processOperator(join);
        }
    }

    @Override
    public void visit(LOCross cs) throws FrontendException {
        if(continueTrimming)
            processOperator(cs);

    }

    @Override
    public void visit(LODistinct dt) throws FrontendException {
        if(continueTrimming)
            processOperator(dt);

    }

    @Override
    public void visit(LOFilter filter) throws FrontendException {
        if (continueTrimming)
            processOperator(filter);
    }
    
    @Override
    public void visit(LOStore store) throws FrontendException {
        if (continueTrimming)
            processOperator(store);
    }

    @Override
    public void visit(LOForEach forEach) throws FrontendException {
        if (continueTrimming)
            processOperator(forEach);
    }

    @Override
    public void visit(LOLimit limOp) throws FrontendException {
        if(continueTrimming)
            processOperator(limOp);

    }

    @Override
    public void visit(LOLoad load) throws FrontendException {
        if (continueTrimming)
            processOperator(load);
    }

    @Override
    public void visit(LOSort s) throws FrontendException {
        if(continueTrimming)
            processOperator(s);

    }

    @Override
    public void visit(LOSplit split) throws FrontendException {
        if(continueTrimming)
            processOperator(split);

    }

    @Override
    public void visit(LOSplitOutput split) throws FrontendException {
        if(continueTrimming)
            processOperator(split);

    }

    @Override
    public void visit(LOUnion u) throws FrontendException {
        if(continueTrimming)
            processOperator(u);

    }

    private Map<LOLoad, DataBag> PruneBaseDataConstrainedCoverage(
            Map<LOLoad, DataBag> baseData,
            LineageTracer lineage,
            Collection<IdentityHashSet<Tuple>> equivalenceClasses) {

        IdentityHashMap<Tuple, Collection<Tuple>> membershipMap = lineage
                .getMembershipMap();
        IdentityHashMap<Tuple, Double> lineageGroupWeights = lineage
                .getWeightedCounts(2f, 1);

        // compute a mapping from lineage group to the set of equivalence
        // classes covered by it
        // IdentityHashMap<Tuple, Set<Integer>> lineageGroupToEquivClasses = new
        // IdentityHashMap<Tuple, Set<Integer>>();
        IdentityHashMap<Tuple, Set<IdentityHashSet<Tuple>>> lineageGroupToEquivClasses = new IdentityHashMap<Tuple, Set<IdentityHashSet<Tuple>>>();
        for (IdentityHashSet<Tuple> equivClass : equivalenceClasses) {
            for (Object t : equivClass) {
                Tuple lineageGroup = lineage.getRepresentative((Tuple) t);
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
        }

        // select lineage groups such that we cover all equivalence classes
        IdentityHashSet<Tuple> selectedLineageGroups = new IdentityHashSet<Tuple>();
        while (!lineageGroupToEquivClasses.isEmpty()) {
            // greedily find the lineage group with the best "score", where
            // score = # equiv classes covered / group weight
            double bestWeight = -1;
            Tuple bestLineageGroup = null;
            Set<IdentityHashSet<Tuple>> bestEquivClassesCovered = null;
            int bestNumEquivClassesCovered = 0;
            for (Tuple lineageGroup : lineageGroupToEquivClasses.keySet()) {
                double weight = lineageGroupWeights.get(lineageGroup);

                Set<IdentityHashSet<Tuple>> equivClassesCovered = lineageGroupToEquivClasses
                        .get(lineageGroup);
                int numEquivClassesCovered = equivClassesCovered.size();

                if ((numEquivClassesCovered > bestNumEquivClassesCovered) ||
                    (numEquivClassesCovered == bestNumEquivClassesCovered && weight < bestWeight)) {

                    if (selectedLineageGroups.contains(lineageGroup)) {
                        bestLineageGroup = lineageGroup;
                        bestEquivClassesCovered = equivClassesCovered;
                        continue;
                    }

                    bestWeight = weight;
                    bestLineageGroup = lineageGroup;
                    bestNumEquivClassesCovered = numEquivClassesCovered;
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
                        it.remove();
                    }
                }
                if (equivClasses.size() == 0)
                    toRemove.add(lineageGroup);

            }
            for (Tuple removeMe : toRemove)
                lineageGroupToEquivClasses.remove(removeMe);
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

    private void processLoad(LOLoad ld) throws FrontendException {
        // prune base records
        if (inputToDataMap.get(ld.getFileSpec()) != null) {
            baseData.put(ld, inputToDataMap.get(ld.getFileSpec()));
            return;
        }
        
        DataBag data = baseData.get(ld);
        if (data == null || data.size() < 2)
            return;
        Set<Tuple> realData = new HashSet<Tuple>(), syntheticData = new HashSet<Tuple>();

        for (Iterator<Tuple> it = data.iterator(); it.hasNext(); ) {
            Tuple t = it.next();
            if (((ExampleTuple)t).synthetic)
                syntheticData.add(t);
            else
              realData.add(t);
        }
        
        Map<LOLoad, DataBag> newBaseData = new HashMap<LOLoad, DataBag>();
        DataBag newData = BagFactory.getInstance().newDefaultBag();
        newBaseData.put(ld, newData);
        for (Map.Entry<LOLoad, DataBag> entry : baseData.entrySet()) {
            if (entry.getKey() != ld) {
                if (!entry.getKey().getFileSpec().equals(ld.getFileSpec()))
                    newBaseData.put(entry.getKey(), entry.getValue());
                else
                    newBaseData.put(entry.getKey(), newData);
            }
        }
        
        if (checkNewBaseData(newData, newBaseData, realData))
            checkNewBaseData(newData, newBaseData, syntheticData);
        
        inputToDataMap.put(ld.getFileSpec(), baseData.get(ld));
    }
    
    private boolean checkNewBaseData(DataBag data, Map<LOLoad, DataBag> newBaseData, Set<Tuple> loadData) throws FrontendException {
        List<Pair<Tuple, Double>> sortedBase = new LinkedList<Pair<Tuple, Double>>();
        DataBag oldData = BagFactory.getInstance().newDefaultBag();
        oldData.addAll(data);
        double tmpCompleteness = completeness;
        for (Tuple t : loadData) {
            data.add(t);
            // obtain the derived data 
            Map<Operator, DataBag> derivedData;
            try {
                derivedData = eg.getData(newBaseData);
            } catch (Exception e) {
                throw new FrontendException("Exception: "+e.getMessage());
            }
            double newCompleteness = MetricEvaluation.getCompleteness(null,
                    derivedData, eg.getLoToEqClassMap(), true);

            sortedBase.add(new Pair<Tuple, Double>(t, Double.valueOf(newCompleteness)));
            if (newCompleteness >= tmpCompleteness)
                break;
        }
        
        Collections.sort(sortedBase, new Comparator<Pair<Tuple, Double>>() {
            @Override
            public int compare(Pair<Tuple, Double> o1,
                               Pair<Tuple, Double> o2) {
                return o1.second > o2.second ? -1 : o1.second == o2.second ? 0 : 1;
            }
        }
        );

        data.clear();
        data.addAll(oldData);
        for (Pair<Tuple, Double> p : sortedBase) {
            data.add(p.first);
            // obtain the derived data 
            Map<Operator, DataBag> derivedData;
            try {
                derivedData = eg.getData(newBaseData);
            } catch (Exception e) {
                throw new FrontendException("Exception: "+e.getMessage());
            }
            double newCompleteness = MetricEvaluation.getCompleteness(null,
                    derivedData, eg.getLoToEqClassMap(), true);

            if (newCompleteness >= completeness) {
                completeness = newCompleteness;
                baseData.putAll(newBaseData);
                return false;
            }
        }
        return true;
    }
    
    private void processOperator(LogicalRelationalOperator op) throws FrontendException {
        
        try {
            if (op instanceof LOLoad) {
                processLoad((LOLoad) op);
                return;
            }
            
            continueTrimming = checkCompleteness(op);

            if (plan.getPredecessors(op) == null)
                return;
            
            if (continueTrimming == false)
                return;

            Operator childOp = plan.getPredecessors(op).get(0);
            if (op instanceof LOForEach && childOp instanceof LOCogroup)
            {
                LOCogroup cg = (LOCogroup) childOp;
                for (Operator input : cg.getInputs(plan)) {
                    AffinityGroups.put(input, eg.getEqClasses());
                    Lineage.put(input, eg.getLineage());
                }
            } else {
                List<Operator> childOps = plan.getPredecessors(op);
                for (Operator lo : childOps) {
                    AffinityGroups.put(lo, eg.getEqClasses());
                    Lineage.put(lo, eg.getLineage());
                }
            }
        } catch (Exception e) {
          e.printStackTrace(System.out);
          throw new FrontendException("Exception: "+e.getMessage());
        }
    }

    private boolean checkCompleteness(LogicalRelationalOperator op) throws Exception {
        LineageTracer lineage = Lineage.get(op);
        Lineage.remove(op);

        Collection<IdentityHashSet<Tuple>> affinityGroups = AffinityGroups
                .get(op);
        AffinityGroups.remove(op);

        Map<LOLoad, DataBag> newBaseData = PruneBaseDataConstrainedCoverage(
                baseData, lineage, affinityGroups);

        // obtain the derived data
        Map<Operator, DataBag> derivedData = eg.getData(newBaseData);
        double newCompleteness = MetricEvaluation.getCompleteness(null,
                derivedData, eg.getLoToEqClassMap(), true);

        if (newCompleteness >= completeness) {
            completeness = newCompleteness;
            baseData.putAll(newBaseData);
        } else {
            continueTrimming = false;
        }

        return continueTrimming;
    }
    
    Map<LOLoad, DataBag> getBaseData() {
        return baseData;
    }
}
