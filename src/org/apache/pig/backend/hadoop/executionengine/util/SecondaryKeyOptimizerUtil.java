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
package org.apache.pig.backend.hadoop.executionengine.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.ColumnChainInfo;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.ColumnInfo;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.SortKeyInfo;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PORelationToExprProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.JoinPackager;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSortedDistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POGlobalRearrangeSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POReduceBySpark;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;

@InterfaceAudience.Private
public class SecondaryKeyOptimizerUtil {
    private static Log log = LogFactory.getLog(SecondaryKeyOptimizerUtil.class.getName());

    public SecondaryKeyOptimizerUtil() {

    }

    private static class POToChange {
        POToChange(PhysicalOperator oper, PhysicalPlan plan, POForEach forEach) {
            this.oper = oper;
            this.plan = plan;
            this.forEach = forEach;
        }

        PhysicalOperator oper;

        PhysicalPlan plan;

        POForEach forEach;
    }

    public static class SecondaryKeyOptimizerInfo {

        private int numSortRemoved = 0;
        private int numDistinctChanged = 0;
        private int numUseSecondaryKey = 0;
        private boolean useSecondaryKey = false;
        private boolean[] secondarySortOrder;

        public int getNumSortRemoved() {
            return numSortRemoved;
        }

        public void incrementNumSortRemoved() {
            this.numSortRemoved++;
        }

        public int getNumDistinctChanged() {
            return numDistinctChanged;
        }

        public void incrementNumDistinctChanged() {
            this.numDistinctChanged++;
        }

        public int getNumUseSecondaryKey() {
            return numUseSecondaryKey;
        }

        public void incrementNumUseSecondaryKey() {
            this.numUseSecondaryKey++;
        }

        public boolean isUseSecondaryKey() {
            return useSecondaryKey;
        }

        public void setUseSecondaryKey(boolean useSecondaryKey) {
            this.useSecondaryKey = useSecondaryKey;
        }

        public boolean[] getSecondarySortOrder() {
            return secondarySortOrder;
        }

        public void setSecondarySortOrder(boolean[] secondarySortOrder) {
            this.secondarySortOrder = secondarySortOrder;
        }

    }

    // Build sort key structure from POLocalRearrange
    private static SortKeyInfo getSortKeyInfo(POLocalRearrange rearrange) throws ExecException {
        SortKeyInfo result = new SortKeyInfo();
        List<PhysicalPlan> plans = rearrange.getPlans();
        nextPlan: for (int i = 0; i < plans.size(); i++) {
            PhysicalPlan plan = plans.get(i);
            ColumnChainInfo columnChainInfo = new ColumnChainInfo();
            if (plan.getRoots() == null) {
                log.debug("POLocalRearrange plan is null");
                return null;
            } else if (plan.getRoots().size() != 1) {
                // POLocalRearrange plan contains more than 1 root.
                // Probably there is an Expression operator in the local
                // rearrangement plan, skip this plan
                continue nextPlan;
            } else {
                List<Integer> columns = new ArrayList<Integer>();
                columns
                        .add(rearrange.getIndex()
                                & PigNullableWritable.idxSpace);

                // The first item inside columnChainInfo is set to type Tuple.
                // This value is not actually in use, but it intends to match
                // the type of POProject in reduce side
                columnChainInfo.insert(columns, DataType.TUPLE);

                PhysicalOperator node = plan.getRoots().get(0);
                while (node != null) {
                    if (node instanceof POProject) {
                        POProject project = (POProject) node;
                        if(project.isProjectToEnd()){
                            columnChainInfo.insert(project.getStartCol(),
                                    project.getResultType());
                        }else {
                            columnChainInfo.insert(
                                    project.getColumns(), project.getResultType());
                        }

                        if (plan.getSuccessors(node) == null)
                            node = null;
                        else if (plan.getSuccessors(node).size() != 1) {
                            log.debug(node + " have more than 1 successor");
                            node = null;
                        } else
                            node = plan.getSuccessors(node).get(0);
                    } else
                        // constant, UDF, we will pass
                        continue nextPlan;
                }
            }
            // Let's assume all main key is sorted ascendant, we can further
            // optimize it to match one of the nested sort/distinct key, because we do not
            // really care about how cogroup key are sorted; But it may not be the case
            // if sometime we switch all the comparator to byte comparator, so just
            // leave it as it is for now
            result.insertColumnChainInfo(i, columnChainInfo, true);
        }
        return result;
    }

    public SecondaryKeyOptimizerInfo applySecondaryKeySort(PhysicalPlan mapPlan, PhysicalPlan reducePlan) throws VisitorException {
        log.trace("Entering SecondaryKeyOptimizerUtil.addSecondaryKeySort");
        SecondaryKeyOptimizerInfo secKeyOptimizerInfo = new SecondaryKeyOptimizerInfo();
        List<SortKeyInfo> sortKeyInfos = new ArrayList<SortKeyInfo>();
        SortKeyInfo secondarySortKeyInfo = null;
        List<POToChange> sortsToRemove = null;
        List<POToChange> distinctsToChange = null;


        List<PhysicalOperator> mapLeaves = mapPlan.getLeaves();
        if (mapLeaves == null || mapLeaves.size() != 1) {
            log.debug("Expected map to have single leaf! Skip secondary key optimizing");
            return null;
        }
        PhysicalOperator mapLeaf = mapLeaves.get(0);

        // Figure out the main key of the map-reduce job from POLocalRearrange
        try {
            if (mapLeaf instanceof POLocalRearrange) {
                SortKeyInfo sortKeyInfo = getSortKeyInfo((POLocalRearrange) mapLeaf);
                if (sortKeyInfo == null) {
                    log.debug("Cannot get sortKeyInfo from POLocalRearrange, skip secondary key optimizing");
                    return null;
                }
                sortKeyInfos.add(sortKeyInfo);
            } else if (mapLeaf instanceof POUnion) {
                List<PhysicalOperator> preds = mapPlan
                        .getPredecessors(mapLeaf);
                for (PhysicalOperator pred : preds) {
                    if (pred instanceof POLocalRearrange) {
                        SortKeyInfo sortKeyInfo = getSortKeyInfo((POLocalRearrange) pred);
                        if (sortKeyInfo == null) {
                            log.debug("Cannot get sortKeyInfo from POLocalRearrange, skip secondary key optimizing");
                            return null;
                        }
                        sortKeyInfos.add(sortKeyInfo);
                    }
                }
            } else {
                log.debug("Cannot find POLocalRearrange or POUnion in map leaf, skip secondary key optimizing");
                return null;
            }
        } catch (ExecException e) {
            log.debug("Cannot get sortKeyInfo from POLocalRearrange, skip secondary key optimizing");
            return null;
        }

        if (reducePlan.isEmpty()) {
            log.debug("Reduce plan is empty, skip secondary key optimizing");
            return null;
        }

        List<PhysicalOperator> reduceRoots = reducePlan.getRoots();
        if (reduceRoots.size() != 1) {
            log.debug("Expected reduce to have single root, skip secondary key optimizing");
            return null;
        }

        PhysicalOperator root = reduceRoots.get(0);
        PhysicalOperator currentNode = getCurrentNode(root,reducePlan);

        // visit the POForEach of the reduce plan. We can have Limit and Filter
        // in the middle

        POForEach foreach = null;
        while (currentNode != null) {
            if (currentNode instanceof POPackage
                    && !(((POPackage) currentNode).getPkgr() instanceof JoinPackager)
                    || currentNode instanceof POFilter
                    || currentNode instanceof POLimit) {
                List<PhysicalOperator> succs = reducePlan
                        .getSuccessors(currentNode);
                if (succs == null) // We didn't find POForEach
                    return null;
                if (succs.size() != 1) {
                    log.debug("See multiple output for " + currentNode
                            + " in reduce plan, skip secondary key optimizing");
                    return null;
                }
                currentNode = succs.get(0);
            } else if (currentNode instanceof POForEach) {
                foreach = (POForEach) currentNode;
                break;
            } else { // Skip optimization
                return null;
            }
        }

        // We do not find a foreach (we shall not come here, a trick to fool findbugs)
        if (foreach==null)
            return null;

        sortsToRemove = new ArrayList<POToChange>();
        distinctsToChange = new ArrayList<POToChange>();

        for (PhysicalPlan innerPlan : foreach.getInputPlans()) {
            // visit inner plans to figure out the sort order for distinct /
            // sort
            SecondaryKeyDiscover innerPlanDiscover = new SecondaryKeyDiscover(
                    innerPlan, sortKeyInfos, secondarySortKeyInfo);
            try {
                innerPlanDiscover.process();
            } catch (FrontendException e) {
                int errorCode = 2213;
                throw new VisitorException("Error visiting inner plan for ForEach", errorCode, e);
            }
            secondarySortKeyInfo = innerPlanDiscover.getSecondarySortKeyInfo();
            if (innerPlanDiscover.getSortsToRemove() != null) {
                for (POSort sort : innerPlanDiscover.getSortsToRemove()) {
                    sortsToRemove.add(new POToChange(sort, innerPlan, foreach));
                }
            }
            if (innerPlanDiscover.getDistinctsToChange() != null) {
                for (PODistinct distinct : innerPlanDiscover
                        .getDistinctsToChange()) {
                    distinctsToChange.add(new POToChange(distinct, innerPlan,
                            foreach));
                }
            }
        }

        try {
            // Change PODistinct to use POSortedDistinct, which assume the input
            // data is sorted
            for (POToChange distinctToChange : distinctsToChange) {
                secKeyOptimizerInfo.incrementNumDistinctChanged();
                PODistinct oldDistinct = (PODistinct) distinctToChange.oper;
                String scope = oldDistinct.getOperatorKey().scope;
                POSortedDistinct newDistinct = new POSortedDistinct(
                        new OperatorKey(scope, NodeIdGenerator.getGenerator()
                                .getNextNodeId(scope)), oldDistinct
                                .getRequestedParallelism(), oldDistinct
                                .getInputs());
                newDistinct.setInputs(oldDistinct.getInputs());
                newDistinct.setResultType(oldDistinct.getResultType());
                distinctToChange.plan.replace(oldDistinct, newDistinct);
                distinctToChange.forEach.getLeaves();
            }
            // Removed POSort, if the successor require a databag, we need to
            // add a PORelationToExprProject
            // to convert tuples into databag
            for (POToChange sortToRemove : sortsToRemove) {
                secKeyOptimizerInfo.incrementNumSortRemoved();
                POSort oldSort = (POSort) sortToRemove.oper;
                String scope = oldSort.getOperatorKey().scope;
                List<PhysicalOperator> preds = sortToRemove.plan
                        .getPredecessors(sortToRemove.oper);
                List<PhysicalOperator> succs = sortToRemove.plan
                .getSuccessors(sortToRemove.oper);
                POProject project = null;
                if ((preds == null
                        || preds.get(0).getResultType() != DataType.BAG
                        && oldSort.getResultType() == DataType.BAG) // sort to remove do change the result type
                        && (succs == null || !(succs.get(0) instanceof PORelationToExprProject))) // successor is not PORelationToExprProject
                {
                    project = new PORelationToExprProject(new OperatorKey(
                            scope, NodeIdGenerator.getGenerator()
                                    .getNextNodeId(scope)), oldSort
                            .getRequestedParallelism());
                    project.setInputs(oldSort.getInputs());
                    project.setResultType(DataType.BAG);
                    project.setStar(true);
                }
                if (project == null)
                    sortToRemove.plan.removeAndReconnect(sortToRemove.oper);
                else
                    sortToRemove.plan.replace(oldSort, project);
                sortToRemove.forEach.getLeaves();
            }
        } catch (PlanException e) {
            int errorCode = 2202;
            throw new VisitorException(
                    "Error change distinct/sort to use secondary key optimizer",
                    errorCode, e);
        }
        if (secondarySortKeyInfo != null) {
            // Adjust POLocalRearrange, POPackage, MapReduceOper to use the
            // secondary key
            secKeyOptimizerInfo.incrementNumUseSecondaryKey();
            secKeyOptimizerInfo.setUseSecondaryKey(true);
            secKeyOptimizerInfo.setSecondarySortOrder(secondarySortKeyInfo.getAscs());
            int indexOfRearrangeToChange = -1;
            for (ColumnChainInfo columnChainInfo : secondarySortKeyInfo
                    .getColumnChains()) {
                ColumnInfo currentColumn = columnChainInfo.getColumnInfos()
                        .get(0);
                int index = currentColumn.getColumns().get(0);
                if (indexOfRearrangeToChange == -1)
                    indexOfRearrangeToChange = index;
                else if (indexOfRearrangeToChange != index) {
                    int errorCode = 2203;
                    throw new VisitorException("Sort on columns from different inputs.", errorCode);
                }
            }
            if (mapLeaf instanceof POLocalRearrange) {
                ((POLocalRearrange) mapLeaf).setUseSecondaryKey(true);
                setSecondaryPlan(mapPlan, (POLocalRearrange) mapLeaf,
                        secondarySortKeyInfo);
            } else if (mapLeaf instanceof POUnion) {
                List<PhysicalOperator> preds = mapPlan
                        .getPredecessors(mapLeaf);
                boolean found = false;
                for (PhysicalOperator pred : preds) {
                    POLocalRearrange rearrange = (POLocalRearrange) pred;
                    rearrange.setUseSecondaryKey(true);
                    if (rearrange.getIndex() == indexOfRearrangeToChange) {
                        // Try to find the POLocalRearrange for the secondary key
                        found = true;
                        setSecondaryPlan(mapPlan, rearrange, secondarySortKeyInfo);
                    }
                }
                if (!found)
                {
                    int errorCode = 2214;
                    throw new VisitorException("Cannot find POLocalRearrange to set secondary plan", errorCode);
                }
            }

            if (root instanceof POGlobalRearrangeSpark) {
                POGlobalRearrangeSpark plg = (POGlobalRearrangeSpark) root;
                plg.setUseSecondaryKey(true);
                plg.setSecondarySortOrder(secondarySortKeyInfo.getAscs());
            } else if (root instanceof POPackage) {
                POPackage pack = (POPackage) root;
                pack.getPkgr().setUseSecondaryKey(true);
            } else if (root instanceof POReduceBySpark) {
                POReduceBySpark reduceBySpark = (POReduceBySpark) root;
                reduceBySpark.setUseSecondaryKey(true);
                reduceBySpark.setSecondarySortOrder(secondarySortKeyInfo.getAscs());
            }
        }
        return secKeyOptimizerInfo;
    }

    protected PhysicalOperator getCurrentNode(PhysicalOperator root, PhysicalPlan reducePlan) {
        PhysicalOperator currentNode = null;
        if (!(root instanceof POPackage)) {
            log.debug("Expected reduce root to be a POPackage, skip secondary key optimizing");
        } else {
            currentNode = root;
        }
        return currentNode;
    }

    private static void setSecondaryPlan(PhysicalPlan plan, POLocalRearrange rearrange,
            SortKeyInfo secondarySortKeyInfo) throws VisitorException {
        // Put plan to project secondary key to the POLocalRearrange
        try {
            String scope = rearrange.getOperatorKey().scope;
            List<PhysicalPlan> secondaryPlanList = new ArrayList<PhysicalPlan>();
            for (ColumnChainInfo columnChainInfo : secondarySortKeyInfo
                    .getColumnChains()) {
                PhysicalPlan secondaryPlan = new PhysicalPlan();
                for (int i = 1; i < columnChainInfo.size(); i++) {
                    // The first item in columnChainInfo indicate the index of
                    // input, we have addressed
                    // already before we come here
                    ColumnInfo columnInfo = columnChainInfo.getColumnInfo(i);
                    POProject project = new POProject(
                            new OperatorKey(scope, NodeIdGenerator
                                    .getGenerator().getNextNodeId(scope)),
                            rearrange.getRequestedParallelism());
                    if(columnInfo.isRangeProject())
                        project.setProjectToEnd(columnInfo.getStartCol());
                    else
                        project
                                .setColumns((ArrayList<Integer>) columnInfo.getColumns());
                    project.setResultType(columnInfo.getResultType());
                    secondaryPlan.addAsLeaf(project);
                }
                if (secondaryPlan.isEmpty()) { // If secondary key sort on the
                                               // input as a whole
                    POProject project = new POProject(
                            new OperatorKey(scope, NodeIdGenerator
                                    .getGenerator().getNextNodeId(scope)),
                            rearrange.getRequestedParallelism());
                    project.setStar(true);
                    secondaryPlan.addAsLeaf(project);
                }
                secondaryPlanList.add(secondaryPlan);
            }
            rearrange.setSecondaryPlans(secondaryPlanList);
        } catch (PlanException e) {
            int errorCode = 2204;
            throw new VisitorException("Error setting secondary key plan",
                    errorCode, e);
        }
    }

    // Find eligible sort and distinct physical operators from the reduce plan.
    // SecondaryKeyChecker will check for sort/distinct keys (for distinct, it
    // is
    // always the entire bag), if it is the same with the main key, then we can
    // just
    // remove it. If it is not, then we can have 1 secondary sort key, put the
    // first
    // such sort/distinct key as the secondary sort key. For subsequent
    // sort/distinct,
    // we cannot do any secondary key optimization because we only have 1
    // secondary
    // sort key.
    private static class SecondaryKeyDiscover {
        PhysicalPlan mPlan;

        List<POSort> sortsToRemove = new ArrayList<POSort>();

        List<PODistinct> distinctsToChange = new ArrayList<PODistinct>();

        List<SortKeyInfo> sortKeyInfos;

        SortKeyInfo secondarySortKeyInfo;

        ColumnChainInfo columnChainInfo = null;

        // PhysicalPlan here is foreach inner plan
        SecondaryKeyDiscover(PhysicalPlan plan,
                List<SortKeyInfo> sortKeyInfos, SortKeyInfo secondarySortKeyInfo) {
            this.mPlan = plan;
            this.sortKeyInfos = sortKeyInfos;
            this.secondarySortKeyInfo = secondarySortKeyInfo;
        }

        public void process() throws FrontendException
        {
            List<PhysicalOperator> roots = mPlan.getRoots();
            for (PhysicalOperator root : roots) {
                columnChainInfo = new ColumnChainInfo();
                processRoot(root);
            }
        }

        public void processRoot(PhysicalOperator root) throws FrontendException {
            PhysicalOperator currentNode = root;
            while (currentNode!=null) {
                boolean sawInvalidPhysicalOper = false;
                if (currentNode instanceof PODistinct)
                    sawInvalidPhysicalOper = processDistinct((PODistinct)currentNode);
                else if (currentNode instanceof POSort)
                    sawInvalidPhysicalOper = processSort((POSort)currentNode);
                else if (currentNode instanceof POProject)
                    sawInvalidPhysicalOper = processProject((POProject)currentNode);
                else if (currentNode instanceof POUserFunc ||
                         currentNode instanceof POUnion ||
                         // We don't process foreach, since foreach is too complex to get right
                         currentNode instanceof POForEach)
                    break;

                if (sawInvalidPhysicalOper)
                    break;

                List<PhysicalOperator> succs = mPlan.getSuccessors(currentNode);
                if (succs==null)
                    currentNode = null;
                else {
                    if (succs.size()>1) {
                        int errorCode = 2215;
                        throw new FrontendException("See more than 1 successors in the nested plan for "+currentNode,
                                errorCode);
                    }
                    currentNode = succs.get(0);
                }
            }
        }

        // We see PODistinct, check which key it is using
        public boolean processDistinct(PODistinct distinct) throws FrontendException {
            SortKeyInfo keyInfos = new SortKeyInfo();
            try {
                keyInfos.insertColumnChainInfo(0,
                        (ColumnChainInfo) columnChainInfo.clone(), true);
            } catch (CloneNotSupportedException e) { // We implement Clonable,
                                                     // impossible to get here
            }

            // if it is part of main key
            for (SortKeyInfo sortKeyInfo : sortKeyInfos) {
                if (sortKeyInfo.moreSpecificThan(keyInfos)) {
                    distinctsToChange.add(distinct);
                    return false;
                }
            }

            // if it is part of secondary key
            if (secondarySortKeyInfo != null
                    && secondarySortKeyInfo.moreSpecificThan(keyInfos)) {
                distinctsToChange.add(distinct);
                return false;
            }

            // Now set the secondary key
            if (secondarySortKeyInfo == null) {
                distinctsToChange.add(distinct);
                secondarySortKeyInfo = keyInfos;
            }
            return false;
        }

        // Accumulate column info
        public boolean processProject(POProject project) throws FrontendException {
            columnChainInfo.insertInReduce(project);
            return false;
        }

        // We see POSort, check which key it is using
        public boolean processSort(POSort sort) throws FrontendException{

            // if sort has a limit, it is already optimized by NestedLimitedSort optimizer
            if (sort.isLimited()) {
                return true;
            }

            SortKeyInfo keyInfo = new SortKeyInfo();
            for (int i = 0; i < sort.getSortPlans().size(); i++) {
                PhysicalPlan sortPlan = sort.getSortPlans().get(i);
                ColumnChainInfo sortChainInfo = null;
                try {
                    sortChainInfo = (ColumnChainInfo) columnChainInfo.clone();
                } catch (CloneNotSupportedException e) { // We implement
                                                         // Clonable, impossible
                                                         // to get here
                }
                boolean r = false;
                try {
                    r = collectColumnChain(sortPlan, sortChainInfo);
                } catch (PlanException e) {
                    int errorCode = 2206;
                    throw new FrontendException("Error visiting POSort inner plan",
                            errorCode, e);
                }
                if (r==true) // if we saw physical operator other than project in sort plan
                {
                    return true;
                }
                keyInfo.insertColumnChainInfo(i, sortChainInfo, sort
                        .getMAscCols().get(i));
            }
            // if it is part of main key
            for (SortKeyInfo sortKeyInfo : sortKeyInfos) {
                if (sortKeyInfo.moreSpecificThan(keyInfo)) {
                    sortsToRemove.add(sort);
                    return false;
                }
            }
            // if it is part of secondary key
            if (secondarySortKeyInfo != null
                    && secondarySortKeyInfo.moreSpecificThan(keyInfo)) {
                sortsToRemove.add(sort);
                return false;
            }

            // Now set the secondary key
            if (secondarySortKeyInfo == null) {
                sortsToRemove.add(sort);
                secondarySortKeyInfo = keyInfo;
            }
            return false;
        }

        public List<POSort> getSortsToRemove() {
            return sortsToRemove;
        }

        public List<PODistinct> getDistinctsToChange() {
            return distinctsToChange;
        }

        public SortKeyInfo getSecondarySortKeyInfo() {
            return secondarySortKeyInfo;
        }
    }

    // Return true if we saw physical operators other than project in the plan
    static private boolean collectColumnChain(PhysicalPlan plan,
            ColumnChainInfo columnChainInfo) throws PlanException {
        if (plan.getRoots().size() != 1) {
            return true;
        }

        PhysicalOperator currentNode = plan.getRoots().get(0);

        while (currentNode != null) {
            if (currentNode instanceof POProject) {
                POProject project = (POProject) currentNode;
                columnChainInfo.insertInReduce(project);
            } else {
                return true;
            }
            List<PhysicalOperator> succs = plan.getSuccessors(currentNode);
            if (succs == null)
                break;
            if (succs.size() != 1) {
                int errorCode = 2208;
                throw new PlanException(
                        "Exception visiting foreach inner plan", errorCode);
            }
            currentNode = succs.get(0);
        }
        return false;
    }
}
