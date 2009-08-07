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

package org.apache.pig.impl.logicalLayer.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.CastFinder;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOCast;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOCross;
import org.apache.pig.impl.logicalLayer.LOJoin;
import org.apache.pig.impl.logicalLayer.LOFilter;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LOLimit;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOStream;
import org.apache.pig.impl.logicalLayer.LOSplitOutput;
import org.apache.pig.impl.logicalLayer.LOUnion;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.UDFFinder;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.pig.PigException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;

/**
 * A visitor to discover if a filter can be pushed as high up the tree as
 * possible.
 */
public class PushUpFilter extends LogicalTransformer {

    // boolean to remember if the filter has to be swapped
    private boolean mSwap = false;

    // boolean to remember if the filter has to be pushed into one of the
    // filter's predecessor's inputs
    private boolean mPushBefore = false;

    // the input of the predecessor where the filter has to be pushed
    private int mPushBeforeInput = -1;

    public PushUpFilter(LogicalPlan plan) {
        super(plan, new DepthFirstWalker<LogicalOperator, LogicalPlan>(plan));
    }

    /**
     * 
     * @return true if the filter has to swapped; false otherwise
     */
    public boolean getSwap() {
        return mSwap;
    }

    /**
     * 
     * @return true if the filter has to be pushed before its predecessor; false
     *         otherwise
     */
    public boolean getPushBefore() {
        return mPushBefore;
    }

    /**
     * 
     * @return return the input of the predecessor where the filter has to be
     *         pushed
     */
    public int getPushBeforeInput() {
        return mPushBeforeInput;
    }

    @Override
    public boolean check(List<LogicalOperator> nodes) throws OptimizerException {
        try {
            LOFilter filter = (LOFilter) getOperator(nodes);
            List<LogicalOperator> predecessors = (mPlan.getPredecessors(filter) == null ? null
                    : new ArrayList<LogicalOperator>(mPlan
                            .getPredecessors(filter)));

            // if there are no predecessors return false
            if (predecessors == null) {
                return false;
            }

            // if the filter has no predecessors or more than one predecessor
            // return false
            if (predecessors.size() == 0 || predecessors.size() > 1) {
                return false;
            }
                
            LogicalOperator predecessor = predecessors.get(0);

            // if the predecessor is one of LOLoad/LOStore/LOStream/LOLimit
            // return false
            if (predecessor instanceof LOLoad || predecessor instanceof LOStore
                    || predecessor instanceof LOStream
                    || predecessor instanceof LOLimit) {
                return false;
            }
            
            // TODO
            // for now filters cannot be combined
            // remove this check when filters can be combined
            if (predecessor instanceof LOFilter)
                return false;

            // TODO
            // same rule as filters
            if (predecessor instanceof LOSplitOutput) {
                return false;
            }
            if (predecessor instanceof LOSplit) {
                return false;
            }

            UDFFinder udfFinder = new UDFFinder(filter.getComparisonPlan());
            udfFinder.visit();

            // if the filter's inner plan contains any UDF then return false
            if (udfFinder.foundAnyUDF()) {
                return false;
            }

            CastFinder castFinder = new CastFinder(filter.getComparisonPlan());
            castFinder.visit();

            // if the filter's inner plan contains any casts then return false
            if (castFinder.foundAnyCast()) {
                return false;
            }

            List<RequiredFields> filterRequiredFields = filter
                    .getRequiredFields();
            if (filterRequiredFields == null) {
                return false;
            }
            RequiredFields requiredField = filterRequiredFields.get(0);

            // the filter's conditions contain constant expression
            // return false
            if (requiredField.needNoFields()) {
                return false;
            }

            // if the predecessor is a multi-input operator then detailed
            // checks are required
            if (predecessor instanceof LOCross
                    || predecessor instanceof LOUnion
                    || predecessor instanceof LOCogroup
                    || predecessor instanceof LOJoin) {

                // check if the filter's required fields in conjunction with the
                // predecessor's projection map. If the filter needs more than
                // one input then the filter's expressions have to be split

                List<LogicalOperator> grandParents = mPlan
                        .getPredecessors(predecessor);

                // if the predecessor does not have predecessors return false
                if (grandParents == null || grandParents.size() == 0) {
                    return false;
                }
                
                // check if the predecessor is a group by
                if (grandParents.size() == 1) {
                    if (predecessor instanceof LOCogroup) {
                        mSwap = true;
                        return true;
                    } else {
                        // only a group by can have a single input
                        return false;
                    }
                }

                if (requiredField.needAllFields()) {
                    return false;
                }

                Pair<Boolean, Set<Integer>> mappingResult = isRequiredFieldMapped(requiredField, predecessor.getProjectionMap());
                boolean mapped = mappingResult.first;
                Set<Integer> grandParentIndexes = mappingResult.second;
                if (!mapped) {
                    return false;
                }
                
                // TODO
                // the filter's conditions requires more than one input of its
                // predecessor
                // when the filter's conditions are splittable return true
                if ((grandParentIndexes == null)
                        || (grandParentIndexes.size() == 0)
                        || (grandParentIndexes.size() > 1)) {
                    return false;
                }

                if (predecessor instanceof LOCogroup) {
                    // check for outer
                    if (isAnyOuter((LOCogroup) predecessor)) {
                        return false;
                    }
                }
                mPushBefore = true;
                mPushBeforeInput = grandParentIndexes.iterator().next();
                return true;

            } else if (predecessor instanceof LOForEach) {

                LOForEach loForEach = (LOForEach) predecessor;
                List<Boolean> mFlatten = loForEach.getFlatten();
                boolean hasFlatten = false;
                for (Boolean b : mFlatten) {
                    if (b.equals(true)) {
                        hasFlatten = true;
                    }
                }

                // TODO
                // A better check is to examine each column in the filter's
                // required fields. If the column is the result of a flatten
                // then
                // return false else return true

                // for now if the foreach has a flatten then return false
                if (hasFlatten) {
                    return false;
                }

                Pair<Boolean, Set<Integer>> mappingResult = isRequiredFieldMapped(requiredField, predecessor.getProjectionMap());
                boolean mapped = mappingResult.first;
                if (!mapped) {
                    return false;
                }
            }

            mSwap = true;
            return true;
        } catch (OptimizerException oe) {
            throw oe;
        } catch (Exception e) {
            int errCode = 2149;
            String msg = "Internal error while trying to check if filters can be pushed up.";
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }
    }

    private LogicalOperator getOperator(List<LogicalOperator> nodes)
            throws FrontendException {
        if ((nodes == null) || (nodes.size() <= 0)) {
            int errCode = 2052;
            String msg = "Internal error. Cannot retrieve operator from null or empty list.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }

        LogicalOperator lo = nodes.get(0);
        if (lo == null || !(lo instanceof LOFilter)) {
            // we should never be called with any other operator class name
            int errCode = 2005;
            String msg = "Expected " + LOFilter.class.getSimpleName()
                    + ", got "
                    + (lo == null ? lo : lo.getClass().getSimpleName());
            throw new OptimizerException(msg, errCode, PigException.INPUT);
        } else {
            return lo;
        }

    }

    @Override
    public void transform(List<LogicalOperator> nodes)
            throws OptimizerException {
        try {
            LOFilter filter = (LOFilter) getOperator(nodes);
            LogicalOperator predecessor = mPlan.getPredecessors(filter).get(0);
            if (mSwap) {
                mPlan.swap(predecessor, filter);
            } else if (mPushBefore) {
                if (mPushBeforeInput == -1) {
                    // something is wrong!
                    int errCode = 2150;
                    String msg = "Internal error. The push before input is not set.";
                    throw new OptimizerException(msg, errCode, PigException.BUG);
                }
                mPlan.pushBefore(predecessor, filter, mPushBeforeInput);
            }
        } catch (OptimizerException oe) {
            throw oe;
        } catch (Exception e) {
            int errCode = 2151;
            String msg = "Internal error while pushing filters up.";
            throw new OptimizerException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public void reset() {
        mPushBefore = false;
        mPushBeforeInput = -1;
        mSwap = false;
    }

    /**
     * 
     * A method to check if there any grouping column has the outer clause in a
     * grouping operator
     * 
     * @param cogroup
     *            the cogroup operator to be examined for presence of outer
     *            clause
     * @return true if the cogroup contains any input that has an outer clause;
     *         false otherwise
     */
    private boolean isAnyOuter(LOCogroup cogroup) {
        boolean[] innerList = cogroup.getInner();
        for (boolean inner : innerList) {
            if (!inner) {
                return true;
            }
        }
        return false;
    }

    /**
     * A method to check if the required field contains elements that are mapped
     * in the predecessor's inputs without a cast
     * 
     * @param requiredField
     *            the required field of the operator
     * @param predProjectionMap
     *            the projection map of the predecessor
     * @return a pair of boolean and a set of integers; the first element of the
     *         pair is true if the field is mapped without a cast; false
     *         otherwise; the second element of the pair is the set of
     *         predecessor's inputs that are required for the mapping
     */
    private Pair<Boolean, Set<Integer>> isRequiredFieldMapped(RequiredFields requiredField,
            ProjectionMap predProjectionMap) {
        
        if(requiredField == null)  {
            return new Pair<Boolean, Set<Integer>>(false, null);
        }

        // if predecessor projection map is null then return false
        if (predProjectionMap == null) {
            return new Pair<Boolean, Set<Integer>>(false, null);
        }

        // if the predecessor does not change its output return true
        if (!predProjectionMap.changes()) {
            return new Pair<Boolean, Set<Integer>>(true, null);
        }

        MultiMap<Integer, ProjectionMap.Column> mappedFields = predProjectionMap
                .getMappedFields();
        // if there is no mapping in the predecessor then return false
        if (mappedFields == null) {
            return new Pair<Boolean, Set<Integer>>(false, null);
        }

        Set<Integer> predInputs = new HashSet<Integer>();
        for (Pair<Integer, Integer> pair : requiredField.getFields()) {
            predInputs.add(pair.second);
        }

        boolean mapped = false;
        Set<Integer> grandParentIndexes = new HashSet<Integer>();

        for (Integer input : predInputs) {
            List<ProjectionMap.Column> inputList = (List<ProjectionMap.Column>) mappedFields
                    .get(input);
            // inputList is null -> the required field is added
            if(inputList == null) {
                return new Pair<Boolean, Set<Integer>>(false, null);
            }
            for (ProjectionMap.Column column : inputList) {
                // TODO
                // Check if the column has a cast
                // if a cast is not used then consider it as mapped
                // in the future this should go away and the cast
                // type should be used to move around the projections
                if (!column.cast()) {
                    mapped = true;
                }
                Pair<Integer, Integer> pair = column.getInputColumn();
                grandParentIndexes.add(pair.first);
            }
        }

        if (!mapped) {
            return new Pair<Boolean, Set<Integer>>(false, null);
        }

        return new Pair<Boolean, Set<Integer>>(true, grandParentIndexes);
    }
}
