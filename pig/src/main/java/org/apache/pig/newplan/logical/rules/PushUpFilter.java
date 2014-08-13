/**
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

package org.apache.pig.newplan.logical.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LONative;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;

public class PushUpFilter extends Rule {

    public PushUpFilter(String n) {
        super(n, false);
    }

    @Override
    public Transformer getNewTransformer() {
        return new PushUpFilterTransformer();
    }

    public class PushUpFilterTransformer extends Transformer {
        private OperatorSubPlan subPlan;

        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {
            // check if it is inner join
            Operator current = matched.getSources().get(0);

            Operator pred = findNonFilterPredecessor( current );
            if( pred == null )
                return false;

            // sort and union are always okay.
            if( pred instanceof LOSort || pred instanceof LOUnion ) {
                return true;
            }

            // if the predecessor is one of LOLoad/LOStore/LOStream/LOLimit/LONative
            // if predecessor is LOForEach, it is optimized by rule FilterAboveForeach
            // return false
            if( pred instanceof LOLoad   || pred instanceof LOStore || pred instanceof LOStream      ||
                pred instanceof LOFilter || pred instanceof LOSplit || pred instanceof LOSplitOutput ||
                pred instanceof LOLimit  || pred instanceof LONative || pred instanceof LOForEach) {
                return false;
            }

            LOFilter filter = (LOFilter)current;
            List<Operator> preds = currentPlan.getPredecessors( pred );
            LogicalExpressionPlan filterPlan = filter.getFilterPlan();

            if (OptimizerUtils.planHasNonDeterministicUdf(filterPlan)) {
                return false;
            }

            //if there is no nondeterministic udf, filter can be pushed above 
            // Distinct
            if(pred instanceof LODistinct){
                return true;
            }

            // collect all uids used in the filter plan
            Set<Long> uids = collectUidFromExpPlan(filterPlan);

            if( pred instanceof LOCogroup ) {
                LOCogroup cogrp = (LOCogroup)pred;
                if( preds.size() == 1 ) {
                    if( hasAll( (LogicalRelationalOperator)preds.get( 0 ), uids )    ) {
                        // Order by is ok if all UIDs can be found from previous operator.
                        return true;
                    }
                } else if ( 1 == cogrp.getExpressionPlans().get( 0 ).size() && !containUDF( filterPlan ) ) {
                    // Optimization is possible if there is only a single key.
                    // For regular cogroup, we cannot use UIDs to determine if filter can be pushed up.
                    // But if there is no UDF, it's okay, as only UDF can take bag field as input.
                    return true;
                }
            }

            // if the predecessor is a multi-input operator then detailed
            // checks are required
            if( pred instanceof LOCross || pred instanceof LOJoin ) {
                boolean[] innerFlags = null;
                boolean isFullOuter = true;
                boolean isInner = true;
                if( pred instanceof LOJoin ) {
                    innerFlags = ((LOJoin)pred).getInnerFlags();
                    // If all innerFlag is false, means a full outer join,
                    for (boolean inner : innerFlags) {
                        if (inner) {
                            isFullOuter = false;
                        } else {
                            isInner = false;
                        }
                    }
                    if (isFullOuter)
                        return false;
                }


                for(int j=0; j<preds.size(); j++) {
                    if (hasAll((LogicalRelationalOperator)preds.get(j), uids)) {
                        // For LOJoin, innerFlag==true indicate that branch is the outer join side
                        // which has the exact opposite semantics
                        if (pred instanceof LOCross || pred instanceof LOJoin && (isInner || innerFlags[j]))
                            return true;
                    }
                }
            }

            return false;
        }

        private boolean containUDF(LogicalExpressionPlan filterPlan) {
            Iterator<Operator> it = filterPlan.getOperators();
            while( it.hasNext() ) {
                if( it.next() instanceof UserFuncExpression )
                    return true;
            }
            return false;
        }

        Set<Long> collectUidFromExpPlan(LogicalExpressionPlan filterPlan) throws FrontendException {
            Set<Long> uids = new HashSet<Long>();
            Iterator<Operator> iter = filterPlan.getOperators();
            while(iter.hasNext()) {
                Operator op = iter.next();
                if (op instanceof ProjectExpression) {
                    long uid = ((ProjectExpression)op).getFieldSchema().uid;
                    uids.add(uid);
                }
            }
            return uids;
        }

        /**
         * Starting from current operator (which is a filter), search its successors until
         * locating a non-filter operator. Null is returned if none is found.
         */
        private Operator findNonFilterPredecessor(Operator current) {
            Operator op = current;
            do {
                List<Operator> predecessors = currentPlan.getPredecessors( op );

                // if there are no predecessors return false
                if( predecessors == null || predecessors.size() == 0 ) {
                    return null;
                }

                Operator pred = predecessors.get( 0 );
                if( pred instanceof LOFilter ) {
                    op = pred;
                    continue;
                } else {
                    return pred;
                }
            } while( true );

        }

        @Override
        public void transform(OperatorPlan matched) throws FrontendException {
            subPlan = new OperatorSubPlan(currentPlan);

            LOFilter filter = (LOFilter)matched.getSources().get(0);

            // This is the one that we will insert filter btwn it and it's input.
            Operator predecessor = this.findNonFilterPredecessor( filter );
            subPlan.add( predecessor) ;

            // Disconnect the filter in the plan without removing it from the plan.
            Operator predec = currentPlan.getPredecessors( filter ).get( 0 );
            Operator succed;

            if (currentPlan.getSuccessors(filter)!=null)
                succed = currentPlan.getSuccessors(filter).get(0);
            else
                succed = null;

            Pair<Integer, Integer> p1 = currentPlan.disconnect(predec, filter);
            if (succed!=null) {
                subPlan.add(succed);
                Pair<Integer, Integer> p2 = currentPlan.disconnect(filter, succed);
                currentPlan.connect(predec, p1.first, succed, p2.second);
            }

            if( predecessor instanceof LOSort || predecessor instanceof LODistinct ||
                ( predecessor instanceof LOCogroup && currentPlan.getPredecessors( predecessor ).size() == 1 ) ) {
                // For sort, put the filter in front of it.
                Operator prev = currentPlan.getPredecessors( predecessor ).get( 0 );

                insertFilter( prev, predecessor, filter );
                return;
            }

            // Find the predecessor of join that contains all required uids.
            LogicalExpressionPlan filterPlan = filter.getFilterPlan();
            List<Operator> preds = currentPlan.getPredecessors( predecessor );
            Map<Integer, Operator> inputs = findInputsToAddFilter( filterPlan, predecessor, preds );

            LOFilter newFilter = null;
            for( Entry<Integer, Operator> entry : inputs.entrySet() ) {
                int inputIndex = entry.getKey();
                Operator pred = entry.getValue();

                // Find projection field offset
                int columnOffset = 0;
                if( predecessor instanceof LOJoin || predecessor instanceof LOCross ) {
                    for( int i = 0; i < inputIndex; i++ ) {
                        columnOffset += ( (LogicalRelationalOperator)preds.get( i ) ).getSchema().size();
                    }
                }

                // Reuse the filter for the first match. For others, need to make a copy of the filter
                // and add it between input and predecessor.
                newFilter = newFilter == null ? filter : new LOFilter( (LogicalPlan)currentPlan );

                currentPlan.add( newFilter );
                subPlan.add( newFilter );
                subPlan.add( pred );
                LogicalExpressionPlan fPlan = filterPlan.deepCopy();
                List<Operator> sinks = fPlan.getSinks();
                List<ProjectExpression> projExprs = new ArrayList<ProjectExpression>();
                for( Operator sink : sinks ) {
                    if( sink instanceof ProjectExpression )
                        projExprs.add( (ProjectExpression)sink );
                }

                if( predecessor instanceof LOCogroup ) {
                    for( ProjectExpression projExpr : projExprs ) {
                        // Need to merge filter condition and cogroup by expression;
                        LogicalExpressionPlan plan = ((LOCogroup) predecessor).getExpressionPlans().get( inputIndex ).iterator().next();
                        LogicalExpressionPlan copy = plan.deepCopy();
                        LogicalExpression root = (LogicalExpression)copy.getSinks().get( 0 );
                        List<Operator> predecessors = fPlan.getPredecessors( projExpr );
                        if( predecessors == null || predecessors.size() == 0 ) {
                            fPlan.remove( projExpr );
                            fPlan.add( root );
                        } else {
                            fPlan.add( root );
                            Operator pred1 = predecessors.get( 0 );
                            Pair<Integer, Integer> pair = fPlan.disconnect( pred1, projExpr );
                            fPlan.connect( pred1, pair.first, root, pair.second );
                            fPlan.remove( projExpr );
                        }
                    }
                }

                // Now, reset the projection expressions in the new filter plan.
                sinks = fPlan.getSinks();
                for( Operator sink : sinks ) {
                    if( sink instanceof ProjectExpression ) {
                        ProjectExpression projE = (ProjectExpression)sink;
                         projE.setAttachedRelationalOp( newFilter );
                         projE.setInputNum( 0 );
                         projE.setColNum( projE.getColNum() - columnOffset );
                    }
                 }
                newFilter.setFilterPlan( fPlan );

                insertFilter( pred, predecessor, newFilter );
            }
        }

        // check if a relational operator contains all of the specified uids
        private boolean hasAll(LogicalRelationalOperator op, Set<Long> uids) throws FrontendException {
            LogicalSchema schema = op.getSchema();
            if (schema==null)
                return false;
            for(long uid: uids) {
                if (schema.findField(uid) == -1) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public OperatorPlan reportChanges() {
            return currentPlan;
        }

        // Insert the filter in between the given two operators.
        private void insertFilter(Operator prev, Operator predecessor, LOFilter filter)
        throws FrontendException {
            Pair<Integer, Integer> p3 = currentPlan.disconnect( prev, predecessor );
            currentPlan.connect( prev, p3.first, filter, 0 );
            currentPlan.connect( filter, 0, predecessor, p3.second );
        }

        // Identify those among preds that will need to have a filter between it and the predecessor.
        private Map<Integer, Operator> findInputsToAddFilter(LogicalExpressionPlan filterPlan, Operator predecessor,
                List<Operator> preds) throws FrontendException {
            Map<Integer, Operator> inputs = new HashMap<Integer, Operator>();

            if( predecessor instanceof LOUnion || predecessor instanceof LOCogroup ) {
                for( int i = 0; i < preds.size(); i++ ) {
                    inputs.put( i, preds.get( i ) );
                }
                return inputs;
            }

            // collect all uids used in the filter plan
            Set<Long> uids = collectUidFromExpPlan(filterPlan);
            boolean[] innerFlags = null;
            boolean isInner = true;
            if (predecessor instanceof LOJoin) {
                innerFlags = ((LOJoin)predecessor).getInnerFlags();
                for (boolean inner : innerFlags) {
                    if (!inner) {
                        isInner = false;
                        break;
                    }
                }
            }

            // Find the predecessor of join that contains all required uids.
            for(int j=0; j<preds.size(); j++) {
                // Filter can push to LOJoin outer branch, but no inner branch
                if( hasAll((LogicalRelationalOperator)preds.get(j), uids) &&
                        (predecessor instanceof LOCross || predecessor instanceof LOJoin && (isInner || innerFlags[j]))) {
                    Operator input = preds.get(j);
                    subPlan.add(input);
                    inputs.put( j, input );
                }
            }
            return  inputs;
        }
    }

    @Override
    protected OperatorPlan buildPattern() {
        LogicalPlan plan = new LogicalPlan();
        LogicalRelationalOperator op1 = new LOFilter(plan);
        plan.add( op1 );

        return plan;
    }

}

