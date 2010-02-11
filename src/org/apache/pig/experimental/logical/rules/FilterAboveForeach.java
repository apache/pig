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
package org.apache.pig.experimental.logical.rules;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.pig.data.DataType;
import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.ProjectExpression;
import org.apache.pig.experimental.logical.relational.LOFilter;
import org.apache.pig.experimental.logical.relational.LOForEach;
import org.apache.pig.experimental.logical.relational.LOGenerate;
import org.apache.pig.experimental.logical.relational.LogicalPlan;
import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.logical.relational.LogicalSchema;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.OperatorSubPlan;
import org.apache.pig.experimental.plan.optimizer.Rule;
import org.apache.pig.experimental.plan.optimizer.Transformer;
import org.apache.pig.impl.util.Pair;

public class FilterAboveForeach extends Rule {

    public FilterAboveForeach(String n) {
        super(n);
    }

    @Override
    protected OperatorPlan buildPattern() {
        // the pattern that this rule looks for
        // is foreach -> flatten -> filter
        LogicalPlan plan = new LogicalPlan();
        LogicalRelationalOperator foreach = new LOForEach(plan);
        LogicalRelationalOperator filter = new LOFilter(plan);
        
        plan.add(foreach);
        plan.add(filter);
        plan.connect(foreach, filter);

        return plan;
    }

    @Override
    public Transformer getNewTransformer() {
        return new FilterAboveFlattenTransformer();
    }
    
    public class FilterAboveFlattenTransformer extends Transformer {

        LOFilter filter = null;
        LOForEach foreach = null;
        LogicalRelationalOperator forEachPred = null;
        OperatorSubPlan subPlan = null;
        
        @Override
        public boolean check(OperatorPlan matched) throws IOException {
            Iterator<Operator> iter = matched.getOperators();
            while( iter.hasNext() ) {
                Operator op = iter.next();
                if( op instanceof LOForEach ) {
                    foreach = (LOForEach)op;
                    break;
                }
            }
            
            // This would be a strange case
            if( foreach == null ) return false;
            
            List<Operator> sinks = foreach.getInnerPlan().getSinks();            
            if( ! ( sinks.size() == 1 && (sinks.get(0) instanceof LOGenerate ) ) ) {
                return false;
            }

//            LOGenerate generate = (LOGenerate)sinks.get(0);
//            // We check if we have any flatten
//            // Other cases are handled by other Optimizers
//            boolean hasFlatten = false;            
//            for( boolean flattenFlag : generate.getFlattenFlags() ) {
//                if( flattenFlag ) {
//                    hasFlatten = true;
//                    break;
//                }
//            }
//
//            if( !hasFlatten )
//                return false;             
            
            iter = matched.getOperators();
            while( iter.hasNext() ) {
                Operator op = iter.next();
                if( ( op instanceof LOFilter ) ) {
                    filter = (LOFilter)op;
                    break;
                }
            }
            
            // This is for cheating, we look up more than one filter in the plan
            while( filter != null ) {
                // Get uids of Filter
                Set<Long> uids = getFilterProjectionUids(filter);


                // See if the previous operators have uids from project
                List<Operator> preds = currentPlan.getPredecessors(foreach);            
                for(int j=0; j< preds.size(); j++) {
                    LogicalRelationalOperator logRelOp = (LogicalRelationalOperator)preds.get(j);
                    if (hasAll( logRelOp, uids) ) {
                        // If any of the uids are of complex type then we 
                        // cannot think about moving this filter.
                        if( containsComplexType(logRelOp.getSchema(), uids ) ) {
                            break;
                        }
                        forEachPred = (LogicalRelationalOperator) preds.get(j);
                        return true;
                    }
                }
                
                // Chances are there are filters below this filter which can be
                // moved up. So searching for those filters
                List<Operator> successors = currentPlan.getSuccessors(filter);
                if( successors != null && successors.size() > 0 && 
                        successors.get(0) instanceof LOFilter ) {
                    filter = (LOFilter)successors.get(0);
                } else {
                    filter = null;
                }
            }
            return false;            
        }
        
        private Set<Long> getFilterProjectionUids( LOFilter filter ) {
            Set<Long> uids = new HashSet<Long>();
            if( filter != null ) {
                LogicalExpressionPlan filterPlan = filter.getFilterPlan();
                Iterator<Operator> iter = filterPlan.getOperators();            
                Operator op = null;
                while( iter.hasNext() ) {
                    op = iter.next();
                    if( op instanceof ProjectExpression ) {
                        uids.add(((ProjectExpression)op).getUid() );
                    }
                }
            }
            return uids;
        }
        
        // check if a relational operator contains all of the specified uids
        private boolean hasAll(LogicalRelationalOperator op, Set<Long> uids) {
            LogicalSchema schema = op.getSchema();
            List<LogicalSchema.LogicalFieldSchema> fields = schema.getFields();
            Set<Long> all = new HashSet<Long>();
            for(LogicalSchema.LogicalFieldSchema f:fields) {
                all.add(f.uid);
            }
            return all.containsAll(uids);
        }
        
        /**
         * This function checks if any of the fields mentioned are a Bug or Tuple.
         * If so we cannot move the filter above the operator having the schema
         * @param schema Schema of the operator we are investigating
         * @param uids Uids of the fields we are checking for
         * @return true if one of the uid belong to a complex type
         */
        private boolean containsComplexType(LogicalSchema schema, Set<Long> uids) {
            List<LogicalSchema.LogicalFieldSchema> fields = schema.getFields();

            for(LogicalSchema.LogicalFieldSchema f:fields) {
                if ( ( f.type == DataType.BAG || f.type == DataType.TUPLE ) ) {
                    if( uids.contains( f.uid ) ) {
                        return true;
                    }
                    if( f.schema != null && containsComplexType(f.schema, uids) ) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public OperatorPlan reportChanges() {            
            return subPlan;
        }

        @Override
        public void transform(OperatorPlan matched) throws IOException {
            
            List<Operator> opSet = currentPlan.getPredecessors(filter);
            if( ! ( opSet != null && opSet.size() > 0 ) ) {
                return;
            }
            Operator filterPred = opSet.get(0);
            
            opSet = currentPlan.getSuccessors(filter);
            if( ! ( opSet != null && opSet.size() > 0 ) ) {
                return;
            }
            Operator filterSuc = opSet.get(0);
            
            subPlan = new OperatorSubPlan(currentPlan);
            
            // Steps below do the following
            /*
             *          ForEachPred
             *               |
             *            ForEach
             *               |
             *             Filter*
             *               |
             *           FilterPred                 
             *  ( has to be a Filter or ForEach )
             *               |
             *             Filter
             *               |
             *            FilterSuc
             *              
             *               |
             *               |
             *        Transforms into 
             *               |
             *              \/            
             *                      
             *            ForEachPred
             *               |
             *            Filter
             *               |
             *            ForEach
             *               |
             *             Filter*
             *               |
             *           FilterPred                 
             *  ( has to be a Filter or ForEach )
             *               |
             *            FilterSuc
             */
            
            Pair<Integer, Integer> forEachPredPlaces = currentPlan.disconnect(forEachPred, foreach);
            Pair<Integer, Integer> filterPredPlaces = currentPlan.disconnect(filterPred, filter);
            Pair<Integer, Integer> filterSucPlaces = currentPlan.disconnect(filter, filterSuc);
            
            currentPlan.connect(forEachPred, forEachPredPlaces.first, filter, filterPredPlaces.second);
            currentPlan.connect(filter, filterSucPlaces.first, foreach, forEachPredPlaces.second);
            currentPlan.connect(filterPred, filterPredPlaces.first, filterSuc, filterSucPlaces.second);
            
            subPlan.add(forEachPred);
            subPlan.add(foreach);
            subPlan.add(filterPred);
            subPlan.add(filter);
            subPlan.add(filterSuc);
        }
    }

}
