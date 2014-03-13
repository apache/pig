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
package org.apache.pig.newplan.logical.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;

/**
 * It's generally a good idea to do flattens as late as possible as
 * they tend to generate more rows (and so more I/O). This optimization
 * swaps the order of SORTs, CROSSes and JOINs that come after 
 * FOREACH..GENERATE..FLATTENs. FILTERs are re-ordered by the 
 * {@link FilterAboveForeach} rule so are ignored here. 
 */
public class PushDownForEachFlatten extends Rule {

    public PushDownForEachFlatten(String name) {
        super( name, false );
    }

    @Override
    protected OperatorPlan buildPattern() {
        LogicalPlan plan = new LogicalPlan();
        LogicalRelationalOperator foreach = new LOForEach(plan);
        plan.add( foreach );
        return plan;
    }

    @Override
    public Transformer getNewTransformer() {
        return new PushDownForEachFlattenTransformer();
    }
    
    class PushDownForEachFlattenTransformer extends Transformer {
        private OperatorSubPlan subPlan;

        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {
            // the foreach with flatten can be swapped with an order by
            // as the order by will have lesser number of records to sort
            // also the sort does not alter the records that are processed
            
            // the foreach with flatten can be pushed down a cross or a join
            // for the same reason. In this case the foreach has to be first
            // unflattened and then a new foreach has to be inserted after
            // the cross or join. In both cross and foreach the actual columns
            // from the foreach are not altered but positions might be changed
            
            // in the case of union the column is transformed and as a result
            // the foreach flatten cannot be pushed down
            
            // for distinct the output before flattening and the output
            // after flattening might be different. For example, consider
            // {(1), (1)}. Distinct of this bag is still {(1), (1)}.
            // distinct(flatten({(1), (1)})) is (1). However,
            // flatten(distinct({(1), (1)})) is (1), (1)
            
            // in both cases correctness is not affected
            
            LOForEach foreach = (LOForEach)matched.getSources().get(0);
            LOGenerate gen = OptimizerUtils.findGenerate( foreach );
            
            if( !OptimizerUtils.hasFlatten( gen ) )
                return false;
            
            // If a foreach contains a nondeterministic udf, we shouldn't push it down.
            for (LogicalExpressionPlan p : gen.getOutputPlans()) {
                if (OptimizerUtils.planHasNonDeterministicUdf(p))
                    return false;
            }
            
            List<Operator> succs = currentPlan.getSuccessors( foreach );
            if( succs == null || succs.size() != 1 )
                return false;
            
            List<Long> uids = getNonFlattenFieldUids( gen );

            Operator succ = succs.get( 0  );
            if( !( succ instanceof LOSort || succ instanceof LOJoin || succ instanceof LOCross ) )
                return false;
            
            if( succ instanceof LOSort ) {
                // Check if the expressions for the foreach generate are purely projection including flatten fields.
                List<LogicalExpressionPlan> exprs = gen.getOutputPlans();
                for( LogicalExpressionPlan expr : exprs ) {
                    if( !isPureProjection( expr ) )
                        return false;
                }

                // Check if flatten fields are required by the successor.
                LOSort sort = (LOSort)succ;
                List<LogicalExpressionPlan> exps = sort.getSortColPlans();
                for( int i = 0; i < exps.size(); i++ ) {
                    LogicalExpressionPlan exp = exps.get( i );
                    ProjectExpression proj = (ProjectExpression)exp.getOperators().next();
                    if( !uids.contains( proj.getFieldSchema().uid ) )
                        return false;
                }

                return true;
            } else {
                List<Operator> preds = currentPlan.getPredecessors( succ );
                
                // We do not optimize if peer is ForEach with flatten. This is 
                // a simplification, may change in the future.
                for( Operator op : preds ) {
                    if( op == foreach )
                        continue;
                    else if( op instanceof LOForEach && 
                            OptimizerUtils.hasFlatten( OptimizerUtils.findGenerate( (LOForEach)op ) ) )
                        return false;
                }
                
                if( ( (LogicalRelationalOperator)succ ).getSchema() == null )
                    return false;
                
                if( succ instanceof LOCross ) {
                    return true;
                } else {
                    LOJoin join = (LOJoin)succ;
                    for( int i = 0; i < preds.size(); i++ ) {
                        Operator op = preds.get( i );
                        if( op == foreach ) {
                            Collection<LogicalExpressionPlan> exprs = join.getJoinPlan( i );
                            for( LogicalExpressionPlan expr : exprs ) {
                                List<ProjectExpression> projs = getProjectExpressions( expr );
                                for( ProjectExpression proj : projs ) {
                                    if( !uids.contains( proj.getFieldSchema().uid ) ) {
                                        return false;
                                    }
                                }
                            }
                            break;
                        }
                    }
                    return true;
                }
            }
        } 
        
        private List<ProjectExpression> getProjectExpressions(LogicalExpressionPlan expr) {
            List<Operator> ops = expr.getSinks();
            List<ProjectExpression> projs = new ArrayList<ProjectExpression>( ops.size() );
            for( Operator op : ops ) {
                if( op instanceof ProjectExpression ) {
                    projs.add( (ProjectExpression)op );
                }
            }
            return projs;
        }

        private List<Long> getNonFlattenFieldUids(LOGenerate gen) throws FrontendException {
            List<Long> uids = new ArrayList<Long>();
            
            List<LogicalExpressionPlan> exprs = gen.getOutputPlans();
            for( int i = 0; i < exprs.size(); i++ ) {
                LogicalExpressionPlan expr = exprs.get( i );
                if( gen.getFlattenFlags()[i] )
                    continue;
                LogicalExpression e = (LogicalExpression)expr.getSources().get( 0 );
                uids.add( e.getFieldSchema().uid );
            }
            
            return uids;
        }
        
        /**
         * Check if the given expression contains only a pure projection.
         * For instance $0 is legal, f1 is legal, but 5 + $2 is not legal.
         * (int)f1 is not legal either.
         */
        private boolean isPureProjection(LogicalExpressionPlan expr) {
            if (expr.size()!=1)
                return false;
            if (!(expr.getSinks().get(0) instanceof ProjectExpression))
                return false;
            return true;
        }
        
        @Override
        public OperatorPlan reportChanges() {
            return subPlan;
        }

        @Override
        public void transform(OperatorPlan matched) throws FrontendException {
            subPlan = new OperatorSubPlan( currentPlan );
            
            LOForEach foreach = (LOForEach)matched.getSources().get(0);
            Operator next = currentPlan.getSuccessors( foreach ).get(0);
            if( next instanceof LOSort ) {
                Operator pred = currentPlan.getPredecessors( foreach ).get( 0 );
                List<Operator> succs = new ArrayList<Operator>();
                succs.addAll(currentPlan.getSuccessors( next ));
                Pair<Integer, Integer> pos1 = currentPlan.disconnect( pred, foreach );
                Pair<Integer, Integer> pos2 = currentPlan.disconnect( foreach, next );
                currentPlan.connect( pred, pos1.first, next, pos2.second );

                if( succs != null ) {
                    for( Operator succ : succs ) {
                        Pair<Integer, Integer> pos = currentPlan.disconnect( next, succ );
                        currentPlan.connect( next, pos.first, foreach, 0 );
                        currentPlan.connect( foreach, 0, succ, pos.second );
                    }
                } else {
                    currentPlan.connect( next, foreach );
                }
                subPlan.add(foreach);
                subPlan.add(next);
            } else if( next instanceof LOCross || next instanceof LOJoin ) {
                List<Operator> preds = currentPlan.getPredecessors( next );
                List<Integer> fieldsToBeFlattaned = new ArrayList<Integer>();
                Map<Integer, LogicalSchema> cachedUserDefinedSchema = new HashMap<Integer, LogicalSchema>();
                boolean[] flags = null;
                int fieldCount = 0;
                for( Operator op : preds ) {
                    if( op == foreach ) {
                        LOGenerate gen = OptimizerUtils.findGenerate( foreach );
                        flags = gen.getFlattenFlags();
                        for( int i = 0; i < flags.length; i++ ) {
                            if( flags[i] ) {
                                fieldsToBeFlattaned.add(fieldCount);
                                if (gen.getUserDefinedSchema()!=null && gen.getUserDefinedSchema().get(i)!=null) {
                                    cachedUserDefinedSchema.put(fieldCount, gen.getUserDefinedSchema().get(i));
                                    cachedUserDefinedSchema.get(fieldCount).mergeUid(gen.getOutputPlanSchemas().get(i));
                                    gen.getUserDefinedSchema().set(i, null);
                                }
                                fieldCount++;
                            } else {
                                fieldCount++;
                            }
                        }
                    } else {
                        fieldCount += ( (LogicalRelationalOperator)op ).getSchema().size();
                    }
                }
                
                
                boolean[] flattenFlags = new boolean[fieldCount];
                List<LogicalSchema> mUserDefinedSchema = null;
                if (cachedUserDefinedSchema!=null) {
                    mUserDefinedSchema = new ArrayList<LogicalSchema>();
                    for (int i=0;i<fieldCount;i++)
                        mUserDefinedSchema.add(null);
                }
                for( Integer i : fieldsToBeFlattaned ) {
                    flattenFlags[i] = true;
                    if (cachedUserDefinedSchema.containsKey(i)) {
                        mUserDefinedSchema.set(i, cachedUserDefinedSchema.get(i));
                    }
                }
                
                // Now create a new foreach after cross/join and insert it into the plan.
                LOForEach newForeach = new LOForEach( currentPlan );
                LogicalPlan innerPlan = new LogicalPlan();
                List<LogicalExpressionPlan> exprs = new ArrayList<LogicalExpressionPlan>( fieldCount );
                LOGenerate gen = new LOGenerate( innerPlan, exprs, flattenFlags );
                if (mUserDefinedSchema!=null)
                    gen.setUserDefinedSchema(mUserDefinedSchema);
                innerPlan.add( gen );
                newForeach.setInnerPlan( innerPlan );
                for( int i = 0; i < fieldCount; i++ ) {
                    LogicalExpressionPlan expr = new LogicalExpressionPlan();
                    expr.add( new ProjectExpression( expr, i, -1, gen ) );
                    exprs.add( expr );
                    
                    LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, newForeach, i);
                    innerPlan.add(innerLoad);
                    innerPlan.connect(innerLoad, gen);
                }
                
                newForeach.setAlias(((LogicalRelationalOperator)next).getAlias());
                
                Operator opAfterX = null;
                List<Operator> succs = currentPlan.getSuccessors( next );
                if( succs == null || succs.size() == 0 ) {
                    currentPlan.add( newForeach );
                    currentPlan.connect( next, newForeach );
                } else {
                    opAfterX = succs.get( 0 );
                    currentPlan.insertBetween(next, newForeach, opAfterX);
                }
                
                // Finally remove flatten flags from the original foreach and regenerate schemas for those impacted.
                for( int i = 0; i < flags.length; i++ ) {
                    flags[i] = false;
                }
                
                subPlan.add(foreach);
                subPlan.add(next);
                subPlan.add(newForeach);
            }
        }
    }
}
