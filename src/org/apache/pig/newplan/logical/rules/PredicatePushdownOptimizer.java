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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.Expression;
import org.apache.pig.Expression.BinaryExpression;
import org.apache.pig.Expression.Column;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPredicatePushdown;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.PredicatePushDownFilterExtractor;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;

public class PredicatePushdownOptimizer extends Rule {

    private static final Log LOG = LogFactory.getLog(PredicatePushdownOptimizer.class);

    public PredicatePushdownOptimizer(String name) {
        super(name, false);
    }

    @Override
    protected OperatorPlan buildPattern() {
        LogicalPlan plan = new LogicalPlan();
        LogicalRelationalOperator load = new LOLoad(null, plan);
        plan.add(load);
        return plan;
    }

    @Override
    public Transformer getNewTransformer() {
        return new PredicatePushDownTransformer();
    }

    class PredicatePushDownTransformer extends Transformer {

        private LOLoad loLoad;
        private LOFilter loFilter;

        private LoadFunc loadFunc;
        private LoadPredicatePushdown loadPredPushdown;

        private List<String> predicateFields;

        /**
         * a map between column names as reported in
         * {@link LoadMetadata#getSchema(String, org.apache.hadoop.conf.Configuration)}
         * and as present in {@link LOLoad#getSchema()}. The two will be different
         * when the user has provided a schema in the load statement
         */
        private Map<String, String> colNameMap = new HashMap<String, String>();

        /**
         * a map between column nameas as present in {@link LOLoad#getSchema()} and
         * as reported in
         * {@link LoadMetadata#getSchema(String, org.apache.hadoop.conf.Configuration)}.
         * The two will be different when the user has provided a schema in the
         * load statement.
         */
        private Map<String, String> reverseColNameMap = new HashMap<String, String>();

        private OperatorSubPlan subPlan;

        private boolean planChanged;

        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {
            loLoad = (LOLoad)matched.getSources().get(0);
            // Match filter.
            List<Operator> succeds = currentPlan.getSuccessors( loLoad );
            if( succeds == null || succeds.size() == 0 || !( succeds.get(0) instanceof LOFilter ) )
                return false;
            loFilter = (LOFilter) succeds.get(0);

            // Filter has dependency other than load, skip optimization
            if (currentPlan.getSoftLinkPredecessors(loFilter) != null)
                return false;

            // we have to check more only if LoadFunc implements LoadPredicatePushdown
            loadFunc = loLoad.getLoadFunc();
            if (!(loadFunc instanceof LoadPredicatePushdown)) {
                return false;
            }

            loadPredPushdown = (LoadPredicatePushdown) loadFunc;
            try {
                predicateFields = loadPredPushdown.getPredicateFields(loLoad.getFileSpec()
                        .getFileName(), new Job(loLoad.getConfiguration()));
            } catch (IOException e) {
                throw new FrontendException(e);
            }
            if (predicateFields == null || predicateFields.size() == 0) {
                return false;
            }

            return true;
        }

        @Override
        public OperatorPlan reportChanges() {
            // Return null in case there is no predicate pushdown filter extracted or it is just
            // a hint which means the plan hasn't changed.
            // If not return the modified plan which has filters removed.
            return planChanged ? subPlan : null;
        }

        @Override
        public void transform(OperatorPlan matched) throws FrontendException {
            subPlan = new OperatorSubPlan( currentPlan );

            setupColNameMaps();

            PredicatePushDownFilterExtractor filterFinder = new PredicatePushDownFilterExtractor(
                    loFilter.getFilterPlan(), getMappedKeys( predicateFields ), loadPredPushdown.getSupportedExpressionTypes() );
            filterFinder.visit();
            Expression pushDownPredicate = filterFinder.getPushDownExpression();

            if(pushDownPredicate != null) {
                // the column names in the filter may be the ones provided by
                // the user in the schema in the load statement - we may need
                // to replace them with partition column names as given by
                // LoadFunc.getSchema()
                updateMappedColNames(pushDownPredicate);
                try {
                    LOG.info("Setting predicate pushdown filter [" + pushDownPredicate + "] on loader " + loadPredPushdown);
                    loadPredPushdown.setPushdownPredicate(pushDownPredicate);
                } catch (IOException e) {
                    throw new FrontendException( e );
                }

                //TODO: PIG-4093
                /*
                if (loadPredPushdown.removeFilterPredicateFromPlan()) {
                    planChanged = true;
                    if(filterFinder.isFilterRemovable()) {
                        currentPlan.removeAndReconnect( loFilter );
                    } else {
                        loFilter.setFilterPlan(filterFinder.getFilteredPlan());
                    }
                }
                */
            }
        }

        protected void updateMappedColNames(Expression expr) {
            if(expr instanceof BinaryExpression) {
                updateMappedColNames(((BinaryExpression) expr).getLhs());
                updateMappedColNames(((BinaryExpression) expr).getRhs());
            } else if (expr instanceof Column) {
                Column col = (Column) expr;
                col.setName(reverseColNameMap.get(col.getName()));
            }
        }

        /**
         * The partition keys in the argument are as reported by
         * {@link LoadMetadata#getPartitionKeys(String, org.apache.hadoop.conf.Configuration)}.
         * The user may have renamed these by providing a schema with different names
         * in the load statement - this method will replace the former names with
         * the latter names.
         * @param predicateFields
         * @return
         */
        protected List<String> getMappedKeys(List<String> predicateFields) {
            List<String> mappedKeys = new ArrayList<String>(predicateFields.size());
            for (int i = 0; i < predicateFields.size(); i++) {
                mappedKeys.add(colNameMap.get(predicateFields.get(i)));
            }
            return mappedKeys;
        }

        protected void setupColNameMaps() throws FrontendException {
            LogicalSchema loLoadSchema = loLoad.getSchema();
            LogicalSchema loadFuncSchema = loLoad.getDeterminedSchema();
             for(int i = 0; i < loadFuncSchema.size(); i++) {
                colNameMap.put(loadFuncSchema.getField(i).alias,
                        (i < loLoadSchema.size() ? loLoadSchema.getField(i).alias :
                            loadFuncSchema.getField(i).alias));

                reverseColNameMap.put((i < loLoadSchema.size() ? loLoadSchema.getField(i).alias :
                            loadFuncSchema.getField(i).alias),
                            loadFuncSchema.getField(i).alias);
            }
        }

    }

}
