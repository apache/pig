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

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.Expression.BinaryExpression;
import org.apache.pig.Expression.Column;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.FilterExtractor;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.PartitionFilterExtractor;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;

public class PartitionFilterOptimizer extends Rule {
    private String[] partitionKeys;

    /**
     * a reference to the LoadMetada implementation
     */
    private LoadMetadata loadMetadata;

    /**
     * a reference to the LoadFunc implementation
     */
    private LoadFunc loadFunc;

    private LOLoad loLoad;
    private LOFilter loFilter;

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

    public PartitionFilterOptimizer(String name) {
        super( name, false );
    }

    @Override
    protected OperatorPlan buildPattern() {
        // match each foreach.
        LogicalPlan plan = new LogicalPlan();
        LogicalRelationalOperator load = new LOLoad (null, plan);
        plan.add( load );
//        LogicalRelationalOperator filter = new LOFilter( plan );
//        plan.add( filter );
//        plan.connect( load, filter );
        return plan;
    }

    @Override
    public Transformer getNewTransformer() {
        return new PartitionFilterPushDownTransformer();
    }

    public class PartitionFilterPushDownTransformer extends Transformer {
        protected OperatorSubPlan subPlan;

        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {
            loLoad = (LOLoad)matched.getSources().get(0);
            // Match filter.
            List<Operator> succeds = currentPlan.getSuccessors( loLoad );
            if( succeds == null || succeds.size() == 0 || !( succeds.get(0) instanceof LOFilter ) )
                return false;
            loFilter =  (LOFilter)succeds.get(0);

            // Filter has dependency other than load, skip optimization
            if (currentPlan.getSoftLinkPredecessors(loFilter)!=null)
                return false;

            // we have to check more only if LoadFunc implements LoadMetada
            loadFunc = loLoad.getLoadFunc();
            if(!( loadFunc instanceof LoadMetadata ) ) {
                return false;
            }

            loadMetadata = (LoadMetadata)loadFunc;
            try {
				partitionKeys = loadMetadata.getPartitionKeys(
						loLoad.getFileSpec().getFileName(), new Job( loLoad.getConfiguration() ) );
			} catch (IOException e) {
				throw new FrontendException( e );
			}
            if( partitionKeys == null || partitionKeys.length == 0 ) {
                return false;
            }

            return true;
        }

        @Override
        public OperatorPlan reportChanges() {
            return subPlan;
        }

        @Override
        public void transform(OperatorPlan matched) throws FrontendException {
        	subPlan = new OperatorSubPlan( currentPlan );

        	setupColNameMaps();

            FilterExtractor filterFinder = new PartitionFilterExtractor(loFilter.getFilterPlan(),
                    getMappedKeys(partitionKeys));
            filterFinder.visit();
            Expression partitionFilter = filterFinder.getPushDownExpression();

            if(partitionFilter != null) {
                // the column names in the filter may be the ones provided by
                // the user in the schema in the load statement - we may need
                // to replace them with partition column names as given by
                // LoadFunc.getSchema()
                updateMappedColNames(partitionFilter);
                try {
                    loadMetadata.setPartitionFilter(partitionFilter);
                } catch (IOException e) {
                    throw new FrontendException( e );
                }
                if(filterFinder.isFilterRemovable()) {
                    currentPlan.removeAndReconnect( loFilter );
                } else {
                    loFilter.setFilterPlan(filterFinder.getFilteredPlan());
                }
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
         * @param partitionKeys
         * @return
         */
        protected List<String> getMappedKeys(String[] partitionKeys) {
            List<String> mappedKeys = new ArrayList<String>(partitionKeys.length);
            for (int i = 0; i < partitionKeys.length; i++) {
                mappedKeys.add(colNameMap.get(partitionKeys[i]));
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
