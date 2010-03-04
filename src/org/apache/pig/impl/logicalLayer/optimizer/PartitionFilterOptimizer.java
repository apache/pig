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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigException;
import org.apache.pig.Expression.BinaryExpression;
import org.apache.pig.Expression.Column;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOFilter;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.PColFilterExtractor;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.optimizer.OptimizerException;

/**
 * When the load statement in a pig script is loading a table from a meta data
 * system (like owl), the load can be followed by a filter which can contain
 * conditions on partition columns. This filter can also contain conditions on
 * non partition columns. This optimizer looks at the logical plan and checks if
 * there is a load followed by such a filter which has conditions on partition
 * columns. If so, it extracts the conditions on partition columns out of the
 * filter.
 */
public class PartitionFilterOptimizer extends
        LogicalTransformer {
    
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
     * to ensure we only do the optimization once for performance reasons
     */
    private Set<LogicalOperator> alreadyChecked = new HashSet<LogicalOperator>();
    
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
    

    protected PartitionFilterOptimizer(LogicalPlan plan) {
        super(plan);
    }

    @Override
    public boolean check(List<LogicalOperator> nodes) throws OptimizerException 
    {
        if((nodes == null) || (nodes.size() <= 0)) {
            int errCode = 2052;
            String msg = "Internal error. Cannot retrieve operator from null " +
            		"or empty list.";
            throw new OptimizerException(msg, errCode, PigException.BUG);
        }
        if(nodes.size() != 1|| !(nodes.get(0) instanceof LOLoad)) {
            return false;
        }
        if (!alreadyChecked.add(nodes.get(0))) {
            return false;
        }
        loLoad = (LOLoad)nodes.get(0);
        List<LogicalOperator> sucs = mPlan.getSuccessors(loLoad);
        if(sucs == null || sucs.size() != 1 || !(sucs.get(0) instanceof LOFilter)) {
            return false;
        }
        loFilter = (LOFilter)sucs.get(0);
        
        // we have to check more only if LoadFunc implements LoadMetada
        loadFunc = loLoad.getLoadFunc();
        if(!(loadFunc instanceof LoadMetadata)) {
            return false;
        }
        loadMetadata = (LoadMetadata)loadFunc;
        try {
            partitionKeys = loadMetadata.getPartitionKeys(
                    loLoad.getInputFile().getFileName(), new Job(loLoad.getConfiguration()));
            if(partitionKeys == null || partitionKeys.length == 0) {
                return false;
            }
        } catch (IOException e) {
            int errCode = 2209;
            throw new OptimizerException(
                    "Internal error while processing any partition filter " +
                    "conditions in the filter after the load" ,
                    errCode,
                    PigException.BUG
            );
        }
        
        // we found a load-filter pattern where the load returns partition keys
        return true;
    }

    @Override
    public void transform(List<LogicalOperator> nodes)
            throws OptimizerException {
        try {
            setupColNameMaps();
            PColFilterExtractor pColFilterFinder = new PColFilterExtractor(
                    loFilter.getComparisonPlan(), getMappedKeys(partitionKeys));
            pColFilterFinder.visit();
            Expression partitionFilter = pColFilterFinder.getPColCondition();
            if(partitionFilter != null) {
                // the column names in the filter may be the ones provided by
                // the user in the schema in the load statement - we may need
                // to replace them with partition column names as given by
                // LoadFunc.getSchema()
                updateMappedColNames(partitionFilter);
                loadMetadata.setPartitionFilter(partitionFilter);
                if(pColFilterFinder.isFilterRemovable()) {
                    // remove this filter from the plan                  
                    mPlan.removeAndReconnect(loFilter);
                }
            }
        } catch (Exception e) {
            int errCode = 2209;
            throw new OptimizerException(
                    "Internal error while processing any partition filter " +
                    "conditions in the filter after the load:" ,
                    errCode,
                    PigException.BUG,
                    e
            );
        }
    }
    
    

    /**
     * @param expr
     */
    private void updateMappedColNames(Expression expr) {
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
    private List<String> getMappedKeys(String[] partitionKeys) {
        List<String> mappedKeys = new ArrayList<String>(partitionKeys.length);
        for (int i = 0; i < partitionKeys.length; i++) {
            mappedKeys.add(colNameMap.get(partitionKeys[i]));
        }
        return mappedKeys;
    }

    
    
    /**
     * @throws FrontendException 
     * 
     */
    private void setupColNameMaps() throws FrontendException {
        Schema loadFuncSchema = loLoad.getDeterminedSchema();
        Schema loLoadSchema = loLoad.getSchema();
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
