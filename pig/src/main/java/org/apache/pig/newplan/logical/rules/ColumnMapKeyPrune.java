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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.optimizer.Transformer;

/**
 * This Rule prunes columns and map keys and set to loader. This rule depends
 * on MapKeysPruneHelper to calculate what keys are required for a loader,
 * and ColumnPruneHelper to calculate the required columns for a loader. Then
 * it combines the map keys and columns info to set into the loader.
 */
public class ColumnMapKeyPrune extends WholePlanRule {
    private boolean hasRun;
    
    public ColumnMapKeyPrune(String n) {
        super(n, false);
        hasRun = false;
    }

    @Override
    public Transformer getNewTransformer() {
        return new ColumnMapKeyPruneTransformer();
    }
    
    public class ColumnMapKeyPruneTransformer extends Transformer {
        private MapKeysPruneHelper mapKeyHelper;
        private ColumnPruneHelper columnHelper;
        private boolean columnPrune;
        private boolean mapKeyPrune;

        /*
         * This is a map of of required columns and map keys for each LOLoad        
         * RequiredMapKeys --> Map<Integer, Set<String> >
         * RequiredColumns --> Set<Integer>
         * 
         * The integer are column indexes.
         */
        private Map<LOLoad,Pair<Map<Integer,Set<String>>,Set<Integer>>> requiredItems = 
            new HashMap<LOLoad,Pair<Map<Integer,Set<String>>,Set<Integer>>>();
        
        @Override
        public boolean check(OperatorPlan matched) throws FrontendException {
            // only run this rule once
            if (hasRun) {
                return false;
            }
            
            hasRun = true;
            mapKeyHelper = new MapKeysPruneHelper(matched);
            columnHelper = new ColumnPruneHelper(matched);
            
            // check if map keys can be pruned
            mapKeyPrune = mapKeyHelper.check();
            // check if columns can be pruned
            columnPrune = columnHelper.check();
            
            return mapKeyPrune || columnPrune;
        }

        @Override
        public OperatorPlan reportChanges() {
            return currentPlan;
        }
        
        @SuppressWarnings("unchecked")
        private void merge() throws FrontendException {            
            // combine annotations
            for( Operator source : currentPlan.getSources() ) {
                Map<Integer,Set<String>> mapKeys = 
                    (Map<Integer, Set<String>>) source.getAnnotation(MapKeysPruneHelper.REQUIRED_MAPKEYS);
                Set<Integer> requiredColumns = null;
                if (source.getAnnotation(ColumnPruneHelper.REQUIREDCOLS) != null) {
                    requiredColumns = new HashSet<Integer>((Set<Integer>) source.getAnnotation(ColumnPruneHelper.REQUIREDCOLS));
                }
                
                // We dont have any information so skip
                if( requiredColumns == null && mapKeys == null ) {
                    continue;
                }
                                
                if( requiredColumns != null && mapKeys != null ) { 

                    Set<Integer> duplicatedCols = new HashSet<Integer>();

                    // Remove the columns already marked by MapKeys
                    for( Integer col : requiredColumns ) {
                        if( mapKeys.containsKey(col) ) {
                            duplicatedCols.add(col);
                        }
                    }
                    requiredColumns.removeAll(duplicatedCols);
                } else if ( mapKeys != null && requiredColumns == null ) {
                    // This is the case where only mapKeys can be pruned. And none
                    // of the columns can be pruned. So we add all columns to the
                    // requiredcolumns part
                    requiredColumns = new HashSet<Integer>();
                    for(int i = 0; i < ((LogicalRelationalOperator)source).getSchema().size(); i++ ) {
                        if( !mapKeys.containsKey(i) ) {
                            requiredColumns.add(i);
                        }
                    }
                }

                requiredItems.put((LOLoad) source, new Pair<Map<Integer,Set<String>>,Set<Integer>>(mapKeys, requiredColumns));
            }         
        }

        @Override
        public void transform(OperatorPlan matched) throws FrontendException {        	            
            merge();
            
            ColumnPruneVisitor columnPruneVisitor = new ColumnPruneVisitor(currentPlan, requiredItems, columnPrune);
            columnPruneVisitor.visit();
        }
    }
}
