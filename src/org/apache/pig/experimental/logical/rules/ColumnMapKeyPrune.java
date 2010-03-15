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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.LoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.data.DataType;
import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.ProjectExpression;
import org.apache.pig.experimental.logical.relational.LOCogroup;
import org.apache.pig.experimental.logical.relational.LOFilter;
import org.apache.pig.experimental.logical.relational.LOForEach;
import org.apache.pig.experimental.logical.relational.LOGenerate;
import org.apache.pig.experimental.logical.relational.LOInnerLoad;
import org.apache.pig.experimental.logical.relational.LOJoin;
import org.apache.pig.experimental.logical.relational.LOLoad;
import org.apache.pig.experimental.logical.relational.LOStore;
import org.apache.pig.experimental.logical.relational.LogicalPlan;
import org.apache.pig.experimental.logical.relational.LogicalPlanVisitor;
import org.apache.pig.experimental.logical.relational.LogicalRelationalOperator;
import org.apache.pig.experimental.logical.relational.LogicalSchema;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.OperatorSubPlan;
import org.apache.pig.experimental.plan.ReverseDependencyOrderWalker;
import org.apache.pig.experimental.plan.optimizer.Transformer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;

/**
 * This Rule prunes columns and map keys and set to loader. This rule depends
 * on MapKeysPruneHelper to calculate what keys are required for a loader,
 * and ColumnPruneHelper to calculate the required columns for a loader. Then
 * it combines the map keys and columns info to set into the loader.
 */
public class ColumnMapKeyPrune extends WholePlanRule {
    private boolean hasRun;
    
    public ColumnMapKeyPrune(String n) {
        super(n);
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
        
        private OperatorSubPlan subPlan = null;
        
        @Override
        public boolean check(OperatorPlan matched) throws IOException {
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
            return subPlan;
        }
        
        @SuppressWarnings("unchecked")
        private void merge() {
            // combine two subplans
            subPlan = new OperatorSubPlan(currentPlan);
            if (columnPrune) {
                Iterator<Operator> iter = columnHelper.reportChanges().getOperators();
                while(iter.hasNext()) {
                    subPlan.add(iter.next());
                }
            }
            
            if (mapKeyPrune) {
                Iterator<Operator> iter = mapKeyHelper.reportChanges().getOperators();
                while(iter.hasNext()) {
                    subPlan.add(iter.next());
                }
            }
            
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
        public void transform(OperatorPlan matched) throws IOException {        	            
            merge();
            
            ColumnPruneVisitor v = new ColumnPruneVisitor(subPlan);           
            v.visit();                   
        }        
    
        
        // visitor to change the plan to remove unnecessary columns
        private class ColumnPruneVisitor extends LogicalPlanVisitor {
            public ColumnPruneVisitor(OperatorPlan plan) {
                super(plan, new ReverseDependencyOrderWalker(plan));                        
            }
            
            public void visitLOLoad(LOLoad load) throws IOException {
                if(! requiredItems.containsKey( load ) ) {
                    return;
                }
                
                Pair<Map<Integer,Set<String>>,Set<Integer>> required = 
                    requiredItems.get(load);
                
                RequiredFieldList requiredFields = new RequiredFieldList();

                LogicalSchema s = load.getSchema();
                for (int i=0;i<s.size();i++) {
                    RequiredField requiredField = null;
                    // As we have done processing ahead, we assume that 
                    // a column is not present in both ColumnPruner and 
                    // MapPruner
                    if( required.first != null && required.first.containsKey(i) ) {
                        requiredField = new RequiredField();
                        requiredField.setIndex(i);
                        requiredField.setType(s.getField(i).type);
                        List<RequiredField> subFields = new ArrayList<RequiredField>();
                        for( String key : required.first.get(i) ) {
                            RequiredField subField = new RequiredField(key,-1,null,DataType.BYTEARRAY);
                            subFields.add(subField);
                        }
                        requiredField.setSubFields(subFields);
                        requiredFields.add(requiredField);
                    }
                    if( required.second != null && required.second.contains(i) ) {
                        requiredField = new RequiredField();
                        requiredField.setIndex(i);
                        requiredField.setType(s.getField(i).type);      
                        requiredFields.add(requiredField);
                    }
                }         
                    
                log.info("Loader for " + load.getAlias() + " is pruned. Load fields " + requiredFields); 
                for(RequiredField rf: requiredFields.getFields()) {
                    List<RequiredField> sub = rf.getSubFields();
                    if (sub != null) {
                        // log.info("For column " + rf.getIndex() + ", set map keys: " + sub.toString());
                        log.info("Map key required for " + load.getAlias() + ": $" + rf.getIndex() + "->" + sub);
                    }
                }
                
                LoadPushDown.RequiredFieldResponse response = null;
                try {
                    LoadFunc loadFunc = load.getLoadFunc();
                    if (loadFunc instanceof LoadPushDown) {
                        response = ((LoadPushDown)loadFunc).pushProjection(requiredFields);
                    }
                                        
                } catch (FrontendException e) {
                    log.warn("pushProjection on "+load+" throw an exception, skip it");
                }                      
                
                // Loader does not support column pruning, insert foreach      
                if (columnPrune) {
                    if (response==null || !response.getRequiredFieldResponse()) {
                        LogicalPlan p = (LogicalPlan)load.getPlan();                        
                        Operator next = p.getSuccessors(load).get(0); 
                        // if there is already a LOForEach after load, we don't need to 
                        // add another LOForEach
                        if (next instanceof LOForEach) {
                            return;
                        }
                        
                        LOForEach foreach = new LOForEach(load.getPlan());
                        
                        // add foreach to the base plan                       
                        p.add(foreach);
                                       
                        Pair<Integer,Integer> disconnectedPos = p.disconnect(load, next);
                        p.connect(load, disconnectedPos.first.intValue(), foreach, 0 );
                        p.connect(foreach, 0, next, disconnectedPos.second.intValue());
                        
                        // add foreach to the subplan
                        subPlan.add(foreach);
                        
                        LogicalPlan innerPlan = new LogicalPlan();
                        foreach.setInnerPlan(innerPlan);
                        
                        // build foreach inner plan
                        List<LogicalExpressionPlan> exps = new ArrayList<LogicalExpressionPlan>();            	
                        LOGenerate gen = new LOGenerate(innerPlan, exps, new boolean[requiredFields.getFields().size()]);
                        innerPlan.add(gen);
                        
                        for (int i=0; i<requiredFields.getFields().size(); i++) {
                            LoadPushDown.RequiredField rf = requiredFields.getFields().get(i);
                            LOInnerLoad innerLoad = new LOInnerLoad(innerPlan, foreach, rf.getIndex());
                            innerLoad.getProjection().setUid(foreach);                    
                            innerPlan.add(innerLoad);          
                            innerPlan.connect(innerLoad, gen);
                            
                            LogicalExpressionPlan exp = new LogicalExpressionPlan();
                            ProjectExpression prj = new ProjectExpression(exp, rf.getType(), i, 0);
                            prj.setUid(gen);
                            exp.add(prj);
                            exps.add(exp);
                        }                
                       
                    } else {
                        // columns are pruned, reset schema for LOLoader
                        LogicalSchema newSchema = new LogicalSchema();
                        List<LoadPushDown.RequiredField> fieldList = requiredFields.getFields();
                        for (int i=0; i<fieldList.size(); i++) {            		
                            newSchema.addField(s.getField(fieldList.get(i).getIndex()));
                        }

                        load.setScriptSchema(newSchema);
                    }
                }
            }
                    

            public void visitLOFilter(LOFilter filter) throws IOException {
            }
            
            public void visitLOStore(LOStore store) throws IOException {
            }
            
            public void visitLOCogroup( LOCogroup cg ) throws IOException {
            }
            
            public void visitLOJoin(LOJoin join) throws IOException {
            }
            
            @SuppressWarnings("unchecked")
            public void visitLOForEach(LOForEach foreach) throws IOException {
                if (!columnPrune) {
                    return;
                }
                
                // get column numbers from input uids
                Set<Long> input = (Set<Long>)foreach.getAnnotation(ColumnPruneHelper.INPUTUIDS);
                LogicalRelationalOperator op = (LogicalRelationalOperator)foreach.getPlan().getPredecessors(foreach).get(0);
                Set<Integer> cols = columnHelper.getColumns(op.getSchema(), input);
                
                LogicalPlan innerPlan = foreach.getInnerPlan();
                LOGenerate gen = (LOGenerate)innerPlan.getSinks().get(0);
                            
                // clean up the predecessors of LOGenerate
                List<Operator> ll = innerPlan.getPredecessors(gen);
                List<Operator> toRemove = new ArrayList<Operator>();
                for(int i=0; i<ll.size(); i++) {
                    
                    // if the LOInnerLoads for this subplan are all in the column set, 
                    // no change required, keep going
                    if (checkInnerLoads((LogicalRelationalOperator)ll.get(i), cols)) {
                        continue;
                    }         
                                       
                    // clean up and adjust expression plans
                    Iterator<LogicalExpressionPlan> iter = gen.getOutputPlans().iterator();
                    int j=-1;
                    while(iter.hasNext()) {
                        j++;
                        LogicalExpressionPlan exp = iter.next();
                        List<Operator> sinks = exp.getSinks();
                        for(Operator s: sinks) {
                            if (s instanceof ProjectExpression) {
                                int inputNo = ((ProjectExpression)s).getInputNum();
                                if (inputNo + toRemove.size() == i) {
                                    // if this expression has this input that is to be removed,
                                    // then remove this expression plan
                                    iter.remove();
                                    
                                    // adjust flatten flags
                                    boolean[] flatten = gen.getFlattenFlags();
                                    for(int k=j; k<flatten.length-1; k++) {
                                        flatten[k] = flatten[k+1]; 
                                    }
                                    break;
                                } else if (inputNo + toRemove.size() > i) {
                                    // adjust input number for all projections whose
                                    // input number is after the one to be removed
                                    ((ProjectExpression)s).setInputNum(inputNo-1);
                                }
                            }
                        }
                    }
                    
                    // this LOInnerLoad and its successors should be removed, add to the remove list
                    toRemove.add(ll.get(i));
                                        
                }
                
                for(Operator pred: toRemove) {
                    removeSubTree((LogicalRelationalOperator)pred);
                }
                
                // trim the flatten flags in case some expressions are removed 
                boolean[] flatten = new boolean[gen.getOutputPlans().size()];
                System.arraycopy(gen.getFlattenFlags(), 0, flatten, 0, flatten.length);
                gen.setFlattenFlags(flatten);
            }
            
            public void visitLOGenerate(LOGenerate gen) throws IOException {
            }
            
            public void visitLOInnerLoad(LOInnerLoad load) throws IOException {
            }
            
            // check if the column number in LOInnerLoad is inside a given column index set
            protected boolean checkInnerLoads(LogicalRelationalOperator op, Set<Integer> cols) throws IOException {
                if (op instanceof LOInnerLoad) {
                    int col = ((LOInnerLoad)op).getColNum();
                    if (!cols.contains(col)) {
                        return false;
                    }
                }
                
                List<Operator> preds = op.getPlan().getPredecessors(op);
                if (preds != null) {
                    for(Operator pred: preds ) {
                        if (!checkInnerLoads((LogicalRelationalOperator)pred, cols)) {
                            return false;
                        }
                    }
                }
                
                return true;
            }             
            
            // remove all the operators starting from an operator
            protected void removeSubTree(LogicalRelationalOperator op) throws IOException {
                LogicalPlan p = (LogicalPlan)op.getPlan();
                List<Operator> ll = p.getPredecessors(op);
                if (ll != null) {
                    for(Operator pred: ll) {    			
                        removeSubTree((LogicalRelationalOperator)pred);
                    }
                }
                        
                if (p.getSuccessors(op) != null) {
                    Operator[] succs = p.getSuccessors(op).toArray(new Operator[0]);
                    
                    for(Operator s: succs) {
                        p.disconnect(op, s);
                    }
                }
                
                p.remove(op);
            }
            
        }
    }

}
