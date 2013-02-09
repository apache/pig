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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.OperatorSubPlan;
import org.apache.pig.newplan.ReverseDependencyOrderWalker;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.MapLookupExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.optimizer.AllExpressionVisitor;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

/**
 * This filter Marks every Load Operator which has a Map 
 * with MAP_MARKER_ANNOTATION. The annotation value is 
 * <code>Map<Integer,Set<String>><code> where Integer is the column number 
 * of the field and Set is the set of Keys in this field ( field is a map field only ).
 * 
 * It does this for only the top level schema in load. 
 * 
 * Algorithm:
 *  Traverse the Plan in ReverseDependency order ( ie. Sink to Source )
 *      For LogicalRelationalOperators having MapLookupExpression in their 
 *          expressionPlan collect uid and keys related to it. This is
 *          retained in the visitor
 *      For ForEach having nested LogicalPlan use the same visitor hence
 *          there is no distinction required
 *      At Sources find all the uids provided by this source and annotate this 
 *      LogicalRelationalOperator ( load ) with <code>Map<Integer,Set<String>></code>
 *      containing only the column numbers that this LogicalRelationalOperator generates
 *      
 * NOTE: This is a simple Map Pruner. If a map key is mentioned in the script
 *      then this pruner assumes you need the key. This pruner is not as optimized
 *      as column pruner ( which removes a column if it is mentioned but never used )
 *
 */
public class MapKeysPruneHelper {

    public static final String REQUIRED_MAPKEYS = "MapPruner:RequiredKeys";
    
    private OperatorPlan currentPlan;
    private OperatorSubPlan subplan;
    
    public MapKeysPruneHelper(OperatorPlan currentPlan) {
        this.currentPlan = currentPlan;
        
        if (currentPlan instanceof OperatorSubPlan) {
            subplan = new OperatorSubPlan(((OperatorSubPlan)currentPlan).getBasePlan());
        } else {
            subplan = new OperatorSubPlan(currentPlan);
        }
    }
  

    @SuppressWarnings("unchecked")
    public boolean check() throws FrontendException {       
        
        // First check if we have a load with a map in it or not
        List<Operator> sources = currentPlan.getSources();
        
        for( Operator source : sources ) {
            LogicalSchema schema = ((LogicalRelationalOperator)source).getSchema();
            // If any of the loads has a null schema we dont know the ramifications here
            // so we skip this optimization
            if( schema == null ) {
                return false;
            }
        }
                    
        // Now we check what keys are needed
        MapMarker marker = new MapMarker(currentPlan);
        marker.visit();
        
        // If the uid is the input uid of LOStore, LOCogroup, LOUnion, UserFunc, that means
        // the entire map may be used. For simplicity, we do not prune any map key in this case
        Set<Long> fullMapUids = new HashSet<Long>();
        FullMapCollector collector = new FullMapCollector(currentPlan, fullMapUids);
        collector.visit();
        
        // If we have found specific keys which are needed then we return true;
        // Else if we dont have any specific keys we return false
        boolean hasAnnotation = false;
        for( Operator source : sources ) {
            Map<Integer,Set<String>> annotationValue = 
                (Map<Integer, Set<String>>) ((LogicalRelationalOperator)source).getAnnotation(REQUIRED_MAPKEYS);
            
            // Now for all full maps found in sinks we cannot prune them at source
            if( ! fullMapUids.isEmpty() && annotationValue != null && 
                    !annotationValue.isEmpty() ) {
                Integer[] annotationKeyArray = annotationValue.keySet().toArray( new Integer[0] );
                LogicalSchema sourceSchema = ((LogicalRelationalOperator)source).getSchema();
                for( Integer col : annotationKeyArray ) {                	
                    if( fullMapUids.contains(sourceSchema.getField(col).uid)) {
                        annotationValue.remove( col );
                    }
                }
            }
            
            if ( annotationValue != null && annotationValue.isEmpty()) {
                ((LogicalRelationalOperator)source).removeAnnotation(REQUIRED_MAPKEYS);
                annotationValue = null;
            }
            
            // Can we still prune any keys
            if( annotationValue != null ) {
                hasAnnotation = true;
                subplan.add(source);
            }
        }
        
        // If all the sinks dont have any schema, we cant to any optimization
        return hasAnnotation;
    }
    
    /**
     * This function checks if the schema has a map.
     * We dont check for a nested structure.
     * @param schema Schema to be checked
     * @return true if it has a map, else false
     * @throws NullPointerException incase Schema is null
     */
    private boolean hasMap(LogicalSchema schema ) {
        for( LogicalFieldSchema field : schema.getFields() ) {
            if( field.type == DataType.MAP ) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * This function returns a set of Uids corresponding to
     * map datatype in the first level of this schema
     * @param schema Schema having fields
     * @return
     */
    private static Set<Long> getMapUids(LogicalSchema schema ) {
        Set<Long> uids = new HashSet<Long>();
        if( schema != null ) {
            for( LogicalFieldSchema field : schema.getFields() ) {
                uids.add( field.uid );
            }
        }
        return uids;
    }

    public OperatorPlan reportChanges() {
        return subplan;
    }

    /**
     * This class collects all the information required to create
     * the list of keys required for a map
     */
    static public class MapMarker extends AllExpressionVisitor {
        
        Map<Long,Set<String>> inputUids = null;

        protected MapMarker(OperatorPlan plan) throws FrontendException {
            super(plan, new ReverseDependencyOrderWalker(plan));
            inputUids = new HashMap<Long,Set<String>>();
        }
        
        @Override
        public void visit(LOLoad load) throws FrontendException {
            if( load.getSchema() != null ) {
                Map<Integer,Set<String>> annotation = new HashMap<Integer,Set<String>>();
                for( int i=0; i<load.getSchema().size(); i++) {
                    LogicalFieldSchema field = load.getSchema().getField(i);
                    if( inputUids.containsKey( field.uid ) ) {
                        annotation.put(i, inputUids.get( field.uid ) );
                    }
                }
                load.annotate(REQUIRED_MAPKEYS, annotation);
            }
        }

        @Override
        public void visit(LOFilter filter) throws FrontendException {
            currentOp = filter;
            MapExprMarker v = (MapExprMarker) getVisitor(filter.getFilterPlan());
            v.visit();
            mergeUidKeys( v.inputUids );
        }
        
        @Override
        public void visit(LOJoin join) throws FrontendException {
            currentOp = join;
            Collection<LogicalExpressionPlan> c = join.getExpressionPlanValues();
            for (LogicalExpressionPlan plan : c) {
                MapExprMarker v = (MapExprMarker) getVisitor(plan);
                v.visit();
                mergeUidKeys( v.inputUids );
            }
        }
        
        @Override
        public void visit(LOGenerate gen) throws FrontendException {
            currentOp = gen;
            Collection<LogicalExpressionPlan> plans = gen.getOutputPlans();
            for( LogicalExpressionPlan plan : plans ) {
                MapExprMarker v = (MapExprMarker) getVisitor(plan);
                v.visit();
                mergeUidKeys( v.inputUids );
            }
        }
        
        @Override
        public void visit(LOSort sort) throws FrontendException {
            currentOp = sort;
            Collection<LogicalExpressionPlan> c = sort.getSortColPlans();
            for (LogicalExpressionPlan plan : c) {
                MapExprMarker v = (MapExprMarker) getVisitor(plan);
                v.visit();
                mergeUidKeys( v.inputUids );
            }
        }
        
        
        @Override
        public void visit(LOSplitOutput splitOutput) throws FrontendException {
            currentOp = splitOutput;
            MapExprMarker v = (MapExprMarker) getVisitor(splitOutput.getFilterPlan());
            v.visit();
            mergeUidKeys( v.inputUids );
            if (splitOutput.getSchema()!=null) {
                for (LogicalFieldSchema fs : splitOutput.getSchema().getFields()) {
                    long inputUid = splitOutput.getInputUids(fs.uid);
                    if( inputUid!=-1) {
                        Set<String> mapKeySet = inputUids.get(fs.uid);
                        if (mapKeySet!=null) {
                            if (inputUids.containsKey(inputUid))
                                inputUids.get(inputUid).addAll(mapKeySet);
                            else
                                inputUids.put(inputUid, mapKeySet);
                        }
                    }
                }
            }
        }
        
        private void mergeUidKeys( Map<Long, Set<String> > inputMap ) {
            for( Map.Entry<Long, Set<String>> entry : inputMap.entrySet() ) {
                if( inputUids.containsKey(entry.getKey()) ) {
                    Set<String> mapKeySet = inputUids.get(entry.getKey());
                    mapKeySet.addAll(entry.getValue());
                } else {
                    inputUids.put(entry.getKey(), inputMap.get(entry.getKey()));
                }
            }
        }

        @Override
        protected LogicalExpressionVisitor getVisitor(LogicalExpressionPlan expr) throws FrontendException {
            return new MapExprMarker(expr );
        }
        
        static class MapExprMarker extends LogicalExpressionVisitor {

            Map<Long,Set<String>> inputUids = null;
            
            protected MapExprMarker(OperatorPlan p) throws FrontendException {
                super(p, new DependencyOrderWalker(p));
                inputUids = new HashMap<Long,Set<String>>();
            }

            @Override
            public void visit(MapLookupExpression op) throws FrontendException {
                Long uid = op.getMap().getFieldSchema().uid;
                String key = op.getLookupKey();
                
                HashSet<String> mapKeySet = null;
                if( inputUids.containsKey(uid) ) {
                    mapKeySet = (HashSet<String>) inputUids.get(uid);                                        
                } else {
                    mapKeySet = new HashSet<String>();
                    inputUids.put(uid, mapKeySet);
                }
                mapKeySet.add(key);
            }
        }
    }
    
    static public class FullMapCollector extends AllExpressionVisitor {
        Set<Long> fullMapUids = new HashSet<Long>();

        protected FullMapCollector(OperatorPlan plan, Set<Long> fullMapUids) throws FrontendException {
            super(plan, new ReverseDependencyOrderWalker(plan));
            this.fullMapUids = fullMapUids;
        }
        
        @Override
        public void visit(LOStore store) throws FrontendException {
            super.visit(store);
            Set<Long> uids = getMapUids(store.getSchema());
            fullMapUids.addAll(uids);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void visit(LOUnion union) throws FrontendException {
            super.visit(union);
            List<Operator> preds = plan.getPredecessors(union);
            if (preds!=null) {
                for (Operator pred : preds) {
                    LogicalSchema schema = ((LogicalRelationalOperator)pred).getSchema();
                    Set<Long> uids = getMapUids(schema);
                    fullMapUids.addAll(uids);
                }
            }
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void visit(LOCogroup cogroup) throws FrontendException {
            super.visit(cogroup);
            List<Operator> preds = plan.getPredecessors(cogroup);
            if (preds!=null) {
                for (Operator pred : preds) {
                    LogicalSchema schema = ((LogicalRelationalOperator)pred).getSchema();
                    Set<Long> uids = getMapUids(schema);
                    fullMapUids.addAll(uids);
                }
            }
        }
        
        @Override
        public void visit(LOSplitOutput splitOutput) throws FrontendException {
            super.visit(splitOutput);
            if (splitOutput.getSchema()!=null) {
                for (LogicalFieldSchema fs : splitOutput.getSchema().getFields()) {
                    if (fullMapUids.contains(fs.uid) && splitOutput.getInputUids(fs.uid)!=-1)
                        fullMapUids.add(splitOutput.getInputUids(fs.uid));
                }
            }
        }

        @Override
        protected LogicalExpressionVisitor getVisitor(LogicalExpressionPlan expr)
                throws FrontendException {
            return new FullMapExpCollector(expr, fullMapUids);
        }
        
        static class FullMapExpCollector extends LogicalExpressionVisitor {
            Set<Long> fullMapUids = new HashSet<Long>();
            protected FullMapExpCollector(OperatorPlan plan, Set<Long> fullMapUids)
                    throws FrontendException {
                super(plan, new DependencyOrderWalker(plan));
                this.fullMapUids = fullMapUids;
            }
            
            @Override
            public void visit(UserFuncExpression userFunc) throws FrontendException {
                List<Operator> succs = userFunc.getPlan().getSuccessors(userFunc);
                if (succs==null) return;
                LogicalExpression succ = (LogicalExpression)succs.get(0);
                if (succ.getFieldSchema()!=null && succ.getFieldSchema().type==DataType.MAP)
                    fullMapUids.add(succ.getFieldSchema().uid);
            }
        }
    }
}
