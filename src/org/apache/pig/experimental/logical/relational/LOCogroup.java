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

package org.apache.pig.experimental.logical.relational;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.pig.data.DataType;
import org.apache.pig.experimental.logical.expression.LogicalExpression;
import org.apache.pig.experimental.logical.expression.LogicalExpressionPlan;
import org.apache.pig.experimental.logical.expression.ProjectExpression;
import org.apache.pig.experimental.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanVisitor;
import org.apache.pig.impl.util.MultiMap;

public class LOCogroup extends LogicalRelationalOperator {
    
    // List of booleans specifying if any of the cogroups is inner
    private boolean[] mIsInner;
    
    // List of expressionPlans according to input
    private MultiMap<Integer,LogicalExpressionPlan> mExpressionPlans;
    
    /**
     * Enum for the type of group
     */
    public static enum GROUPTYPE {
        REGULAR,    // Regular (co)group
        COLLECTED   // Collected group
    };
    
    private GROUPTYPE mGroupType;
    
    /*
     * This member would store the schema of group key,
     * we store it to retain uid numbers between 
     * resetSchema and getSchema 
     */
    private LogicalFieldSchema groupKeySchema = null;
    /*
     * This is a map storing Uids which have been generated for an input
     * This map is required to make the uids persistant between calls of
     * resetSchema and getSchema
     */
    private Map<Integer,Long> generatedInputUids = null;
    
    final static String GROUP_COL_NAME = "group";
        
    public LOCogroup(OperatorPlan plan, MultiMap<Integer,LogicalExpressionPlan> 
    expressionPlans, boolean[] isInner ) {
        this( plan, expressionPlans, GROUPTYPE.REGULAR, isInner, -1 );
    }

    public LOCogroup(OperatorPlan plan, MultiMap<Integer,LogicalExpressionPlan> 
    expressionPlans, GROUPTYPE groupType, boolean[] isInner, int requestedParrellism) {
        super("LOCogroup", plan);
        this.mExpressionPlans = expressionPlans;
        if( isInner != null ) {
            mIsInner = Arrays.copyOf(isInner, isInner.length);
        }
        this.mGroupType = groupType;
        this.generatedInputUids = new HashMap<Integer,Long>();
    }
    
    /**
     * Given an expression plan this function returns a LogicalFieldSchema
     * that can be generated using this expression plan
     * @param exprPlan ExpressionPlan which generates this field
     * @return
     */
    private LogicalFieldSchema getPlanSchema( LogicalExpressionPlan exprPlan ) {
        LogicalExpression sourceExp = (LogicalExpression) exprPlan.getSources().get(0);
        byte sourceType = sourceExp.getType();
        // We dont support bags for Cogroup
        if( sourceType == DataType.BAG ) {
            return null;
        }
        LogicalSchema fieldSchema = null;
        String alias = null;

        // If we have a projection then caculate the schema of the projection
        if (sourceExp instanceof ProjectExpression) {                
            LogicalRelationalOperator op = null;
            try{
                op = ((ProjectExpression)sourceExp).findReferent(this);
            }catch(Exception e) {
                throw new RuntimeException(e);
            }
            LogicalSchema s = op.getSchema();
            if (s != null) {
                fieldSchema = s.getField(((ProjectExpression)sourceExp).getColNum()).schema;
                alias = s.getField(((ProjectExpression)sourceExp).getColNum()).alias;
            }
        }
        
        return new LogicalFieldSchema(alias, fieldSchema, sourceType, sourceExp.getUid());
    }

    @Override
    public LogicalSchema getSchema() {
        // if schema is calculated before, just return
        if (schema != null) {
            return schema;
        }

        List<Operator> inputs = null;
        try {
            inputs = plan.getPredecessors(this);
            if (inputs == null) {
                return null;
            }
        }catch(Exception e) {
            throw new RuntimeException("Unable to get predecessors of " + name 
                    + " operator. ", e);
        }

        List<LogicalFieldSchema> fieldSchemaList = new ArrayList<LogicalFieldSchema>();

        // We only calculate this if we havent. This would not be null in a case
        // where we calculate the schema and then reset it.
        if( groupKeySchema == null ) {
            // See if we have more than one expression plans, if so the
            // schema of the group column will be a tuple
            boolean hasMultipleKeys = false;
            for( Integer key : mExpressionPlans.keySet() ) {
                if( mExpressionPlans.get(key).size() > 1 ) {
                    hasMultipleKeys = true;
                    break;
                }
            }

            // Generate the groupField Schema
            if( hasMultipleKeys ) {
                LogicalSchema keySchema = new LogicalSchema();
                // We sort here to maintain the correct order of inputs
                TreeSet<Integer> keySet = new TreeSet<Integer>();
                keySet.addAll( mExpressionPlans.keySet() );
                for( Integer key : keySet ) {
                    Collection<LogicalExpressionPlan> plans = 
                        mExpressionPlans.get(key);

                    for( LogicalExpressionPlan plan : plans ) {
                        LogicalFieldSchema fieldSchema = getPlanSchema(plan);
                        // if any plan schema is null, that means we can't calculate
                        // further schemas so we bail out
                        if( fieldSchema == null ) {
                            schema = null;
                            return schema;
                        }
                        // Change the uid of this field
                        fieldSchema.uid = LogicalExpression.getNextUid();
                        keySchema.addField(fieldSchema);
                    }
                    // We only need fields from one input and not all
                    break;
                }
                groupKeySchema = new LogicalFieldSchema(GROUP_COL_NAME, keySchema, DataType.TUPLE, 
                        LogicalExpression.getNextUid() );
            } else {
                // We sort here to maintain the correct order of inputs
                TreeSet<Integer> keySet = new TreeSet<Integer>();
                keySet.addAll( mExpressionPlans.keySet() );
                for( Integer key : keySet ) {
                    Collection<LogicalExpressionPlan> plans = mExpressionPlans.get(key);
                    for( LogicalExpressionPlan plan : plans ) {
                        groupKeySchema = getPlanSchema(plan);
                        // if any plan schema is null, that means we can't calculate
                        // further schemas so we bail out
                        if( groupKeySchema == null ) {
                            schema = null;
                            return schema;
                        }
                        // Change the uid of this field
                        groupKeySchema.alias = GROUP_COL_NAME;
                        groupKeySchema.uid = LogicalExpression.getNextUid();
                        break;
                    }
                    break;
                }
            }
        }
        
        fieldSchemaList.add( groupKeySchema );

        // Generate the Bag Schema
        int counter = 0;
        for (Operator op : inputs) {
            LogicalSchema inputSchema = ((LogicalRelationalOperator)op).getSchema();
            // the schema of one input is unknown, so the join schema is unknown, just return 
            if (inputSchema == null) {
                schema = null;
                return schema;
            }
           
            // Check if we already have calculated Uid for this bag for given 
            // input operator
            long bagUid = -1;
            if( generatedInputUids.containsKey(counter) ) {
                bagUid = generatedInputUids.get(counter);
            } else {
                bagUid = LogicalExpression.getNextUid();
                generatedInputUids.put( counter, bagUid );
            }
            
            LogicalFieldSchema newBagSchema = new LogicalFieldSchema(
                    ((LogicalRelationalOperator)op).getAlias(), inputSchema, 
                    DataType.BAG, bagUid);

            fieldSchemaList.add( newBagSchema );
            counter ++;
        }

        schema = new LogicalSchema();
        for(LogicalFieldSchema fieldSchema: fieldSchemaList) {
            schema.addField(fieldSchema);
        }         

        return schema;
    }

    @Override
    public void accept(PlanVisitor v) throws IOException {
        if (!(v instanceof LogicalPlanVisitor)) {
            throw new IOException("Expected LogicalPlanVisitor");
        }
        ((LogicalPlanVisitor)v).visitLOCogroup(this);
    }

    @Override
    public boolean isEqual(Operator other) {
        if (other != null && other instanceof LOCogroup) {
            LOCogroup oc = (LOCogroup)other;
            if( mGroupType == oc.mGroupType && 
                    mIsInner.length == oc.mIsInner.length 
                    && mExpressionPlans.size() == oc.mExpressionPlans.size() ) {
                for( int i = 0; i < mIsInner.length; i++ ) {
                    if( mIsInner[i] != oc.mIsInner[i] ) {
                        return false;
                    }
                }
                for( Integer key : mExpressionPlans.keySet() ) {                    
                    if( ! oc.mExpressionPlans.containsKey(key) ) {
                        return false;
                    }
                    Collection<LogicalExpressionPlan> exp1 = 
                        mExpressionPlans.get(key);
                    Collection<LogicalExpressionPlan> exp2 = 
                        oc.mExpressionPlans.get(key);

                    if(! ( exp1 instanceof ArrayList<?> 
                    || exp2 instanceof ArrayList<?> ) ) {
                        throw new RuntimeException( "Expected an ArrayList " +
                        "of Expression Plans" );
                    }

                    ArrayList<LogicalExpressionPlan> expList1 = 
                        (ArrayList<LogicalExpressionPlan>) exp1;
                    ArrayList<LogicalExpressionPlan> expList2 = 
                        (ArrayList<LogicalExpressionPlan>) exp2;

                    for (int i = 0; i < expList1.size(); i++) {
                        if (!expList1.get(i).isEqual(expList2.get(i))) {
                            return false;
                        }
                    }
                }
                return checkEquality((LogicalRelationalOperator) other);
            }
        }
        return false;
    }

    public GROUPTYPE getGroupType() {
        return mGroupType;
    }
    
    /**
     * Returns an Unmodifiable Map of Input Number to Uid 
     * @return Unmodifiable Map<Integer,Long>
     */
    public Map<Integer,Long> getGeneratedInputUids() {
        return Collections.unmodifiableMap( generatedInputUids );
    }
    
    public MultiMap<Integer,LogicalExpressionPlan> getExpressionPlans() {
        return mExpressionPlans;
    }
    
    public boolean[] getInner() {
        return mIsInner;
    }

}
