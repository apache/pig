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

package org.apache.pig.newplan.logical.relational;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

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
        COLLECTED,  // Collected group
        MERGE       // Map-side CoGroup on sorted data
    };
    
    private GROUPTYPE mGroupType;
    
    private LogicalFieldSchema groupKeyUidOnlySchema; 
    
    /*
     * This is a map storing Uids which have been generated for an input
     * This map is required to make the uids persistant between calls of
     * resetSchema and getSchema
     */
    private Map<Integer,Long> generatedInputUids = new HashMap<Integer,Long>();
    
    final static String GROUP_COL_NAME = "group";
    
    /** 
     * static constant to refer to the option of selecting a group type
     */
    public final static Integer OPTION_GROUPTYPE = 1;
    
    /**
     * Constructor for use in defining rule patterns
     * @param plan
     */
    public LOCogroup(LogicalPlan plan) {
        super("LOCogroup", plan);     
    }
        
    public LOCogroup(OperatorPlan plan, MultiMap<Integer,LogicalExpressionPlan> 
    expressionPlans, boolean[] isInner ) {
        this( plan, expressionPlans, GROUPTYPE.REGULAR, isInner );
    }

    public LOCogroup(OperatorPlan plan, MultiMap<Integer,LogicalExpressionPlan> 
    expressionPlans, GROUPTYPE groupType, boolean[] isInner) {
        super("LOCogroup", plan);
        this.mExpressionPlans = expressionPlans;
        if( isInner != null ) {
            mIsInner = Arrays.copyOf(isInner, isInner.length);
        }
        this.mGroupType = groupType;
    }
    
    /**
     * Given an expression plan this function returns a LogicalFieldSchema
     * that can be generated using this expression plan
     * @param exprPlan ExpressionPlan which generates this field
     * @return
     */
    private LogicalFieldSchema getPlanSchema( LogicalExpressionPlan exprPlan ) throws FrontendException {
        LogicalExpression sourceExp = (LogicalExpression) exprPlan.getSources().get(0);
        LogicalFieldSchema planSchema = null;
        if (sourceExp.getFieldSchema()!=null)
            planSchema = sourceExp.getFieldSchema().deepCopy();
        return planSchema;
    }

    @Override
    public LogicalSchema getSchema() throws FrontendException {
        // if schema is calculated before, just return
        if (schema != null) {
            return schema;
        }

        List<Operator> inputs = null;
        inputs = plan.getPredecessors(this);
        if (inputs == null) {
            throw new FrontendException(this, "Cannot get predecessor for " + this, 2233);
        }
        
        List<LogicalFieldSchema> fieldSchemaList = new ArrayList<LogicalFieldSchema>();

        // See if we have more than one expression plans, if so the
        // schema of the group column will be a tuple
        boolean hasMultipleKeys = false;
        for( Integer key : mExpressionPlans.keySet() ) {
            if( mExpressionPlans.get(key).size() > 1 ) {
                hasMultipleKeys = true;
                break;
            }
        }

        LogicalFieldSchema groupKeySchema = null;
        // Generate the groupField Schema
        if( hasMultipleKeys ) {
            LogicalSchema keySchema = new LogicalSchema();
            // We sort here to maintain the correct order of inputs
            for( Integer key : mExpressionPlans.keySet()) {
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
                    fieldSchema = new LogicalFieldSchema(fieldSchema);
                    keySchema.addField(fieldSchema);
                }
                // We only need fields from one input and not all
                break;
            }
            groupKeySchema = new LogicalFieldSchema(GROUP_COL_NAME, keySchema, DataType.TUPLE);
        } else {
            // We sort here to maintain the correct order of inputs
            for( Integer key : mExpressionPlans.keySet() ) {
                Collection<LogicalExpressionPlan> plans = mExpressionPlans.get(key);
                for( LogicalExpressionPlan plan : plans ) {
                    groupKeySchema = getPlanSchema(plan);
                    // if any plan schema is null, that means we cannot figure out
                    // the arity of keys, just give an empty tuple
                    if( groupKeySchema == null ) {
                        groupKeySchema = new LogicalSchema.LogicalFieldSchema("group", null, DataType.TUPLE);
                        break;
                    }
                    groupKeySchema = new LogicalSchema.LogicalFieldSchema(groupKeySchema);
                    // Change the uid of this field
                    groupKeySchema.alias = GROUP_COL_NAME;
                    break;
                }
                break;
            }           
        }
        if(mExpressionPlans.size() > 1){
            //reset the uid, because the group column is associated with more
            // than one input
            groupKeySchema.resetUid();
        }
        
        if (groupKeySchema==null) {
            throw new FrontendException(this, "Cannot get group key schema for " + this, 2234);
        }
        groupKeyUidOnlySchema = groupKeySchema.mergeUid(groupKeyUidOnlySchema);

        fieldSchemaList.add( groupKeySchema );

        // Generate the Bag Schema
        int counter = 0;
        for (Operator op : inputs) {
            LogicalSchema inputSchema = ((LogicalRelationalOperator)op).getSchema();
           
            // Check if we already have calculated Uid for this bag for given 
            // input operator
            long bagUid;
            if (generatedInputUids.get(counter)!=null)
                bagUid = generatedInputUids.get(counter);
            else {
                bagUid = LogicalExpression.getNextUid();
                generatedInputUids.put( counter, bagUid );
            }
            
            LogicalFieldSchema newTupleFieldSchema = new LogicalFieldSchema(
                    null, inputSchema, DataType.TUPLE, LogicalExpression.getNextUid());
            
            LogicalSchema bagSchema = new LogicalSchema();
            bagSchema.addField(newTupleFieldSchema);
            
            LogicalFieldSchema newBagFieldSchema = new LogicalFieldSchema(
                    ((LogicalRelationalOperator)op).getAlias(), bagSchema, 
                    DataType.BAG, bagUid);

            fieldSchemaList.add( newBagFieldSchema );
            counter ++;
        }

        schema = new LogicalSchema();
        for(LogicalFieldSchema fieldSchema: fieldSchemaList) {
            schema.addField(fieldSchema);
        }         

        return schema;
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalRelationalNodesVisitor)) {
            throw new FrontendException("Expected LogicalPlanVisitor", 2223);
        }
        ((LogicalRelationalNodesVisitor)v).visit(this);
    }

    @Override
    public boolean isEqual(Operator other) throws FrontendException {
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
                        throw new FrontendException( "Expected an ArrayList " +
                        "of Expression Plans", 2235 );
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
    
    public void resetGroupType() {
        mGroupType = GROUPTYPE.REGULAR;
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
    
    public void setExpressionPlans(MultiMap<Integer,LogicalExpressionPlan> plans) {
        this.mExpressionPlans = plans;
    }
    
    public void setGroupType(GROUPTYPE gt) {
        mGroupType = gt;
    }
    
    public void setInnerFlags(boolean[] flags) {
        if( flags != null ) {
            mIsInner = Arrays.copyOf( flags, flags.length );
        }
    }
    
    public boolean[] getInner() {
        return mIsInner;
    }

    @Override
    public void resetUid() {
        groupKeyUidOnlySchema = null;
        generatedInputUids = new HashMap<Integer,Long>();
    }
    
    public List<Operator> getInputs(LogicalPlan plan) {
      return plan.getPredecessors(this);
    }
}
