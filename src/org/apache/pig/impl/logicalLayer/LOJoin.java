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
package org.apache.pig.impl.logicalLayer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.pig.PigException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.logicalLayer.optimizer.SchemaRemover;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.ProjectionMap;

public class LOJoin extends RelationalOperator {
    private static final long serialVersionUID = 2L;

    /**
     * Enum for the type of join
     */
	public static enum JOINTYPE {
        REGULAR, // Regular join
        REPLICATED, // Fragment Replicated join
        SKEWED, // Skewed Join
        MERGE   // Sort Merge Join
    };

    /**
     * LOJoin contains a list of logical operators corresponding to the
     * relational operators and a list of generates for each relational
     * operator. Each generate operator in turn contains a list of expressions
     * for the columns that are projected
     */
    private static Log log = LogFactory.getLog(LOJoin.class);
    private MultiMap<LogicalOperator, LogicalPlan> mJoinPlans;
    private boolean[] mInnerFlags;
	private JOINTYPE mJoinType; // Retains the type of the join
	private List<LogicalOperator> mSchemaInputMapping = new ArrayList<LogicalOperator>();

    /**
     * 
     * @param plan
     *            LogicalPlan this operator is a part of.
     * @param k
     *            OperatorKey for this operator
     * @param joinPlans
     *            the join columns
     * @param jt
     *            indicates the type of join - regular, skewed fragment replicated or merge join
     */
    public LOJoin(
            LogicalPlan plan,
            OperatorKey k,
            MultiMap<LogicalOperator, LogicalPlan> joinPlans,
            JOINTYPE jt,
            boolean[] isInner) {
        super(plan, k);
        mJoinPlans = joinPlans;
		mJoinType = jt;
		mInnerFlags = getCopy(isInner);
    }
    
    private boolean[] getCopy(boolean[] flags) {
        boolean[] retVal = new boolean[flags.length];
        for (int i = 0; i < flags.length; i++) {
            retVal[i] = flags[i];
        }
        return retVal;
    }

    public List<LogicalOperator> getInputs() {
        return mPlan.getPredecessors(this);
    }

    public MultiMap<LogicalOperator, LogicalPlan> getJoinPlans() {
        return mJoinPlans;
    }    

    public void setJoinPlans(MultiMap<LogicalOperator, LogicalPlan> joinPlans) {
        mJoinPlans = joinPlans;
    }    

	/**
     * Returns the type of join.
     */
    public JOINTYPE getJoinType() {
        return mJoinType;
    }

    @Override
    public String name() {
        return "LOJoin " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public Schema getSchema() throws FrontendException {
        List<LogicalOperator> inputs = mPlan.getPredecessors(this);
        mType = DataType.BAG;//mType is from the super class
        Hashtable<String, Integer> nonDuplicates = new Hashtable<String, Integer>();
        if(!mIsSchemaComputed){
            List<Schema.FieldSchema> fss = new ArrayList<Schema.FieldSchema>();
            mSchemaInputMapping = new ArrayList<LogicalOperator>();
            int i=-1;
            boolean seeUnknown = false;
            for (LogicalOperator op : inputs) {
                try {
                    Schema cSchema = op.getSchema();
                    if(cSchema!=null){
                        
                        for (FieldSchema schema : cSchema.getFields()) {
                            ++i;
                            FieldSchema newFS = null;
                            if(schema.alias != null) {
                                if(nonDuplicates.containsKey(schema.alias)) {
                                    if (nonDuplicates.get(schema.alias) != -1) {
                                        nonDuplicates.remove(schema.alias);
                                        nonDuplicates.put(schema.alias, -1);
                                    }
                                } else {
                                    nonDuplicates.put(schema.alias, i);
                                }
                                newFS = new FieldSchema(op.getAlias()+"::"+schema.alias,schema.schema,schema.type);
                            } else {
                                newFS = new Schema.FieldSchema(null, schema.type);
                            }
                            newFS.setParent(schema.canonicalName, op);
                            fss.add(newFS);
                            mSchemaInputMapping.add(op);
                        }
                    } else {
                        seeUnknown = true;
                    }
                } catch (FrontendException ioe) {
                    mIsSchemaComputed = false;
                    mSchema = null;
                    throw ioe;
                }
            }
            mIsSchemaComputed = true;
            mSchema = null;
            if (!seeUnknown)
            {
                mSchema = new Schema(fss);
                for (Entry<String, Integer> ent : nonDuplicates.entrySet()) {
                    int ind = ent.getValue();
                    if(ind==-1) continue;
                    FieldSchema prevSch = fss.get(ind);
                    // this is a non duplicate and hence can be referred to
                    // with just the field schema alias or outeralias::field schema alias
                    // In mSchema we have outeralias::fieldschemaalias. To allow
                    // using just the field schema alias, add it to mSchemas
                    // as an alias for this field.
                    mSchema.addAlias(ent.getKey(), prevSch);
                }
            }
        }
        return mSchema;
    }

    public boolean isTupleJoinCol() {
        List<LogicalOperator> inputs = mPlan.getPredecessors(this);
        if (inputs == null || inputs.size() == 0) {
            throw new AssertionError("LOJoin.isTupleJoinCol() can only becalled "
                                     + "after it has an input ") ;
        }
        return mJoinPlans.get(inputs.get(0)).size() > 1 ;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    /***
     *
     * This does switch the mapping
     *
     * oldOp -> List of inner plans
     *         to
     * newOp -> List of inner plans
     *
     * which is useful when there is a structural change in LogicalPlan
     *
     * @param oldOp the old operator
     * @param newOp the new operator
     */
    public void switchJoinColPlanOp(LogicalOperator oldOp,
                                    LogicalOperator newOp) {
        Collection<LogicalPlan> innerPlans = mJoinPlans.removeKey(oldOp) ;
        mJoinPlans.put(newOp, innerPlans);
    }

    public void unsetSchema() throws VisitorException{
        for(LogicalOperator input: getInputs()) {
            Collection<LogicalPlan> joinPlans = mJoinPlans.get(input);
            if(joinPlans!=null)
                for(LogicalPlan plan : joinPlans) {
                    SchemaRemover sr = new SchemaRemover(plan);
                    sr.visit();
                }
        }
        super.unsetSchema();
    }

    /**
     * This can be used to get the merged type of output join col
     * only when the join col is of atomic type
     * @return The type of the join col 
     */
    public byte getAtomicJoinColType() throws FrontendException {
        if (isTupleJoinCol()) {
            int errCode = 1010;
            String msg = "getAtomicJoinColType is used only when"
                + " dealing with atomic group col";
            throw new FrontendException(msg, errCode, PigException.INPUT, false, null) ;
        }

        byte groupType = DataType.BYTEARRAY ;
        // merge all the inner plan outputs so we know what type
        // our group column should be
        for(int i=0;i < getInputs().size(); i++) {
            LogicalOperator input = getInputs().get(i) ;
            List<LogicalPlan> innerPlans
                        = new ArrayList<LogicalPlan>(getJoinPlans().get(input)) ;
            if (innerPlans.size() != 1) {
                int errCode = 1012;
                String msg = "Each COGroup input has to have "
                + "the same number of inner plans";
                throw new FrontendException(msg, errCode, PigException.INPUT, false, null) ;
            }
            byte innerType = innerPlans.get(0).getSingleLeafPlanOutputType() ;
            groupType = DataType.mergeType(groupType, innerType) ;
            if (groupType==-1)
            {
                int errCode = 1107;
                String msg = "Cannot merge join keys, incompatible types";
                throw new FrontendException(msg, errCode, PigException.INPUT) ;
            }
        }

        return groupType ;
    }

    /*
        This implementation is based on the assumption that all the
        inputs have the same join col tuple arity.
     */
    public Schema getTupleJoinSchema() throws FrontendException {
        if (!isTupleJoinCol()) {
            int errCode = 1011;
            String msg = "getTupleJoinSchema is used only when"
                + " dealing with tuple join col";
            throw new FrontendException(msg, errCode, PigException.INPUT, false, null) ;
        }

        // this fsList represents all the columns in group tuple
        List<Schema.FieldSchema> fsList = new ArrayList<Schema.FieldSchema>() ;

        int outputSchemaSize = getJoinPlans().get(getInputs().get(0)).size() ;

        // by default, they are all bytearray
        // for type checking, we don't care about aliases
        for(int i=0; i<outputSchemaSize; i++) {
            fsList.add(new Schema.FieldSchema(null, DataType.BYTEARRAY)) ;
        }

        // merge all the inner plan outputs so we know what type
        // our group column should be
        for(int i=0;i < getInputs().size(); i++) {
            LogicalOperator input = getInputs().get(i) ;
            List<LogicalPlan> innerPlans
                        = new ArrayList<LogicalPlan>(getJoinPlans().get(input)) ;

            boolean seenProjectStar = false;
            for(int j=0;j < innerPlans.size(); j++) {
                byte innerType = innerPlans.get(j).getSingleLeafPlanOutputType() ;
                ExpressionOperator eOp = (ExpressionOperator)innerPlans.get(j).getSingleLeafPlanOutputOp();

                if(eOp instanceof LOProject) {
                    if(((LOProject)eOp).isStar()) {
                        seenProjectStar = true;
                    }
                }
                        
                Schema.FieldSchema groupFs = fsList.get(j);
                groupFs.type = DataType.mergeType(groupFs.type, innerType) ;
                Schema.FieldSchema fs = eOp.getFieldSchema();
                if(null != fs) {
                    groupFs.setParent(eOp.getFieldSchema().canonicalName, eOp);
                } else {
                    groupFs.setParent(null, eOp);
                }
            }

            if(seenProjectStar) {
                int errCode = 1013;
                String msg = "Grouping attributes can either be star (*) or a list of expressions, but not both.";
                throw new FrontendException(msg, errCode, PigException.INPUT, false, null);                
            }

        }

        return new Schema(fsList) ;
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LogicalOperator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        
        // first start with LogicalOperator clone
        LOJoin  joinClone = (LOJoin)super.clone();
        
        // create deep copy of other cogroup specific members
        joinClone.mJoinPlans = new MultiMap<LogicalOperator, LogicalPlan>();
        for (Iterator<LogicalOperator> it = mJoinPlans.keySet().iterator(); it.hasNext();) {
            LogicalOperator relOp = it.next();
            Collection<LogicalPlan> values = mJoinPlans.get(relOp);
            for (Iterator<LogicalPlan> planIterator = values.iterator(); planIterator.hasNext();) {
                LogicalPlanCloneHelper lpCloneHelper = new LogicalPlanCloneHelper(planIterator.next());
                joinClone.mJoinPlans.put(relOp, lpCloneHelper.getClonedPlan());
            }
        }
        
        return joinClone;
    }

    @Override
    public ProjectionMap getProjectionMap() {
        
        if(mIsProjectionMapComputed) return mProjectionMap;
        mIsProjectionMapComputed = true;
        
        Schema outputSchema;
        
        try {
            outputSchema = getSchema();
        } catch (FrontendException fee) {
            mProjectionMap = null;
            return mProjectionMap;
        }
        
        if(outputSchema == null) {
            mProjectionMap = null;
            return mProjectionMap;
        }
        
        List<LogicalOperator> predecessors = (ArrayList<LogicalOperator>)mPlan.getPredecessors(this);
        if(predecessors == null) {
            mProjectionMap = null;
            return mProjectionMap;
        }
        
        MultiMap<Integer, ProjectionMap.Column> mapFields = new MultiMap<Integer, ProjectionMap.Column>();
        List<Integer> addedFields = new ArrayList<Integer>();
        boolean[] unknownSchema = new boolean[predecessors.size()];
        boolean anyUnknownInputSchema = false;
        int outputColumnNum = 0;
        
        for(int inputNum = 0; inputNum < predecessors.size(); ++inputNum) {
            LogicalOperator predecessor = predecessors.get(inputNum);
            Schema inputSchema = null;        
            
            try {
                inputSchema = predecessor.getSchema();
            } catch (FrontendException fee) {
                mProjectionMap = null;
                return mProjectionMap;
            }
            
            if(inputSchema == null) {
                unknownSchema[inputNum] = true;
                outputColumnNum++;
                addedFields.add(inputNum);
                anyUnknownInputSchema = true;
            } else {
                unknownSchema[inputNum] = false;
                for(int inputColumn = 0; inputColumn < inputSchema.size(); ++inputColumn) {
                    mapFields.put(outputColumnNum++, 
                            new ProjectionMap.Column(new Pair<Integer, Integer>(inputNum, inputColumn)));
                }
            }
        }
        
        //TODO
        /*
         * For now, if there is any input that has an unknown schema
         * flag it and return a null ProjectionMap.
         * In the future, when unknown schemas are handled
         * mark inputs that have unknown schemas as output columns
         * that have been added.
         */

        if(anyUnknownInputSchema) {
            mProjectionMap = null;
            return mProjectionMap;
        }
        
        if(addedFields.size() == 0) {
            addedFields = null;
        }

        mProjectionMap = new ProjectionMap(mapFields, null, addedFields);
        return mProjectionMap;
    }

    @Override
    public List<RequiredFields> getRequiredFields() {        
        List<LogicalOperator> predecessors = mPlan.getPredecessors(this);
        
        if(predecessors == null) {
            return null;
        }
        
        List<RequiredFields> requiredFields = new ArrayList<RequiredFields>();
        
        for(int inputNum = 0; inputNum < predecessors.size(); ++inputNum) {
            Set<Pair<Integer, Integer>> fields = new HashSet<Pair<Integer, Integer>>();
            Set<LOProject> projectSet = new HashSet<LOProject>();
            boolean groupByStar = false;

            for (LogicalPlan plan : mJoinPlans.get(predecessors.get(inputNum))) {
                TopLevelProjectFinder projectFinder = new TopLevelProjectFinder(plan);
                try {
                    projectFinder.visit();
                } catch (VisitorException ve) {
                    requiredFields.clear();
                    requiredFields.add(null);
                    return requiredFields;
                }
                projectSet.addAll(projectFinder.getProjectSet());
                if(projectFinder.getProjectStarSet() != null) {
                    groupByStar = true;
                }
            }

            if(groupByStar) {
                requiredFields.add(new RequiredFields(true));
            } else {                
                for (LOProject project : projectSet) {
                    for (int inputColumn : project.getProjection()) {
                        fields.add(new Pair<Integer, Integer>(inputNum, inputColumn));
                    }
                }
        
                if(fields.size() == 0) {
                    requiredFields.add(new RequiredFields(false, true));
                } else {                
                    requiredFields.add(new RequiredFields(new ArrayList<Pair<Integer, Integer>>(fields)));
                }
            }
        }
        
        return (requiredFields.size() == 0? null: requiredFields);
    }

    /* (non-Javadoc)
     * @see org.apache.pig.impl.plan.Operator#rewire(org.apache.pig.impl.plan.Operator, org.apache.pig.impl.plan.Operator)
     */
    @Override
    @SuppressWarnings("unchecked")
    public void rewire(Operator oldPred, int oldPredIndex, Operator newPred, boolean useOldPred) throws PlanException {
        super.rewire(oldPred, oldPredIndex, newPred, useOldPred);
        LogicalOperator previous = (LogicalOperator) oldPred;
        LogicalOperator current = (LogicalOperator) newPred;
        Set<LogicalOperator> joinInputs = new HashSet<LogicalOperator>(mJoinPlans.keySet()); 
        for(LogicalOperator input: joinInputs) {
            if(input.equals(previous)) {
                //replace the references to the key(i.e., previous) in the values with current
                for(LogicalPlan plan: mJoinPlans.get(input)) {
                    try {
                        ProjectFixerUpper projectFixer = new ProjectFixerUpper(
                                plan, previous, oldPredIndex, current, useOldPred, this);
                        projectFixer.visit();
                    } catch (VisitorException ve) {
                        int errCode = 2144;
                        String msg = "Problem while fixing project inputs during rewiring.";
                        throw new PlanException(msg, errCode, PigException.BUG, ve);
                    }
                }
                //remove the key and the values
                List<LogicalPlan> plans = (List<LogicalPlan>)mJoinPlans.get(previous);
                mJoinPlans.removeKey(previous);
                
                //reinsert new key and values
                mJoinPlans.put(current, plans);
            }
        }
    }
    
    @Override
    public List<RequiredFields> getRelevantInputs(int output, int column) throws FrontendException {
        if (!mIsSchemaComputed)
            getSchema();
        
        if (output!=0)
            return null;
        
        if (column<0)
            return null;
        
        if (mSchema==null)
            return null;
        
        // remove this check after PIG-592 fixed
        if (mSchemaInputMapping==null || mSchemaInputMapping.size()==0 || 
                mSchema.size()!=mSchemaInputMapping.size())
            return null;
        
        if (column>mSchema.size()-1)
            return null;
        
        List<LogicalOperator> predecessors = (ArrayList<LogicalOperator>)mPlan.getPredecessors(this);
        
        if(predecessors == null) {
            return null;
        }

        // Figure out the # of input does this output column belong to, and the # of column of that input.
        // When we call getSchema, we will cache mSchemaInputMapping for a mapping of output column and it's input. 
        // We count the number of different inputs we've seen from mSchemaInputMapping[0] to
        // mSchemaInputMapping[column] to find out the # of input
        List<RequiredFields> result = new ArrayList<RequiredFields>();
        
        for (int i=0;i<predecessors.size();i++)
            result.add(null);
        
        int inputNum = -1;
        int inputColumn = 0;
        LogicalOperator op = null;
        for (int i=0;i<=column;i++)
        {
            if (mSchemaInputMapping.get(i)!=op)
            {
                inputNum++;
                inputColumn = 0;
                op = mSchemaInputMapping.get(i);
            }
            else
                inputColumn++;
        }

        ArrayList<Pair<Integer, Integer>> inputList = new ArrayList<Pair<Integer, Integer>>();
        inputList.add(new Pair<Integer, Integer>(inputNum, inputColumn));
        RequiredFields requiredFields = new RequiredFields(inputList);
        result.set(inputNum, requiredFields);
        return result;
    }

    /**
     * @return the mInnerFlags
     */
    public boolean[] getInnerFlags() {
        return getCopy(mInnerFlags);
    }
    
    @Override
    public boolean pruneColumns(List<Pair<Integer, Integer>> columns)
            throws FrontendException {
        if (!mIsSchemaComputed)
            getSchema();
        if (mSchema == null) {
            log
                    .warn("Cannot prune columns in cogroup, no schema information found");
            return false;
        }

        List<LogicalOperator> predecessors = mPlan.getPredecessors(this);

        if (predecessors == null) {
            int errCode = 2190;
            throw new FrontendException("Cannot find predecessors for join",
                    errCode, PigException.BUG);
        }

        for (Pair<Integer, Integer> column : columns) {
            if (column.first < 0 || column.first > predecessors.size()) {
                int errCode = 2191;
                throw new FrontendException("No input " + column.first
                        + " to prune in join", errCode, PigException.BUG);
            }
            if (column.second < 0) {
                int errCode = 2192;
                throw new FrontendException("column to prune does not exist", errCode, PigException.BUG);
            }
            for (LogicalPlan plan : mJoinPlans.get(predecessors
                    .get(column.first))) {
                pruneColumnInPlan(plan, column.second);
            }
        }
        super.pruneColumns(columns);
        return true;
    }
}
