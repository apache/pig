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
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.optimizer.SchemaRemover;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;

/**
 * This is the logical operator for the Fragment Replicate Join
 * It holds the user specified information and is responsible for 
 * the schema computation. This mimics the LOCogroup operator except
 * the schema computation.
 */
public class LOFRJoin extends LogicalOperator {
    private static final long serialVersionUID = 2L;
    
//    private boolean[] mIsInner;
    private static Log log = LogFactory.getLog(LOFRJoin.class);
    private MultiMap<LogicalOperator, LogicalPlan> mJoinColPlans;
    private LogicalOperator fragOp;
    
    public LOFRJoin(
            LogicalPlan plan,
            OperatorKey k,
            MultiMap<LogicalOperator, LogicalPlan> joinColPlans,
            boolean[] isInner, LogicalOperator fragOp) {
        super(plan, k);
        mJoinColPlans = joinColPlans;
//        mIsInner = isInner;
        this.fragOp = fragOp;
    }

    @Override
    /**
     * Uses the schema from its input operators and dedups
     * those fields that have the same alias and sets the
     * schema for the join
     */
    public Schema getSchema() throws FrontendException {
        List<LogicalOperator> inputs = mPlan.getPredecessors(this);
        mType = DataType.BAG;//mType is from the super class
        Hashtable<String, Integer> nonDuplicates = new Hashtable<String, Integer>();
        if(!mIsSchemaComputed){
            List<Schema.FieldSchema> fss = new ArrayList<Schema.FieldSchema>();
            int i=-1;
            for (LogicalOperator op : inputs) {
                try {
                    Schema cSchema = op.getSchema();
                    if(cSchema!=null){
                        
                        for (FieldSchema schema : cSchema.getFields()) {
                            ++i;
                            if(nonDuplicates.containsKey(schema.alias))
                                {
                                    if(nonDuplicates.get(schema.alias)!=-1) {
                                        nonDuplicates.remove(schema.alias);
                                        nonDuplicates.put(schema.alias, -1);
                                    }
                                }
                            else
                                nonDuplicates.put(schema.alias, i);
                            FieldSchema newFS = new FieldSchema(op.getAlias()+"::"+schema.alias,schema.schema,schema.type);
                            newFS.setParent(schema.canonicalName, op);
                            fss.add(newFS);
                        }
                    }
                    else
                        fss.add(new FieldSchema(null,DataType.BYTEARRAY));
                } catch (FrontendException ioe) {
                    mIsSchemaComputed = false;
                    mSchema = null;
                    throw ioe;
                }
            }
            mIsSchemaComputed = true;
            for (Entry<String, Integer> ent : nonDuplicates.entrySet()) {
                int ind = ent.getValue();
                if(ind==-1) continue;
                FieldSchema prevSch = fss.get(ind);
                fss.set(ind, new FieldSchema(ent.getKey(),prevSch.schema,prevSch.type));
            }
            mSchema = new Schema(fss);
        }
        return mSchema;
    }

    public MultiMap<LogicalOperator, LogicalPlan> getJoinColPlans() {
        return mJoinColPlans;
    }
    
    public void switchJoinColPlanOp(LogicalOperator oldOp,
            LogicalOperator newOp) {
        Collection<LogicalPlan> innerPlans = mJoinColPlans.removeKey(oldOp) ;
        mJoinColPlans.put(newOp, innerPlans);
        if(fragOp.getOperatorKey().equals(oldOp.getOperatorKey()))
            fragOp = newOp;
    }
    
    public void unsetSchema() throws VisitorException{
        for(LogicalOperator input: getInputs()) {
            Collection<LogicalPlan> grpPlans = mJoinColPlans.get(input);
            if(grpPlans!=null)
                for(LogicalPlan plan : grpPlans) {
                    SchemaRemover sr = new SchemaRemover(plan);
                    sr.visit();
                }
        }
        super.unsetSchema();
    }
    
    public List<LogicalOperator> getInputs() {
        return mPlan.getPredecessors(this);
    }
    
    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    @Override
    public String name() {
        return "FRJoin " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    public LogicalOperator getFragOp() {
        return fragOp;
    }

    public void setFragOp(LogicalOperator fragOp) {
        this.fragOp = fragOp;
    }
    
    public boolean isTupleJoinCol() {
        List<LogicalOperator> inputs = mPlan.getPredecessors(this);
        if (inputs == null || inputs.size() == 0) {
            throw new AssertionError("join.isTuplejoinCol() can be called "
                                     + "after it has an input only") ;
        }
		// NOTE: we depend on the number of inner plans to determine
		// if the join col is a tuple. This could be an issue when there
		// is only one inner plan with Project(*). For that case if the
		// corresponding input to the Project had a schema then the front end 
		// would translate the single Project(*) (through ProjectStarTranslator)
		// to many individual Projects. So the number of inner plans would then 
		// be > 1 BEFORE reaching here. For the Project(*) case when the corresponding
		// input for the Project has no schema, treating it as an atomic col join
		// does not cause any problems since no casts need to be inserted in that case
		// anyway.
        return mJoinColPlans.get(inputs.get(0)).size() > 1 ;
    }
    public byte getAtomicJoinColType() throws FrontendException {
        if (isTupleJoinCol()) {
            int errCode = 1010;
            String msg = "getAtomicGroupByType is used only when"
                + " dealing with atomic join col";
            throw new FrontendException(msg, errCode, PigException.INPUT, false, null) ;
        }

        byte joinColType = DataType.BYTEARRAY ;
        // merge all the inner plan outputs so we know what type
        // our join column should be
        for(int i=0;i < getInputs().size(); i++) {
            LogicalOperator input = getInputs().get(i) ;
            List<LogicalPlan> innerPlans
                        = new ArrayList<LogicalPlan>(getJoinColPlans().get(input)) ;
            if (innerPlans.size() != 1) {
                int errCode = 1012;
                String msg = "Each join input has to have "
                + "the same number of inner plans";
                throw new FrontendException(msg, errCode, PigException.INPUT, false, null) ;
            }
            byte innerType = innerPlans.get(0).getSingleLeafPlanOutputType() ;
            joinColType = DataType.mergeType(joinColType, innerType) ;
        }

        return joinColType ;
    }

    public Schema getTupleJoinColSchema() throws FrontendException {
        if (!isTupleJoinCol()) {
            int errCode = 1011;
            String msg = "getTupleGroupBySchema is used only when"
                + " dealing with tuple join col";
            throw new FrontendException(msg, errCode, PigException.INPUT, false, null) ;
        }

        // this fsList represents all the columns in join tuple
        List<Schema.FieldSchema> fsList = new ArrayList<Schema.FieldSchema>() ;

        int outputSchemaSize = getJoinColPlans().get(getInputs().get(0)).size() ;

        // by default, they are all bytearray
        // for type checking, we don't care about aliases
        for(int i=0; i<outputSchemaSize; i++) {
            fsList.add(new Schema.FieldSchema(null, DataType.BYTEARRAY)) ;
        }

        // merge all the inner plan outputs so we know what type
        // our join column should be
        for(int i=0;i < getInputs().size(); i++) {
            LogicalOperator input = getInputs().get(i) ;
            List<LogicalPlan> innerPlans
                        = new ArrayList<LogicalPlan>(getJoinColPlans().get(input)) ;

            boolean seenProjectStar = false;
            for(int j=0;j < innerPlans.size(); j++) {
                byte innerType = innerPlans.get(j).getSingleLeafPlanOutputType() ;
                ExpressionOperator eOp = (ExpressionOperator)innerPlans.get(j).getSingleLeafPlanOutputOp();

                if(eOp instanceof LOProject) {
                    if(((LOProject)eOp).isStar()) {
                        seenProjectStar = true;
                    }
                }
                        
                Schema.FieldSchema joinFs = fsList.get(j);
                joinFs.type = DataType.mergeType(joinFs.type, innerType) ;
                Schema.FieldSchema fs = eOp.getFieldSchema();
                if(null != fs) {
                    joinFs.setParent(eOp.getFieldSchema().canonicalName, eOp);
                } else {
                    joinFs.setParent(null, eOp);
                }
            }

            if(seenProjectStar && innerPlans.size() > 1) {                
                int errCode = 1013;
                String msg = "Join attributes can either be star (*) or a list of expressions, but not both.";
                throw new FrontendException(msg, errCode, PigException.INPUT, false, null);
                
            }

        }

        return new Schema(fsList) ;
    }

    @Override
    public ProjectionMap getProjectionMap() {
        Schema outputSchema;
        
        try {
            outputSchema = getSchema();
        } catch (FrontendException fee) {
            return null;
        }
        
        if(outputSchema == null) {
            return null;
        }
        
        List<LogicalOperator> predecessors = (ArrayList<LogicalOperator>)mPlan.getPredecessors(this);
        if(predecessors == null) {
            return null;
        }
        
        MultiMap<Integer, Pair<Integer, Integer>> mapFields = new MultiMap<Integer, Pair<Integer, Integer>>();
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
                return null;
            }
            
            if(inputSchema == null) {
                unknownSchema[inputNum] = true;
                outputColumnNum++;
                addedFields.add(inputNum);
                anyUnknownInputSchema = true;
            } else {
                unknownSchema[inputNum] = false;
                for(int inputColumn = 0; inputColumn < inputSchema.size(); ++inputColumn) {
                    mapFields.put(outputColumnNum++, new Pair<Integer, Integer>(inputNum, inputColumn));
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
            return null;
        }
        
        if(addedFields.size() == 0) {
            addedFields = null;
        }

        return new ProjectionMap(mapFields, null, addedFields);
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

            for (LogicalPlan plan : this.getJoinColPlans().get(predecessors.get(inputNum))) {
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

}
