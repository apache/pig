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
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.pig.PigException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.optimizer.SchemaRemover;
import org.apache.pig.impl.logicalLayer.schema.SchemaMergeException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;

public class LOCogroup extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    /**
     * Cogroup contains a list of logical operators corresponding to the
     * relational operators and a list of generates for each relational
     * operator. Each generate operator in turn contains a list of expressions
     * for the columns that are projected
     */
    private boolean[] mIsInner;
    private static Log log = LogFactory.getLog(LOCogroup.class);
    private MultiMap<LogicalOperator, LogicalPlan> mGroupByPlans;

    /**
     * 
     * @param plan
     *            LogicalPlan this operator is a part of.
     * @param k
     *            OperatorKey for this operator
     * @param groupByPlans
     *            the group by columns
     * @param isInner
     *            indicates whether the cogroup is inner for each relation
     */
    public LOCogroup(
            LogicalPlan plan,
            OperatorKey k,
            MultiMap<LogicalOperator, LogicalPlan> groupByPlans,
            boolean[] isInner) {
        super(plan, k);
        mGroupByPlans = groupByPlans;
        mIsInner = isInner;
    }

    public List<LogicalOperator> getInputs() {
        return mPlan.getPredecessors(this);
    }

    public MultiMap<LogicalOperator, LogicalPlan> getGroupByPlans() {
        return mGroupByPlans;
    }    

    public void setGroupByPlans(MultiMap<LogicalOperator, LogicalPlan> groupByPlans) {
        mGroupByPlans = groupByPlans;
    }    

    public boolean[] getInner() {
        return mIsInner;
    }

    public void setInner(boolean[] inner) {
        mIsInner = inner;
    }

    @Override
    public String name() {
        return "CoGroup " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public Schema getSchema() throws FrontendException {
        List<LogicalOperator> inputs = mPlan.getPredecessors(this);
        /*
         * Dumping my understanding of how the schema of a Group/CoGroup will
         * look. The first field of the resulting tuple will have the alias
         * 'group'. The schema for this field is a union of the group by columns
         * for each input. The subsequent fields in the output tuple will have
         * the alias of the input as the alias for a bag that contains the
         * tuples from the input that match the grouping criterion
         */
        if (!mIsSchemaComputed) {
            List<Schema.FieldSchema> fss = new ArrayList<Schema.FieldSchema>(
                    inputs.size() + 1);
            // one more to account for the "group"
            // the alias of the first field is group and hence the
            // string "group"

            /*
             * Here goes an attempt to describe how the schema for the first
             * column - 'group' should look like. If the number of group by
             * columns = 1 then the schema for 'group' is the
             * schema(fieldschema(col)) If the number of group by columns > 1
             * then find the set union of the group by columns and form the
             * schema as schema(list<fieldschema of the cols>)
             * The parser will ensure that the number of group by columns are
             * the same across all inputs. The computation of the schema for group
             * is as follows:
             * For each input of cogroup, for each operator (projection ,udf, constant), etc.
             * compute the multimaps <group_column_number, alias> and <group_column_number, operator>
             * and <alias, expression_operator>
             * Also set the lookup table for each alias to false
             */

            Schema groupBySchema = null;
            List<Schema.FieldSchema> groupByFss = new ArrayList<Schema.FieldSchema>();
            Map<String, Boolean> aliasLookup = new HashMap<String, Boolean>();
            MultiMap<String, ExpressionOperator> aliasExop = new MultiMap<String, ExpressionOperator>();
            MultiMap<Integer, String> positionAlias = new MultiMap<Integer, String>();
            MultiMap<Integer, ExpressionOperator> positionOperators = new MultiMap<Integer, ExpressionOperator>();
            
            for (LogicalOperator op : inputs) {
                int position = 0;
                for(LogicalPlan plan: mGroupByPlans.get(op)) {
                    for(LogicalOperator eOp: plan.getLeaves()) {
                        Schema.FieldSchema fs = ((ExpressionOperator)eOp).getFieldSchema();
                        if (null != fs) {
                            String alias = fs.alias;
                            if(null != alias) {
                                aliasLookup.put(alias, false);
                                aliasExop.put(alias, (ExpressionOperator)eOp);                            
                                positionAlias.put(position, alias);
                            }
                            //store the operators for each position in the group
                        } else {
                            log.warn("Field Schema of an expression operator cannot be null"); 
                        }
                        positionOperators.put(position, (ExpressionOperator)eOp);
                    }
                    ++position;
                }
            }
            
            /*
             * Now that the multi maps and the look up table are computed, do the following:
             * for each column in the group, in order check if the alias is alaready used or not
             * If the alias is already used, check for the next unused alias.
             * IF none of the aliases can be used then the alias of that column is null
             * If an alias is found usable, then use that alias and the schema of the expression operator
             * corresponding to that position. Note that the first operator for that position is
             * picked. The type checker will ensure that the correct schema is merged
             */
            int arity = mGroupByPlans.get(inputs.get(0)).size();
            for (int i = 0; i < arity; ++i) {
                Schema.FieldSchema groupByFs;
                Collection<String> cAliases = positionAlias.get(i);
                if(null != cAliases) {
                    Object[] aliases = cAliases.toArray();
                    for(int j = 0; j < aliases.length; ++j) {
                        String alias = (String) aliases[j];
                        if(null != alias) {
                            //Collection<ExpressionOperator> cEops = aliasExop.get(alias);
                            Collection<ExpressionOperator> cEops = positionOperators.get(i);
                            if(null != cEops) {
                                ExpressionOperator eOp = (ExpressionOperator) (cEops.toArray())[0];
                                if(null != eOp) {
                                    if(!aliasLookup.get(alias)) {
                                        Schema.FieldSchema fs = eOp.getFieldSchema();
                                        if(null != fs) {
                                            groupByFs = new Schema.FieldSchema(alias, fs.schema, fs.type);
                                            groupByFss.add(groupByFs);
                                            aliasLookup.put(alias, true);
                                        } else {
                                            groupByFs = new Schema.FieldSchema(alias, null, DataType.BYTEARRAY);
                                            groupByFss.add(groupByFs);
                                        }
                                        setFieldSchemaParent(groupByFs, positionOperators, i);
                                        break;
                                    } else {
                                        if((j + 1) < aliases.length) {
                                            continue;
                                        } else {
                                            //we have seen this alias before
                                            //just add the schema of the expression operator with the null alias
                                            Schema.FieldSchema fs = eOp.getFieldSchema();
                                            if(null != fs) {
                                                groupByFs = new Schema.FieldSchema(null, fs.schema, fs.type);
                                                groupByFss.add(groupByFs);
                                                for(ExpressionOperator op: cEops) {
                                                    Schema.FieldSchema opFs = op.getFieldSchema();
                                                    if(null != opFs) {
                                                        groupByFs.setParent(opFs.canonicalName, eOp);
                                                    } else {
                                                        groupByFs.setParent(null, eOp);
                                                    }
                                                }
                                            } else {
                                                groupByFs = new Schema.FieldSchema(null, null, DataType.BYTEARRAY);
                                                groupByFss.add(groupByFs);
                                            }
                                            setFieldSchemaParent(groupByFs, positionOperators, i);
                                            break;
                                        }
                                    }
                                } else {
                                    //should not be here
                                    log.debug("Cannot be here: we cannot have a collection of null expression operators");
                                }
                            } else {
                                //should not be here
                                log.debug("Cannot be here: we should have an expression operator at each position");
                            }
                        } else {
                            //should not be here
                            log.debug("Cannot be here: we cannot have a collection of null aliases ");
                        }
                    }
                } else {
                    //We do not have any alias for this position in the group by columns
                    //We have positions $1, $2, etc.
                    Collection<ExpressionOperator> cEops = positionOperators.get(i);
                    if(null != cEops) {
                        ExpressionOperator eOp = (ExpressionOperator) (cEops.toArray())[0];
                        if(null != eOp) {
                            Schema.FieldSchema fs = eOp.getFieldSchema();
                            if(null != fs) {
                                groupByFs = new Schema.FieldSchema(null, fs.schema, fs.type);
                                groupByFss.add(groupByFs);
                            } else {
                                groupByFs = new Schema.FieldSchema(null, null, DataType.BYTEARRAY);
                                groupByFss.add(groupByFs);
                            }
                        } else {
                            groupByFs = new Schema.FieldSchema(null, DataType.BYTEARRAY);
                            groupByFss.add(groupByFs);
                        }
                    } else {
                        groupByFs = new Schema.FieldSchema(null, DataType.BYTEARRAY);
                        groupByFss.add(groupByFs);
                    }
                    setFieldSchemaParent(groupByFs, positionOperators, i);
                }
            }            

            groupBySchema = new Schema(groupByFss);

            if(1 == arity) {
                byte groupByType = getAtomicGroupByType();
                Schema groupSchema = groupByFss.get(0).schema;
                Schema.FieldSchema groupByFs = new Schema.FieldSchema("group", groupSchema, groupByType);
                setFieldSchemaParent(groupByFs, positionOperators, 0);
                fss.add(groupByFs);
            } else {
                Schema mergedGroupSchema = getTupleGroupBySchema();
                if(mergedGroupSchema.size() != groupBySchema.size()) {
                    mSchema = null;
                    mIsSchemaComputed = false;
                    int errCode = 2000;
                    String msg = "Internal error. Mismatch in group by arities. Expected: " + mergedGroupSchema + ". Found: " + groupBySchema;
                    throw new FrontendException(msg, errCode, PigException.BUG, false, null);
                } else {
                    for(int i = 0; i < mergedGroupSchema.size(); ++i) {
                        Schema.FieldSchema mergedFs = mergedGroupSchema.getField(i);
                        Schema.FieldSchema groupFs = groupBySchema.getField(i);
                        mergedFs.alias = groupFs.alias;
                        mergedGroupSchema.addAlias(mergedFs.alias, mergedFs);
                    }
                }
                
                Schema.FieldSchema groupByFs = new Schema.FieldSchema("group", mergedGroupSchema);
                fss.add(groupByFs);
                for(int i = 0; i < arity; ++i) {
                    setFieldSchemaParent(groupByFs, positionOperators, i);
                }
            }
            for (LogicalOperator op : inputs) {
                try {
                    Schema.FieldSchema bagFs = new Schema.FieldSchema(op.getAlias(),
                            op.getSchema(), DataType.BAG);
                    fss.add(bagFs);
                    setFieldSchemaParent(bagFs, op);
                } catch (FrontendException ioe) {
                    mIsSchemaComputed = false;
                    mSchema = null;
                    throw ioe;
                }
            }
            mIsSchemaComputed = true;
            mSchema = new Schema(fss);
            mType = DataType.BAG;//mType is from the super class
        }
        return mSchema;
    }

    public boolean isTupleGroupCol() {
        List<LogicalOperator> inputs = mPlan.getPredecessors(this);
        if (inputs == null || inputs.size() == 0) {
            throw new AssertionError("COGroup.isTupleGroupCol() can be called "
                                     + "after it has an input only") ;
        }
        return mGroupByPlans.get(inputs.get(0)).size() > 1 ;
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
    public void switchGroupByPlanOp(LogicalOperator oldOp,
                                    LogicalOperator newOp) {
        Collection<LogicalPlan> innerPlans = mGroupByPlans.removeKey(oldOp) ;
        mGroupByPlans.put(newOp, innerPlans);
    }

    public void unsetSchema() throws VisitorException{
        for(LogicalOperator input: getInputs()) {
            for(LogicalPlan plan: mGroupByPlans.get(input)) {
                SchemaRemover sr = new SchemaRemover(plan);
                sr.visit();
            }
        }
        super.unsetSchema();
    }

    /**
     * This can be used to get the merged type of output group col
     * only when the group col is of atomic type
     * TODO: This doesn't work with group by complex type
     * @return The type of the group by
     */
    public byte getAtomicGroupByType() throws FrontendException {
        if (isTupleGroupCol()) {
            int errCode = 1010;
            String msg = "getAtomicGroupByType is used only when"
                + " dealing with atomic group col";
            throw new FrontendException(msg, errCode, PigException.INPUT, false, null) ;
        }

        byte groupType = DataType.BYTEARRAY ;
        // merge all the inner plan outputs so we know what type
        // our group column should be
        for(int i=0;i < getInputs().size(); i++) {
            LogicalOperator input = getInputs().get(i) ;
            List<LogicalPlan> innerPlans
                        = new ArrayList<LogicalPlan>(getGroupByPlans().get(input)) ;
            if (innerPlans.size() != 1) {
                int errCode = 1012;
                String msg = "Each COGroup input has to have "
                + "the same number of inner plans";
                throw new FrontendException(msg, errCode, PigException.INPUT, false, null) ;
            }
            byte innerType = innerPlans.get(0).getSingleLeafPlanOutputType() ;
            groupType = DataType.mergeType(groupType, innerType) ;
        }

        return groupType ;
    }

    /*
        This implementation is based on the assumption that all the
        inputs have the same group col tuple arity.
        TODO: This doesn't work with group by complex type
     */
    public Schema getTupleGroupBySchema() throws FrontendException {
        if (!isTupleGroupCol()) {
            int errCode = 1011;
            String msg = "getTupleGroupBySchema is used only when"
                + " dealing with tuple group col";
            throw new FrontendException(msg, errCode, PigException.INPUT, false, null) ;
        }

        // this fsList represents all the columns in group tuple
        List<Schema.FieldSchema> fsList = new ArrayList<Schema.FieldSchema>() ;

        int outputSchemaSize = getGroupByPlans().get(getInputs().get(0)).size() ;

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
                        = new ArrayList<LogicalPlan>(getGroupByPlans().get(input)) ;

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

    private void setFieldSchemaParent(Schema.FieldSchema fs, MultiMap<Integer, ExpressionOperator> positionOperators, int position) throws FrontendException {
        for(ExpressionOperator op: positionOperators.get(position)) {
            Schema.FieldSchema opFs = op.getFieldSchema();
            if(null != opFs) {
                fs.setParent(opFs.canonicalName, op);
            } else {
                fs.setParent(null, op);
            }
        }
    }

    private void setFieldSchemaParent(Schema.FieldSchema fs, LogicalOperator op) throws FrontendException {
        Schema s = op.getSchema();
        if(null != s) {
            for(Schema.FieldSchema inputFs: s.getFields()) {
                if(null != inputFs) {
                    fs.setParent(inputFs.canonicalName, op);
                } else {
                    fs.setParent(null, op);
                }
            }
        } else {
            fs.setParent(null, op);
        }
    }

    /**
     * @see org.apache.pig.impl.logicalLayer.LogicalOperator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        
        // first start with LogicalOperator clone
        LOCogroup  cogroupClone = (LOCogroup)super.clone();
        
        // create deep copy of other cogroup specific members
        cogroupClone.mIsInner = new boolean[mIsInner.length];
        for (int i = 0; i < mIsInner.length; i++) {
            cogroupClone.mIsInner[i] = mIsInner[i];
        }
        
        cogroupClone.mGroupByPlans = new MultiMap<LogicalOperator, LogicalPlan>();
        for (Iterator<LogicalOperator> it = mGroupByPlans.keySet().iterator(); it.hasNext();) {
            LogicalOperator relOp = it.next();
            Collection<LogicalPlan> values = mGroupByPlans.get(relOp);
            for (Iterator<LogicalPlan> planIterator = values.iterator(); planIterator.hasNext();) {
                LogicalPlanCloneHelper lpCloneHelper = new LogicalPlanCloneHelper(planIterator.next());
                cogroupClone.mGroupByPlans.put(relOp, lpCloneHelper.getClonedPlan());
            }
        }
        
        return cogroupClone;
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
        
        //the column with the alias 'group' can be mapped in several ways
        //1. group A by $0;
        //Here the mapping is 0 -> (0, 0)
        //2. group A by ($0, $1);
        //Here there is no direct mapping and 'group' is an added column
        //3. cogroup A by $0, B by $0;
        //Here the mapping is 0 -> ((0, 0), (1, 0))
        //4. cogroup A by ($0, $1), B by ($0, $1);
        //Here there is no direct mapping and 'group' is an added column
        //For anything other than a simple project 'group' is an added column
        
        MultiMap<LogicalOperator, LogicalPlan> groupByPlans = getGroupByPlans();
        
        boolean groupByAdded = false;
        MultiMap<Integer, Pair<Integer, Integer>> mapFields = new MultiMap<Integer, Pair<Integer, Integer>>();
        List<Pair<Integer, Integer>> removedFields = new ArrayList<Pair<Integer, Integer>>();
        
        for(int inputNum = 0; (inputNum < predecessors.size()) && (!groupByAdded); ++inputNum) {
            LogicalOperator predecessor = predecessors.get(inputNum);

            List<LogicalPlan> predecessorPlans = (ArrayList<LogicalPlan>) groupByPlans.get(predecessor);

            int inputColumn = -1;
            for(LogicalPlan predecessorPlan: predecessorPlans) {                
                List<LogicalOperator> leaves = predecessorPlan.getLeaves();
                if(leaves == null || leaves.size() > 1) {
                    groupByAdded = true;
                    break;
                }
                
                if(leaves.get(0) instanceof LOProject) {
                    //find out if this project is a chain of projects
                    if(LogicalPlan.chainOfProjects(predecessorPlan)) {
                        LOProject rootProject = (LOProject)predecessorPlan.getRoots().get(0);
                        inputColumn = rootProject.getCol();
                        mapFields.put(0, new Pair<Integer, Integer>(inputNum, inputColumn));
                    }
                } else {
                    groupByAdded = true;
                }                
            }
            
            Schema inputSchema;            
            try {
                inputSchema = predecessor.getSchema();
            } catch (FrontendException fee) {
                return null;
            }
            
            if(inputSchema != null) {
                for(int column = 0; column < inputSchema.size(); ++column) {
                    if(!groupByAdded && inputColumn != column) {
                        removedFields.add(new Pair<Integer, Integer>(inputNum, column));
                    }
                }
            }

        }

        List<Integer> addedFields = new ArrayList<Integer>();

        if(groupByAdded) {
            addedFields.add(0); //for the column 'group'
            mapFields = null; //since 'group' is an added column there is no mapping            
        }
        
        //the columns 1 through n - 1 are generated by cogroup
        for(int i = 0; i < groupByPlans.keySet().size(); ++i) {
            addedFields.add(i+ 1);
        }
        
        if(removedFields.size() == 0) {
            removedFields = null;
        }

        return new ProjectionMap(mapFields, removedFields, addedFields);
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

            for (LogicalPlan plan : getGroupByPlans().get(predecessors.get(inputNum))) {
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
