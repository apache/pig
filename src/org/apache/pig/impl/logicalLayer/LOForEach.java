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
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;

import org.apache.pig.PigException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaMergeException;
import org.apache.pig.impl.logicalLayer.optimizer.SchemaRemover;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.data.DataType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class LOForEach extends LogicalOperator {

    private static final long serialVersionUID = 2L;

    /**
     * The foreach operator supports nested query plans. At this point its one
     * level of nesting. Foreach can have a list of operators that need to be
     * applied over the input.
     */

    private ArrayList<LogicalPlan> mForEachPlans;
    private ArrayList<Boolean> mFlatten;
    private ArrayList<Schema> mUserDefinedSchema = null;
    private static Log log = LogFactory.getLog(LOForEach.class);

    /**
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param foreachPlans
     *            the list of plans that are applied for each input
     * @param flattenList
     *            boolean list that tells which elements of the foreach
     *            projection should be flattened.
     */

    public LOForEach(LogicalPlan plan, OperatorKey k,
            ArrayList<LogicalPlan> foreachPlans, ArrayList<Boolean> flattenList) {

        super(plan, k);
        mForEachPlans = foreachPlans;
        mFlatten = flattenList;
    }

    public LOForEach(LogicalPlan plan, OperatorKey k,
            ArrayList<LogicalPlan> foreachPlans, ArrayList<Boolean> flattenList,
            ArrayList<Schema> userDefinedSchemaList) {

        super(plan, k);
        mForEachPlans = foreachPlans;
        mFlatten = flattenList;
        mUserDefinedSchema = userDefinedSchemaList;
    }

    public ArrayList<LogicalPlan> getForEachPlans() {
        return mForEachPlans;
    }

    public void setForEachPlans(ArrayList<LogicalPlan> foreachPlans) {
        mForEachPlans = foreachPlans;
    }

    public List<Boolean> getFlatten() {
        return mFlatten;
    }

    public void setFlatten(ArrayList<Boolean> flattenList) {
        mFlatten = flattenList;
    }

    public List<Schema> getUserDefinedSchema() {
        return mUserDefinedSchema;
    }

    public void setUserDefinedSchema(ArrayList<Schema> userDefinedSchema) {
        mUserDefinedSchema = userDefinedSchema;
    }

    @Override
    public String name() {
        return "ForEach " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    public byte getType() {
        return DataType.BAG ;
    }

    private void updateAliasCount(Map<String, Integer> aliases, String alias) {
        if((null == aliases) || (null == alias)) return;
		Integer count = aliases.get(alias);
		if(null == count) {
			aliases.put(alias, 1);
		} else {
			aliases.put(alias, ++count);
		}
    }

    @Override
    public Schema getSchema() throws FrontendException {
        log.debug("Entering getSchema");
        if (!mIsSchemaComputed) {
            List<Schema.FieldSchema> fss = new ArrayList<Schema.FieldSchema>(
                    mForEachPlans.size());

            for (LogicalPlan plan : mForEachPlans) {
                log.debug("Number of leaves in " + plan + " = " + plan.getLeaves().size());
                for(int i = 0; i < plan.getLeaves().size(); ++i) {
                    log.debug("Leaf" + i + "= " + plan.getLeaves().get(i));
                }
                //LogicalOperator op = plan.getRoots().get(0);
                LogicalOperator op = plan.getLeaves().get(0);
                log.debug("op: " + op.getClass().getName() + " " + op);
            }
            log.debug("Printed the leaves of the generate plans");

            Map<Schema.FieldSchema, String> flattenAlias = new HashMap<Schema.FieldSchema, String>();
            Map<String, Boolean> inverseFlattenAlias = new HashMap<String, Boolean>();
            Map<String, Integer> aliases = new HashMap<String, Integer>();

            for (int planCtr = 0; planCtr < mForEachPlans.size(); ++planCtr) {
                LogicalPlan plan = mForEachPlans.get(planCtr);
                LogicalOperator op = plan.getLeaves().get(0);
                log.debug("op: " + op.getClass().getName() + " " + op);
                log.debug("Flatten: " + mFlatten.get(planCtr));
                Schema.FieldSchema planFs;

                if(op instanceof LOProject) {
                    //the check for the type is required for statements like
                    //foreach cogroup {
                    // a1 = order a by *;
                    // generate a1;
                    //}
                    //In the above script, the generate a1, will translate to 
                    //project(a1) -> project(*) and will not be translated to a sequence of projects
                    //As a result the project(*) will remain but the return type is a bag
                    //project(*) with a data type set to tuple indicates a project(*) from an input
                    //that has no schema
                    if( (((LOProject)op).isStar() ) && (((LOProject)op).getType() == DataType.TUPLE) ) {
                        mSchema = null;
                        mIsSchemaComputed = true;
                        return mSchema;
                    }
                }
                
                try {
	                planFs = ((ExpressionOperator)op).getFieldSchema();
                    log.debug("planFs: " + planFs);
                    Schema userDefinedSchema = null;
                    if(null != mUserDefinedSchema) {
                        userDefinedSchema = mUserDefinedSchema.get(planCtr);
                    }
					if(null != planFs) {
						String outerCanonicalAlias = op.getAlias();
						if(null == outerCanonicalAlias) {
							outerCanonicalAlias = planFs.alias;
						}
						log.debug("Outer canonical alias: " + outerCanonicalAlias);
						if(mFlatten.get(planCtr)) {
							//need to extract the children and create the aliases
							//assumption here is that flatten is only for one column
							//i.e., flatten(A), flatten(A.x) and NOT
							//flatten(B.(x,y,z))
							Schema s = planFs.schema;
							if(null != s && s.isTwoLevelAccessRequired()) {
							    // this is the case where the schema is that of
					            // a bag which has just one tuple fieldschema which
					            // in turn has a list of fieldschemas. The schema
							    // after flattening would consist of the fieldSchemas
							    // present in the tuple
					            
					            // check that indeed we only have one field schema
					            // which is that of a tuple
					            if(s.getFields().size() != 1) {
					                int errCode = 1008;
					                String msg = "Expected a bag schema with a single " +
                                    "element of type "+ DataType.findTypeName(DataType.TUPLE) +
                                    " but got a bag schema with multiple elements.";
					                throw new FrontendException(msg, errCode, PigException.INPUT, false, null);
					            }
					            Schema.FieldSchema tupleFS = s.getField(0);
					            if(tupleFS.type != DataType.TUPLE) {
					                int errCode = 1009;
					                String msg = "Expected a bag schema with a single " +
                                    "element of type "+ DataType.findTypeName(DataType.TUPLE) +
                                    " but got an element of type " +
                                    DataType.findTypeName(tupleFS.type);
					                throw new FrontendException(msg, errCode, PigException.INPUT, false, null);
					            }
					            s = tupleFS.schema;
							    
							}
							if(null != s) {
								for(int i = 0; i < s.size(); ++i) {
                                    Schema.FieldSchema fs;
                                    fs = new Schema.FieldSchema(s.getField(i));
                                    fs.setParent(s.getField(i).canonicalName, op);
									log.debug("fs: " + fs);
                                    if(null != userDefinedSchema) {
                                        Schema.FieldSchema userDefinedFieldSchema;
                                        try {
                                            if(i < userDefinedSchema.size()) {
                                                userDefinedFieldSchema = userDefinedSchema.getField(i);
                                                fs = fs.mergePrefixFieldSchema(userDefinedFieldSchema);
                                            }
                                        } catch (SchemaMergeException sme) {
                                            int errCode = 1016;
                                            String msg = "Problems in merging user defined schema";
                                            throw new FrontendException(msg, errCode, PigException.INPUT, false, null, sme);
                                        }
                                        outerCanonicalAlias = null;
                                    }
									String innerCanonicalAlias = fs.alias;
                                    Schema.FieldSchema newFs;
									if((null != outerCanonicalAlias) && (null != innerCanonicalAlias)) {
										String disambiguatorAlias = outerCanonicalAlias + "::" + innerCanonicalAlias;
										newFs = new Schema.FieldSchema(disambiguatorAlias, fs.schema, fs.type);
                                        newFs.setParent(s.getField(i).canonicalName, op);
                                        fss.add(newFs);
                                        updateAliasCount(aliases, disambiguatorAlias);
										//it's fine if there are duplicates
										//we just need to record if its due to
										//flattening
									} else {
										newFs = new Schema.FieldSchema(fs);
                                        newFs.setParent(s.getField(i).canonicalName, op);
										fss.add(newFs);
									}
                                    updateAliasCount(aliases, innerCanonicalAlias);
									flattenAlias.put(newFs, innerCanonicalAlias);
									inverseFlattenAlias.put(innerCanonicalAlias, true);
								}
							} else {
                                Schema.FieldSchema newFs;
                                if(null != userDefinedSchema) {
                                    if(!DataType.isSchemaType(planFs.type)) {
                                        if(userDefinedSchema.size() > 1) {
                                            int errCode = 1017;
                                            String msg = "Schema mismatch. A basic type on flattening cannot have more than one column. User defined schema: " + userDefinedSchema;
                                            throw new FrontendException(msg, errCode, PigException.INPUT, false, null);
                                        }
								        newFs = new Schema.FieldSchema(null, planFs.type);
                                        try {
                                            newFs = newFs.mergePrefixFieldSchema(userDefinedSchema.getField(0));
                                        } catch (SchemaMergeException sme) {
                                            int errCode = 1016;
                                            String msg = "Problems in merging user defined schema";
                                            throw new FrontendException(msg, errCode, PigException.INPUT, false, null, sme);
                                        }
                                        updateAliasCount(aliases, newFs.alias);
                                        fss.add(newFs);
                                        newFs.setParent(null, op);
                                    } else {
                                        for(Schema.FieldSchema ufs: userDefinedSchema.getFields()) {
                                            Schema.FieldSchema.setFieldSchemaDefaultType(ufs, DataType.BYTEARRAY);
                                            newFs = new Schema.FieldSchema(ufs);
                                            fss.add(newFs);
                                            newFs.setParent(null, op);
                                            updateAliasCount(aliases, ufs.alias);
                                        }
                                    }
								} else {
                                    if(!DataType.isSchemaType(planFs.type)) {
								        newFs = new Schema.FieldSchema(planFs.alias, planFs.type);
                                    } else {
								        newFs = new Schema.FieldSchema(null, DataType.BYTEARRAY);
                                    }
                                    fss.add(newFs);
                                    newFs.setParent(null, op);
                                }
							}
						} else {
							//just populate the schema with the field schema of the expression operator
                            //check if the user has defined a schema for the operator; compare the schema
                            //with that of the expression operator field schema and then add it to the list
                            Schema.FieldSchema newFs = new Schema.FieldSchema(planFs);
                            if(null != userDefinedSchema) {
                                try {
                                    newFs = newFs.mergePrefixFieldSchema(userDefinedSchema.getField(0));
                                    updateAliasCount(aliases, newFs.alias);
                                } catch (SchemaMergeException sme) {
                                    int errCode = 1016;
                                    String msg = "Problems in merging user defined schema";
                                    throw new FrontendException(msg, errCode, PigException.INPUT, false, null, sme);
                                }
                            }
                            newFs.setParent(planFs.canonicalName, op);
                            fss.add(newFs);
						}
					} else {
						//did not get a valid list of field schemas
                        String outerCanonicalAlias = null;
                        if(null != userDefinedSchema) {
                            Schema.FieldSchema userDefinedFieldSchema = new Schema.FieldSchema(userDefinedSchema.getField(0));
                            fss.add(userDefinedFieldSchema);
                            userDefinedFieldSchema.setParent(null, op);
                            updateAliasCount(aliases, userDefinedFieldSchema.alias);
                        } else {
                            mSchema = null;
                            mIsSchemaComputed = true;
                            return mSchema;
                        }
					}
                } catch (FrontendException fee) {
                    mSchema = null;
                    mIsSchemaComputed = false;
                    throw fee;
                }
            }
			//check for duplicate column names and throw an error if there are duplicates
			//ensure that flatten gets rid of duplicate column names when the checks are
			//being done
			log.debug(" flattenAlias: " + flattenAlias);
			log.debug(" inverseFlattenAlias: " + inverseFlattenAlias);
			log.debug(" aliases: " + aliases);
			log.debug(" fss.size: " + fss.size());
			boolean duplicates = false;
			Map<String, Integer> duplicateAliases = new HashMap<String, Integer>();
			for(String alias: aliases.keySet()) {
				Integer count = aliases.get(alias);
				if(count > 1) {//not checking for null here as counts are intitalized to 1
					Boolean inFlatten = false;
					log.debug("inFlatten: " + inFlatten + " inverseFlattenAlias: " + inverseFlattenAlias);
					inFlatten = inverseFlattenAlias.get(alias);
					log.debug("inFlatten: " + inFlatten + " inverseFlattenAlias: " + inverseFlattenAlias);
					if((null == inFlatten) || (!inFlatten)) {
						duplicates = true;
						duplicateAliases.put(alias, count);
					}
				}
			}
			if(duplicates) {
				String errMessage = "Found duplicates in schema. ";
				if(duplicateAliases.size() > 0) {
					Set<String> duplicateCols = duplicateAliases.keySet();
					Iterator<String> iter = duplicateCols.iterator();
					String col = iter.next();
					errMessage += col + ": " + duplicateAliases.get(col) + " columns";
					while(iter.hasNext()) {
						col = iter.next();
						errMessage += ", " + col + ": " + duplicateAliases.get(col) + " columns";
					}
				}
				errMessage += ". Please alias the columns with unique names.";
				log.debug(errMessage);
				int errCode = 1007;
				throw new FrontendException(errMessage, errCode, PigException.INPUT, false, null);
			}
            mSchema = new Schema(fss);
			//add the aliases that are unique after flattening
			for(Schema.FieldSchema fs: mSchema.getFields()) {
				String alias = flattenAlias.get(fs);
				Integer count = aliases.get(alias);
				if (null == count) count = 1;
				log.debug("alias: " + alias);
				if((null != alias) && (count == 1)) {
					mSchema.addAlias(alias, fs);
				}
			}
            mIsSchemaComputed = true;
        }
        log.debug("Exiting getSchema");
        return mSchema;
    }

    public void unsetSchema() throws VisitorException{
        for(LogicalPlan plan: mForEachPlans) {
            SchemaRemover sr = new SchemaRemover(plan);
            sr.visit();
        }
        super.unsetSchema();
    }

    /**
     * @see org.apache.pig.impl.plan.Operator#clone()
     * Do not use the clone method directly. Operators are cloned when logical plans
     * are cloned using {@link LogicalPlanCloner}
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        // Do generic LogicalOperator cloning
        LOForEach forEachClone = (LOForEach)super.clone();
        
        // create deep copies of attributes specific to foreach
        if(mFlatten != null) {
            forEachClone.mFlatten = new ArrayList<Boolean>();
            for (Iterator<Boolean> it = mFlatten.iterator(); it.hasNext();) {
                forEachClone.mFlatten.add(new Boolean(it.next()));
            }
        }
        
        if(mForEachPlans != null) {
            forEachClone.mForEachPlans = new ArrayList<LogicalPlan>();
            for (Iterator<LogicalPlan> it = mForEachPlans.iterator(); it.hasNext();) {
                LogicalPlanCloneHelper lpCloneHelper = new LogicalPlanCloneHelper(it.next());
                forEachClone.mForEachPlans.add(lpCloneHelper.getClonedPlan());
            }
        }
        
        if(mUserDefinedSchema != null) {
            forEachClone.mUserDefinedSchema = new ArrayList<Schema>();
            for (Iterator<Schema> it = mUserDefinedSchema.iterator(); it.hasNext();) {
                Schema s = it.next();
                forEachClone.mUserDefinedSchema.add(s != null ? s.clone() : null);
            }
        }
        return forEachClone;
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
        
        LogicalOperator predecessor = predecessors.get(0);
        
        Schema inputSchema;
        
        try {
            inputSchema = predecessor.getSchema();
        } catch (FrontendException fee) {
            return null;
        }
        
        List<LogicalPlan> foreachPlans = getForEachPlans();
        List<Boolean> flattenList = getFlatten();
        
        MultiMap<Integer, Pair<Integer, Integer>> mapFields = new MultiMap<Integer, Pair<Integer, Integer>>();
        List<Integer> addedFields = new ArrayList<Integer>();
        int outputColumn = 0;
        
        for(int i = 0; i < foreachPlans.size(); ++i) {
            LogicalPlan foreachPlan = foreachPlans.get(i);
            List<LogicalOperator> leaves = foreachPlan.getLeaves();
            if(leaves == null || leaves.size() > 1) {
                return null;
            }
            
            int inputColumn = -1;
            boolean mapped = false;
            
            if(leaves.get(0) instanceof LOProject) {
                //find out if this project is a chain of projects
                if(LogicalPlan.chainOfProjects(foreachPlan)) {
                    LOProject rootProject = (LOProject)foreachPlan.getRoots().get(0);
                    inputColumn = rootProject.getCol();
                    if(inputSchema != null) {
                        mapped = true;
                    }
                }
            }
            
            Schema.FieldSchema leafFS;
            try {
                leafFS = ((ExpressionOperator)leaves.get(0)).getFieldSchema();
            } catch (FrontendException fee) {
                return null;
            }
            
            if(leafFS == null) {
                return null;
            }
            
            if(flattenList.get(i)) {
                Schema innerSchema = leafFS.schema;
                
                if(innerSchema != null) {                    
                    if(innerSchema.isTwoLevelAccessRequired()) {
                        // this is the case where the schema is that of
                        // a bag which has just one tuple fieldschema which
                        // in turn has a list of fieldschemas. The schema
                        // after flattening would consist of the fieldSchemas
                        // present in the tuple
                        
                        // check that indeed we only have one field schema
                        // which is that of a tuple
                        if(innerSchema.getFields().size() != 1) {
                            return null;
                        }
                        Schema.FieldSchema tupleFS;
                        try {
                            tupleFS = innerSchema.getField(0);
                        } catch (FrontendException fee) {
                            return null;
                        }
                        
                        if(tupleFS.type != DataType.TUPLE) {
                            return null;
                        }
                        innerSchema = tupleFS.schema;
                    }
                    
                    //innerSchema could be modified and hence the second check
                    if(innerSchema != null) {
                        for(int j = 0; j < innerSchema.size(); ++j) {
                            if(mapped) {
                                //map each flattened column to the original column
                                mapFields.put(outputColumn++, new Pair<Integer, Integer>(0, inputColumn));
                            } else {
                                addedFields.add(outputColumn++);
                            }
                        }
                    } else {
                        //innerSchema is null; check for schema type
                        if(DataType.isSchemaType(leafFS.type)) {
                            //flattening a null schema results in a bytearray
                            if(mapped) {
                                //map each flattened column to the original column
                                mapFields.put(outputColumn++, new Pair<Integer, Integer>(0, inputColumn));
                            } else {
                                addedFields.add(outputColumn++);
                            }
                        } else {
                            mapFields.put(outputColumn++, new Pair<Integer, Integer>(0, inputColumn));
                        }
                    }
                } else {
                    //innerSchema is null; check for schema type
                    if(DataType.isSchemaType(leafFS.type)) {
                        //flattening a null schema results in a bytearray
                        if(mapped) {
                            //map each flattened column to the original column
                            mapFields.put(outputColumn++, new Pair<Integer, Integer>(0, inputColumn));
                        } else {
                            addedFields.add(outputColumn++);
                        }
                    } else {
                        mapFields.put(outputColumn++, new Pair<Integer, Integer>(0, inputColumn));
                    }
                }
            } else {
                //not a flattened column
                if(mapped) {
                    mapFields.put(outputColumn++, new Pair<Integer, Integer>(0, inputColumn));
                } else {
                    addedFields.add(outputColumn++);
                }
            }
        }
        
        List<Pair<Integer, Integer>> removedFields = new ArrayList<Pair<Integer, Integer>>();
       
        if(inputSchema == null) {
            //if input schema is null then there are no mappedFields and removedFields
            mapFields = null;
            removedFields = null;
        } else {
            
            //if the size of the map is zero then set it to null
            if(mapFields.size() == 0) {
                mapFields = null;
            }
            
            if(addedFields.size() == 0) {
                addedFields = null;
            }
            
            //input schema is not null. Need to compute the removedFields
            //compute the set difference between the input schema and mapped fields
            
            Set<Integer> removedSet = new HashSet<Integer>();
            for(int i = 0; i < inputSchema.size(); ++i) {
                removedSet.add(i);
            }
            
            if(mapFields != null) {
                Set<Integer> mappedSet = new HashSet<Integer>();
                for(Integer key: mapFields.keySet()) {
                    List<Pair<Integer, Integer>> values = (ArrayList<Pair<Integer, Integer>>)mapFields.get(key);
                    for(Pair<Integer, Integer> value: values) {
                        mappedSet.add(value.second);
                    }
                }
                removedSet.removeAll(mappedSet);
            }
            
            if(removedSet.size() == 0) {
                removedFields = null;
            } else {
                for(Integer i: removedSet) {
                    removedFields.add(new Pair<Integer, Integer>(0, i));
                }
            }
        }

        return new ProjectionMap(mapFields, removedFields, addedFields);
    }

    @Override
    public List<RequiredFields> getRequiredFields() {
        List<RequiredFields> requiredFields = new ArrayList<RequiredFields>();
        Set<Pair<Integer, Integer>> fields = new HashSet<Pair<Integer, Integer>>();
        Set<LOProject> projectSet = new HashSet<LOProject>();
        boolean starRequired = false;

        for (LogicalPlan plan : getForEachPlans()) {
            TopLevelProjectFinder projectFinder = new TopLevelProjectFinder(
                    plan);
            try {
                projectFinder.visit();
            } catch (VisitorException ve) {
                requiredFields.clear();
                requiredFields.add(null);
                return requiredFields;
            }
            projectSet.addAll(projectFinder.getProjectSet());
            if(projectFinder.getProjectStarSet() != null) {
                starRequired = true;
            }
        }

        if(starRequired) {
            requiredFields.add(new RequiredFields(true));
            return requiredFields;
        } else {
            for (LOProject project : projectSet) {
                for (int inputColumn : project.getProjection()) {
                    fields.add(new Pair<Integer, Integer>(0, inputColumn));
                }
            }
    
            if(fields.size() == 0) {
                requiredFields.add(new RequiredFields(false, true));
            } else {                
                requiredFields.add(new RequiredFields(new ArrayList<Pair<Integer, Integer>>(fields)));
            }
            return (requiredFields.size() == 0? null: requiredFields);
        }
    }

}
