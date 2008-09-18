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
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaMergeException;
import org.apache.pig.impl.logicalLayer.optimizer.SchemaRemover;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.logicalLayer.parser.QueryParser ;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
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

    public List<Boolean> getFlatten() {
        return mFlatten;
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
							if(null != s) {
								for(int i = 0; i < s.size(); ++i) {
                                    Schema.FieldSchema fs;
                                    try {
                                        fs = s.getField(i);
                                    } catch (ParseException pe) {
                                        throw new FrontendException(pe.getMessage());
                                    }
									log.debug("fs: " + fs);
                                    if(null != userDefinedSchema) {
                                        Schema.FieldSchema userDefinedFieldSchema;
                                        try {
                                            if(i < userDefinedSchema.size()) {
                                                userDefinedFieldSchema = userDefinedSchema.getField(i);
                                                fs = fs.mergePrefixFieldSchema(userDefinedFieldSchema);
                                            }
                                        } catch (ParseException pe) {
                                            throw new FrontendException(pe.getMessage());
                                        } catch (SchemaMergeException sme) {
                                            throw new FrontendException(sme.getMessage());
                                        }
                                        outerCanonicalAlias = null;
                                    }
									String innerCanonicalAlias = fs.alias;
                                    Schema.FieldSchema newFs;
									if((null != outerCanonicalAlias) && (null != innerCanonicalAlias)) {
										String disambiguatorAlias = outerCanonicalAlias + "::" + innerCanonicalAlias;
										newFs = new Schema.FieldSchema(disambiguatorAlias, fs.schema, fs.type);
										fss.add(newFs);
                                        updateAliasCount(aliases, disambiguatorAlias);
										//it's fine if there are duplicates
										//we just need to record if its due to
										//flattening
									} else {
										newFs = new Schema.FieldSchema(fs.alias, fs.schema, fs.type);
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
                                            throw new FrontendException("Schema mismatch. A basic type on flattening cannot have more than one column. User defined schema: " + userDefinedSchema);
                                        }
								        newFs = new Schema.FieldSchema(null, planFs.type);
                                        try {
                                            newFs = newFs.mergePrefixFieldSchema(userDefinedSchema.getField(0));
                                        } catch (SchemaMergeException sme) {
                                            throw new FrontendException(sme.getMessage());
                                        } catch (ParseException pe) {
                                            throw new FrontendException(pe.getMessage());
                                        }
                                        updateAliasCount(aliases, newFs.alias);
                                        fss.add(newFs);
                                    } else {
                                        for(Schema.FieldSchema ufs: userDefinedSchema.getFields()) {
                                            QueryParser.SchemaUtils.setFieldSchemaDefaultType(ufs, DataType.BYTEARRAY);
                                            fss.add(new Schema.FieldSchema(ufs.alias, ufs.schema, ufs.type));
                                            updateAliasCount(aliases, ufs.alias);
                                        }
                                    }
								} else {
                                    if(!DataType.isSchemaType(planFs.type)) {
								        newFs = new Schema.FieldSchema(null, planFs.type);
                                    } else {
								        newFs = new Schema.FieldSchema(null, DataType.BYTEARRAY);
                                    }
                                    fss.add(newFs);
                                }
							}
						} else {
							//just populate the schema with the field schema of the expression operator
                            //check if the user has defined a schema for the operator; compare the schema
                            //with that of the expression operator field schema and then add it to the list
                            if(null != userDefinedSchema) {
                                try {
                                    planFs = planFs.mergePrefixFieldSchema(userDefinedSchema.getField(0));
                                    updateAliasCount(aliases, planFs.alias);
                                } catch (SchemaMergeException sme) {
                                    throw new FrontendException(sme.getMessage());
                                } catch (ParseException pe) {
                                    throw new FrontendException(pe.getMessage());
                                }
                            }
	                   		fss.add(planFs);
						}
					} else {
						//did not get a valid list of field schemas
                        String outerCanonicalAlias = null;
                        if(null != userDefinedSchema) {
                            try {
                                Schema.FieldSchema userDefinedFieldSchema = userDefinedSchema.getField(0);
                                fss.add(userDefinedFieldSchema);
                                updateAliasCount(aliases, userDefinedFieldSchema.alias);
                            } catch (ParseException pe) {
                                throw new FrontendException(pe.getMessage());
                            }
                        } else {
						    fss.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
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
				String errMessage = "Found duplicates in schema! ";
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
				throw new FrontendException(errMessage);
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
					// alias is unambiguous - so set it
					// as the alias in the field schema
					fs.alias = alias;
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

}
