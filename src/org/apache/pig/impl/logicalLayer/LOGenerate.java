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
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LOGenerate extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    /**
     * The projection list of this generate.
     */
    //private ArrayList<ExpressionOperator> mProjections;
    private ArrayList<LogicalPlan> mGeneratePlans;
	private ArrayList<Boolean> mFlatten;
    private static Log log = LogFactory.getLog(LOGenerate.class);

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param projections
     *            the projection list of the generate
     */

    public LOGenerate(LogicalPlan plan, OperatorKey key,
            ArrayList<LogicalPlan> generatePlans, ArrayList<Boolean> flatten) {
        super(plan, key);
        mGeneratePlans = generatePlans;
        mFlatten = flatten;
    }

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param projection
     *            the projection of the generate
     */

    public LOGenerate(LogicalPlan plan, OperatorKey key,
            LogicalPlan generatePlan, boolean flatten) {
        super(plan, key);
        mGeneratePlans = new ArrayList<LogicalPlan>();
        mGeneratePlans.add(generatePlan);
        mFlatten = new ArrayList<Boolean>();
		mFlatten.add(flatten);
    }


    public List<LogicalPlan> getGeneratePlans() {
        return mGeneratePlans;
    }

	public List<Boolean> getFlatten() {
		return mFlatten;
	}

    @Override
    public String name() {
        return "Generate " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    @Override
    public Schema getSchema() throws FrontendException {
        log.debug("Entering getSchema");
        if (!mIsSchemaComputed && (null == mSchema)) {
            List<Schema.FieldSchema> fss = new ArrayList<Schema.FieldSchema>(
                    mGeneratePlans.size());

            for (LogicalPlan plan : mGeneratePlans) {
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

            for (int planCtr = 0; planCtr < mGeneratePlans.size(); ++planCtr) {
				LogicalPlan plan = mGeneratePlans.get(planCtr);
				LogicalOperator op = plan.getLeaves().get(0);
                log.debug("op: " + op.getClass().getName() + " " + op);
                //Set<String> aliases;
                //Iterator<String> iter = null;
                //String opSchemaAlias = null;
                //Schema s = null;
				Schema.FieldSchema planFs;

                try {
	                planFs = ((ExpressionOperator)op).getFieldSchema();
					if(null != planFs) {
						log.debug("planFs alias: " + planFs.alias);
						if(mFlatten.get(planCtr)) {
							//need to extract the children and create the aliases
							//assumption here is that flatten is only for one column
							//i.e., flatten(A), flatten(A.x) and NOT
							//flatten(B.(x,y,z))
							Schema s = planFs.schema;
							if(null != s) {
								for(Schema.FieldSchema fs: s.getFields()) {
									log.debug("fs: " + fs);
									log.debug("fs.alias: " + fs.alias);
									if(null != fs.alias) {
										String disambiguatorAlias = planFs.alias + "::" + fs.alias;
										Schema.FieldSchema newFs = new Schema.FieldSchema(disambiguatorAlias, fs.schema, fs.type);
										fss.add(newFs);
										Integer count;
										count = aliases.get(fs.alias);
										if(null == count) {
											aliases.put(fs.alias, 0);
										} else {
											aliases.put(fs.alias, ++count);
										}
										count = aliases.get(disambiguatorAlias);
										if(null == count) {
											aliases.put(disambiguatorAlias, 0);
										} else {
											aliases.put(disambiguatorAlias, ++count);
										}
										flattenAlias.put(newFs, fs.alias);
										inverseFlattenAlias.put(fs.alias, true);
										//it's fine if there are duplicates
										//we just need to record if its due to
										//flattening
									} else {
										Schema.FieldSchema newFs = new Schema.FieldSchema(null, DataType.BYTEARRAY);
										fss.add(newFs);
									}
								}
							} else {
								Schema.FieldSchema newFs = new Schema.FieldSchema(null, DataType.BYTEARRAY);
								fss.add(newFs);
							}
						} else {
							//just populate the schema with the field schema of the expression operator
	                   		fss.add(planFs);
							if(null != planFs.alias) {
								Integer count = aliases.get(planFs.alias);
								if(null == count) {
									aliases.put(planFs.alias, 0);
								} else {
									aliases.put(planFs.alias, ++count);
								}
							}
						}
					} else {
						//did not get a valid list of field schemas
						fss.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
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
			Set<String> duplicateAliases = new HashSet<String>();
			for(String alias: aliases.keySet()) {
				Integer count = aliases.get(alias);
				if(count > 0) {
					Boolean inFlatten = false;
					log.debug("inFlatten: " + inFlatten + " inverseFlattenAlias: " + inverseFlattenAlias);
					inFlatten = inverseFlattenAlias.get(alias);
					log.debug("inFlatten: " + inFlatten + " inverseFlattenAlias: " + inverseFlattenAlias);
					if((null != inFlatten) && (!inFlatten)) {
						duplicates = true;
						duplicateAliases.add(alias);
					}
				}
			}
			if(duplicates) {
				String errMessage = "Found duplicates in schema ";
				if(duplicateAliases.size() > 0) {
					Iterator<String> iter = duplicateAliases.iterator();
					errMessage += ": " + iter.next();
					while(iter.hasNext()) {
						errMessage += ", " + iter.next();
					}
				}
				throw new FrontendException(errMessage);
			}
            mSchema = new Schema(fss);
			//add the aliases that are unique after flattening
			for(Schema.FieldSchema fs: mSchema.getFields()) {
				String alias = flattenAlias.get(fs);
				Integer count = aliases.get(alias);
				if (null == count) count = 0;
				log.debug("alias: " + alias);
				if((null != alias) && (count == 0)) {
					mSchema.addAlias(alias, fs);
				}
			}
            mIsSchemaComputed = true;
        }
        log.debug("Exiting getSchema");
        return mSchema;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

}
