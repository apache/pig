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
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
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
    private static Log log = LogFactory.getLog(LOForEach.class);

    /**
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param foreachPlan
     *            the list of operators that are applied for each input
     */

    public LOForEach(LogicalPlan plan, OperatorKey k,
            ArrayList<LogicalPlan> foreachPlans, ArrayList<Boolean> flattenList) {

        super(plan, k);
        mForEachPlans = foreachPlans;
        mFlatten = flattenList;
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
								for(Schema.FieldSchema fs: s.getFields()) {
									log.debug("fs: " + fs);
									log.debug("fs.alias: " + fs.alias);
									String innerCanonicalAlias = fs.alias;
									if((null != outerCanonicalAlias) && (null != innerCanonicalAlias)) {
										String disambiguatorAlias = outerCanonicalAlias + "::" + innerCanonicalAlias;
										Schema.FieldSchema newFs = new Schema.FieldSchema(disambiguatorAlias, fs.schema, fs.type);
										fss.add(newFs);
										Integer count;
										count = aliases.get(innerCanonicalAlias);
										if(null == count) {
											aliases.put(innerCanonicalAlias, 1);
										} else {
											aliases.put(innerCanonicalAlias, ++count);
										}
										count = aliases.get(disambiguatorAlias);
										if(null == count) {
											aliases.put(disambiguatorAlias, 1);
										} else {
											aliases.put(disambiguatorAlias, ++count);
										}
										flattenAlias.put(newFs, innerCanonicalAlias);
										inverseFlattenAlias.put(innerCanonicalAlias, true);
										//it's fine if there are duplicates
										//we just need to record if its due to
										//flattening
									} else {
										Schema.FieldSchema newFs = new Schema.FieldSchema(null, fs.schema, fs.type);
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
							if(null != outerCanonicalAlias) {
								Integer count = aliases.get(outerCanonicalAlias);
								if(null == count) {
									aliases.put(outerCanonicalAlias, 1);
								} else {
									aliases.put(outerCanonicalAlias, ++count);
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
				}
			}
            mIsSchemaComputed = true;
        }
        log.debug("Exiting getSchema");
        return mSchema;
    }
}
