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
				log.debug("Number of roots in " + plan + " = " + plan.getRoots().size());
				for(int i = 0; i < plan.getRoots().size(); ++i) {
					log.debug("Root" + i + "= " + plan.getRoots().get(i));
				}
				LogicalOperator op = plan.getRoots().get(0);
                log.debug("op: " + op.getClass().getName() + " " + op);
			}
			log.debug("Printed the roots of the generate plans");

			Map<Schema.FieldSchema, String> flattenAlias = new HashMap<Schema.FieldSchema, String>();

            for (int planCtr = 0; planCtr < mGeneratePlans.size(); ++planCtr) {
				LogicalPlan plan = mGeneratePlans.get(planCtr);
				LogicalOperator op = plan.getRoots().get(0);
                log.debug("op: " + op.getClass().getName() + " " + op);
                Set<String> aliases;
                Iterator<String> iter = null;
                String opSchemaAlias = null;
                Schema s = null;

                try {
                    s = op.getSchema();
                    if (null != s) {
                        log.debug("Printing aliases in LOGenerate");
                        s.printAliases();
                        aliases = op.getSchema().getAliases();
                        iter = aliases.iterator();
                    }
                } catch (FrontendException ioe) {
                    mSchema = null;
                    mIsSchemaComputed = false;
                    throw ioe;
                }

                // ASSUMPTION
                // Here I am assuming that the LOProject does not have multiple
                // columns in the project, i.e., generate A.($1,$2,$3) as
                // (name,age,gpa)

                if ((null != iter) && (iter.hasNext())) {
                    opSchemaAlias = iter.next();
                    log.debug("iter.next: " + opSchemaAlias);
                }
                

                log.debug("Type: " + DataType.findTypeName(op.getType())
                        + " Alias: " + opSchemaAlias);

                if (mFlatten.get(planCtr)) {
                    log.debug("Flatten");
                    for(Schema.FieldSchema fs: s.getFields()) {
						Schema internalSchema = fs.schema;
						Schema.FieldSchema newFs;
						log.debug("Flatten level 1 internalSchema: " + internalSchema);
						if(null != internalSchema) {
							for(Schema.FieldSchema internalfs: internalSchema.getFields()) {
                        		newFs = new Schema.FieldSchema(internalfs.alias, internalfs.schema);
								fss.add(newFs);
								log.debug("Flatten alias: " + opSchemaAlias+"::"+internalfs.alias);
								flattenAlias.put(newFs, opSchemaAlias+"::"+internalfs.alias);
							}
						}
						else {
                        	newFs= new Schema.FieldSchema(fs.alias, fs.schema);
                        	fss.add(newFs);
							log.debug("Flatten alias: " + opSchemaAlias+"::"+fs.alias);
							flattenAlias.put(newFs, opSchemaAlias+"::"+fs.alias);
						}
                    }
                } else {
                    fss.add(new Schema.FieldSchema(opSchemaAlias, s));
                }
            }
            mSchema = new Schema(fss);
			for(Schema.FieldSchema fs: mSchema.getFields()) {
				String alias = flattenAlias.get(fs);
				if(null != alias) {
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
