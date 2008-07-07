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

import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.plan.MultiMap;

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

    public boolean[] getInner() {
        return mIsInner;
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
            // TODO The type of the field named "group" requires
            // type promotion and the like

            /*
             * Here goes an attempt to describe how the schema for the first
             * column - 'group' should look like If the number of group by
             * columns = 1 then the schema for 'group' is the
             * schema(fieldschema(col)) If the number of group by columns > 1
             * then find the set union of the group by columns and form the
             * schema as schema(list<fieldschema of the cols>)
             * The parser will ensure that the number of group by columns are
             * the same across all inputs. The computation of the schema for group
             * is 
             */

            Schema groupBySchema = null;
            List<Schema.FieldSchema> groupByFss = new ArrayList<Schema.FieldSchema>();
            Set<String> groupByAliases = new HashSet<String>();
            Map<String, Boolean> lookup = new HashMap<String, Boolean>();
            MultiMap<String, ExpressionOperator> aliasExop = new MultiMap<String, ExpressionOperator>();
            MultiMap<Integer, String> positionAlias= new MultiMap<Integer, String>();
            
            for (LogicalOperator op : inputs) {
                log.debug("GBY Input: " + op.getClass().getName());
                for(LogicalPlan plan: mGroupByPlans.get(op)) {
                    int position = 0;
                    for(LogicalOperator eOp: plan.getLeaves()) {
                        log.debug("Leaf: " + eOp);
                        Schema.FieldSchema fs = ((ExpressionOperator)eOp).getFieldSchema();
                        if(null != fs) {
                            Schema eOpSchema = fs.schema;
                            log.debug("Computing the lookup tables");
                            if (null != fs) {
                                String alias = fs.alias;
                                //for (String alias : eOpSchema.getAliases()) {
                                if(null != alias) {
                                    log.debug("Adding alias to GBY: " + alias);
                                    groupByAliases.add(alias);
                                    lookup.put(alias, false);
                                    aliasExop.put(alias, (ExpressionOperator)eOp);                            
                                    positionAlias.put(position, alias);
                                }
                            }
                        }
                    }
                    ++position;
                }
            }
            
            log.debug("Computed the lookup table");

            int arity = mGroupByPlans.get(inputs.get(0)).size();
            log.debug("Arity: " + arity);
            for (int i = 0; i < arity; ++i) {
                Collection<String> cAliases;
                cAliases = positionAlias.get(i);
                if(null != cAliases) {
                    Object[] aliases = cAliases.toArray();
                    for(int j = 0; j < aliases.length; ++j) {
                        String alias = (String) aliases[j];
                        if(null != alias) {
                            Collection<ExpressionOperator> cEops = aliasExop.get(alias);
                            if(null != cEops) {
                                ExpressionOperator eOp = (ExpressionOperator) (cEops.toArray())[0];
                                if(null != eOp) {
                                    if(!lookup.get(alias)) {
                                        Schema.FieldSchema fs = eOp.getFieldSchema();
                                        if(null != fs) {
                                            log.debug("Added fs with alias " + alias + " and fs.schema " + fs.schema);
                                            groupByFss.add(new Schema.FieldSchema(alias, fs.schema));
                                            lookup.put(alias, true);
                                        } else {
                                            log.debug("Added fs with alias " + alias + " and schema null");
                                            groupByFss.add(new Schema.FieldSchema(alias, null));
                                        }
                                    } else {
                                        if(j < aliases.length) {
                                            continue;
                                        } else {
                                            //we have seen this alias before
                                            //just add the schema of the expression operator with the null alias
                                            Schema.FieldSchema fs = eOp.getFieldSchema();
                                            if(null != fs) {
                                                log.debug("Added fs with alias null and schema " + fs.schema);
                                                groupByFss.add(new Schema.FieldSchema(null, fs.schema));
                                            } else {
                                                log.debug("Added fs with alias null and schema null");
                                                groupByFss.add(new Schema.FieldSchema(null, null));
                                            }
                                            break;
                                        }
                                    }
                                } else {
                                    //should not be here
                                    log.debug("Cannot be here: we cannot have a collection of null expression operators");
                                }
                            } else {
                                //should not be here
                                log.debug("Cannot be here: we cannot have an alias without an expression operator");
                            }
                        } else {
                            //should not be here
                            log.debug("Cannot be here: we cannot have a collection of null aliases ");
                        }
                    }
                } else {
                    //We do not have any alias for this position in the group by columns
                    //We have positions $1, $2, etc.
                    //The schema for these columns is the schema of the expression operatore
                    //and so the alias is null
                    log.debug("Added fs with alias null and type bytearray");
                    groupByFss.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));                    
                }
            }            

            groupBySchema = new Schema(groupByFss);
            log.debug("Printing group by schema aliases");
            groupBySchema.printAliases();

            if(1 == arity) {
                log.debug("Arity == 1");
                byte groupByType = groupByFss.get(0).type;
                Schema groupSchema = groupByFss.get(0).schema;
                log.debug("Type == " + DataType.findTypeName(groupByType));
                fss.add(new Schema.FieldSchema("group", groupSchema, groupByType));
            } else {
                fss.add(new Schema.FieldSchema("group", groupBySchema));
            }
            for (LogicalOperator op : inputs) {
                log.debug("Op: " + op.getClass().getName());
                log.debug("Op Alias: " + op.getAlias());
                try {
                    Schema cSchema = op.getSchema();
                    if (null != cSchema) {
                        log.debug("Printing constituent schema aliases");
                        cSchema.printAliases();
                    }
                    fss.add(new Schema.FieldSchema(op.getAlias(), op
                            .getSchema(), DataType.BAG));
                } catch (FrontendException ioe) {
                    log.debug("Caught an exception: " + ioe.getMessage());
                    mIsSchemaComputed = false;
                    mSchema = null;
                    throw ioe;
                }
            }
            mIsSchemaComputed = true;
            mSchema = new Schema(fss);
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

}
