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
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.io.IOException;

import org.apache.pig.PigException;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LOCross extends LogicalOperator {

    private static final long serialVersionUID = 2L;
    private static Log log = LogFactory.getLog(LOCross.class);

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     */
    public LOCross(LogicalPlan plan, OperatorKey k) {
        super(plan, k);
    }

    public List<LogicalOperator>  getInputs() {
        return mPlan.getPredecessors(this);
    }
    
    @Override
    public Schema getSchema() throws FrontendException {
        List<LogicalOperator> inputs = mPlan.getPredecessors(this);
        if (!mIsSchemaComputed) {
            List<Schema.FieldSchema> fss = new ArrayList<Schema.FieldSchema>();
            Map<Schema.FieldSchema, String> flattenAlias = new HashMap<Schema.FieldSchema, String>();
            Map<String, Boolean> inverseFlattenAlias = new HashMap<String, Boolean>();
            Map<String, Integer> aliases = new HashMap<String, Integer>();

            for (LogicalOperator op : inputs) {
                String opAlias = op.getAlias();
                Schema s = op.getSchema();
                Schema.FieldSchema newFs;

                //need to extract the children and create the aliases
                //assumption here is that flatten is only for one column
                //i.e., flatten(A), flatten(A.x) and NOT
                //flatten(B.(x,y,z))
                if(null != s) {
                    for(Schema.FieldSchema fs: s.getFields()) {
                        log.debug("fs: " + fs);
                        log.debug("fs.alias: " + fs.alias);
                        if(null != fs.alias) {
                            String disambiguatorAlias = opAlias + "::" + fs.alias;
                            newFs = new Schema.FieldSchema(disambiguatorAlias, fs.schema, fs.type);
                            fss.add(newFs);
                            Integer count;
                            count = aliases.get(fs.alias);
                            if(null == count) {
                                aliases.put(fs.alias, 1);
                            } else {
                                aliases.put(fs.alias, ++count);
                            }
                            count = aliases.get(disambiguatorAlias);
                            if(null == count) {
                                aliases.put(disambiguatorAlias, 1);
                            } else {
                                aliases.put(disambiguatorAlias, ++count);
                            }
                            flattenAlias.put(newFs, fs.alias);
                            inverseFlattenAlias.put(fs.alias, true);
                            //it's fine if there are duplicates
                            //we just need to record if its due to
                            //flattening
                        } else {
                            newFs = new Schema.FieldSchema(null, DataType.BYTEARRAY);
                            fss.add(newFs);
                        }
                        newFs.setParent(fs.canonicalName, op);
                    }
                } else {
                    mSchema = null;
                    mIsSchemaComputed = true;
                    return mSchema;
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
                if(count > 1) {
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
                String errMessage = "Found duplicates in schema. ";
                if(duplicateAliases.size() > 0) {
                    Iterator<String> iter = duplicateAliases.iterator();
                    errMessage += ": " + iter.next();
                    while(iter.hasNext()) {
                        errMessage += ", " + iter.next();
                    }
                }
                errMessage += ". Please alias the columns with unique names.";
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
        return mSchema;
    }

    @Override
    public String name() {
        return "Cross " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    @Override
    public byte getType() {
        return DataType.BAG ;
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
            requiredFields.add(new RequiredFields(true));
        }
        
        return (requiredFields.size() == 0? null: requiredFields);
    }

}
