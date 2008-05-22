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

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.PlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LOCross extends LogicalOperator {

    private static final long serialVersionUID = 2L;
    private ArrayList<LogicalOperator> mInputs;
    private static Log log = LogFactory.getLog(LOCross.class);

    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     */
    public LOCross(LogicalPlan plan, OperatorKey k, ArrayList<LogicalOperator> inputs) {

        super(plan, k);
        mInputs = inputs;
    }

    public List<LogicalOperator>  getInputs() {
        return mInputs;
    }
    
    public void addInput(LogicalOperator input) {
        mInputs.add(input);
    }
    
    @Override
    public Schema getSchema() throws FrontendException {
        if (!mIsSchemaComputed && (null == mSchema)) {
            List<Schema.FieldSchema> fss = new ArrayList<Schema.FieldSchema>();
            Map<Schema.FieldSchema, String> flattenAlias = new HashMap<Schema.FieldSchema, String>();
            Map<String, Boolean> inverseFlattenAlias = new HashMap<String, Boolean>();
            Map<String, Integer> aliases = new HashMap<String, Integer>();

            for (LogicalOperator op : mInputs) {
                String opAlias = op.getAlias();
                Schema s = op.getSchema();

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
                            Schema.FieldSchema newFs = new Schema.FieldSchema(disambiguatorAlias, fs.schema, fs.type);
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
                            Schema.FieldSchema newFs = new Schema.FieldSchema(null, DataType.BYTEARRAY);
                            fss.add(newFs);
                        }
                    }
                } else {
                    Schema.FieldSchema newFs = new Schema.FieldSchema(null, DataType.BYTEARRAY);
                    fss.add(newFs);
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
    public boolean supportsMultipleOutputs() {
        return false;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    @Override
    public byte getType() {
        return DataType.BAG ;
    }

}
