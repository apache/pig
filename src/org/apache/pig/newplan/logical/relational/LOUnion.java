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

package org.apache.pig.newplan.logical.relational;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.PigException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

public class LOUnion extends LogicalRelationalOperator {
    private boolean onSchema;

    private static String UID_SEPARATOR = "_";
    
    // uid mapping from output uid to input uid
    private List<Pair<Long, String>> uidMapping = new ArrayList<Pair<Long, String>>();
    
    public LOUnion(OperatorPlan plan) {
        super("LOUnion", plan);
    }
    
    public LOUnion(OperatorPlan plan, boolean onSchema) {
        this( plan );
        this.onSchema = onSchema;
    }
    
    public boolean isOnSchema() {
        return onSchema;
    }
    
    @Override
    public LogicalSchema getSchema() throws FrontendException {
        if (schema != null) {
            return schema;
        }
        
        List<Operator> inputs = plan.getPredecessors(this);
        // If any predecessor's schema is null, then the schema for union is null
        for (Operator input : inputs) {
            LogicalRelationalOperator op = (LogicalRelationalOperator)input;
            if( op.getSchema() == null ) {
                if( isOnSchema() ) {
                    String msg = "Schema of relation " + op.getAlias()
                        + " is null." 
                        + " UNION ONSCHEMA cannot be used with relations that"
                        + " have null schema.";
                    throw new FrontendException(this, msg, 1116, PigException.INPUT);

                } else {
                    return null;
                }
            }
        }
        
        LogicalSchema mergedSchema = null;
        if ( inputs.size() == 1 )
            return schema = ((LogicalRelationalOperator)inputs.get(0)).getSchema();
        
        List<String> inputAliases = new ArrayList<String>(inputs.size());
        List<LogicalSchema> inputSchemas = new ArrayList<LogicalSchema>(inputs.size());
        for (Operator input : inputs) {
            LogicalRelationalOperator lop = (LogicalRelationalOperator)input;
            inputAliases.add(lop.getAlias());
            inputSchemas.add(lop.getSchema());
        }
        
        if( isOnSchema() ) {
            mergedSchema = createMergedSchemaOnAlias( inputSchemas, inputAliases );
        } else {
            LogicalSchema s0 = inputSchemas.get(0);
            LogicalSchema s1 = inputSchemas.get(1);
            mergedSchema = LogicalSchema.merge(s0, s1, LogicalSchema.MergeMode.Union);
            if (mergedSchema==null)
                return null;
            
            // Merge schema
            for (int i=2;i<inputSchemas.size();i++) {
                LogicalSchema otherSchema = inputSchemas.get(i);
                if (mergedSchema==null || otherSchema==null)
                    return null;
                mergedSchema = LogicalSchema.merge(mergedSchema, otherSchema, LogicalSchema.MergeMode.Union);
                if (mergedSchema == null)
                    return null;
            }
        }

        // Bring back cached uid if any; otherwise, cache uid generated
        setMergedSchemaUids(mergedSchema, inputSchemas, "");

        return schema = mergedSchema;
    }

    /**
     * create schema for union-onschema
     */
    private LogicalSchema createMergedSchemaOnAlias(List<LogicalSchema> inputSchemas,
            List<String> inputAliases)
    throws FrontendException {
        ArrayList<LogicalSchema> schemas = new ArrayList<LogicalSchema>();
        for (int i = 0; i < inputSchemas.size(); i++){
            LogicalSchema sch = inputSchemas.get(i);
            for( LogicalFieldSchema fs : sch.getFields() ) {
                if(fs.alias == null){
                    String msg = "Schema of relation " + inputAliases.get(i)
                        + " has a null fieldschema for column(s). Schema :" + sch.toString(false);
                    throw new FrontendException( this, msg, 1116, PigException.INPUT );
                }
            }
            schemas.add( sch );
        }

        //create the merged schema
        LogicalSchema mergedSchema = null;
        try {
            mergedSchema = LogicalSchema.mergeSchemasByAlias( schemas );
        } catch(FrontendException e)                 {
            String msg = "Error merging schemas for union operator : "
                + e.getMessage();
            throw new FrontendException(this, msg, 1116, PigException.INPUT, e);
        }

        return mergedSchema;
    }

    private void setMergedSchemaUids(LogicalSchema mergedSchema, List<LogicalSchema> inputSchemas, String nested_uids)
    throws FrontendException {

        for (int i=0;i<mergedSchema.size();i++) {
            LogicalSchema.LogicalFieldSchema outputFieldSchema = mergedSchema.getField(i);

            long uid = -1;
            List<LogicalSchema> fieldInputSchemas = new ArrayList<>(inputSchemas.size());
            
            // Search all the cached uid mappings by input field to see if 
            // we've cached an output uid for this output field
            for (LogicalSchema inputSchema : inputSchemas) {
                LogicalSchema.LogicalFieldSchema inputFieldSchema;
                if (onSchema) {
                    inputFieldSchema = inputSchema.getFieldSubNameMatch(outputFieldSchema.alias);
                } else {
                    inputFieldSchema = inputSchema.getField(i);
                }
                
                if (inputFieldSchema != null) {
                    if (inputFieldSchema.schema != null) {
                        fieldInputSchemas.add(inputFieldSchema.schema);
                    }

                    if (uid < 0) {
                        uid = getCachedOuputUid(createNestedUids(nested_uids,inputFieldSchema.uid));
                        if (uid >= 0 && outputFieldSchema.schema == null) break;
                    }
                }
            }
            
            // No cached uid. Allocate one, and locate and cache all inputs.
            if (uid==-1) {
                uid = LogicalExpression.getNextUid();
                for (LogicalSchema inputSchema : inputSchemas) {
                    long inputUid;
                    LogicalFieldSchema matchedInputFieldSchema;
                    if (onSchema) {
                        matchedInputFieldSchema = inputSchema.getFieldSubNameMatch(mergedSchema.getField(i).alias);
                        if (matchedInputFieldSchema!=null) {
                            inputUid = matchedInputFieldSchema.uid;
                            uidMapping.add(new Pair<Long, String>(uid, createNestedUids(nested_uids,inputUid)));
                        }
                    }
                    else {
                        matchedInputFieldSchema = mergedSchema.getField(i);
                        inputUid = inputSchema.getField(i).uid;
                        uidMapping.add(new Pair<Long, String>(uid, createNestedUids(nested_uids,inputUid)));
                    }
                }
            }

            outputFieldSchema.uid = uid;

            // This field has a schema. Assign uids to it as well
            if (outputFieldSchema.schema != null) {
                setMergedSchemaUids(outputFieldSchema.schema, fieldInputSchemas, createNestedUids(nested_uids,outputFieldSchema.uid));
            }
        }
    }

    private String createNestedUids(String nested_uids, long new_uid) {
        StringBuilder sb = new StringBuilder(nested_uids);
        sb.append(UID_SEPARATOR);
        sb.append(new_uid);
        return sb.toString();
    }

    private long getLeafUid(String nested_uids) {
        String [] uid_root_to_leaf = nested_uids.split(UID_SEPARATOR);
        return Long.valueOf(uid_root_to_leaf[uid_root_to_leaf.length-1]);
    }

    private long getCachedOuputUid(String nested_input_uids) {
        long uid = -1;
        
        for (Pair<Long, String> pair : uidMapping) {
            if (pair.second.equals(nested_input_uids)) {
                uid = pair.first;
                break;
            }
        }
        
        return uid;
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalRelationalNodesVisitor)) {
            throw new FrontendException("Expected LogicalPlanVisitor", 2223);
        }
        ((LogicalRelationalNodesVisitor)v).visit(this);
    }

    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (other != null && other instanceof LOUnion) { 
            return checkEquality((LOUnion)other);
        } else {
            return false;
        }
    }

    // Get input uids mapping to the output uid
    public Set<Long> getInputUids(long outputuid) {
        Set<Long> result = new HashSet<Long>();
        for (Pair<Long, String> pair : uidMapping) {
            if (pair.first==outputuid)
                result.add(getLeafUid(pair.second));
        }
        return result;
    }
    
    @Override
    public void resetUid() {
        uidMapping = new ArrayList<Pair<Long, String>>();
    }
    
    public List<Operator> getInputs() {
        return plan.getPredecessors(this);
    }
    
    public List<Operator> getInputs(LogicalPlan plan) {
        return plan.getPredecessors(this);
    }
    
    public void setUnionOnSchema(boolean flag) {
        onSchema = flag;
    }
}
