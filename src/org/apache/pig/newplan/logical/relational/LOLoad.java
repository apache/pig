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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.Util;

public class LOLoad extends LogicalRelationalOperator {
    
    private LogicalSchema scriptSchema;
    private final FileSpec fs;
    private transient LoadFunc loadFunc;
    transient private Configuration conf;
    private final LogicalSchema determinedSchema;
    private List<Integer> requiredFields = null;
    private boolean castInserted = false;
    private LogicalSchema uidOnlySchema;
    private final String schemaFile;
    private final String signature;
    private long limit = -1;

    /**
     * used for pattern matching
     * 
     * @param schema schema user specified in script, or null if not
     * specified.
     * @param plan logical plan this load is part of.
     */
    public LOLoad(LogicalSchema schema, LogicalPlan plan) {
        this(null, schema, plan, null, null, null);
    }

    /**
     * Used from the LogicalPlanBuilder
     *
     * @param loader FuncSpec for load function to use for this load.
     * @param schema schema user specified in script, or null if not specified.
     * @param plan logical plan this load is part of.
     * @param conf
     * @param loadFunc the LoadFunc that was instantiated from loader
     * @param signature the signature that will be passed to the LoadFunc
     */
    public LOLoad(FileSpec loader, LogicalSchema schema, LogicalPlan plan, Configuration conf, LoadFunc loadFunc, String signature) {
        super("LOLoad", plan);
        this.scriptSchema = schema;
        this.fs = loader;
        this.schemaFile = loader == null ? null : loader.getFileName();
        this.conf = conf;
        this.loadFunc = loadFunc;
        this.signature = signature;
        storeScriptSchema(conf, scriptSchema, signature);
        if (loadFunc != null) {
            this.loadFunc.setUDFContextSignature(signature);
            try {
                this.determinedSchema = getSchemaFromMetaData();
            } catch (FrontendException e) {
                throw new RuntimeException("Can not retrieve schema from loader " + loadFunc, e);
            }
        } else {
            this.determinedSchema = null;
        }
    }

    public String getSchemaFile() {
        return schemaFile;
    }
    
    public LoadFunc getLoadFunc() throws FrontendException {
        try { 
            if (loadFunc == null && fs!=null) {
                loadFunc = (LoadFunc)PigContext.instantiateFuncFromSpec(fs.getFuncSpec());
                loadFunc.setUDFContextSignature(signature);
            }
            
            return loadFunc;
        }catch (ClassCastException cce) {
            throw new FrontendException(this, fs.getFuncSpec() + " should implement the LoadFunc interface.", 2236);    		
        }
    }
    
    public void setScriptSchema(LogicalSchema schema) {
        scriptSchema = schema;
    }
    
    public void setRequiredFields(List<Integer> requiredFields) {
        this.requiredFields = requiredFields;
    }
    
    /**
     * Get the schema for this load.  The schema will be either be what was
     * given by the user in the script or what the load functions getSchema
     * call returned.  Otherwise null will be returned, indicating that the
     * schema is unknown.
     * @return schema, or null if unknown
     */
    @Override
    public LogicalSchema getSchema() throws FrontendException {
        if (schema != null)
            return schema;
        
        LogicalSchema originalSchema = null;
        
        if (scriptSchema != null && determinedSchema != null) {
            originalSchema = LogicalSchema.merge(scriptSchema, determinedSchema, LogicalSchema.MergeMode.LoadForEach);
        } else if (scriptSchema != null)  originalSchema = scriptSchema;
        else if (determinedSchema != null) originalSchema = determinedSchema;
        
        if (isCastInserted()) {
            for (int i=0;i<originalSchema.size();i++) {
                LogicalSchema.LogicalFieldSchema fs = originalSchema.getField(i);
                if(determinedSchema == null) {
                    // Reset the loads field schema to byte array so that it
                    // will reflect reality.
                    fs.type = DataType.BYTEARRAY;
                } else {
                    // Reset the type to what determinedSchema says it is
                    fs.type = determinedSchema.getField(i).type;
                }
            }
        }
        
        if (originalSchema!=null) {
            uidOnlySchema = originalSchema.mergeUid(uidOnlySchema);
        }
        
        if (requiredFields!=null) {
            schema = new LogicalSchema();
            for (int i=0;i<originalSchema.size();i++) {
                if (requiredFields.contains(i))
                    schema.addField(originalSchema.getField(i));
            }
        } else
            schema = originalSchema;
        
        return schema;
    }

    private LogicalSchema getSchemaFromMetaData() throws FrontendException {
        if (getLoadFunc()!=null && getLoadFunc() instanceof LoadMetadata) {
            try {
                ResourceSchema resourceSchema = ((LoadMetadata)loadFunc).getSchema(getFileSpec().getFileName(), new Job(conf));
                Schema oldSchema = Schema.getPigSchema(resourceSchema);
                return Util.translateSchema(oldSchema);
            } catch (IOException e) {
                throw new FrontendException( this, "Cannot get schema from loadFunc " + loadFunc.getClass().getName(), 2245, e);
            }
        }
        return null;
    }

	/**
	 * This method will store the scriptSchema:Schema using ObjectSerializer to
	 * the current configuration.<br/>
	 * The schema can be retrieved by load functions or UDFs to know the schema
	 * the user entered in the as clause.<br/>
	 * The name format is:<br/>
	 * 
	 * <pre>
	 * ${UDFSignature}.scriptSchema = ObjectSerializer.serialize(scriptSchema)
	 * </pre>
	 * <p/>
     * Note that this is not the schema the load function returns but will
	 * always be the as clause schema.<br/>
	 * That is a = LOAD 'input' as (a:chararray, b:chararray)<br/>
	 * The schema wil lbe (a:chararray, b:chararray)<br/>
	 * <p/>
	 * 
	 * TODO Find better solution to make script schema available to LoadFunc see
	 * https://issues.apache.org/jira/browse/PIG-1717
	 */
    private void storeScriptSchema(Configuration conf, LogicalSchema scriptSchema, String signature) {
      if (conf != null && scriptSchema != null && signature != null) {
			try {
          conf.set(
              Utils.getScriptSchemaKey(signature),
              ObjectSerializer.serialize(Util.translateSchema(scriptSchema)));
			} catch (IOException ioe) {
				int errCode = 1018;
				String msg = "Problem serializing script schema";
				FrontendException fee = new FrontendException(this, msg, errCode,
						PigException.INPUT, false, null, ioe);
				throw new RuntimeException(fee);
			}
		}
	}

    public FileSpec getFileSpec() {
        return fs;
    }
    
    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalRelationalNodesVisitor)) {
            throw new FrontendException("Expected LogicalPlanVisitor", 2223);
        }
        ((LogicalRelationalNodesVisitor)v).visit(this);

    }
    
    public LogicalSchema getDeterminedSchema() {
        return determinedSchema;
    }
    
    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (other != null && other instanceof LOLoad) {
            LOLoad ol = (LOLoad)other;
            if (!checkEquality(ol)) return false;
            if (fs == null) {
                if (ol.fs == null) {
                    return true;
                }else{
                    return false;
                }
            }
            
            return fs.equals(ol.fs);
        } else {
            return false;
        }
    }
    
    public void setCastInserted(boolean flag) {
        castInserted = flag;
    }
    
    public boolean isCastInserted() {
        return castInserted;
    }
    
    public Configuration getConfiguration() {
        return conf;
    }
    
    @Override
    public void resetUid() {
        uidOnlySchema = null;
    }
    
    @Override
    public String toString(){
        String str = super.toString();
        return (str + "RequiredFields:" + requiredFields);
    }
    
    public String getSignature() {
        return signature;
    }
    
    public LogicalSchema getScriptSchema() {
        return scriptSchema;
    }

    public long getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

}
