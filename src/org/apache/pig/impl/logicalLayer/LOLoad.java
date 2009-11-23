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

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaMergeException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;

public class LOLoad extends RelationalOperator {
    private static final long serialVersionUID = 2L;
    protected boolean splittable = true;

    private FileSpec mInputFileSpec;
    transient private LoadFunc mLoadFunc;
    private String mSchemaFile;
    private Schema mEnforcedSchema = null ;
    transient private DataStorage mStorage;
    private ExecType mExecType;
    private static Log log = LogFactory.getLog(LOLoad.class);
    private Schema mDeterminedSchema = null;

    /**
     * @param plan
     *            LogicalPlan this operator is a part of.
     * @param key
     *            OperatorKey for this operator
     * @param inputFileSpec
     *            the file to be loaded *
     * @param execType
     *            the execution mode @see org.apache.pig.ExecType
     * @param storage
     *            the underlying storage
     * @param splittable
     *            if the input file is splittable (.gz is not)
     *            
     * 
     */
    public LOLoad(LogicalPlan plan, OperatorKey key, FileSpec inputFileSpec,
            ExecType execType, DataStorage storage, boolean splittable) throws IOException {
        super(plan, key);
        mInputFileSpec = inputFileSpec;
        //mSchemaFile = schemaFile;
        // schemaFile is the input file since we are trying
        // to deduce the schema by looking at the input file
        mSchemaFile = inputFileSpec.getFileName();
        mStorage = storage;
        mExecType = execType;
        this.splittable = splittable;

         try { 
             mLoadFunc = (LoadFunc)
                  PigContext.instantiateFuncFromSpec(inputFileSpec.getFuncSpec()); 
        }catch (ClassCastException cce) {
            log.error(inputFileSpec.getFuncSpec() + " should implement the LoadFunc interface.");
            throw WrappedIOException.wrap(cce);
        }
         catch (Exception e){ 
             throw WrappedIOException.wrap(e);
        }
    }

    public FileSpec getInputFile() {
        return mInputFileSpec;
    }
    
    
    public void setInputFile(FileSpec inputFileSpec) throws IOException {
       try { 
            mLoadFunc = (LoadFunc)
                 PigContext.instantiateFuncFromSpec(inputFileSpec.getFuncSpec()); 
       }catch (ClassCastException cce) {
           log.error(inputFileSpec.getFuncSpec() + " should implement the LoadFunc interface.");
           IOException ioe = new IOException(cce.getMessage()); 
           ioe.setStackTrace(cce.getStackTrace());
           throw ioe;
       }
        catch (Exception e){ 
           IOException ioe = new IOException(e.getMessage()); 
           ioe.setStackTrace(e.getStackTrace());
           throw ioe; 
       }
        mInputFileSpec = inputFileSpec;
    }

    public String getSchemaFile() {
        return mSchemaFile;
    }

    public LoadFunc getLoadFunc() {
        return mLoadFunc;
    }

    @Override
    public String name() {
        return "Load " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public Schema getSchema() throws FrontendException {
        if (!mIsSchemaComputed) {
            // get the schema of the load function
            try {
                if (mEnforcedSchema != null) {
                    mSchema = mEnforcedSchema ;
                    return mSchema ;
                }

                if(null == mDeterminedSchema) {
                    mSchema = determineSchema();
                    mDeterminedSchema  = mSchema;    
                }
                mIsSchemaComputed = true;
            } catch (IOException ioe) {
                int errCode = 1018;
                String msg = "Problem determining schema during load";
                FrontendException fee = new FrontendException(msg, errCode, PigException.INPUT, false, null, ioe);
                mIsSchemaComputed = false;
                mSchema = null;
                throw fee;
            }
        }
        return mSchema;
    }
    
    private Schema determineSchema() throws IOException {
        if(LoadMetadata.class.isAssignableFrom(mLoadFunc.getClass())) {
            // XXX: FIXME - mStorage should no longer be needed, we
            // should use Configuration directly by passing a 
            // Configuration object while creating LOLoad rather than
            // a DataStorage object
            LoadMetadata loadMetadata = (LoadMetadata)mLoadFunc;
            ResourceSchema rSchema = loadMetadata.getSchema(
                    mInputFileSpec.getFileName(), 
                    ConfigurationUtil.toConfiguration(
                            mStorage.getConfiguration()));
            return Schema.getPigSchema(rSchema);
        } else {
            return null;
        }
    }
    /* (non-Javadoc)
     * @see org.apache.pig.impl.logicalLayer.LogicalOperator#setSchema(org.apache.pig.impl.logicalLayer.schema.Schema)
     */
    @Override
    public void setSchema(Schema schema) throws FrontendException {
        // In general, operators don't generate their schema until they're
        // asked, so ask them to do it.
        try {
            getSchema();
        } catch (FrontendException ioe) {
            // It's fine, it just means we don't have a schema yet.
        }
        if (mSchema == null) {
            log.debug("Operator schema is null; Setting it to new schema");
            mSchema = schema;
        } else {
            log.debug("Reconciling schema");
            log.debug("mSchema: " + mSchema + " schema: " + schema);
            try {
                mSchema = mSchema.mergePrefixSchema(schema, true, true);
            } catch (SchemaMergeException e) {
                int errCode = 1019;
                String msg = "Unable to merge schemas";
                throw new FrontendException(msg, errCode, PigException.INPUT, false, null, e);
            }
        }
    }
    

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    public Schema getEnforcedSchema() {
        return mEnforcedSchema;
    }

    /***
     * Set this when user enforces schema
     * @param enforcedSchema
     */
    public void setEnforcedSchema(Schema enforcedSchema) {
        this.mEnforcedSchema = enforcedSchema;
    }

    public boolean isSplittable() {
        return splittable;
    }

    @Override
    public byte getType() {
        return DataType.BAG ;
    }

    /**
     * @return the DeterminedSchema
     */
    public Schema getDeterminedSchema() {
        return mDeterminedSchema;
    }
    
    @Override
    public ProjectionMap getProjectionMap() {
        if(mIsProjectionMapComputed) return mProjectionMap;
        mIsProjectionMapComputed = true;
        
        Schema outputSchema;
        
        try {
            outputSchema = getSchema();
        } catch (FrontendException fee) {
            mProjectionMap = null;
            return mProjectionMap;
        }
        
        Schema inputSchema = null;        
        
        List<LogicalOperator> predecessors = (ArrayList<LogicalOperator>)mPlan.getPredecessors(this);
        if(predecessors != null) {
            try {
                inputSchema = predecessors.get(0).getSchema();
            } catch (FrontendException fee) {
                mProjectionMap = null;
                return mProjectionMap;
            }
        } else {
            try {
                inputSchema = determineSchema();
            } catch (IOException ioe) {
                mProjectionMap = null;
                return mProjectionMap;
            }
        }
        
        if(inputSchema == null) {
            if(outputSchema != null) {
                //determine schema returned null and the user specified a schema
                //OR
                //the predecessor did not have a schema and the user specified a schema
                mProjectionMap = new ProjectionMap(false);
                return mProjectionMap;
            }
        }
        
        if(Schema.equals(inputSchema, outputSchema, false, true)) {
            //there is a one is to one mapping between input and output schemas
            mProjectionMap = new ProjectionMap(false);
            return mProjectionMap;
        } else {
            MultiMap<Integer, ProjectionMap.Column> mapFields = new MultiMap<Integer, ProjectionMap.Column>();
            //compute the mapping assuming its a prefix projection
            for(int i = 0; i < inputSchema.size(); ++i) {
                mapFields.put(i, new ProjectionMap.Column(new Pair<Integer, Integer>(0, i)));
            }
            
            mProjectionMap = new ProjectionMap(mapFields, null, null); 
            return mProjectionMap;
        }
    }

    @Override
    public List<RequiredFields> getRequiredFields() {
        List<RequiredFields> requiredFields = new ArrayList<RequiredFields>();
        requiredFields.add(new RequiredFields(false, true));
        return requiredFields;
    }

    @Override
    public List<RequiredFields> getRelevantInputs(int output, int column) {
        if (output!=0)
            return null;
        
        if (column<0)
            return null;
        
        // if we have schema information, check if output column is valid
        if (mSchema!=null)
        {
            if (column >= mSchema.size())
                return null;
        }
        
        List<RequiredFields> result = new ArrayList<RequiredFields>();
        result.add(new RequiredFields(true));
        return result;
    }

}
