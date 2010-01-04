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
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.LoadPushDown.RequiredField;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.LoadPushDown.RequiredFieldResponse;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaMergeException;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.ProjectionMap;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;

public class LOLoad extends RelationalOperator {
    private static final long serialVersionUID = 2L;

    private FileSpec mInputFileSpec;
    transient private LoadFunc mLoadFunc;
    private String mSchemaFile;
    private Schema mEnforcedSchema = null ;
    transient private Configuration conf;
    private static Log log = LogFactory.getLog(LOLoad.class);
    private Schema mDeterminedSchema = null;
    private RequiredFieldList requiredFieldList;

    /**
     * @param plan
     *            LogicalPlan this operator is a part of.
     * @param key
     *            OperatorKey for this operator
     * @param inputFileSpec
     *            the file to be loaded *
     * @param conf
     *            the read-only configuration object
     *            
     */
    public LOLoad(LogicalPlan plan, OperatorKey key, FileSpec inputFileSpec,
            Configuration conf) throws IOException {
        super(plan, key);
        mInputFileSpec = inputFileSpec;
        //mSchemaFile = schemaFile;
        // schemaFile is the input file since we are trying
        // to deduce the schema by looking at the input file
        mSchemaFile = inputFileSpec.getFileName();
        this.conf = conf;
        // Generate a psudo alias. Since in the following script, we do not have alias for LOLoad, however, alias is required.
        // a = foreach (load '1') generate b0;
        this.mAlias = ""+key.getId();

         try { 
             mLoadFunc = (LoadFunc)
                  PigContext.instantiateFuncFromSpec(inputFileSpec.getFuncSpec());
        }catch (ClassCastException cce) {
            log.error(inputFileSpec.getFuncSpec() + " should implement the LoadFunc interface.");
            throw new IOException(cce);
        }
         catch (Exception e){ 
             throw new IOException(e);
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
            LoadMetadata loadMetadata = (LoadMetadata)mLoadFunc;
            ResourceSchema rSchema = loadMetadata.getSchema(
                    mInputFileSpec.getFileName(), conf);
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

    @Override
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
    public List<RequiredFields> getRelevantInputs(int output, int column) throws FrontendException {
        if (!mIsSchemaComputed)
            getSchema();
        
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
    
    public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) throws FrontendException
    {
        RequiredFieldResponse response = new RequiredFieldResponse(false);
        if (mSchema == null)
            return response;
        
        if (requiredFieldList.isAllFieldsRequired())
            return response;
        
        if (requiredFieldList.getFields()==null)
        {
            return response;
        }
        
        this.requiredFieldList = requiredFieldList;
        if(mLoadFunc instanceof LoadPushDown) {
            response = ((LoadPushDown)mLoadFunc).pushProjection(requiredFieldList);
        } else {
            // loadfunc does not support pushing projections
            response = new RequiredFieldResponse(false);
        }
        if (!response.getRequiredFieldResponse())
            return response;
        
        // Change LOLoad schema to reflect this pruning
        TreeSet<Integer> prunedIndexSet = new TreeSet<Integer>();
        for (int i=0;i<mSchema.size();i++)
            prunedIndexSet.add(i);

        for (int i=0;i<requiredFieldList.getFields().size();i++)
        {
            RequiredField requiredField = requiredFieldList.getFields().get(i); 
            if (requiredField.getIndex()>=0)
                prunedIndexSet.remove(requiredField.getIndex());
            else
            {
                
                try {
                    int index = mSchema.getPosition(requiredField.getAlias());
                    if (index>0)
                        prunedIndexSet.remove(index);
                } catch (FrontendException e) {
                    return new RequiredFieldResponse(false);
                }
                
            }
        }
        
        Integer index;
        while ((index = prunedIndexSet.pollLast())!=null)
            mSchema.getFields().remove(index.intValue());
        
        mIsProjectionMapComputed = false;
        getProjectionMap();
        return response;

    }

    @Override
    public boolean pruneColumns(List<Pair<Integer, Integer>> columns)
            throws FrontendException {
        throw new FrontendException("Not implemented");
    }
    
    public RequiredFieldList getRequiredFieldList()
    {
        return requiredFieldList;
    }
    
}
