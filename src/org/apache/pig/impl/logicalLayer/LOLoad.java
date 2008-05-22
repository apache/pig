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

import org.apache.pig.LoadFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LOLoad extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    private FileSpec mInputFileSpec;
    private LoadFunc mLoadFunc;
    private URL mSchemaFile;
    private Schema mEnforcedSchema = null ;
    private static Log log = LogFactory.getLog(LOLoad.class);

    /**
     * @param plan
     *            LogicalPlan this operator is a part of.
     * @param key
     *            OperatorKey for this operator
     * @param inputFileSpec
     *            the file to be loaded *
     * @param schemaFile
     *            the file with the schema for the data to be loaded
     * 
     */
    public LOLoad(LogicalPlan plan, OperatorKey key, FileSpec inputFileSpec,
            URL schemaFile) throws IOException {
        super(plan, key);

        mInputFileSpec = inputFileSpec;
        mSchemaFile = schemaFile;

         try { 
             mLoadFunc = (LoadFunc)
                  PigContext.instantiateFuncFromSpec(inputFileSpec.getFuncSpec()); 
        } catch (Exception e){ 
            IOException ioe = new IOException(e.getMessage()); 
            ioe.setStackTrace(e.getStackTrace());
            throw ioe; 
        }
    }

    public FileSpec getInputFile() {
        return mInputFileSpec;
    }

    public URL getSchemaFile() {
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
        if (!mIsSchemaComputed && (null == mSchema)) {
            // get the schema of the load function
            try {
                //DEBUG
                //System.out.println("Schema file: " + mSchema);
                
                if (mEnforcedSchema != null) {
                    mSchema = mEnforcedSchema ;
                    return mSchema ;
                }

                if(null != mSchemaFile) {
                    mSchema = mLoadFunc.determineSchema(mSchemaFile);
                }
                mIsSchemaComputed = true;
            } catch (IOException ioe) {
                FrontendException fee = new FrontendException(ioe.getMessage());
                fee.initCause(ioe);
                mIsSchemaComputed = false;
                mSchema = null;
                throw fee;
            }
        }
        return mSchema;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
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

    @Override
    public byte getType() {
        return DataType.BAG ;
    }
}
