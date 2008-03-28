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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.net.URL;

import org.apache.pig.LoadFunc; // import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.PlanVisitor;

public class LOLoad extends LogicalOperator {
    private static final long serialVersionUID = 2L;

    private FileSpec mInputFileSpec;
    private LoadFunc mLoadFunc;
    private URL mSchemaFile;

    /**
     * @param plan
     *            LogicalPlan this operator is a part of.
     * @param key
     *            OperatorKey for this operator
     * @param rp
     *            Requested level of parallelism to be used in the sort.
     * @param inputFileSpec
     *            the file to be loaded
     */
    public LOLoad(LogicalPlan plan, OperatorKey key, int rp,
            FileSpec inputFileSpec, URL schemaFile) throws IOException {
        super(plan, key, rp);

        mInputFileSpec = inputFileSpec;
        mSchemaFile = schemaFile;

        // TODO FIX
        // The code below is commented out as PigContext pulls in
        // HExecutionEngine which in turn is completely commented out
        // Also remove the commented out import org.apache.pig.impl.PigContext

        /*
         * try { mLoadFunc = (LoadFunc)
         * PigContext.instantiateFuncFromSpec(inputFileSpec.getFuncSpec()); }
         * catch (Exception e){ IOException ioe = new
         * IOException(e.getMessage()); ioe.setStackTrace(e.getStackTrace());
         * throw ioe; }
         */
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
    public Schema getSchema() throws IOException {
        if (!mIsSchemaComputed && (null == mSchema)) {
            // get the schema of the load function
            try {
                mSchema = mLoadFunc.determineSchema(mSchemaFile);
                mIsSchemaComputed = true;
            } catch (Exception e) {
                IOException ioe = new IOException(e.getMessage());
                ioe.setStackTrace(e.getStackTrace());
                throw ioe;
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

    public void visit(LOVisitor v) throws ParseException {
        v.visit(this);
    }
}
