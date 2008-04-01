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
import java.util.List;
import java.util.Map;

import org.apache.pig.StoreFunc;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;


public class LOStore extends LogicalOperator {
    private static final long serialVersionUID = 1L;

    protected FileSpec outputFileSpec;

    protected boolean append;


    public LOStore(Map<OperatorKey, LogicalOperator> opTable,
                   String scope,
                   long id,
                   OperatorKey input,
                   FileSpec fileSpec,
                   boolean append) throws IOException {
        super(opTable, scope, id, input);
        this.outputFileSpec = fileSpec;
        this.append = append;

        //See if the store function spec is valid
        try {
            StoreFunc StoreFunc =
                (StoreFunc) PigContext.instantiateFuncFromSpec(
                    fileSpec.getFuncSpec());
        } catch(Exception e) {
            IOException ioe = new IOException(e.getMessage());
            ioe.setStackTrace(e.getStackTrace());
            throw ioe;
        } getOutputType();
    }


    public FileSpec getOutputFileSpec() {
        return outputFileSpec;
    }

    public void setOutputFileSpec(FileSpec spec) {
        outputFileSpec = spec;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder(super.toString());
        result.append(" (append: ");
        result.append(append);
        result.append(')');
        return result.toString();
    }


    @Override
    public String name() {
        StringBuilder sb = new StringBuilder();
        sb.append("Store ");
        sb.append(scope);
        sb.append("-");
        sb.append(id);
        return sb.toString();
    }

    @Override
    public TupleSchema outputSchema() {
        throw new
            RuntimeException
            ("Internal error: Asking for schema of a store operator.");
    }

    @Override
    public int getOutputType() {
        switch (opTable.get(getInputs().get(0)).getOutputType()) {
        case FIXED:
            return FIXED;
        case MONOTONE:
            return MONOTONE;
        default:
            throw new RuntimeException("Illegal input type for store operator");
        }
    }

    @Override
    public List<String> getFuncs() {
        List<String> funcs = super.getFuncs();
        funcs.add(outputFileSpec.getFuncName());
        return funcs;
    }


    public boolean isAppend() {
        return append;
    }

    public void visit(LOVisitor v) {
        v.visitStore(this);
    }
}
