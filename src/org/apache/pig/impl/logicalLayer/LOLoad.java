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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;



public class LOLoad extends LogicalOperator {
    private static final long serialVersionUID = 1L;
    
    private final Log log = LogFactory.getLog(getClass());

    protected FileSpec inputFileSpec;

    protected int outputType = FIXED;
    
    protected boolean splittable = true;

    public LOLoad(Map<OperatorKey, LogicalOperator> opTable, 
                  String scope, 
                  long id, 
                  FileSpec inputFileSpec, boolean splittable) 
    throws IOException, ParseException {
        super(opTable, scope, id);
        this.inputFileSpec = inputFileSpec;
        this.splittable = splittable;
        
        // check if we can instantiate load func
        PigContext.instantiateFuncFromSpec(inputFileSpec.getFuncSpec());

        // TODO: Handle Schemas defined by Load Functions
        schema = new TupleSchema();
    }

    @Override
    public String name() {
        StringBuilder sb = new StringBuilder();
        sb.append("Load ");
        sb.append(scope);
        sb.append("-");
        sb.append(id);
        return sb.toString();
    }

    public FileSpec getInputFileSpec() {
        return inputFileSpec;
    }

    public void setInputFileSpec(FileSpec spec) {
        inputFileSpec = spec;
    }

    @Override
    public String arguments() {
        return inputFileSpec.toString();
    }

    @Override
    public TupleSchema outputSchema() {
        schema.setAlias(alias);
        return this.schema;
    }

    @Override
    public int getOutputType() {
        return outputType;
    }

    public void setOutputType(int type) {
        if (type < FIXED || type > AMENDABLE) {
            throw new RuntimeException("Illegal output type");
        }
        outputType = type;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder(super.toString());
        result.append(" (outputType: ");
        result.append(outputType);
        result.append(')');
        return result.toString();
    }

    @Override
    public List<String> getFuncs() {
        List<String> funcs = super.getFuncs();
        funcs.add(inputFileSpec.getFuncName());
        return funcs;
    }

    public boolean isSplittable() {
        return splittable;
    }
    
    public void visit(LOVisitor v) {
        v.visitLoad(this);
    }
}
