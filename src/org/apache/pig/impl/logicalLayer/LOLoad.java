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

import org.apache.pig.LoadFunc;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;



public class LOLoad extends LogicalOperator {
    private static final long serialVersionUID = 2L;
    
    private FileSpec mInput;
    private LoadFunc mLoadFunc;

    public LOLoad(OperatorKey k,
                  FileSpec inputFileSpec,
                  String loader) throws IOException, ParseException {
        super(k);
        this.mInput = inputFileSpec;
        
        // check if we can instantiate load func
        LoadFunc storageFunc =
            (LoadFunc)PigContext.instantiateFuncFromSpec(loader);

        // TODO: Handle Schemas defined by Load Functions
        //schema = new TupleSchema();
    }

    @Override
    public String name() {
        return "Load " + scope + "-" + id;
    }

    public FileSpec getInputFileSpec() {
        return mInput;
    }

    @Override
    public String arguments() {
        return mInput.toString();
    }

    @Override
    public String toString() {
        StringBuffer result = new StringBuffer(super.toString());
        result.append(" (outputType: ");
        result.append(outputType);
        result.append(')');
        return result.toString();
    }

    @Override
    public List<String> getFuncs() {
        List<String> funcs = super.getFuncs();
        funcs.add(mInput.getFuncName());
        return funcs;
    }

    public void visit(LOVisitor v) {
        v.visitLoad(this);
    }
}
