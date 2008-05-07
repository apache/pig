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
package org.apache.pig.impl.eval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.pig.impl.FunctionInstantiator;
import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.logicalLayer.schema.Schema;



/**
 * Follows the composite design pattern
 * @author utkarsh
 *
 */

public class CompositeEvalSpec extends EvalSpec {
    private static final long serialVersionUID = 1L;
    
    private List<EvalSpec> specs = new ArrayList<EvalSpec>();
    
    public CompositeEvalSpec(EvalSpec spec){
        specs.add(spec);
        properties.putAll(spec.getProperties());
    }
        
    @Override
    protected DataCollector setupDefaultPipe(Properties properties,
                                             DataCollector endOfPipe){
        for (int i=specs.size()-1; i>=0; i--){
            endOfPipe = specs.get(i).setupDefaultPipe(properties, endOfPipe);
        }
        return endOfPipe;
    }

    @Override
    public List<String> getFuncs(){
        List<String> funcs = new ArrayList<String>();
        for(EvalSpec spec: specs){
            funcs.addAll(spec.getFuncs());
        }
        return funcs;
    }
    
    @Override
    public EvalSpec addSpec(EvalSpec spec){
        specs.add(spec);
        properties.putAll(spec.getProperties());
        return this;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        int i=0;
        for (EvalSpec spec: specs){
            sb.append(spec.toString());
            i++;
            if (i != specs.size())
                sb.append("->");
        }
        return sb.toString();
    }

    
    @Override
    public boolean isAsynchronous() {
        for (EvalSpec spec: specs)
            if (spec.isAsynchronous())
                return true;
        return false;
    }

    @Override
    protected Schema mapInputSchema(Schema schema) {
        for (EvalSpec spec: specs)
            schema = spec.mapInputSchema(schema);
        return schema;
    }
    

    @Override
    public void instantiateFunc(FunctionInstantiator fInstantiaor)
            throws IOException {
        super.instantiateFunc(fInstantiaor);
        for (EvalSpec spec: specs)
            spec.instantiateFunc(fInstantiaor);   
    }

    public List<EvalSpec> getSpecs() {
        return specs;
    }
    
    @Override
    public void visit(EvalSpecVisitor v) {
        v.visitCompositeEval(this);
    }
    
}
