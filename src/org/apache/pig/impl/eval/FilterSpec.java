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
import java.util.List;
import java.util.Properties;

import org.apache.pig.data.Datum;
import org.apache.pig.impl.FunctionInstantiator;
import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.eval.cond.Cond;
import org.apache.pig.impl.logicalLayer.schema.Schema;



public class FilterSpec extends EvalSpec {
    private static final long serialVersionUID = 1L;
    
    public Cond cond;

    public FilterSpec(Cond cond) {
        this.cond = cond;
    }
    
    @Override
    public List<String> getFuncs() {
        return cond.getFuncs();
    }
    
    @Override
    protected Schema mapInputSchema(Schema schema) {
        return schema;
    }
    
    @Override
    protected DataCollector setupDefaultPipe(Properties properties,
                                             DataCollector endOfPipe) {
        return new DataCollector(endOfPipe) {

            @Override
            public void add(Datum d){
                if (checkDelimiter(d))
                    addToSuccessor(d);
                else if (cond.eval(d)) 
                    addToSuccessor(d);
            }
            
            @Override
            protected boolean needFlatteningLocally() {
                return true;
            }
            
            @Override
            protected void finish() {
                cond.finish();
            }
            
        };
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[FILTER BY ");
        sb.append(cond.toString());
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void instantiateFunc(FunctionInstantiator instantiaor)
            throws IOException {
        super.instantiateFunc(instantiaor);
        cond.instantiateFunc(instantiaor);
    }

    @Override
    public void visit(EvalSpecVisitor v) {
        v.visitFilter(this);
    }
    
   
    
}
