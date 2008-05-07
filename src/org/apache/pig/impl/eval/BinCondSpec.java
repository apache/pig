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
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;



public class BinCondSpec extends EvalSpec {
    private static final long serialVersionUID = 1L;
    
    protected Cond cond;
    protected EvalSpec ifTrue;
    protected EvalSpec ifFalse;
    

    public BinCondSpec(Cond cond, EvalSpec ifTrue, EvalSpec ifFalse) throws IOException{
        this.cond = cond;
        this.ifTrue = ifTrue;
        this.ifFalse = ifFalse;
        
        if (ifTrue.isAsynchronous() || ifFalse.isAsynchronous())
            throw new IOException("Can't use the output of an asynchronous function as one of the branches of a bincond");
    
    }
    
    @Override
    public List<String> getFuncs() {
        List<String> funcs = cond.getFuncs();
        funcs.addAll(ifTrue.getFuncs());
        funcs.addAll(ifFalse.getFuncs());
        return funcs;
    }
    
    @Override
    public void instantiateFunc(FunctionInstantiator fInstantiaor) throws IOException{
        super.instantiateFunc(fInstantiaor);
        cond.instantiateFunc(fInstantiaor);
        ifTrue.instantiateFunc(fInstantiaor);
        ifFalse.instantiateFunc(fInstantiaor);
    };

    @Override
    protected Schema mapInputSchema(Schema schema) {
        return new TupleSchema();
    }
    
    @Override
    protected DataCollector setupDefaultPipe(Properties properties,
                                             DataCollector endOfPipe) {
        return new DataCollector(endOfPipe){
            @Override
            public void add(Datum d) {
                if (cond.eval(d)){
                    addToSuccessor(ifTrue.simpleEval(d));
                }else{
                    addToSuccessor(ifFalse.simpleEval(d));
                }
            }
            
            @Override
            protected void finish(){
                cond.finish();
                ifTrue.finish();
                ifFalse.finish();
            }
        };
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[(");
        sb.append(cond);
        sb.append(" ? ");
        sb.append(ifTrue);
        sb.append(" : ");
        sb.append(ifFalse);
        sb.append(")]");
        return sb.toString();
    }

    public EvalSpec ifTrue() { return ifTrue; }
    public EvalSpec ifFalse() { return ifFalse; }
    
    @Override
    public void visit(EvalSpecVisitor v) {
        v.visitBinCond(this);
    }
    
}
