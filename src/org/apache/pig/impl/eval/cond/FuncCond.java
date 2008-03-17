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
package org.apache.pig.impl.eval.cond;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Datum;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.FunctionInstantiator;
import org.apache.pig.impl.eval.EvalSpec;



public class FuncCond extends Cond {
    
    private final Log log = LogFactory.getLog(getClass());

    private static final long serialVersionUID = 1L;
    
    public String funcName;
    transient public FilterFunc func;
    public EvalSpec args;

    public FuncCond(FunctionInstantiator fInstantiaor, String funcName, EvalSpec args) throws IOException{       
        this.funcName = funcName; 
        this.args = args;
        if (args!=null && args.isAsynchronous())
            throw new IOException("Can't use the output of an asynchronous function as an argument");
        instantiateFunc(fInstantiaor);
        
    }

    @Override
    public void instantiateFunc(FunctionInstantiator instantiaor) throws IOException{
           if(instantiaor != null)
               func = (FilterFunc)instantiaor.instantiateFuncFromAlias(funcName);
    }
    
    @Override
    public List<String> getFuncs() {
        List<String> funcs = new ArrayList<String>();
        funcs.add(funcName);
        return funcs;
    }

    @Override
    public boolean eval(Datum input){
        try {
            
            Datum d = null;
            if (args!=null)
                d = args.simpleEval(input);
            
            if (d!=null && !(d instanceof Tuple))
                throw new RuntimeException("Internal error: Non-tuple returned on evaluation of arguments.");
            
            return func.exec((Tuple)d);
        } catch (IOException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Warning: filter function ");
            sb.append(funcName);
            sb.append(" failed. Substituting default value \'false\'.");
            log.error(sb.toString(), e);
            return false;
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(funcName);
        sb.append("(");
        sb.append(args);
        sb.append(")");
        return sb.toString();
    }
    
    @Override
    public void finish() {
        if (args!=null)
            args.finish();
        func.finish();
    }
}
