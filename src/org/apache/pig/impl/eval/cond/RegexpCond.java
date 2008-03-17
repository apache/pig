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

import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Datum;
import org.apache.pig.impl.FunctionInstantiator;
import org.apache.pig.impl.eval.EvalSpec;


public class RegexpCond extends Cond {
    private static final long serialVersionUID = 1L;
    
    public EvalSpec left;
    public String re;
    

    public RegexpCond(EvalSpec left, String re) throws IOException{
        this.left = left;
        this.re = re;
        
        if (left.isAsynchronous())
            throw new IOException("Can't compare the output of an asynchronous function.");
    }
    
    @Override
    public List<String> getFuncs() {
        return new ArrayList<String>();
    }

    @Override
    public boolean eval(Datum input){
        
        Datum d = left.simpleEval(input);
        
        if (!(d instanceof DataAtom))
            throw new RuntimeException("Cannot match non-atomic value against a regular expression. Use a filter function instead.");

        return ((DataAtom)d).strval().matches(re);
    }
      
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append(left);
        sb.append(" MATCHES '");
        sb.append(re);
        sb.append("')");
        return sb.toString();
    }
    
    @Override
    public void finish() {
        left.finish();
    }

    @Override
    public void instantiateFunc(FunctionInstantiator instantiaor)
            throws IOException {
        left.instantiateFunc(instantiaor);
        
    }

}
