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
import java.util.List;

import org.apache.pig.data.Datum;
import org.apache.pig.impl.FunctionInstantiator;



public class NotCond extends Cond {
    private static final long serialVersionUID = 1L;
    
    public Cond cond;

    public NotCond(Cond cond) {
        this.cond = cond;
    }

    @Override
    public List<String> getFuncs() {
        return cond.getFuncs();
    }

    @Override
    public boolean eval(Datum input){
        return !cond.eval(input);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(NOT ");
        sb.append(cond);
        sb.append(")");
        return sb.toString();
    }
    
    @Override
    public void finish() {
        cond.finish();
    }

    @Override
    public void instantiateFunc(FunctionInstantiator instantiaor)
            throws IOException {
        cond.instantiateFunc(instantiaor);
        
    }
}
