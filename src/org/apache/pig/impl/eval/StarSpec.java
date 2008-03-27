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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.logicalLayer.schema.Schema;


final public class StarSpec extends EvalSpec {
    private static final long serialVersionUID = 1L;
    
    @Override
    public String toString() {
        return "[*]";
    }

    @Override
    public List<String> getFuncs() {
        return new ArrayList<String>();
    }
    
    @Override
    protected DataCollector setupDefaultPipe(Properties properties,
                                             DataCollector endOfPipe) {
        return endOfPipe;
    }
    
    @Override
    protected Schema mapInputSchema(Schema input){
        return input;
    }

    @Override
    public void visit(EvalSpecVisitor v) {
        v.visitStar(this);
    }
    
    

}
