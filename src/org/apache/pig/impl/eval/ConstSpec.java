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
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;



public class ConstSpec extends SimpleEvalSpec {
    private static final long serialVersionUID = 1L;
    public Object mConst;
    
    
    public ConstSpec(Object constant){
        mConst = constant;
    }
    
    /**
     * Extend the default deserialization
     * @param in
     * @throws IOException
     * @throws ClassNotFoundException
     */
     /* Why do I need this?
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException{
        in.defaultReadObject();
        init();
    }
    */
    
    @Override
    public List<String> getFuncs() {
        return new ArrayList<String>();
    }

    @Override
    protected Schema mapInputSchema(Schema schema) {
        return new TupleSchema();
    }

    @Override
    protected Object eval(Object d) {
        return mConst;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append("'");
        sb.append(mConst);
        sb.append("'");
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void visit(EvalSpecVisitor v) {
        v.visitConst(this);
    }

    public String value() { return (String)mConst; }
    

}
