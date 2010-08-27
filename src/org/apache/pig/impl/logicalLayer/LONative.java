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

import java.util.List;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.RequiredFields;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Utils;

public class LONative extends RelationalOperator {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private String nativeMRJar;
    private String[] params = null;

    public LONative(LogicalPlan plan, OperatorKey k, 
            String nativeJar, String[] parameters) {
        super(plan, k);
        nativeMRJar = nativeJar;
        params = parameters;
    }
 
    @Override
    public List<RequiredFields> getRelevantInputs(int output, int column)
            throws FrontendException {
        return null;
    }
    
    public String getNativeMRJar() {
        return nativeMRJar;
    }

    public String[] getParams() {
        return params;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    @Override
    public String name() {
        return getAliasString() + "Native " + mKey.scope + "-" + mKey.id 
        + " Run: hadoop jar " + nativeMRJar + " " + Utils.getStringFromArray(params) ;
     }
    
    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public Schema getSchema() throws FrontendException {
        return null;
    }

}
