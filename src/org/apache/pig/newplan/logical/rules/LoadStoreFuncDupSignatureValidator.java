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
package org.apache.pig.newplan.logical.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.DepthFirstWalker;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;

/***
 * Check for duplicate alias. This is because we use alias as LoadFunc signature. If
 * user use the same alias in two load statement, we cannot distinguish two loadFunc.
 * LoadStoreFuncDupSignatureValidator solve this problem by appending an index if signature
 * conflicts
 */

public class LoadStoreFuncDupSignatureValidator {
    OperatorPlan plan;
    public LoadStoreFuncDupSignatureValidator(OperatorPlan plan) {
        this.plan = plan;
    }
    
    public void validate() throws FrontendException {
        LoadStoreFuncDupSignatureVisitor visitor = new LoadStoreFuncDupSignatureVisitor(plan);
        visitor.visit();
        visitor.finish();
    }
    
    static class LoadStoreFuncDupSignatureVisitor extends LogicalRelationalNodesVisitor {
        Map<String, List<LOLoad>> loadSignatures = new HashMap<String, List<LOLoad>>();
        Map<String, List<LOStore>> storeSignatures = new HashMap<String, List<LOStore>>();
        protected LoadStoreFuncDupSignatureVisitor(OperatorPlan plan)
                throws FrontendException {
            super(plan, new DepthFirstWalker(plan));
        }

        @Override
        public void visit(LOLoad load) throws FrontendException {
            if (loadSignatures.containsKey(load.getSignature())) {
                List<LOLoad> loads = loadSignatures.get(load.getSignature());
                loads.add(load);
            } else {
                List<LOLoad> loads = new ArrayList<LOLoad>();
                loads.add(load);
                loadSignatures.put(load.getSignature(), loads);
            }
        }
        
        @Override
        public void visit(LOStore store) throws FrontendException {
            if (storeSignatures.containsKey(store.getSignature())) {
                List<LOStore> stores = storeSignatures.get(store.getSignature());
                stores.add(store);
            } else {
                List<LOStore> stores = new ArrayList<LOStore>();
                stores.add(store);
                storeSignatures.put(store.getSignature(), stores);
            }
        }
        
        public void finish() {
            for (Map.Entry<String, List<LOLoad>> entry : loadSignatures.entrySet()) {
                String key = entry.getKey();
                List<LOLoad> loads = entry.getValue();
                if (loads.size()>1) {
                    for (int i=0;i<loads.size();i++) {
                        loads.get(i).setSignature(key+"$"+i);
                    }
                }
            }
            
            for (Map.Entry<String, List<LOStore>> entry : storeSignatures.entrySet()) {
                String key = entry.getKey();
                List<LOStore> stores = entry.getValue();
                if (stores.size()>1) {
                    for (int i=0;i<stores.size();i++) {
                        stores.get(i).setSignature(key+"$"+i);
                    }
                }
            }
        }
    }
}
