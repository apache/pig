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
package org.apache.pig.impl.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.Algebraic;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POCast;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.UDFContext.UDFContextKey;

public class UDFContextSeparator extends PhyPlanVisitor {

    public static enum UDFType {
        LOADFUNC,
        STOREFUNC,
        USERFUNC,
    };

    private String planOpKey;
    private DepthFirstWalker<PhysicalOperator, PhysicalPlan> dfw;
    private Map<String, Map<Enum<UDFType>, List<UDFContext.UDFContextKey>>> udfContextsPerPlan;
    private UDFContext udfContext;
    private Set<UDFContext.UDFContextKey> allKeys;
    private Set<UDFContext.UDFContextKey> knownKeys;
    private Set<UDFContext.UDFContextKey> unKnownKeys;
    private Set<UDFContext.UDFContextKey> algebraicUDFKeys;

    public UDFContextSeparator(){
        super(null, null);
        dfw = new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(null);
        udfContext = UDFContext.getUDFContext();
        allKeys = udfContext.getUdfConfs().keySet();
        knownKeys = new HashSet<UDFContext.UDFContextKey>();
        algebraicUDFKeys = udfContext.getUdfConfs().keySet();
        udfContextsPerPlan = new HashMap<String, Map<Enum<UDFType>, List<UDFContext.UDFContextKey>>>();
    }

    public Set<UDFContext.UDFContextKey> getUnKnownKeys() {
        if (unKnownKeys == null) {
            unKnownKeys = new HashSet<UDFContext.UDFContextKey>(allKeys);
            unKnownKeys.removeAll(knownKeys);
            for (Entry<UDFContextKey, Properties> entry : udfContext.getUdfConfs().entrySet()) {
                if (entry.getValue().isEmpty()) {
                    // Remove empty values
                    unKnownKeys.remove(entry.getKey());
                }
            }
        }
        return unKnownKeys;
    }

    public void setPlan(PhysicalPlan plan, String planOpKey){
        mPlan = plan;
        dfw.setPlan(plan);
        mCurrentWalker = dfw;
        this.planOpKey = planOpKey;
        this.udfContextsPerPlan.put(planOpKey, new HashMap<Enum<UDFType>, List<UDFContextKey>>());
    }

    @Override
    public void visitUserFunc(POUserFunc userFunc) throws VisitorException {
        if (userFunc.getFunc() instanceof Algebraic) {
            for (UDFContext.UDFContextKey key : allKeys) {
                if (key.getClassName().equals(userFunc.getFunc().getClass().getName())) {
                    // If Algebraic handle differently. To be on the safer side
                    // as user might be just accessing properties by base class name
                    // instead of by Initial, Intermediate and Final classes
                    algebraicUDFKeys.add(key);
                }
            }
        } else {
            findAndAddKeys(userFunc.getFunc().getClass().getName(),
                    userFunc.getSignature(), UDFType.USERFUNC);
        }
    }

    @Override
    public void visitLoad(POLoad ld) throws VisitorException {
        findAndAddKeys(ld.getLoadFunc().getClass().getName(),
                ld.getSignature(), UDFType.LOADFUNC);
    }


    @Override
    public void visitStore(POStore st) throws VisitorException {
        findAndAddKeys(st.getStoreFunc().getClass().getName(),
                st.getSignature(), UDFType.STOREFUNC);
    }

    @Override
    public void visitCast(POCast op) {
        if (op.getFuncSpec() != null) {
            findAndAddKeys(op.getFuncSpec().getClass().getName(),
                    null, UDFType.USERFUNC);
        }
    }

    private void findAndAddKeys(String keyClassName, String signature, UDFType udfType) {
        for (UDFContext.UDFContextKey key : allKeys) {
            if (key.getClassName().equals(keyClassName)
                    && (key.getArgs() == null
                    || signature == null
                    || Arrays.asList(key.getArgs()).contains(signature))) {
                Map<Enum<UDFType>, List<UDFContextKey>> udfKeysByType = udfContextsPerPlan
                        .get(planOpKey);
                List<UDFContextKey> keyList = udfContextsPerPlan.get(planOpKey)
                        .get(udfType);
                if (keyList == null) {
                    keyList = new ArrayList<UDFContext.UDFContextKey>();
                    udfKeysByType.put(udfType, keyList);
                }
                keyList.add(key);
                knownKeys.add(key);
            }
        }
    }

    public void serializeUDFContext(Configuration conf, String planOpKey,
            UDFType... udfTypes) throws IOException {
        Map<UDFContextKey, Properties> udfConfs = udfContext.getUdfConfs();
        HashMap<UDFContextKey, Properties> udfConfsToSerialize = new HashMap<UDFContextKey, Properties>();
        Map<Enum<UDFType>, List<UDFContextKey>> udfKeysByType = udfContextsPerPlan.get(planOpKey);
        if (udfKeysByType != null) {
            for (UDFType udfType : udfTypes) {
                List<UDFContextKey> keyList = udfContextsPerPlan.get(planOpKey).get(udfType);
                if (keyList != null) {
                    for (UDFContextKey key : keyList) {
                        udfConfsToSerialize.put(key, udfConfs.get(key));
                    }
                }
                if (udfType.equals(UDFType.USERFUNC)) {
                    for (UDFContextKey key : algebraicUDFKeys) {
                        udfConfsToSerialize.put(key, udfConfs.get(key));
                    }
                }
            }
        }
        serialize(conf, udfConfsToSerialize);
    }

    public void serializeUDFContext(Configuration conf, String planOpKey,
            POStore store) throws IOException {
        Map<UDFContextKey, Properties> udfConfs = udfContext.getUdfConfs();
        HashMap<UDFContextKey, Properties> udfConfsToSerialize = new HashMap<UDFContextKey, Properties>();
        // Find keys specific to just this StoreFunc
        Map<Enum<UDFType>, List<UDFContextKey>> udfKeysByType = udfContextsPerPlan.get(planOpKey);
        if (udfKeysByType != null) {
            List<UDFContextKey> keyList = udfContextsPerPlan.get(planOpKey).get(
                    UDFType.STOREFUNC);
            if (keyList != null) {
                String keyClassName = store.getStoreFunc().getClass().getName();
                String signature = store.getSignature();
                for (UDFContextKey key : keyList) {
                    if (key.getClassName().equals(keyClassName)
                            && (key.getArgs() == null
                            || Arrays.asList(key.getArgs()).contains(signature))) {
                        udfConfsToSerialize.put(key, udfConfs.get(key));
                    }
                }
            }
        }
        serialize(conf, udfConfsToSerialize);
    }

    private void serialize(Configuration conf,
            HashMap<UDFContextKey, Properties> udfConfsToSerialize)
            throws IOException {
        HashMap<UDFContextKey, Properties> udfConfs = udfContext.getUdfConfs();
        // Add unknown ones for serialization
        for (UDFContextKey key : getUnKnownKeys()) {
            udfConfsToSerialize.put(key, udfConfs.get(key));
        }
        conf.set(UDFContext.UDF_CONTEXT, ObjectSerializer.serialize(udfConfsToSerialize));
        conf.set(UDFContext.CLIENT_SYS_PROPS, ObjectSerializer.serialize(udfContext.getClientSystemProps()));
    }

}
