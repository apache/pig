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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.pig.CommittableStoreFunc;
import org.apache.pig.StoreConfig;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;

/**
 * A specialization of the default FileOutputCommitter to allow
 * pig to call commit() on the StoreFunc's associated with the stores
 * in a job IF the StoreFunc's are CommittableStoreFunc's
 */
@SuppressWarnings("deprecation")
public class PigOutputCommitter extends FileOutputCommitter {
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.FileOutputCommitter#cleanupJob(org.apache.hadoop.mapred.JobContext)
     */
    @SuppressWarnings({ "unchecked", "deprecation" })
    @Override
    public void cleanupJob(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        // the following is needed to correctly deserialize udfs in
        // the map and reduce plans below
        PigContext.setPackageImportList((ArrayList<String>)ObjectSerializer.
                deserialize(conf.get("udf.import.list")));
        super.cleanupJob(context);
        
        
        // call commit() on the StoreFunc's associated with the stores
        // in the job IF the StoreFunc's are CommittableStoreFunc's
        // look for storeFuncs in the conf - there are two cases
        // 1) Job with single store - in this case, there would be storefunc
        // stored in the conf which we can use
        // 2) Multi store case - in this case, there is no single storefunc
        // in the conf - instead we would need to look at the
        // map and reduce plans and get the POStores out of it and then get hold
        // of the respective StoreFuncs
        String sFuncString = conf.get("pig.storeFunc");
        PhysicalPlan mp = (PhysicalPlan) ObjectSerializer.deserialize(
                    conf.get("pig.mapPlan"));
        List<POStore> mapStores = PlanHelper.getStores(mp);
        PhysicalPlan rp = (PhysicalPlan) ObjectSerializer.deserialize(
                    conf.get("pig.reducePlan"));
        List<POStore> reduceStores = new ArrayList<POStore>();
        if(rp != null) {
            reduceStores = PlanHelper.getStores(rp);    
        }
        // In single store case, we would have removed the store from the
        // plan in JobControlCompiler
        if(sFuncString != null && (mapStores.size() + reduceStores.size() == 0)) {
            // single store case
            StoreFunc sFunc = MapRedUtil.getStoreFunc(new JobConf(conf));
            commit(sFunc, conf, conf.get(JobControlCompiler.PIG_STORE_CONFIG),
                    sFuncString);
        } else {
            // multi store case
            commitStores(mapStores, conf);
            commitStores(reduceStores, conf);
            
        }
    }

    private void commit(StoreFunc sFunc, Configuration conf,
            StoreConfig storeConfig, String sFuncString) throws IOException {
        if(sFunc != null && CommittableStoreFunc.class.isAssignableFrom(
                sFunc.getClass())) {
            CommittableStoreFunc csFunc = (CommittableStoreFunc)sFunc;
            // make a copy of the conf since we may be committing multiple
            // stores and set storeFunc and StoreConfig 
            // pertaining to this store in the copy and use it
            Configuration confCopy = new Configuration(conf);
            confCopy.set("pig.storeFunc", ObjectSerializer.serialize(
                    sFuncString));
            confCopy.set(JobControlCompiler.PIG_STORE_CONFIG, 
                    ObjectSerializer.serialize(storeConfig));
            
            csFunc.commit(confCopy);
        }
    }
    
    private void commit(StoreFunc sFunc, Configuration conf,
            String storeConfigSerializedString, String sFuncString) throws IOException {
        if(sFunc != null && CommittableStoreFunc.class.isAssignableFrom(
                sFunc.getClass())) {
            CommittableStoreFunc csFunc = (CommittableStoreFunc)sFunc;
            // make a copy of the conf since we may be committing multple
            // sores and set storeFunc and StoreConfig 
            // pertaining to this store in the copy and use it
            Configuration confCopy = new Configuration(conf);
            confCopy.set("pig.storeFunc", ObjectSerializer.serialize(
                    sFuncString));
            confCopy.set(JobControlCompiler.PIG_STORE_CONFIG, 
                    storeConfigSerializedString);
            
            csFunc.commit(confCopy);
        }
    }
    
    private void commitStores(List<POStore> stores, Configuration conf)
    throws IOException {
        for (POStore store : stores) {
            StoreFunc sFunc = (StoreFunc)PigContext.instantiateFuncFromSpec(
                    store.getSFile().getFuncSpec());
            StoreConfig storeConfig = new StoreConfig(store.getSFile().
                    getFileName(), store.getSchema(), store.getSortInfo());
            commit(sFunc, conf, storeConfig, 
                    store.getSFile().getFuncSpec().toString());
        }
    }
}
