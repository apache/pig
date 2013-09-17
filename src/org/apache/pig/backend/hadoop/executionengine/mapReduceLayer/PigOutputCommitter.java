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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.StoreMetadata;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.PigConfiguration;

/**
 * A specialization of the default FileOutputCommitter to allow
 * pig to inturn delegate calls to the OutputCommiter(s) of the 
 * StoreFunc(s)' OutputFormat(s).
 */
public class PigOutputCommitter extends OutputCommitter {
    
    /**
     * OutputCommitter(s) of Store(s) in the map
     */
    List<Pair<OutputCommitter, POStore>> mapOutputCommitters;
    
    /**
     * OutputCommitter(s) of Store(s) in the reduce
     */
    List<Pair<OutputCommitter, POStore>> reduceOutputCommitters;
    
    boolean recoverySupported;
    
    /**
     * @param context
     * @param mapStores 
     * @param reduceStores 
     * @throws IOException
     */
    public PigOutputCommitter(TaskAttemptContext context,
            List<POStore> mapStores, List<POStore> reduceStores)
            throws IOException {
        // create and store the map and reduce output committers
        mapOutputCommitters = getCommitters(context, mapStores);
        reduceOutputCommitters = getCommitters(context, reduceStores);
        recoverySupported = context.getConfiguration().getBoolean(PigConfiguration.PIG_OUTPUT_COMMITTER_RECOVERY, false);
    }

    /**
     * @param conf
     * @param mapStores
     * @return
     * @throws IOException 
     */
    @SuppressWarnings("unchecked")
    private List<Pair<OutputCommitter, POStore>> getCommitters(
            TaskAttemptContext context,
            List<POStore> stores) throws IOException {
        List<Pair<OutputCommitter, POStore>> committers = 
            new ArrayList<Pair<OutputCommitter,POStore>>();
        for (POStore store : stores) {
            StoreFuncInterface sFunc = store.getStoreFunc();
            
            TaskAttemptContext updatedContext = setUpContext(context, store);
            try {
                committers.add(new Pair<OutputCommitter, POStore>(
                        sFunc.getOutputFormat().getOutputCommitter(
                                updatedContext), store));
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        return committers;
        
    }
    
    private TaskAttemptContext setUpContext(TaskAttemptContext context, 
            POStore store) throws IOException {
        // Setup UDFContext so StoreFunc can make use of it
        MapRedUtil.setupUDFContext(context.getConfiguration());
        // make a copy of the context so that the actions after this call
        // do not end up updating the same context
        TaskAttemptContext contextCopy = HadoopShims.createTaskAttemptContext(
                context.getConfiguration(), context.getTaskAttemptID());
        
        // call setLocation() on the storeFunc so that if there are any
        // side effects like setting map.output.dir on the Configuration
        // in the Context are needed by the OutputCommitter, those actions
        // will be done before the committer is created. 
        PigOutputFormat.setLocation(contextCopy, store);
        return contextCopy;   
    }
    
    static public JobContext setUpContext(JobContext context, 
            POStore store) throws IOException {
        // make a copy of the context so that the actions after this call
        // do not end up updating the same context
        JobContext contextCopy = HadoopShims.createJobContext(
                context.getConfiguration(), context.getJobID());
        MapRedUtil.setupUDFContext(context.getConfiguration());
        
        // call setLocation() on the storeFunc so that if there are any
        // side effects like setting map.output.dir on the Configuration
        // in the Context are needed by the OutputCommitter, those actions
        // will be done before the committer is created. Also the String 
        // version of StoreFunc for the specific store need
        // to be set up in the context in case the committer needs them
        PigOutputFormat.setLocation(contextCopy, store);
        return contextCopy;   
    }

    static public void storeCleanup(POStore store, Configuration conf)
            throws IOException {
        StoreFuncInterface storeFunc = store.getStoreFunc();
        if (storeFunc instanceof StoreMetadata) {
            Schema schema = store.getSchema();
            if (schema != null) {
                ((StoreMetadata) storeFunc).storeSchema(
                        new ResourceSchema(schema, store.getSortInfo()), store.getSFile()
                                .getFileName(), new Job(conf));
            }
        }
    }

    public boolean isRecoverySupported() {
        if (!recoverySupported)
            return false;
        boolean allOutputCommitterSupportRecovery = true;
        // call recoverTask on all map and reduce committers
        for (Pair<OutputCommitter, POStore> mapCommitter : mapOutputCommitters) {
            if (mapCommitter.first!=null) {
                try {
                    // Use reflection, Hadoop 1.x line does not have such method
                    Method m = mapCommitter.first.getClass().getMethod("isRecoverySupported");
                    allOutputCommitterSupportRecovery = allOutputCommitterSupportRecovery
                            && (Boolean)m.invoke(mapCommitter.first);
                } catch (NoSuchMethodException e) {
                    allOutputCommitterSupportRecovery = false;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (!allOutputCommitterSupportRecovery)
                    return false;
            }
        }
        for (Pair<OutputCommitter, POStore> reduceCommitter :
            reduceOutputCommitters) {
            if (reduceCommitter.first!=null) {
                try {
                    // Use reflection, Hadoop 1.x line does not have such method
                    Method m = reduceCommitter.first.getClass().getMethod("isRecoverySupported");
                    allOutputCommitterSupportRecovery = allOutputCommitterSupportRecovery
                            && (Boolean)m.invoke(reduceCommitter.first);
                } catch (NoSuchMethodException e) {
                    allOutputCommitterSupportRecovery = false;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (!allOutputCommitterSupportRecovery)
                    return false;
            }
        }
        return true;
    }

    public void recoverTask(TaskAttemptContext context) throws IOException {
        // call recoverTask on all map and reduce committers
        for (Pair<OutputCommitter, POStore> mapCommitter : mapOutputCommitters) {
            if (mapCommitter.first!=null) {
                TaskAttemptContext updatedContext = setUpContext(context,
                        mapCommitter.second);
                try {
                    // Use reflection, Hadoop 1.x line does not have such method
                    Method m = mapCommitter.first.getClass().getMethod("recoverTask", TaskAttemptContext.class);
                    m.invoke(mapCommitter.first, updatedContext);
                } catch (NoSuchMethodException e) {
                    // We are using Hadoop 1.x, ignore
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
        }
        for (Pair<OutputCommitter, POStore> reduceCommitter :
            reduceOutputCommitters) {
            if (reduceCommitter.first!=null) {
                TaskAttemptContext updatedContext = setUpContext(context,
                        reduceCommitter.second);
                try {
                    // Use reflection, Hadoop 1.x line does not have such method
                    Method m = reduceCommitter.first.getClass().getMethod("recoverTask", TaskAttemptContext.class);
                    m.invoke(reduceCommitter.first, updatedContext);
                } catch (NoSuchMethodException e) {
                    // We are using Hadoop 1.x, ignore
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
        }
    }
    
    @Override
    public void cleanupJob(JobContext context) throws IOException {
        // call clean up on all map and reduce committers
        for (Pair<OutputCommitter, POStore> mapCommitter : mapOutputCommitters) {            
            if (mapCommitter.first!=null) {
                JobContext updatedContext = setUpContext(context, 
                        mapCommitter.second);
                storeCleanup(mapCommitter.second, updatedContext.getConfiguration());
                mapCommitter.first.cleanupJob(updatedContext);
            }
        }
        for (Pair<OutputCommitter, POStore> reduceCommitter : 
            reduceOutputCommitters) {            
            if (reduceCommitter.first!=null) {
                JobContext updatedContext = setUpContext(context, 
                        reduceCommitter.second);
                storeCleanup(reduceCommitter.second, updatedContext.getConfiguration());
                reduceCommitter.first.cleanupJob(updatedContext);
            }
        }
       
    }
    
    // This method only be called in 20.203+/0.23
    public void commitJob(JobContext context) throws IOException {
        // call commitJob on all map and reduce committers
        for (Pair<OutputCommitter, POStore> mapCommitter : mapOutputCommitters) {
            if (mapCommitter.first!=null) {
                JobContext updatedContext = setUpContext(context,
                        mapCommitter.second);
                // PIG-2642 promote files before calling storeCleanup/storeSchema 
                try {
                    // Use reflection, 20.2 does not have such method
                    Method m = mapCommitter.first.getClass().getMethod("commitJob", JobContext.class);
                    m.setAccessible(true);
                    m.invoke(mapCommitter.first, updatedContext);
                } catch (Exception e) {
                    throw new IOException(e);
                }
                storeCleanup(mapCommitter.second, updatedContext.getConfiguration());
            }
        }
        for (Pair<OutputCommitter, POStore> reduceCommitter :
            reduceOutputCommitters) {
            if (reduceCommitter.first!=null) {
                JobContext updatedContext = setUpContext(context,
                        reduceCommitter.second);
                // PIG-2642 promote files before calling storeCleanup/storeSchema 
                try {
                    // Use reflection, 20.2 does not have such method
                    Method m = reduceCommitter.first.getClass().getMethod("commitJob", JobContext.class);
                    m.setAccessible(true);
                    m.invoke(reduceCommitter.first, updatedContext);
                } catch (Exception e) {
                    throw new IOException(e);
                }
                storeCleanup(reduceCommitter.second, updatedContext.getConfiguration());
            }
        }
    }
    
    // This method only be called in 20.203+/0.23
    public void abortJob(JobContext context, State state) throws IOException {
     // call abortJob on all map and reduce committers
        for (Pair<OutputCommitter, POStore> mapCommitter : mapOutputCommitters) {
            if (mapCommitter.first!=null) {
                JobContext updatedContext = setUpContext(context,
                        mapCommitter.second);
                try {
                    // Use reflection, 20.2 does not have such method
                    Method m = mapCommitter.first.getClass().getMethod("abortJob", JobContext.class, State.class);
                    m.setAccessible(true);
                    m.invoke(mapCommitter.first, updatedContext, state);
                } catch (Exception e) {
                    throw new IOException(e);
                }
                storeCleanup(mapCommitter.second, updatedContext.getConfiguration());
            }
        }
        for (Pair<OutputCommitter, POStore> reduceCommitter :
            reduceOutputCommitters) {
            if (reduceCommitter.first!=null) {
                JobContext updatedContext = setUpContext(context,
                        reduceCommitter.second);
                try {
                    // Use reflection, 20.2 does not have such method
                    Method m = reduceCommitter.first.getClass().getMethod("abortJob", JobContext.class, State.class);
                    m.setAccessible(true);
                    m.invoke(reduceCommitter.first, updatedContext, state);
                } catch (Exception e) {
                    throw new IOException(e);
                }
                storeCleanup(reduceCommitter.second, updatedContext.getConfiguration());
            }
        }
    }


    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {        
        if(HadoopShims.isMap(context.getTaskAttemptID())) {
            for (Pair<OutputCommitter, POStore> mapCommitter : 
                mapOutputCommitters) {
                if (mapCommitter.first!=null) {
                    TaskAttemptContext updatedContext = setUpContext(context, 
                            mapCommitter.second);
                    mapCommitter.first.abortTask(updatedContext);
                }
            } 
        } else {
            for (Pair<OutputCommitter, POStore> reduceCommitter : 
                reduceOutputCommitters) {
                if (reduceCommitter.first!=null) {
                    TaskAttemptContext updatedContext = setUpContext(context, 
                            reduceCommitter.second);
                    reduceCommitter.first.abortTask(updatedContext);
                }
            } 
        }
    }
    
    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
        if(HadoopShims.isMap(context.getTaskAttemptID())) {
            for (Pair<OutputCommitter, POStore> mapCommitter : 
                mapOutputCommitters) {
                if (mapCommitter.first!=null) {
                    TaskAttemptContext updatedContext = setUpContext(context, 
                            mapCommitter.second);
                    mapCommitter.first.commitTask(updatedContext);
                }
            } 
        } else {
            for (Pair<OutputCommitter, POStore> reduceCommitter : 
                reduceOutputCommitters) {
                if (reduceCommitter.first!=null) {
                    TaskAttemptContext updatedContext = setUpContext(context, 
                            reduceCommitter.second);
                    reduceCommitter.first.commitTask(updatedContext);
                }
            } 
        }
    }
    
    @Override
    public boolean needsTaskCommit(TaskAttemptContext context)
            throws IOException {
        boolean needCommit = false;
        if(HadoopShims.isMap(context.getTaskAttemptID())) {
            for (Pair<OutputCommitter, POStore> mapCommitter : 
                mapOutputCommitters) {
                if (mapCommitter.first!=null) {
                    TaskAttemptContext updatedContext = setUpContext(context, 
                            mapCommitter.second);
                    needCommit = needCommit || 
                    mapCommitter.first.needsTaskCommit(updatedContext);
                }
            } 
            return needCommit;
        } else {
            for (Pair<OutputCommitter, POStore> reduceCommitter : 
                reduceOutputCommitters) {
                if (reduceCommitter.first!=null) {
                    TaskAttemptContext updatedContext = setUpContext(context, 
                            reduceCommitter.second);
                    needCommit = needCommit || 
                    reduceCommitter.first.needsTaskCommit(updatedContext);
                }
            } 
            return needCommit;
        }
    }
    
    @Override
    public void setupJob(JobContext context) throws IOException {
        // call set up on all map and reduce committers
        for (Pair<OutputCommitter, POStore> mapCommitter : mapOutputCommitters) {
            if (mapCommitter.first!=null) {
                JobContext updatedContext = setUpContext(context, 
                        mapCommitter.second);
                mapCommitter.first.setupJob(updatedContext);
            }
        }
        for (Pair<OutputCommitter, POStore> reduceCommitter : 
            reduceOutputCommitters) {
            if (reduceCommitter.first!=null) {
                JobContext updatedContext = setUpContext(context, 
                        reduceCommitter.second);
                reduceCommitter.first.setupJob(updatedContext);
            }
        }
    }
    
    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
        if(HadoopShims.isMap(context.getTaskAttemptID())) {
            for (Pair<OutputCommitter, POStore> mapCommitter : 
                mapOutputCommitters) {
                if (mapCommitter.first!=null) {
                    TaskAttemptContext updatedContext = setUpContext(context, 
                            mapCommitter.second);
                    mapCommitter.first.setupTask(updatedContext);
                }
            } 
        } else {
            for (Pair<OutputCommitter, POStore> reduceCommitter : 
                reduceOutputCommitters) {
                if (reduceCommitter.first!=null) {
                    TaskAttemptContext updatedContext = setUpContext(context, 
                            reduceCommitter.second);
                    reduceCommitter.first.setupTask(updatedContext);
                }
            } 
        }
    }
}
