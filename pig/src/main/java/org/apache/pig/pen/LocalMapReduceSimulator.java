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
package org.apache.pig.pen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapBase;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapOnly;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduceCounter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.builtin.ReadScalars;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.ConfigurationValidator;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.pen.util.LineageTracer;



/**
 * Main class that launches pig for Map Reduce
 *
 */
public class LocalMapReduceSimulator {
    
    private MapReduceLauncher launcher = new MapReduceLauncher();
    
    private Map<PhysicalOperator, PhysicalOperator> phyToMRMap = new HashMap<PhysicalOperator, PhysicalOperator>();;

    @SuppressWarnings("unchecked")
    public void launchPig(PhysicalPlan php, Map<LOLoad, DataBag> baseData,
                              LineageTracer lineage,
                              IllustratorAttacher attacher,
                              ExampleGenerator eg,
                              PigContext pc) throws PigException, IOException, InterruptedException {
        phyToMRMap.clear();
        MROperPlan mrp = launcher.compile(php, pc);
                
        ConfigurationValidator.validatePigProperties(pc.getProperties());
        Configuration conf = ConfigurationUtil.toConfiguration(pc.getProperties());
        
        JobControlCompiler jcc = new JobControlCompiler(pc, conf);
        
        JobControl jc;
        int numMRJobsCompl = 0;
        DataBag input;
        List<Pair<PigNullableWritable, Writable>> intermediateData = new ArrayList<Pair<PigNullableWritable, Writable>>();

        Map<Job, MapReduceOper> jobToMroMap = jcc.getJobMroMap();
        HashMap<String, DataBag> output = new HashMap<String, DataBag>();
        Configuration jobConf;
        // jc is null only when mrp.size == 0
        boolean needFileInput;
        final ArrayList<OperatorKey> emptyInpTargets = new ArrayList<OperatorKey>();
        pc.getProperties().setProperty("pig.illustrating", "true");
        while(mrp.size() != 0) {
            jc = jcc.compile(mrp, "Illustrator");
            if(jc == null) {
                throw new ExecException("Native execution is not supported");
            }
            List<Job> jobs = jc.getWaitingJobs();
            for (Job job : jobs) {
                jobConf = job.getJobConf();
                FileLocalizer.setInitialized(false);
                ArrayList<ArrayList<OperatorKey>> inpTargets =
                    (ArrayList<ArrayList<OperatorKey>>)
                      ObjectSerializer.deserialize(jobConf.get("pig.inpTargets"));
                intermediateData.clear();
                MapReduceOper mro = jobToMroMap.get(job);
                PigSplit split = null;
                List<POStore> stores = null;
                PhysicalOperator pack = null;
                // revisit as there are new physical operators from MR compilation 
                if (!mro.mapPlan.isEmpty())
                    attacher.revisit(mro.mapPlan);
                if (!mro.reducePlan.isEmpty()) {
                    attacher.revisit(mro.reducePlan);
                    pack = mro.reducePlan.getRoots().get(0);
                }
                
                List<POLoad> lds = PlanHelper.getPhysicalOperators(mro.mapPlan, POLoad.class);
                if (!mro.mapPlan.isEmpty()) {
                    stores = PlanHelper.getPhysicalOperators(mro.mapPlan, POStore.class);
                }
                if (!mro.reducePlan.isEmpty()) {
                    if (stores == null)
                        stores = PlanHelper.getPhysicalOperators(mro.reducePlan, POStore.class);
                    else
                        stores.addAll(PlanHelper.getPhysicalOperators(mro.reducePlan, POStore.class));
                }

                for (POStore store : stores) {
                    output.put(store.getSFile().getFileName(), attacher.getDataMap().get(store));
                }
               
                OutputAttacher oa = new OutputAttacher(mro.mapPlan, output);
                oa.visit();
                
                if (!mro.reducePlan.isEmpty()) {
                    oa = new OutputAttacher(mro.reducePlan, output);
                    oa.visit();
                }
                int index = 0;
                for (POLoad ld : lds) {
                    input = output.get(ld.getLFile().getFileName());
                    if (input == null && baseData != null) {
                        for (LogicalRelationalOperator lo : baseData.keySet()) {
                            if (((LOLoad) lo).getSchemaFile().equals(ld.getLFile().getFileName()))
                            {
                                 input = baseData.get(lo);
                                 break;
                            }
                        }
                    }
                    if (input != null)
                        mro.mapPlan.remove(ld);
                }
                for (POLoad ld : lds) {
                    // check newly generated data first
                    input = output.get(ld.getLFile().getFileName());
                    if (input == null && baseData != null) {
                        if (input == null && baseData != null) {
                            for (LogicalRelationalOperator lo : baseData.keySet()) {
                                if (((LOLoad) lo).getSchemaFile().equals(ld.getLFile().getFileName()))
                                {
                                     input = baseData.get(lo);
                                     break;
                                }
                            }
                        } 
                    }
                    needFileInput = (input == null);
                    split = new PigSplit(null, index, needFileInput ? emptyInpTargets : inpTargets.get(index), 0);
                    ++index;
                    Mapper<Text, Tuple, PigNullableWritable, Writable> map;

                    if (mro.reducePlan.isEmpty()) {
                        // map-only
                        map = new PigMapOnly.Map();
                        Mapper<Text, Tuple, PigNullableWritable, Writable>.Context context = ((PigMapOnly.Map) map)
                          .getIllustratorContext(jobConf, input, intermediateData, split);
                        if(mro.isCounterOperation()) {
                            if(mro.isRowNumber()) {
                                map = new PigMapReduceCounter.PigMapCounter();
                            }
                            context = ((PigMapReduceCounter.PigMapCounter) map).getIllustratorContext(jobConf, input, intermediateData, split);
                        }
                        ((PigMapBase) map).setMapPlan(mro.mapPlan);
                        map.run(context);
                    } else {
                        if ("true".equals(jobConf.get("pig.usercomparator")))
                            map = new PigMapReduce.MapWithComparator();
                        else if (!"".equals(jobConf.get("pig.keyDistFile", "")))
                            map = new PigMapReduce.MapWithPartitionIndex();
                        else
                            map = new PigMapReduce.Map();
                        Mapper<Text, Tuple, PigNullableWritable, Writable>.Context context = ((PigMapBase) map)
                          .getIllustratorContext(jobConf, input, intermediateData, split);
                        ((PigMapBase) map).setMapPlan(mro.mapPlan);
                        map.run(context);
                    }
                }
                
                if (!mro.reducePlan.isEmpty())
                {
                    if (pack instanceof POPackage)
                        mro.reducePlan.remove(pack);
                    // reducer run
                    PigMapReduce.Reduce reduce;
                    if ("true".equals(jobConf.get("pig.usercomparator")))
                        reduce = new PigMapReduce.ReduceWithComparator();
                    else
                        reduce = new PigMapReduce.Reduce();
                    Reducer<PigNullableWritable, NullableTuple, PigNullableWritable, Writable>.Context
                        context = reduce.getIllustratorContext(job, intermediateData, (POPackage) pack);

                    if(mro.isCounterOperation()) {
                        reduce = new PigMapReduceCounter.PigReduceCounter();
                        context = ((PigMapReduceCounter.PigReduceCounter)reduce).getIllustratorContext(job, intermediateData, (POPackage) pack);
                    }

                    ((PigMapReduce.Reduce) reduce).setReducePlan(mro.reducePlan);
                    reduce.run(context);
                }
                for (PhysicalOperator key : mro.phyToMRMap.keySet())
                    for (PhysicalOperator value : mro.phyToMRMap.get(key))
                        phyToMRMap.put(key, value);
            }
            
            
            int removedMROp = jcc.updateMROpPlan(new LinkedList<Job>());
            
            numMRJobsCompl += removedMROp;
        }
                
        jcc.reset();
    }

    private class OutputAttacher extends PhyPlanVisitor {
        private Map<String, DataBag> outputBuffer;
        OutputAttacher(PhysicalPlan plan, Map<String, DataBag> output) {
            super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
                    plan));
            this.outputBuffer = output;
        }
        
        @Override
        public void visitUserFunc(POUserFunc userFunc) throws VisitorException {
            if (userFunc.getFunc() != null && userFunc.getFunc() instanceof ReadScalars) {
                ((ReadScalars) userFunc.getFunc()).setOutputBuffer(outputBuffer);
            }
        }
    }
    public Map<PhysicalOperator, PhysicalOperator> getPhyToMRMap() {
        return phyToMRMap;
    }
}
