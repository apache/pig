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
package org.apache.pig.impl.mapReduceLayer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.data.DataType;
import org.apache.pig.data.IndexedTuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.impl.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.impl.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.ObjectSerializer;

/**
 * This is compiler class that takes an MROperPlan and converts
 * it into a JobControl object with the relevant dependency info
 * maintained. The JobControl Object is made up of Jobs each of
 * which has a JobConf. The MapReduceOper corresponds to a Job
 * and the getJobCong method returns the JobConf that is configured
 * as per the MapReduceOper
 */
public class JobControlCompiler{
    MROperPlan plan;
    Configuration conf;
    PigContext pigContext;
    
    /**
     * The map between MapReduceOpers and their corresponding Jobs
     */
    Map<OperatorKey, Job> seen = new Hashtable<OperatorKey, Job>();
    
    /**
     * Top level compile method that issues a call to the recursive
     * compile method.
     * @param plan - The MROperPlan to be compiled
     * @param grpName - The name given to the JobControl
     * @param conf - The Configuration object having the various properties
     * @param pigContext - PigContext passed on from the execution engine
     * @return JobControl object
     * @throws JobCreationException
     */
    public JobControl compile(MROperPlan plan, String grpName, Configuration conf, PigContext pigContext) throws JobCreationException{
        this.plan = plan;
        this.conf = conf;
        this.pigContext = pigContext;
        JobControl jobCtrl = new JobControl(grpName);
        
        List<MapReduceOper> leaevs = new ArrayList<MapReduceOper>();
        leaevs = plan.getLeaves();
        
        for (MapReduceOper mro : leaevs) {
            jobCtrl.addJob(compile(mro,jobCtrl));
        }
        return jobCtrl;
    }
    
    /**
     * The recursive compilation method that works by doing a depth first 
     * traversal of the MROperPlan. Compiles a Job for the input MapReduceOper
     * with the dependencies maintained in jobCtrl
     * @param mro - Input MapReduceOper for which a Job needs to be compiled
     * @param jobCtrl - The running JobCtrl object to maintain dependencies b/w jobs
     * @return Job corresponding to the input mro
     * @throws JobCreationException
     */
    private Job compile(MapReduceOper mro, JobControl jobCtrl) throws JobCreationException {
        List<MapReduceOper> pred = new ArrayList<MapReduceOper>();
        pred = plan.getPredecessors(mro);
        
        JobConf currJC = null;
        
        try{
            if(pred==null || pred.size()<=0){
                //No dependencies! Create the JobConf
                //Construct the Job object with it and return
                Job ret = null;
                if(seen.containsKey(mro.getOperatorKey()))
                    ret = seen.get(mro.getOperatorKey());
                else{
                    currJC = getJobConf(mro, conf, pigContext);
                    ret = new Job(currJC,null);
                    seen.put(mro.getOperatorKey(), ret);
                }
                return ret;
            }
            
            //Has dependencies. So compile all the inputs
            List compiledInputs = new ArrayList(pred.size());
            
            for (MapReduceOper oper : pred) {
                Job ret = null;
                if(seen.containsKey(oper.getOperatorKey()))
                    ret = seen.get(oper.getOperatorKey());
                else{
                    ret = compile(oper, jobCtrl);
                    jobCtrl.addJob(ret);
                    seen.put(oper.getOperatorKey(),ret);
                }
                compiledInputs.add(ret);
            }
            //Get JobConf for the current MapReduceOper
            currJC = getJobConf(mro, conf, pigContext);
            
            //Create a new Job with the obtained JobConf
            //and the compiled inputs as dependent jobs
            return new Job(currJC,(ArrayList)compiledInputs);
        }catch(Exception e){
            JobCreationException jce = new JobCreationException(e);
            throw jce;
        }
    }
    
    /**
     * The method that creates the JobConf corresponding to a MapReduceOper
     * Doesn't support Sort or Distinct jobs yet. The assumption is that
     * every MapReduceOper will have a load and a store. The JobConf removes
     * the load operator and serializes the input filespec so that PigInputFormat can
     * take over the creation of splits. It also removes the store operator
     * and serializes the output filespec so that PigOutputFormat can take over
     * record writing. The remaining portion of the map plan and reduce plans are
     * serialized and stored for the PigMapReduce or PigMapOnly objects to take over
     * the actual running of the plans.
     * The Mapper & Reducer classes and the required key value formats are set.
     * Checks if this is a map only job and uses PigMapOnly class as the mapper
     * and uses PigMapReduce otherwise.
     * If it is a Map Reduce job, it is bound to have a package operator. Remove it from
     * the reduce plan and serializes it so that the PigMapReduce class can use it to package
     * the indexed tuples received by the reducer.
     * @param mro - The MapReduceOper for which the JobConf is required
     * @param conf - the Configuration object from which JobConf is built
     * @param pigContext - The PigContext passed on from execution engine
     * @return JobConf corresponding to mro
     * @throws JobCreationException
     */
    private JobConf getJobConf(MapReduceOper mro, Configuration conf, PigContext pigContext) throws JobCreationException{
        JobConf jobConf = new JobConf(conf);
        ArrayList<FileSpec> inp = new ArrayList<FileSpec>();
        ArrayList<List<OperatorKey>> inpTargets = new ArrayList<List<OperatorKey>>();
        
        //Set the User Name for this job. This will be
        //used as the working directory
        String user = System.getProperty("user.name");
        jobConf.setUser(user != null ? user : "Pigster");
        
        //Process the POLoads
        List<PhysicalOperator> lds = getRoots(mro.mapPlan);
        if(lds!=null && lds.size()>0){
            for (PhysicalOperator operator : lds) {
                POLoad ld = (POLoad)operator;
                //Store the inp filespecs
                inp.add(ld.getLFile());
                //Store the target operators for tuples read
                //from this input
                List<PhysicalOperator> ldSucs = mro.mapPlan.getSuccessors(ld);
                List<OperatorKey> ldSucKeys = new ArrayList<OperatorKey>();
                if(ldSucs!=null){
                    for (PhysicalOperator operator2 : ldSucs) {
                        ldSucKeys.add(operator2.getOperatorKey());
                    }
                }
                inpTargets.add(ldSucKeys);
                //Remove the POLoad from the plan
                mro.mapPlan.remove(ld);
            }
        }
        try{
            //Create the jar of all functions reuired
            File submitJarFile = File.createTempFile("Job", ".jar");
            FileOutputStream fos = new FileOutputStream(submitJarFile);
            JarManager.createJar(fos, mro.UDFs, pigContext);
            
            //Start setting the JobConf properties
            jobConf.setJar(submitJarFile.getPath());
            jobConf.set("pig.inputs", ObjectSerializer.serialize(inp));
            jobConf.set("pig.inpTargets", ObjectSerializer.serialize(inpTargets));
            jobConf.set("pig.pigContext", ObjectSerializer.serialize(pigContext));
    
            jobConf.setInputFormat(PigInputFormat.class);
            jobConf.setOutputFormat(PigOutputFormat.class);
            
            //Process POStore and remove it from the plan
            POStore st = null;
            if(mro.reducePlan.isEmpty()){
                st = (POStore) mro.mapPlan.getLeaves().get(0);
                mro.mapPlan.remove(st);
            }
            else{
                st = (POStore) mro.reducePlan.getLeaves().get(0);
                mro.reducePlan.remove(st);
            }
            //set out filespecs
            String outputPath = st.getSFile().getFileName();
            String outputFuncSpec = st.getSFile().getFuncSpec();
            jobConf.setOutputPath(new Path(outputPath));
            jobConf.set("pig.storeFunc", outputFuncSpec);
            
            if(mro.reducePlan.isEmpty()){
                //MapOnly Job
                jobConf.setMapperClass(PigMapOnly.Map.class);
                jobConf.setNumReduceTasks(0);
                jobConf.set("pig.mapPlan", ObjectSerializer.serialize(mro.mapPlan));
            }
            else{
                //Map Reduce Job
                //Process the POPackage operator and remove it from the reduce plan
                if(!mro.combinePlan.isEmpty()){
                    POPackage combPack = (POPackage)mro.combinePlan.getRoots().get(0);
                    mro.combinePlan.remove(combPack);
                    jobConf.setCombinerClass(PigCombiner.Combine.class);
                    jobConf.set("pig.combinePlan", ObjectSerializer.serialize(mro.combinePlan));
                    jobConf.set("pig.combine.package", ObjectSerializer.serialize(combPack));
                }
                POPackage pack = (POPackage)mro.reducePlan.getRoots().get(0);
                mro.reducePlan.remove(pack);
                jobConf.setMapperClass(PigMapReduce.Map.class);
                jobConf.setReducerClass(PigMapReduce.Reduce.class);
                jobConf.setNumReduceTasks((mro.requestedParallelism>0)?mro.requestedParallelism:1);
                jobConf.set("pig.mapPlan", ObjectSerializer.serialize(mro.mapPlan));
                jobConf.set("pig.reducePlan", ObjectSerializer.serialize(mro.reducePlan));
                jobConf.set("pig.reduce.package", ObjectSerializer.serialize(pack));
                Class<? extends WritableComparable> keyClass = DataType.getWritableComparableTypes(pack.getKeyType()).getClass();
                jobConf.setOutputKeyClass(keyClass);
                if(keyClass.equals(TupleFactory.getInstance().tupleClass())){
                    jobConf.setOutputKeyComparatorClass(PigWritableComparator.class);
                }
                jobConf.setOutputValueClass(IndexedTuple.class);
            }
            
            if(mro.isGlobalSort()){
                jobConf.set("pig.quantilesFile", mro.getQuantFile());
                jobConf.setPartitionerClass(SortPartitioner.class);
            }
    
            return jobConf;
        }catch(Exception e){
            JobCreationException jce = new JobCreationException(e);
            throw jce;
        }
    }
    
    private List<PhysicalOperator> getRoots(PhysicalPlan php){
        List<PhysicalOperator> ret = new ArrayList<PhysicalOperator>();
        for (PhysicalOperator operator : php.getRoots()) {
            ret.add(operator);
        }
        return ret;
    }
    
    public static class PigWritableComparator extends WritableComparator {
        public PigWritableComparator() {
            super(TupleFactory.getInstance().tupleClass());
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }
}
