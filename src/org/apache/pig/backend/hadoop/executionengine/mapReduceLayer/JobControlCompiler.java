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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;

import org.apache.pig.ComparisonFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.NullableBytesWritable;
import org.apache.pig.impl.io.NullableDoubleWritable;
import org.apache.pig.impl.io.NullableFloatWritable;
import org.apache.pig.impl.io.NullableIntWritable;
import org.apache.pig.impl.io.NullableLongWritable;
import org.apache.pig.impl.io.NullableText;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Pair;

/**
 * This is compiler class that takes an MROperPlan and converts
 * it into a JobControl object with the relevant dependency info
 * maintained. The JobControl Object is made up of Jobs each of
 * which has a JobConf. The MapReduceOper corresponds to a Job
 * and the getJobCong method returns the JobConf that is configured
 * as per the MapReduceOper
 *
 * <h2>Comparator Design</h2>
 * <p>
 * A few words on how comparators are chosen.  In almost all cases we use raw
 * comparators (the one exception being when the user provides a comparison
 * function for order by).  For order by queries the PigTYPERawComparator
 * functions are used, where TYPE is Int, Long, etc.  These comparators are
 * null aware and asc/desc aware.  The first byte of each of the
 * NullableTYPEWritable classes contains info on whether the value is null.
 * Asc/desc is written as an array into the JobConf with the key pig.sortOrder
 * so that it can be read by each of the comparators as part of their 
 * setConf call.
 * <p>
 * For non-order by queries, PigTYPEWritableComparator classes are used.
 * These are all just type specific instances of WritableComparator.
 *
 */
public class JobControlCompiler{
    MROperPlan plan;
    Configuration conf;
    PigContext pigContext;
    
    private final Log log = LogFactory.getLog(getClass());

    public static final String LOG_DIR = "_logs";

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
        
        List<MapReduceOper> leaves ;
        leaves = plan.getLeaves();
        
        for (MapReduceOper mro : leaves) {
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
        List<MapReduceOper> pred = plan.getPredecessors(mro);
        
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
            List<Job> compiledInputs = new ArrayList<Job>(pred.size());
            
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
            return new Job(currJC,(ArrayList<Job>)compiledInputs);
        } catch (JobCreationException jce) {
        	throw jce;
        } catch(Exception e) {
            int errCode = 2017;
            String msg = "Internal error creating job configuration.";
            throw new JobCreationException(msg, errCode, PigException.BUG, e);
        }
    }
    
    /**
     * The method that creates the JobConf corresponding to a MapReduceOper.
     * The assumption is that
     * every MapReduceOper will have a load and a store. The JobConf removes
     * the load operator and serializes the input filespec so that PigInputFormat can
     * take over the creation of splits. It also removes the store operator
     * and serializes the output filespec so that PigOutputFormat can take over
     * record writing. The remaining portion of the map plan and reduce plans are
     * serialized and stored for the PigMapReduce or PigMapOnly objects to take over
     * the actual running of the plans.
     * The Mapper &amp; Reducer classes and the required key value formats are set.
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
        ArrayList<Pair<FileSpec, Boolean>> inp = new ArrayList<Pair<FileSpec, Boolean>>();
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
                
                Pair<FileSpec, Boolean> p = new Pair<FileSpec, Boolean>(ld.getLFile(), ld.isSplittable());
                //Store the inp filespecs
                inp.add(p);
                
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
            // ensure the job jar is deleted on exit
            submitJarFile.deleteOnExit();
            FileOutputStream fos = new FileOutputStream(submitJarFile);
            JarManager.createJar(fos, mro.UDFs, pigContext);
            
            //Start setting the JobConf properties
            jobConf.setJar(submitJarFile.getPath());
            jobConf.set("pig.inputs", ObjectSerializer.serialize(inp));
            jobConf.set("pig.inpTargets", ObjectSerializer.serialize(inpTargets));
            jobConf.set("pig.pigContext", ObjectSerializer.serialize(pigContext));
            // this is for unit tests since some don't create PigServer
            if (pigContext.getProperties().getProperty(PigContext.JOB_NAME) != null)
                jobConf.setJobName(pigContext.getProperties().getProperty(PigContext.JOB_NAME));
    
            // Setup the DistributedCache for this job
            setupDistributedCache(pigContext, jobConf, pigContext.getProperties(), 
                                  "pig.streaming.ship.files", true);
            setupDistributedCache(pigContext, jobConf, pigContext.getProperties(), 
                                  "pig.streaming.cache.files", false);

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
            FuncSpec outputFuncSpec = st.getSFile().getFuncSpec();
            FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));
            jobConf.set("pig.storeFunc", outputFuncSpec.toString());

            // Setup the logs directory for streaming jobs
            jobConf.set("pig.streaming.log.dir", 
                        new Path(new Path(outputPath), LOG_DIR).toString());
            jobConf.set("pig.streaming.task.output.dir", outputPath);

            
            // store map key type
            // this is needed when the key is null to create
            // an appropriate NullableXXXWritable object
            jobConf.set("pig.map.keytype", ObjectSerializer.serialize(new byte[] { mro.mapKeyType }));

            // set parent plan in all operators in map and reduce plans
            // currently the parent plan is really used only when POStream is present in the plan
            PhysicalPlan[] plans = new PhysicalPlan[] { mro.mapPlan, mro.reducePlan };
            for (int i = 0; i < plans.length; i++) {
                for (Iterator<PhysicalOperator> it = plans[i].iterator(); it.hasNext();) {
                    PhysicalOperator op = it.next();
                    op.setParentPlan(plans[i]);                
                }    
            }
            POPackage pack = null;
            if(mro.reducePlan.isEmpty()){
                //MapOnly Job
                jobConf.setMapperClass(PigMapOnly.Map.class);
                jobConf.setNumReduceTasks(0);
                jobConf.set("pig.mapPlan", ObjectSerializer.serialize(mro.mapPlan));
                if(mro.isStreamInMap()) {
                    // this is used in Map.close() to decide whether the
                    // pipeline needs to be rerun one more time in the close()
                    // The pipeline is rerun only if there was a stream
                    jobConf.set("pig.stream.in.map", "true");
                }
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
                } else if (mro.needsDistinctCombiner()) {
                    jobConf.setCombinerClass(DistinctCombiner.Combine.class);
                    log.info("Setting identity combiner class.");
                }
                pack = (POPackage)mro.reducePlan.getRoots().get(0);
                mro.reducePlan.remove(pack);
                jobConf.setMapperClass(PigMapReduce.Map.class);
                jobConf.setReducerClass(PigMapReduce.Reduce.class);
                if (mro.requestedParallelism>0)
                    jobConf.setNumReduceTasks(mro.requestedParallelism);

                jobConf.set("pig.mapPlan", ObjectSerializer.serialize(mro.mapPlan));
                if(mro.isStreamInMap()) {
                    // this is used in Map.close() to decide whether the
                    // pipeline needs to be rerun one more time in the close()
                    // The pipeline is rerun only if there was a stream
                    jobConf.set("pig.stream.in.map", "true");
                }
                jobConf.set("pig.reducePlan", ObjectSerializer.serialize(mro.reducePlan));
                if(mro.isStreamInReduce()) {
                    // this is used in Map.close() to decide whether the
                    // pipeline needs to be rerun one more time in the close()
                    // The pipeline is rerun only if there was a stream
                    jobConf.set("pig.stream.in.reduce", "true");
                }
                jobConf.set("pig.reduce.package", ObjectSerializer.serialize(pack));
                Class<? extends WritableComparable> keyClass = HDataType.getWritableComparableTypes(pack.getKeyType()).getClass();
                jobConf.setOutputKeyClass(keyClass);
                jobConf.set("pig.reduce.key.type", Byte.toString(pack.getKeyType())); 
                selectComparator(mro, pack.getKeyType(), jobConf);
                jobConf.setOutputValueClass(NullableTuple.class);
            }
        
            if(mro.isGlobalSort() || mro.isLimitAfterSort()){
                // Only set the quantiles file and sort partitioner if we're a
                // global sort, not for limit after sort.
                if (mro.isGlobalSort()) {
                    jobConf.set("pig.quantilesFile", mro.getQuantFile());
                    jobConf.setPartitionerClass(SortPartitioner.class);
                }
                if(mro.UDFs.size()==1){
                    String compFuncSpec = mro.UDFs.get(0);
                    Class comparator = PigContext.resolveClassName(compFuncSpec);
                    if(ComparisonFunc.class.isAssignableFrom(comparator)) {
                        jobConf.setMapperClass(PigMapReduce.MapWithComparator.class);
                        jobConf.setReducerClass(PigMapReduce.ReduceWithComparator.class);
                        jobConf.set("pig.reduce.package", ObjectSerializer.serialize(pack));
                        jobConf.set("pig.usercomparator", "true");
                        jobConf.setOutputKeyClass(NullableTuple.class);
                        jobConf.setOutputKeyComparatorClass(comparator);
                    }
                } else {
                    jobConf.set("pig.sortOrder",
                        ObjectSerializer.serialize(mro.getSortOrder()));
                }
            }
            return jobConf;
        } catch (JobCreationException jce) {
        	throw jce;
        } catch(Exception e) {
            int errCode = 2017;
            String msg = "Internal error creating job configuration.";
            throw new JobCreationException(msg, errCode, PigException.BUG, e);
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
        protected PigWritableComparator(Class c) {
            super(c);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
            return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class PigIntWritableComparator extends PigWritableComparator {
        public PigIntWritableComparator() {
            super(NullableIntWritable.class);
        }
    }

    public static class PigLongWritableComparator extends PigWritableComparator {
        public PigLongWritableComparator() {
            super(NullableLongWritable.class);
        }
    }

    public static class PigFloatWritableComparator extends PigWritableComparator {
        public PigFloatWritableComparator() {
            super(NullableFloatWritable.class);
        }
    }

    public static class PigDoubleWritableComparator extends PigWritableComparator {
        public PigDoubleWritableComparator() {
            super(NullableDoubleWritable.class);
        }
    }

    public static class PigCharArrayWritableComparator extends PigWritableComparator {
        public PigCharArrayWritableComparator() {
            super(NullableText.class);
        }
    }

    public static class PigDBAWritableComparator extends PigWritableComparator {
        public PigDBAWritableComparator() {
            super(NullableBytesWritable.class);
        }
    }

    public static class PigTupleWritableComparator extends PigWritableComparator {
        public PigTupleWritableComparator() {
            super(TupleFactory.getInstance().tupleClass());
        }
    }

    public static class PigBagWritableComparator extends PigWritableComparator {
        public PigBagWritableComparator() {
            super(BagFactory.getInstance().newDefaultBag().getClass());
        }
    }

    private void selectComparator(
            MapReduceOper mro,
            byte keyType,
            JobConf jobConf) throws JobCreationException {
        // If this operator is involved in an order by, use the pig specific raw
        // comparators.  If it has a cogroup, we need to set the comparator class
        // to the raw comparator and the grouping comparator class to pig specific
        // raw comparators (which skip the index).  Otherwise use the hadoop provided
        // raw comparator.
        
        // An operator has an order by if global sort is set or if it's successor has
        // global sort set (because in that case it's the sampling job) or if
        // it's a limit after a sort. 
        boolean hasOrderBy = false;
        if (mro.isGlobalSort() || mro.isLimitAfterSort()) {
            hasOrderBy = true;
        } else {
            List<MapReduceOper> succs = plan.getSuccessors(mro);
            if (succs != null) {
                MapReduceOper succ = succs.get(0);
                if (succ.isGlobalSort()) hasOrderBy = true;
            }
        }
        if (hasOrderBy) {
            switch (keyType) {
            case DataType.INTEGER:
                jobConf.setOutputKeyComparatorClass(PigIntRawComparator.class);
                break;

            case DataType.LONG:
                jobConf.setOutputKeyComparatorClass(PigLongRawComparator.class);
                break;

            case DataType.FLOAT:
                jobConf.setOutputKeyComparatorClass(PigFloatRawComparator.class);
                break;

            case DataType.DOUBLE:
                jobConf.setOutputKeyComparatorClass(PigDoubleRawComparator.class);
                break;

            case DataType.CHARARRAY:
                jobConf.setOutputKeyComparatorClass(PigTextRawComparator.class);
                break;

            case DataType.BYTEARRAY:
                jobConf.setOutputKeyComparatorClass(PigBytesRawComparator.class);
                break;

            case DataType.MAP:
                int errCode = 1068;
                String msg = "Using Map as key not supported.";
                throw new JobCreationException(msg, errCode, PigException.INPUT);

            case DataType.TUPLE:
                jobConf.setOutputKeyComparatorClass(PigTupleRawComparator.class);
                break;

            case DataType.BAG:
                errCode = 1068;
                msg = "Using Bag as key not supported.";
                throw new JobCreationException(msg, errCode, PigException.INPUT);

            default:
                break;
            }
            return;
        }

        switch (keyType) {
        case DataType.INTEGER:
            jobConf.setOutputKeyComparatorClass(PigIntWritableComparator.class);
            break;

        case DataType.LONG:
            jobConf.setOutputKeyComparatorClass(PigLongWritableComparator.class);
            break;

        case DataType.FLOAT:
            jobConf.setOutputKeyComparatorClass(PigFloatWritableComparator.class);
            break;

        case DataType.DOUBLE:
            jobConf.setOutputKeyComparatorClass(PigDoubleWritableComparator.class);
            break;

        case DataType.CHARARRAY:
            jobConf.setOutputKeyComparatorClass(PigCharArrayWritableComparator.class);
            break;

        case DataType.BYTEARRAY:
            jobConf.setOutputKeyComparatorClass(PigDBAWritableComparator.class);
            break;

        case DataType.MAP:
            int errCode = 1068;
            String msg = "Using Map as key not supported.";
            throw new JobCreationException(msg, errCode, PigException.INPUT);

        case DataType.TUPLE:
            jobConf.setOutputKeyComparatorClass(PigTupleWritableComparator.class);
            break;

        case DataType.BAG:
            errCode = 1068;
            msg = "Using Bag as key not supported.";
            throw new JobCreationException(msg, errCode, PigException.INPUT);

        default:
            errCode = 2036;
            msg = "Unhandled key type " + DataType.findTypeName(keyType);
            throw new JobCreationException(msg, errCode, PigException.BUG);
        }
    }

    private static void setupDistributedCache(PigContext pigContext,
                                              Configuration conf, 
                                              Properties properties, String key, 
                                              boolean shipToCluster) 
    throws IOException {
        // Turn on the symlink feature
        DistributedCache.createSymlink(conf);

        // Set up the DistributedCache for this job        
        String fileNames = properties.getProperty(key);
        if (fileNames != null) {
            String[] paths = fileNames.split(",");
            
            for (String path : paths) {
                path = path.trim();
                if (path.length() != 0) {
                    Path src = new Path(path);
                    
                    // Ensure that 'src' is a valid URI
                    URI srcURI = null;
                    try {
                        srcURI = new URI(src.toString());
                    } catch (URISyntaxException ue) {
                        int errCode = 6003;
                        String msg = "Invalid cache specification. " +
                        "File doesn't exist: " + src;
                        throw new ExecException(msg, errCode, PigException.USER_ENVIRONMENT);
                    }
                    
                    // Ship it to the cluster if necessary and add to the
                    // DistributedCache
                    if (shipToCluster) {
                        Path dst = 
                            new Path(FileLocalizer.getTemporaryPath(null, pigContext).toString());
                        FileSystem fs = dst.getFileSystem(conf);
                        fs.copyFromLocalFile(src, dst);
                        
                        // Construct the dst#srcName uri for DistributedCache
                        URI dstURI = null;
                        try {
                            dstURI = new URI(dst.toString() + "#" + src.getName());
                        } catch (URISyntaxException ue) {
                            byte errSrc = pigContext.getErrorSource();
                            int errCode = 0;
                            switch(errSrc) {
                            case PigException.REMOTE_ENVIRONMENT:
                                errCode = 6004;
                                break;
                            case PigException.USER_ENVIRONMENT:
                                errCode = 4004;
                                break;
                            default:
                                errCode = 2037;
                                break;
                            }
                            String msg = "Invalid ship specification. " +
                            "File doesn't exist: " + dst;
                            throw new ExecException(msg, errCode, errSrc);
                        }
                        DistributedCache.addCacheFile(dstURI, conf);
                    } else {
                        DistributedCache.addCacheFile(srcURI, conf);
                    }
                }
            }
        }
    }
}
