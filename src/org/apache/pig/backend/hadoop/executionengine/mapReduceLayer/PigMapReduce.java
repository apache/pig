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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POJoinPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.data.DataType;
import org.apache.pig.data.TargetedTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.SpillableMemoryManager;
import org.apache.pig.impl.util.WrappedIOException;

/**
 * This class is the static Mapper &amp; Reducer classes that
 * are used by Pig to execute Pig Map Reduce jobs. Since
 * there is a reduce phase, the leaf is bound to be a 
 * POLocalRearrange. So the map phase has to separate the
 * key and tuple and collect it into the output
 * collector.
 * 
 * The shuffle and sort phase sorts these keys &amp; tuples
 * and creates key, List&lt;Tuple&gt; and passes the key and
 * iterator to the list. The deserialized POPackage operator
 * is used to package the key, List&lt;Tuple&gt; into pigKey, 
 * Bag&lt;Tuple&gt; where pigKey is of the appropriate pig type and
 * then the result of the package is attached to the reduce
 * plan which is executed if its not empty. Either the result 
 * of the reduce plan or the package res is collected into
 * the output collector. 
 *
 * The index of the tuple (that is, which bag it should be placed in by the
 * package) is packed into the key.  This is done so that hadoop sorts the
 * keys in order of index for join.
 *
 */
public class PigMapReduce {

    public static JobConf sJobConf = null;
    private final static Tuple DUMMYTUPLE = null;
    
    public static class Map extends PigMapBase implements
            Mapper<Text, Tuple, PigNullableWritable, Writable> {

        @Override
        public void collect(OutputCollector<PigNullableWritable, Writable> oc, Tuple tuple) throws ExecException, IOException {
            Byte index = (Byte)tuple.get(0);
            PigNullableWritable key =
                HDataType.getWritableComparableTypes(tuple.get(1), keyType);
            NullableTuple val = new NullableTuple((Tuple)tuple.get(2));
            // Both the key and the value need the index.  The key needs it so
            // that it can be sorted on the index in addition to the key
            // value.  The value needs it so that POPackage can properly
            // assign the tuple to its slot in the projection.
            key.setIndex(index);
            val.setIndex(index);
            oc.collect(key, val);
        }
    }
    
    /**
     * This "specialized" map class is ONLY to be used in pig queries with
     * order by a udf. A UDF used for comparison in the order by expects
     * to be handed tuples. Hence this map class ensures that the "key" used
     * in the order by is wrapped into a tuple (if it isn't already a tuple)
     */
    public static class MapWithComparator extends PigMapBase implements
            Mapper<Text, Tuple, PigNullableWritable, Writable> {

        @Override
        public void collect(OutputCollector<PigNullableWritable, Writable> oc,
                Tuple tuple) throws ExecException, IOException {
            Object keyTuple = null;
            if(keyType != DataType.TUPLE) {
                Object k = tuple.get(1);
                keyTuple = tf.newTuple(k);
            } else {
                keyTuple = tuple.get(1);
            }
            

            Byte index = (Byte)tuple.get(0);
            PigNullableWritable key =
                HDataType.getWritableComparableTypes(keyTuple, DataType.TUPLE);
            NullableTuple val = new NullableTuple((Tuple)tuple.get(2));
            // Both the key and the value need the index.  The key needs it so
            // that it can be sorted on the index in addition to the key
            // value.  The value needs it so that POPackage can properly
            // assign the tuple to its slot in the projection.
            key.setIndex(index);
            val.setIndex(index);
            oc.collect(key, val);
        }
    }

    public static class Reduce extends MapReduceBase
            implements
            Reducer<PigNullableWritable, NullableTuple, PigNullableWritable, Writable> {
        protected final Log log = LogFactory.getLog(getClass());
        
        //The reduce plan
        protected PhysicalPlan rp;

        // Store operators
        protected List<POStore> stores;
        
        //The POPackage operator which is the
        //root of every Map Reduce plan is
        //obtained through the job conf. The portion
        //remaining after its removal is the reduce
        //plan
        protected POPackage pack;
        
        ProgressableReporter pigReporter;

        protected OutputCollector<PigNullableWritable, Writable> outputCollector;

        protected boolean errorInReduce = false;
        
        PhysicalOperator[] roots;

        private PhysicalOperator leaf;
        
        PigContext pigContext = null;
        protected volatile boolean initialized = false;
        
        /**
         * Configures the Reduce plan, the POPackage operator
         * and the reporter thread
         */
        @Override
        public void configure(JobConf jConf) {
            super.configure(jConf);
            SpillableMemoryManager.configure(ConfigurationUtil.toProperties(jConf));
            sJobConf = jConf;
            try {
                rp = (PhysicalPlan) ObjectSerializer.deserialize(jConf
                        .get("pig.reducePlan"));
                stores = PlanHelper.getStores(rp);

                pack = (POPackage)ObjectSerializer.deserialize(jConf.get("pig.reduce.package"));
                // To be removed
                if(rp.isEmpty())
                    log.debug("Reduce Plan empty!");
                else{
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    rp.explain(baos);
                    log.debug(baos.toString());
                }
                // till here
                
                long sleepTime = jConf.getLong("pig.reporter.sleep.time", 10000);

                pigReporter = new ProgressableReporter();
                if(!(rp.isEmpty())) {
                    roots = rp.getRoots().toArray(new PhysicalOperator[1]);
                    leaf = rp.getLeaves().get(0);
                }
                
                pigContext = (PigContext)ObjectSerializer.deserialize(jConf.get("pig.pigContext"));
                
            } catch (IOException ioe) {
                String msg = "Problem while configuring reduce plan.";
                throw new RuntimeException(msg, ioe);
            }
        }
        
        /**
         * The reduce function which packages the key and List&lt;Tuple&gt;
         * into key, Bag&lt;Tuple&gt; after converting Hadoop type key into Pig type.
         * The package result is either collected as is, if the reduce plan is
         * empty or after passing through the reduce plan.
         */
        public void reduce(PigNullableWritable key,
                Iterator<NullableTuple> tupIter,
                OutputCollector<PigNullableWritable, Writable> oc,
                Reporter reporter) throws IOException {
            
            if (!initialized) {
                initialized = true;
                
                // cache the collector for use in runPipeline()
                // which could additionally be called from close()
                this.outputCollector = oc;
                pigReporter.setRep(reporter);
                PhysicalOperator.setReporter(pigReporter);

                boolean aggregateWarning = "true".equalsIgnoreCase(pigContext.getProperties().getProperty("aggregate.warning"));
	        
                PigHadoopLogger pigHadoopLogger = PigHadoopLogger.getInstance();
                pigHadoopLogger.setAggregate(aggregateWarning);
                pigHadoopLogger.setReporter(reporter);
                PhysicalOperator.setPigLogger(pigHadoopLogger);

                for (POStore store: stores) {
                    MapReducePOStoreImpl impl 
                        = new MapReducePOStoreImpl(PigMapReduce.sJobConf);
                    impl.setReporter(reporter);
                    store.setStoreImpl(impl);
                    store.setUp();
                }
            }

            // In the case we optimize the join, we combine
            // POPackage and POForeach - so we could get many
            // tuples out of the getnext() call of POJoinPackage
            // In this case, we process till we see EOP from 
            // POJoinPacakage.getNext()
            if (pack instanceof POJoinPackage)
            {
                pack.attachInput(key, tupIter);
                while (true)
                {
                    if (processOnePackageOutput(oc))
                        break;
                }
            }
            else {
                // join is not optimized, so package will
                // give only one tuple out for the key
                pack.attachInput(key, tupIter);
                processOnePackageOutput(oc);
            }
        }
        
        // return: false-more output
        //         true- end of processing
        public boolean processOnePackageOutput(OutputCollector<PigNullableWritable, Writable> oc) throws IOException
        {
            try {
                Result res = pack.getNext(DUMMYTUPLE);
                if(res.returnStatus==POStatus.STATUS_OK){
                    Tuple packRes = (Tuple)res.result;
                    
                    if(rp.isEmpty()){
                        oc.collect(null, packRes);
                        return false;
                    }
                    for (int i = 0; i < roots.length; i++) {
                        roots[i].attachInput(packRes);
                    }
                    runPipeline(leaf);
                    
                }
                
                if(res.returnStatus==POStatus.STATUS_NULL) {
                    return false;
                }
                
                if(res.returnStatus==POStatus.STATUS_ERR){
                    int errCode = 2093;
                    String msg = "Encountered error in package operator while processing group.";
                    throw new ExecException(msg, errCode, PigException.BUG);
                }
                
                if(res.returnStatus==POStatus.STATUS_EOP) {
                    return true;
                }
                    
                return false;
            } catch (ExecException e) {
                throw e;
            }
        }
        
        /**
         * @param leaf
         * @throws ExecException 
         * @throws IOException 
         */
        protected void runPipeline(PhysicalOperator leaf) throws ExecException, IOException {
            while(true)
            {
                Result redRes = leaf.getNext(DUMMYTUPLE);
                if(redRes.returnStatus==POStatus.STATUS_OK){
                    outputCollector.collect(null, (Tuple)redRes.result);
                    continue;
                }
                
                if(redRes.returnStatus==POStatus.STATUS_EOP) {
                    return;
                }
                
                if(redRes.returnStatus==POStatus.STATUS_NULL)
                    continue;
                
                if(redRes.returnStatus==POStatus.STATUS_ERR){
                    // remember that we had an issue so that in 
                    // close() we can do the right thing
                    errorInReduce   = true;
                    // if there is an errmessage use it
                    String msg;
                    if(redRes.result != null) {
                        msg = "Received Error while " +
                        "processing the reduce plan: " + redRes.result;
                    } else {
                        msg = "Received Error while " +
                        "processing the reduce plan.";
                    }
                    int errCode = 2090;
                    throw new ExecException(msg, errCode, PigException.BUG);
                }
            }

        
        }
        
        /**
         * Will be called once all the intermediate keys and values are
         * processed. So right place to stop the reporter thread.
         */
        @Override
        public void close() throws IOException {
            super.close();
            
            if(errorInReduce) {
                // there was an error in reduce - just return
                return;
            }
            
            if(PigMapReduce.sJobConf.get("pig.stream.in.reduce", "false").equals("true")) {
                // If there is a stream in the pipeline we could 
                // potentially have more to process - so lets
                // set the flag stating that all map input has been sent
                // already and then lets run the pipeline one more time
                // This will result in nothing happening in the case
                // where there is no stream in the pipeline
                rp.endOfAllInput = true;
                try {
                    runPipeline(leaf);
                } catch (ExecException e) {
                     throw e;
                }
            }

            for (POStore store: stores) {
                if (!initialized) {
                    MapReducePOStoreImpl impl 
                        = new MapReducePOStoreImpl(PigMapReduce.sJobConf);
                    store.setStoreImpl(impl);
                    store.setUp();
                }
                store.tearDown();
            }
                        
            //Calling EvalFunc.finish()
            UDFFinishVisitor finisher = new UDFFinishVisitor(rp, new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(rp));
            try {
                finisher.visit();
            } catch (VisitorException e) {
                throw new IOException("Error trying to finish UDFs",e);
            }
            
            PhysicalOperator.setReporter(null);
            initialized = false;
        }
    }
    
    /**
     * This "specialized" reduce class is ONLY to be used in pig queries with
     * order by a udf. A UDF used for comparison in the order by expects
     * to be handed tuples. Hence a specialized map class (PigMapReduce.MapWithComparator)
     * ensures that the "key" used in the order by is wrapped into a tuple (if it 
     * isn't already a tuple). This reduce class unwraps this tuple in the case where
     * the map had wrapped into a tuple and handes the "unwrapped" key to the POPackage
     * for processing
     */
    public static class ReduceWithComparator extends PigMapReduce.Reduce {
        
        private byte keyType;
        
        /**
         * Configures the Reduce plan, the POPackage operator
         * and the reporter thread
         */
        @Override
        public void configure(JobConf jConf) {
            super.configure(jConf);
            keyType = pack.getKeyType();
        }

        /**
         * The reduce function which packages the key and List&lt;Tuple&gt;
         * into key, Bag&lt;Tuple&gt; after converting Hadoop type key into Pig type.
         * The package result is either collected as is, if the reduce plan is
         * empty or after passing through the reduce plan.
         */
        public void reduce(PigNullableWritable key,
                Iterator<NullableTuple> tupIter,
                OutputCollector<PigNullableWritable, Writable> oc,
                Reporter reporter) throws IOException {
            
            if (!initialized) {
                initialized = true;
                
                // cache the collector for use in runPipeline()
                // which could additionally be called from close()
                this.outputCollector = oc;
                pigReporter.setRep(reporter);
                PhysicalOperator.setReporter(pigReporter);

                boolean aggregateWarning = "true".equalsIgnoreCase(pigContext.getProperties().getProperty("aggregate.warning"));
                
                PigHadoopLogger pigHadoopLogger = PigHadoopLogger.getInstance();
                pigHadoopLogger.setAggregate(aggregateWarning);
                pigHadoopLogger.setReporter(reporter);
                PhysicalOperator.setPigLogger(pigHadoopLogger);
                
                for (POStore store: stores) {
                    MapReducePOStoreImpl impl 
                        = new MapReducePOStoreImpl(PigMapReduce.sJobConf);
                    impl.setReporter(reporter);
                    store.setStoreImpl(impl);
                    store.setUp();
                }
            }
            
            // If the keyType is not a tuple, the MapWithComparator.collect()
            // would have wrapped the key into a tuple so that the 
            // comparison UDF used in the order by can process it.
            // We need to unwrap the key out of the tuple and hand it
            // to the POPackage for processing
            if(keyType != DataType.TUPLE) {
                Tuple t = (Tuple)(key.getValueAsPigType());
                try {
                    key = HDataType.getWritableComparableTypes(t.get(0), keyType);
                } catch (ExecException e) {
                    throw e;
                }
            }
            
            pack.attachInput(key, tupIter);
            
            try {
                Result res = pack.getNext(DUMMYTUPLE);
                if(res.returnStatus==POStatus.STATUS_OK){
                    Tuple packRes = (Tuple)res.result;
                    
                    if(rp.isEmpty()){
                        oc.collect(null, packRes);
                        return;
                    }
                    
                    rp.attachInput(packRes);

                    List<PhysicalOperator> leaves = rp.getLeaves();
                    
                    PhysicalOperator leaf = leaves.get(0);
                    runPipeline(leaf);
                    
                }
                
                if(res.returnStatus==POStatus.STATUS_NULL) {
                    return;
                }
                
                if(res.returnStatus==POStatus.STATUS_ERR){
                    int errCode = 2093;
                    String msg = "Encountered error in package operator while processing group.";
                    throw new ExecException(msg, errCode, PigException.BUG);
                }
                    
                
            } catch (ExecException e) {
                throw e;
            }
        }

    }
    
}
