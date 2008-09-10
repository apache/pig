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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.data.DataType;
import org.apache.pig.data.IndexedTuple;
import org.apache.pig.data.TargetedTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.SpillableMemoryManager;

/**
 * This class is the static Mapper &amp; Reducer classes that
 * are used by Pig to execute Pig Map Reduce jobs. Since
 * there is a reduce phase, the leaf is bound to be a 
 * POLocalRearrange. So the map phase has to separate the
 * key and indexed tuple and collect it into the output
 * collector.
 * 
 * The shuffle and sort phase sorts these key &amp; indexed tuples
 * and creates key, List&lt;IndexedTuple&gt; and passes the key and
 * iterator to the list. The deserialized POPackage operator
 * is used to package the key, List&lt;IndexedTuple&gt; into pigKey, 
 * Bag&lt;Tuple&gt; where pigKey is of the appropriate pig type and
 * then the result of the package is attached to the reduce
 * plan which is executed if its not empty. Either the result 
 * of the reduce plan or the package res is collected into
 * the output collector. 
 *
 */
public class PigMapReduce {

    public static JobConf sJobConf = null;
    
    public static class Map extends PigMapBase implements
            Mapper<Text, TargetedTuple, WritableComparable, Writable> {

        @Override
        public void collect(OutputCollector<WritableComparable, Writable> oc, Tuple tuple) throws ExecException, IOException {
            Object key = tuple.get(0);
            IndexedTuple it = (IndexedTuple)tuple.get(1);
            WritableComparable wcKey = HDataType.getWritableComparableTypes(key, keyType);
            oc.collect(wcKey, it);
        }
    }
    
    public static class MapWithComparator extends PigMapBase implements
            Mapper<Text, TargetedTuple, WritableComparable, Writable> {

        @Override
        public void collect(OutputCollector<WritableComparable, Writable> oc,
                Tuple tuple) throws ExecException, IOException {
            Object key = tuple.get(0);
            Tuple keyTuple = tf.newTuple(1);
            keyTuple.set(0, key);
            IndexedTuple it = (IndexedTuple) tuple.get(1);
            oc.collect(keyTuple, it);
        }
    }

    public static class Reduce extends MapReduceBase
            implements
            Reducer<WritableComparable, IndexedTuple, WritableComparable, Writable> {
        private final Log log = LogFactory.getLog(getClass());
        
        //The reduce plan
        private PhysicalPlan rp;
        
        //The POPackage operator which is the
        //root of every Map Reduce plan is
        //obtained through the job conf. The portion
        //remaining after its removal is the reduce
        //plan
        private POPackage pack;
        
        ProgressableReporter pigReporter;

        private OutputCollector<WritableComparable, Writable> outputCollector;

        private boolean errorInReduce = false;
        
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
            } catch (IOException e) {
                log.error(e.getMessage() + "was caused by:");
                log.error(e.getCause().getMessage());
            }
        }
        
        /**
         * The reduce function which packages the key and List<IndexedTuple>
         * into key, Bag<Tuple> after converting Hadoop type key into Pig type.
         * The package result is either collected as is, if the reduce plan is
         * empty or after passing through the reduce plan.
         */
        public void reduce(WritableComparable key,
                Iterator<IndexedTuple> indInp,
                OutputCollector<WritableComparable, Writable> oc,
                Reporter reporter) throws IOException {
            
            // cache the collector for use in runPipeline()
            // which could additionally be called from close()
            this.outputCollector = oc;
            pigReporter.setRep(reporter);
            
            Object k = HDataType.convertToPigType(key);
            pack.attachInput(k, indInp);
            
            try {
                Tuple t=null;
                Result res = pack.getNext(t);
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
                    IOException ioe = new IOException("Packaging error while processing group");
                    throw ioe;
                }
                    
                
            } catch (ExecException e) {
                IOException ioe = new IOException(e.getMessage());
                ioe.initCause(e.getCause());
                throw ioe;
            }
        }
        
        /**
         * @param leaf
         * @throws ExecException 
         * @throws IOException 
         */
        private void runPipeline(PhysicalOperator leaf) throws ExecException, IOException {
            while(true)
            {
                Tuple dummyTuple = null;  
                Result redRes = leaf.getNext(dummyTuple);
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
                    String errMsg;
                    if(redRes.result != null) {
                        errMsg = "Received Error while " +
                        "processing the reduce plan: " + redRes.result;
                    } else {
                        errMsg = "Received Error while " +
                        "processing the reduce plan.";
                    }
                    
                    IOException ioe = new IOException(errMsg);
                    throw ioe;
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
            /*if(runnableReporter!=null)
                runnableReporter.setDone(true);*/
            PhysicalOperator.setReporter(null);
            
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
                List<PhysicalOperator> leaves = rp.getLeaves();
                PhysicalOperator leaf = leaves.get(0);
                try {
                    runPipeline(leaf);
                } catch (ExecException e) {
                     IOException ioe = new IOException("Error running pipeline in close() of reduce");
                     ioe.initCause(e);
                     throw ioe;
                }
            }
        }
    }
    
}
