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
import org.apache.pig.data.DataType;
import org.apache.pig.data.IndexedTuple;
import org.apache.pig.data.TargetedTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.physicalLayer.topLevelOperators.POPackage;
import org.apache.pig.impl.physicalLayer.topLevelOperators.PhysicalOperator;
import org.apache.pig.impl.util.ObjectSerializer;

/**
 * This class is the static Mapper & Reducer classes that
 * are used by Pig to execute Pig Map Reduce jobs. Since
 * there is a reduce phase, the leaf is bound to be a 
 * POLocalRearrange. So the map phase has to separate the
 * key and indexed tuple and collect it into the output
 * collector.
 * 
 * The shuffle and sort phase sorts these key & indexed tuples
 * and creates key, List<IndexedTuple> and passes the key and
 * iterator to the list. The deserialized POPackage operator
 * is used to package the key, List<IndexedTuple> into pigKey, 
 * Bag<Tuple> where pigKey is of the appropriate pig type and
 * then the result of the package is attached to the reduce
 * plan which is executed if its not empty. Either the result 
 * of the reduce plan or the package res is collected into
 * the output collector. 
 *
 */
public class PigMapReduce {
    
    public static class Map extends PigMapBase implements
            Mapper<Text, TargetedTuple, WritableComparable, Writable> {

        @Override
        public void collect(OutputCollector<WritableComparable, Writable> oc, Tuple tuple) throws ExecException, IOException {
            Object key = tuple.get(0);
            IndexedTuple it = (IndexedTuple)tuple.get(1);
            WritableComparable wcKey = DataType.getWritableComparableTypes(key);
            oc.collect(wcKey, it);
        }
    }

    public static class Reduce extends MapReduceBase
            implements
            Reducer<WritableComparable, IndexedTuple, WritableComparable, Writable> {
        private final Log log = LogFactory.getLog(getClass());
        
        //The reduce plan
        private PhysicalPlan<PhysicalOperator> rp;
        
        //The POPackage operator which is the
        //root of every Map Reduce plan is
        //obtained through the job conf. The portion
        //remaining after its removal is the reduce
        //plan
        private POPackage pack;
        
        ProgressableReporter pigReporter;
        
        /**
         * Configures the Reduce plan, the POPackage operator
         * and the reporter thread
         */
        @Override
        public void configure(JobConf jConf) {
            super.configure(jConf);
            try {
                rp = (PhysicalPlan<PhysicalOperator>) ObjectSerializer.deserialize(jConf
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
            
            pigReporter.setRep(reporter);
            
            Object k = DataType.convertToPigType(key);
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
                    while(true){
                        Result redRes = leaf.getNext(t);
                        
                        if(redRes.returnStatus==POStatus.STATUS_OK){
                            oc.collect(null, (Tuple)redRes.result);
                            continue;
                        }
                        
                        if(redRes.returnStatus==POStatus.STATUS_EOP)
                            return;
                        
                        if(redRes.returnStatus==POStatus.STATUS_NULL)
                            continue;
                        
                        if(redRes.returnStatus==POStatus.STATUS_ERR){
                            IOException ioe = new IOException("Received Error while " +
                                    "processing the reduce plan.");
                            throw ioe;
                        }
                    }
                }
                
                if(res.returnStatus==POStatus.STATUS_NULL)
                    return;
                
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
         * Will be called once all the intermediate keys and values are
         * processed. So right place to stop the reporter thread.
         */
        @Override
        public void close() throws IOException {
            super.close();
            /*if(runnableReporter!=null)
                runnableReporter.setDone(true);*/
            PhysicalOperator.setReporter(null);
        }
    }
    
    /*interface MapOutputCollector<K extends WritableComparable, V extends Writable>
    extends OutputCollector<K, V> {

        public void close() throws IOException;

        public void flush() throws IOException;

    }

    static class DirectMapOutputCollector<K extends WritableComparable, V extends Writable>
            implements MapOutputCollector<K, V> {

        private RecordWriter<K, V> out = null;

        private Reporter reporter = null;

        @SuppressWarnings("unchecked")
        public DirectMapOutputCollector(JobConf job, Reporter reporter)
                throws IOException {
            this.reporter = reporter;
            String finalName = job.getOutputPath().toString();
            FileSystem fs = FileSystem.get(job);

            out = job.getOutputFormat().getRecordWriter(fs, job, finalName,
                    reporter);
        }

        public void close() throws IOException {
            if (this.out != null) {
                out.close(this.reporter);
            }

        }

        public void flush() throws IOException {
            // TODO Auto-generated method stub

        }

        public void collect(K key, V value) throws IOException {
            System.out.println(value.toString());
        }
    }
    
    public static void main(String[] args) throws IOException {
        Random r = new Random();
        PhysicalPlan<PhysicalOperator> rp = new PhysicalPlan<PhysicalOperator>();
        POForEach fe = GenPhyOp.topForEachOPWithPlan(1);
        rp.add(fe);
        PigMapReduce.Reduce red = new PigMapReduce.Reduce();
        POPackage pk = GenPhyOp.topPackageOp();
        pk.setKeyType(DataType.INTEGER);
        pk.setNumInps(1);
        boolean[] inner = {false}; 
        pk.setInner(inner);
        
        JobConf jConf = new JobConf();
        jConf.set("pig.reducePlan", ObjectSerializer.serialize(rp));
        jConf.set("pig.reduce.package",ObjectSerializer.serialize(pk));
        jConf.setOutputFormat(PigOutputFormat.class);
        jConf.setOutputPath(new Path("pigmrtst1"));
        red.configure(jConf);
        
        WritableComparable key = new IntWritable(1);
        List<IndexedTuple> itLst = new ArrayList<IndexedTuple>();
        for(int i=0;i<2;i++){
            Tuple t = TupleFactory.getInstance().newTuple();
            t.append(GenRandomData.genRandString(r));
            t.append(1);
            IndexedTuple it = new IndexedTuple(t,0);
            itLst.add(it);
        }
        red.reduce(key,itLst.iterator(),new DirectMapOutputCollector(jConf,reporter), reporter);
    }*/
}