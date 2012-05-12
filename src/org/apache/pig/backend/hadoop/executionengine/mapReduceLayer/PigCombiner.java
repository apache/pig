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
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.PropertyConfigurator;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POJoinPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.tools.pigstats.PigStatusReporter;

public class PigCombiner {

    public static JobContext sJobContext = null;
    
    public static class Combine 
            extends Reducer<PigNullableWritable, NullableTuple, PigNullableWritable, Writable> {
        
        private final Log log = LogFactory.getLog(getClass());

        private final static Tuple DUMMYTUPLE = null;
        
        private byte keyType;
        
        //The reduce plan
        private PhysicalPlan cp;
        
        //The POPackage operator which is the
        //root of every Map Reduce plan is
        //obtained through the job conf. The portion
        //remaining after its removal is the reduce
        //plan
        private POPackage pack;
        
        ProgressableReporter pigReporter;
        
        PhysicalOperator[] roots;
        PhysicalOperator leaf;
        
        PigContext pigContext = null;
        private volatile boolean initialized = false;
        
        /**
         * Configures the Reduce plan, the POPackage operator
         * and the reporter thread
         */
        @SuppressWarnings("unchecked")
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            sJobContext = context;
            Configuration jConf = context.getConfiguration();
            try {
                PigContext.setPackageImportList((ArrayList<String>)ObjectSerializer.deserialize(jConf.get("udf.import.list")));
                pigContext = (PigContext)ObjectSerializer.deserialize(jConf.get("pig.pigContext"));
                if (pigContext.getLog4jProperties()!=null)
                    PropertyConfigurator.configure(pigContext.getLog4jProperties());
                
                cp = (PhysicalPlan) ObjectSerializer.deserialize(jConf
                        .get("pig.combinePlan"));
                pack = (POPackage)ObjectSerializer.deserialize(jConf.get("pig.combine.package"));
                // To be removed
                if(cp.isEmpty())
                    log.debug("Combine Plan empty!");
                else{
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    cp.explain(baos);
                    log.debug(baos.toString());
                }
                
                keyType = ((byte[])ObjectSerializer.deserialize(jConf.get("pig.map.keytype")))[0];
                // till here
                
                pigReporter = new ProgressableReporter();
                if(!(cp.isEmpty())) {
                    roots = cp.getRoots().toArray(new PhysicalOperator[1]);
                    leaf = cp.getLeaves().get(0);
                }
            } catch (IOException ioe) {
                String msg = "Problem while configuring combiner's reduce plan.";
                throw new RuntimeException(msg, ioe);
            }
            log.info("Aliases being processed per job phase (AliasName[line,offset]): " + jConf.get("pig.alias.location"));
        }
        
        /**
         * The reduce function which packages the key and List &lt;Tuple&gt;
         * into key, Bag&lt;Tuple&gt; after converting Hadoop type key into Pig type.
         * The package result is either collected as is, if the reduce plan is
         * empty or after passing through the reduce plan.
         */
        @Override
        protected void reduce(PigNullableWritable key, Iterable<NullableTuple> tupIter, Context context) 
                throws IOException, InterruptedException {
            if(!initialized) {
                initialized = true;
                pigReporter.setRep(context);
                PhysicalOperator.setReporter(pigReporter);

                boolean aggregateWarning = "true".equalsIgnoreCase(pigContext.getProperties().getProperty("aggregate.warning"));

                PigHadoopLogger pigHadoopLogger = PigHadoopLogger.getInstance();
                pigHadoopLogger.setAggregate(aggregateWarning);
                PigStatusReporter.setContext(context);
                pigHadoopLogger.setReporter(PigStatusReporter.getInstance());

                PhysicalOperator.setPigLogger(pigHadoopLogger);
            }
            
            // In the case we optimize, we combine
            // POPackage and POForeach - so we could get many
            // tuples out of the getnext() call of POJoinPackage
            // In this case, we process till we see EOP from 
            // POJoinPacakage.getNext()
            if (pack instanceof POJoinPackage)
            {
                pack.attachInput(key, tupIter.iterator());
                while (true)
                {
                    if (processOnePackageOutput(context))
                        break;
                }
            }
            else {
                // not optimized, so package will
                // give only one tuple out for the key
                pack.attachInput(key, tupIter.iterator());
                processOnePackageOutput(context);
            }
            
        }
        
        // return: false-more output
        //         true- end of processing
        public boolean processOnePackageOutput(Context oc) throws IOException, InterruptedException {
            try {
                Result res = pack.getNext(DUMMYTUPLE);
                if(res.returnStatus==POStatus.STATUS_OK){
                    Tuple packRes = (Tuple)res.result;
                    
                    if(cp.isEmpty()){
                        oc.write(null, packRes);
                        return false;
                    }
                    
                    for (int i = 0; i < roots.length; i++) {
                        roots[i].attachInput(packRes);
                    }
                    while(true){
                        Result redRes = leaf.getNext(DUMMYTUPLE);
                        
                        if(redRes.returnStatus==POStatus.STATUS_OK){
                            Tuple tuple = (Tuple)redRes.result;
                            Byte index = (Byte)tuple.get(0);
                            PigNullableWritable outKey =
                                HDataType.getWritableComparableTypes(tuple.get(1), this.keyType);
                            NullableTuple val =
                                new NullableTuple((Tuple)tuple.get(2));
                            // Both the key and the value need the index.  The key needs it so
                            // that it can be sorted on the index in addition to the key
                            // value.  The value needs it so that POPackage can properly
                            // assign the tuple to its slot in the projection.
                            outKey.setIndex(index);
                            val.setIndex(index);

                            oc.write(outKey, val);

                            continue;
                        }
                        
                        if(redRes.returnStatus==POStatus.STATUS_EOP) {
                            break;
                        }
                        
                        if(redRes.returnStatus==POStatus.STATUS_NULL) {
                            continue;
                        }
                        
                        if(redRes.returnStatus==POStatus.STATUS_ERR){
                            int errCode = 2090;
                            String msg = "Received Error while " +
                            "processing the combine plan.";
                            if(redRes.result != null) {
                                msg += redRes.result;
                            }
                            throw new ExecException(msg, errCode, PigException.BUG);
                        }
                    }
                }
                
                if(res.returnStatus==POStatus.STATUS_NULL) {
                    return false;
                }
                
                if(res.returnStatus==POStatus.STATUS_ERR){
                    int errCode = 2091;
                    String msg = "Packaging error while processing group.";
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
         * Will be called once all the intermediate keys and values are
         * processed.
         * cleanup references to the PhysicalPlan
         */
        @Override        
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            leaf = null;
            pack = null;
            pigReporter = null;
            pigContext = null;
            roots = null;
            cp = null;
        }

        /**
         * @return the keyType
         */
        public byte getKeyType() {
            return keyType;
        }

        /**
         * @param keyType the keyType to set
         */
        public void setKeyType(byte keyType) {
            this.keyType = keyType;
        }
    }
    
}
