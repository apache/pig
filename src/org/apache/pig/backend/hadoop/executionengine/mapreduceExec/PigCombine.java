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
package org.apache.pig.backend.hadoop.executionengine.mapreduceExec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Datum;
import org.apache.pig.data.IndexedTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.util.ObjectSerializer;


public class PigCombine implements 
                Reducer<Tuple, IndexedTuple, Tuple, IndexedTuple>  {

    private final Log log = LogFactory.getLog(getClass());
    
    private JobConf         job;
    private CombineDataOutputCollector finalout;
    private DataCollector   evalPipe;
    private int             index;
    private int             inputCount;
    private DataBag         bags[];
    private PigContext pigContext;
    private EvalSpec esp;

    public void reduce(Tuple key, Iterator<IndexedTuple> values, 
                        OutputCollector<Tuple, IndexedTuple> output, Reporter reporter)
            throws IOException {

        try {
            if (evalPipe == null) {
                finalout = new CombineDataOutputCollector(output);
                evalPipe = esp.setupPipe(null, finalout);
                //throw new RuntimeException("combine spec: " + evalSpec + " combine pipe: " + esp.toString());
                
                bags = new DataBag[inputCount];
                for (int i = 0; i < inputCount; i++) {
                    bags[i] = BagFactory.getInstance().newDefaultBag();
                }
            }

            if (PigInputFormat.getActiveSplit() == null) {
            } else {
                index = PigInputFormat.getActiveSplit().getIndex();
            }

            Datum groupName = key.getField(0);
            finalout.group = key;
            finalout.index = index;
            
            Tuple t = new Tuple(1 + inputCount);
            t.setField(0, groupName);
            for (int i = 1; i < 1 + inputCount; i++) {
                bags[i - 1].clear();
                t.setField(i, bags[i - 1]);
            }

            while (values.hasNext()) {
                IndexedTuple it = values.next();
                t.getBagField(it.index + 1).add(it.toTuple());
            }
            for (int i = 0; i < inputCount; i++) {  // XXX: shouldn't we only do this if INNER flag is set?
                if (t.getBagField(1 + i).size() == 0)
                    return;
            }
//          throw new RuntimeException("combine input: " + t.toString());
            evalPipe.add(t);
            // evalPipe.add(null); // EOF marker
        } catch (Throwable tr) {
            log.error(tr);
            RuntimeException exp = new RuntimeException(tr.getMessage());
            exp.setStackTrace(tr.getStackTrace());
            throw exp;
        }
    }

    /**
     * Just save off the PigJobConf for later use.
     */
    public void configure(JobConf job) {
        this.job = job;
        try {
            this.pigContext = (PigContext) ObjectSerializer.deserialize(job.get("pig.pigContext"));
            this.inputCount = ((ArrayList<FileSpec>)ObjectSerializer.deserialize(job.get("pig.inputs"))).size();
            String evalSpec = job.get("pig.combineFunc", "");
            this.esp = (EvalSpec)ObjectSerializer.deserialize(evalSpec);
            if(esp != null) {
              esp.instantiateFunc(pigContext);
            }
        } catch (IOException e) {
            throw new RuntimeException("unable to deserialize data", e);
        }
    }

    /**
     * Nothing happens here.
     */
    public void close() throws IOException {
    }

    private static class CombineDataOutputCollector 
                        extends DataCollector {
        OutputCollector<Tuple, IndexedTuple> oc    = null;
        Tuple           group = null;
        int             index = -1;

        public CombineDataOutputCollector(OutputCollector oc) {
            super(null);
            this.oc = oc;
        }

        @Override
        public void add(Datum d){
            if (d == null) return;  // EOF marker from eval pipeline; ignore
            try{
                // oc.collect(group, new IndexedTuple(((Tuple)d).getTupleField(0),index));
                oc.collect(group, new IndexedTuple(((Tuple)d),index));
            }catch (IOException e){
                throw new RuntimeException(e);
            }
        }
    }

}
