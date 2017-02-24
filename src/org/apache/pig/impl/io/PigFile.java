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
package org.apache.pig.impl.io;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;


public class PigFile {
    private String file = null;
    boolean append = false;

    public PigFile(String filename, boolean append) {
        file = filename;
        this.append = append;
    }
    
    public PigFile(String filename){
        file = filename;
    }
    
    public DataBag load(LoadFunc lfunc, PigContext pigContext) throws IOException {
        DataBag content = BagFactory.getInstance().newDefaultBag();
        ReadToEndLoader loader = new ReadToEndLoader(lfunc, 
                ConfigurationUtil.toConfiguration(pigContext.getProperties()), file, 0);
        Tuple f = null;
        while ((f = loader.getNext()) != null) {
            content.add(f);
        }
        return content;
    }

    
    public void store(DataBag data, FuncSpec storeFuncSpec, PigContext pigContext) throws IOException {
        Configuration conf = ConfigurationUtil.toConfiguration(pigContext.getProperties());
        // create a simulated JobContext
        JobContext jc = HadoopShims.createJobContext(conf, new JobID());
        StoreFuncInterface sfunc = (StoreFuncInterface)PigContext.instantiateFuncFromSpec(
                storeFuncSpec);
        OutputFormat<?,?> of = sfunc.getOutputFormat();
        
        POStore store = new POStore(new OperatorKey());
        store.setSFile(new FileSpec(file, storeFuncSpec));
        PigOutputFormat.setLocation(jc, store);
        OutputCommitter oc;
        // create a simulated TaskAttemptContext
        
        TaskAttemptContext tac = HadoopShims.createTaskAttemptContext(conf, HadoopShims.getNewTaskAttemptID());
        PigOutputFormat.setLocation(tac, store);
        RecordWriter<?,?> rw ;
        try {
            of.checkOutputSpecs(jc);
            oc = of.getOutputCommitter(tac);
            oc.setupJob(jc);
            oc.setupTask(tac);
            rw = of.getRecordWriter(tac);
            sfunc.prepareToWrite(rw);
        
            for (Iterator<Tuple> it = data.iterator(); it.hasNext();) {
                Tuple row = it.next();
                sfunc.putNext(row);
            }
            rw.close(tac);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        if(oc.needsTaskCommit(tac)) {
            oc.commitTask(tac);
        }
        HadoopShims.commitOrCleanup(oc, jc);
    }

    @Override
    public String toString() {
        return "PigFile: file: " + this.file + ", append: " + this.append;
    }
}
