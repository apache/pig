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
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.pig.backend.hadoop.executionengine.SplitSpec;
import org.apache.pig.backend.hadoop.executionengine.mapreduceExec.PigOutputFormat.PigRecordWriter;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Datum;
import org.apache.pig.data.IndexedTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.eval.StarSpec;
import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.eval.cond.Cond;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.SpillableMemoryManager;


/**
 * This class is a wrapper of sorts for Pig Map/Reduce jobs. Both the Mapper and the Reducer are
 * implemented by this class. The methods of this class are driven by job configuration variables:
 * <dl>
 * <dt>pig.inputs</dt>
 * <dd>A semi-colon separated list of inputs. If an input uses a special parser, it will be
 * specified by adding a colon and the name of the parser to the input. For example:
 * /tmp/names.txt;/tmp/logs.dat:com.yahoo.research.pig.parser.LogParser will parse /tmp/names.txt
 * using the default parser and /tmp/logs.dat using com.yahoo.research.pig.parser.LogParser.</dd>
 * <dt>pig.mapFuncs</dt>
 * <dd>A semi-colon separated list of functions-specification to be applied to the inputs in the
 * Map phase. This list must have the same number of items as pig.inputs because the each
 * functions-spectification will be matched to the corresponding input.</dd>
 * <dt>pig.groupFuncs</dt>
 * <dd>A semi-colon separated list of group functions. As with pig.mapFuncs, this list must have
 * the same number of items as pig.inputs because the each group function will be matched to the
 * corresponding input.</dd>
 * <dt>pig.reduceFuncs</dt>
 * <dd>functions-specification to be applied to the tuples passed into the Reduce phase.</dd>
 * </dl>
 * 
 * @author breed
 */
public class PigMapReduce implements MapRunnable<WritableComparable, Tuple, WritableComparable, Writable>, 
                                    Reducer<Tuple, IndexedTuple, WritableComparable, Writable> {

    private final Log log = LogFactory.getLog(getClass());
    
    public static Reporter reporter = null;

    JobConf                           job;
    private Properties                properties = new Properties();
    private DataCollector             evalPipe;
    private OutputCollector           oc;
    private EvalSpec                  group;
    private PigRecordWriter           pigWriter;
    private int                       index;
    private int                       inputCount;
    private boolean                   isInner[];
    private static PigContext pigContext = null;
    ArrayList<PigRecordWriter> sideFileWriters = new ArrayList<PigRecordWriter>();

    /**
     * This function is called in MapTask by Hadoop as the Mapper.run() method. We basically pull
     * the tuples from our PigRecordReader (see ugly ThreadLocal hack), pipe the tuples through the
     * function pipeline and then close the writer.
     */
public void run(RecordReader<WritableComparable, Tuple> input, 
                OutputCollector<WritableComparable, Writable> output, Reporter reporter) throws IOException {
        PigMapReduce.reporter = reporter;

        oc = output;

        try {
            setupMapPipe(properties, reporter);

            // allocate key & value instances that are re-used for all entries
            WritableComparable key = input.createKey();
            Tuple value = input.createValue();
            while (input.next(key, value)) {
                evalPipe.add(value);
            }
        } finally {
            try {
                if (evalPipe != null) {
                    evalPipe.finishPipe();  // EOF marker
                    evalPipe = null;
                }
            } finally {
                // Close the writer
                if (pigWriter != null) {
                    pigWriter.close(reporter);
                    pigWriter = null;
                }
            }
        }
    }

    public void reduce(Tuple key, Iterator<IndexedTuple> values, OutputCollector<WritableComparable, Writable>  output, Reporter reporter)
            throws IOException {

        PigMapReduce.reporter = reporter;
        
        try {
            oc = output;
            if (evalPipe == null) {
                setupReducePipe(properties);
            }

            DataBag[] bags = new DataBag[inputCount];
            Datum groupName = key.getField(0);
            Tuple t = new Tuple(1 + inputCount);
            t.setField(0, groupName);
            for (int i = 1; i < 1 + inputCount; i++) {
                bags[i - 1] = BagFactory.getInstance().newDefaultBag();
                t.setField(i, bags[i - 1]);
            }

            while (values.hasNext()) {
                IndexedTuple it = values.next();
                t.getBagField(it.index + 1).add(it.toTuple());
            }
            
            for (int i = 0; i < inputCount; i++) {
                if (isInner[i] && t.getBagField(1 + i).size() == 0)
                    return;
            }
            
            evalPipe.add(t);
        } catch (Throwable tr) {
            log.error(tr);
            
            // Convert to IOException to ensure Hadoop handles it correctly ...
            IOException ioe = new IOException(tr.getMessage());
            ioe.setStackTrace(tr.getStackTrace());
            throw ioe;
        }
    }

    public void configure(JobConf jobConf) {
        job = jobConf;
        
        // Set up the pigContext
        try {
            pigContext = 
                (PigContext)ObjectSerializer.deserialize(job.get("pig.pigContext"));
        } catch (IOException ioe) {
            log.fatal("Failed to deserialize PigContext with: " + ioe);
            throw new RuntimeException(ioe);
        }
        pigContext.setJobConf(job);
        
        // Get properties from the JobConf and save it in the 
        // <code>properties</code> so that it can be used in the Eval pipeline 
        properties.setProperty("pig.output.dir", 
                               jobConf.get("pig.output.dir", ""));
        properties.setProperty("pig.streaming.log.dir", 
                               jobConf.get("pig.streaming.log.dir", "_logs"));
        properties.setProperty("pig.streaming.task.id", 
                               jobConf.get("mapred.task.id"));
        properties.setProperty("pig.streaming.task.output.dir", 
                               jobConf.getOutputPath().toString());
        properties.setProperty("pig.spill.size.threshold", 
                                jobConf.get("pig.spill.size.threshold"));
        properties.setProperty("pig.spill.gc.activation.size", 
                                jobConf.get("pig.spill.gc.activation.size"));
 
        SpillableMemoryManager.configure(properties) ;
    }

    /**
     * Nothing happens here.
     */
    public void close() throws IOException {
        try {
        if (evalPipe!=null)
            evalPipe.finishPipe();
        } catch (Throwable t) {
            log.error(t);
            
            // Convert to IOException to ensure Hadoop handles it correctly ...
            IOException ioe = new IOException(t.getMessage());
            ioe.setStackTrace(t.getStackTrace());
            throw ioe;
        }
    }

    public static PigContext getPigContext() {
        if (pigContext!=null)
            return pigContext;
        try {
            InputStream is = PigMapReduce.class.getResourceAsStream("/pigContext");
            if (is == null) throw new RuntimeException("/pigContext not found!");
            return (PigContext)new ObjectInputStream(is).readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getTaskId(){
        String taskidParts[] = job.get("mapred.task.id").split("_"); // The format is
        // tip_job_m_partition_try
        return taskidParts[taskidParts.length - 2];
    }
    
    private void setupMapPipe(Properties properties, Reporter reporter) 
    throws IOException {
        SliceWrapper split = PigInputFormat.getActiveSplit();
        index = split.getIndex();
        EvalSpec evalSpec = split.getEvalSpec();
        
        EvalSpec groupSpec = split.getGroupbySpec();
        if (groupSpec == null) {

            String fileName = "map-" + getTaskId();
            
            SplitSpec splitSpec = (SplitSpec) ObjectSerializer.deserialize(job.get("pig.splitSpec", ""));
            if(splitSpec  != null) splitSpec.instantiateFunc(pigContext);
            
            if (splitSpec == null){
                pigWriter = (PigRecordWriter) job.getOutputFormat().getRecordWriter(FileSystem.get(job), job, fileName,
                        reporter);
                oc = new OutputCollector<WritableComparable, Tuple>() {
                    public void collect(WritableComparable key, Tuple value) throws IOException {
                        pigWriter.write(key, value);
                    }
                };
            }else{
                oc = getSplitCollector(splitSpec);    
            }
            evalPipe = evalSpec.setupPipe(properties, 
                                          new MapDataOutputCollector());
        } else {
            group = groupSpec;
            DataCollector groupInput = 
                group.setupPipe(properties, new MapDataOutputCollector());
            evalPipe = evalSpec.setupPipe(properties, groupInput);
        }
        
    }
    
    private OutputCollector getSplitCollector(final SplitSpec splitSpec) throws IOException{
         
        PigOutputFormat outputFormat = (PigOutputFormat)job.getOutputFormat(); 
        
        for (String name: splitSpec.tempFiles){
            sideFileWriters.add( outputFormat.getRecordWriter(FileSystem.get(job), job, new Path(name), "split-" + getTaskId(), reporter));
        }
        return new OutputCollector<WritableComparable, Tuple>(){
            public void collect(WritableComparable key, Tuple value) throws IOException {
                ArrayList<Cond> conditions = splitSpec.conditions;
                for (int i=0; i< conditions.size(); i++){
                    Cond cond = conditions.get(i);
                    if (cond.eval(value)){
                        //System.out.println("Writing " + t + " to condition " + cond);
                        sideFileWriters.get(i).write(null, value);
                    }
                }
                
            }
        };
    }
    
    private void setupReducePipe(Properties properties) throws IOException {
        EvalSpec evalSpec = (EvalSpec)ObjectSerializer.deserialize(job.get("pig.reduceFunc", ""));
        
        if (evalSpec == null) 
            evalSpec = new StarSpec();
        else
            evalSpec.instantiateFunc(pigContext);
        
        ArrayList<EvalSpec> groupSpecs = (ArrayList<EvalSpec>) ObjectSerializer.deserialize(job.get("pig.groupFuncs", ""));        
        
        isInner = new boolean[groupSpecs.size()];
        for (int i = 0; i < groupSpecs.size(); i++) {
            isInner[i] = groupSpecs.get(i).isInner();
            
        }

        SplitSpec splitSpec = (SplitSpec) ObjectSerializer.deserialize(job.get("pig.splitSpec", ""));        
        if (splitSpec != null){
            splitSpec.instantiateFunc(pigContext);
            oc = getSplitCollector(splitSpec);
        }
    
        evalPipe = evalSpec.setupPipe(properties, 
                                      new ReduceDataOutputCollector());
        
        inputCount = ((ArrayList<FileSpec>)ObjectSerializer.deserialize(job.get("pig.inputs"))).size();

    }
    
    public void closeSideFiles(){
        IOException ioe = null;
        for (PigRecordWriter writer: sideFileWriters){
            try{
                writer.close(reporter);
            }catch(IOException e){
                log.error(e);
                
                // Save the first IOException which occurred ...
                if (ioe == null) {
                    ioe = e;
                }
            }
        }
        
        if (ioe != null) {
            throw new RuntimeException(ioe);
        }
    }

    class MapDataOutputCollector extends DataCollector {
        
        public MapDataOutputCollector(){
            super(null);
        }
        
        @Override
        public void add(Datum d){
            try{
                if (group == null) {
                    oc.collect(null, (Tuple)d);
                } else {
                    Datum[] groupAndTuple = LOCogroup.getGroupAndTuple(d);
                    // wrap group label in a tuple, so it becomes writable.
                    oc.collect(new Tuple(groupAndTuple[0]), new IndexedTuple((Tuple)groupAndTuple[1], index));
                }
            }catch(IOException e){
                throw new RuntimeException(e);
            }
        }
        
        @Override
        public void finish(){
            try {
                closeSideFiles();
                
                if (group != null) {
                    group.finish();
                    group = null;
                }
            } catch (Exception e) {
                try {
                    if (group != null) {
                        group.finish();
                        group = null;
                    }
                } catch (Exception innerE) {
                    log.warn("Failed to cleanup groups with: " + innerE);
                }
                
                // Propagate the original exception
                throw new RuntimeException(e);
            }
        }
    }

    class ReduceDataOutputCollector extends DataCollector {
        
        public ReduceDataOutputCollector(){
            super(null);
        }
        
        @Override
        public void add(Datum d){    
            try{
                //System.out.println("Adding " + d + " to reduce output");
                oc.collect(null, (Tuple)d);
            }catch(IOException e){
                throw new RuntimeException(e);
            }
        }
        
        @Override
        protected void finish(){
            closeSideFiles();
        }
    }

}
