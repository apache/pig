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
package org.apache.pig.impl.mapreduceExec;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Iterator;

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
import org.apache.pig.impl.mapreduceExec.PigOutputFormat.PigRecordWriter;
import org.apache.pig.impl.physicalLayer.SplitSpec;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.tools.timer.PerformanceTimerFactory;


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
public class PigMapReduce implements MapRunnable, Reducer {

    public static Reporter reporter = null;

    JobConf                           job;
    private DataCollector             evalPipe;
    private OutputCollector           oc;
    private EvalSpec                  group;
    private PigRecordWriter           pigWriter;
    private int                       index;
    private int                       inputCount;
    private boolean                   isInner[];
    private File                      tmpdir;
    private static PigContext pigContext = null;
    ArrayList<PigRecordWriter> sideFileWriters = new ArrayList<PigRecordWriter>();

    /**
     * This function is called in MapTask by Hadoop as the Mapper.run() method. We basically pull
     * the tuples from our PigRecordReader (see ugly ThreadLocal hack), pipe the tuples through the
     * function pipeline and then close the writer.
     */
    public void run(RecordReader input, OutputCollector output, Reporter reporter) throws IOException {
        PigMapReduce.reporter = reporter;

        oc = output;
        tmpdir = new File(job.get("mapred.task.id"));
        tmpdir.mkdirs();
        BagFactory.init(tmpdir);

        setupMapPipe(reporter);

        // allocate key & value instances that are re-used for all entries
        WritableComparable key = input.createKey();
        Writable value = input.createValue();
        while (input.next(key, value)) {
            evalPipe.add((Tuple) value);
        }
        evalPipe.finishPipe();  // EOF marker
        evalPipe= null;
        if (pigWriter != null) {
            pigWriter.close(reporter);
        }
    }

    public void reduce(WritableComparable key, Iterator values, OutputCollector output, Reporter reporter)
            throws IOException {

        PigMapReduce.reporter = reporter;
        
        try {
            tmpdir = new File(job.get("mapred.task.id"));
            tmpdir.mkdirs();

            BagFactory.init(tmpdir);

            oc = output;
            if (evalPipe == null) {
                setupReducePipe();
            }

            DataBag[] bags = new DataBag[inputCount];
            Datum groupName = ((Tuple) key).getField(0);
            Tuple t = new Tuple(1 + inputCount);
            t.setField(0, groupName);
            for (int i = 1; i < 1 + inputCount; i++) {
                bags[i - 1] = BagFactory.getInstance().getNewBag();
                t.setField(i, bags[i - 1]);
            }

            while (values.hasNext()) {
                IndexedTuple it = (IndexedTuple) values.next();
                t.getBagField(it.index + 1).add(it.toTuple());
            }
            
            for (int i = 0; i < inputCount; i++) {
                if (isInner[i] && t.getBagField(1 + i).isEmpty())
                    return;
            }
            
            evalPipe.add(t);
        } catch (Throwable tr) {
            tr.printStackTrace();
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
    }

    /**
     * Nothing happens here.
     */
    public void close() throws IOException {
    	if (evalPipe!=null)
    		evalPipe.finishPipe();
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
    
    private void setupMapPipe(Reporter reporter) throws IOException {
        PigSplit split = PigInputFormat.PigRecordReader.getPigRecordReader().getPigFileSplit();
        pigContext = (PigContext)ObjectSerializer.deserialize(job.get("pig.pigContext"));
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
	            oc = new OutputCollector() {
	                public void collect(WritableComparable key, Writable value) throws IOException {
	                    pigWriter.write(key, value);
	                }
	            };
            }else{
	            oc = getSplitCollector(splitSpec);	
            }
            evalPipe = evalSpec.setupPipe(new MapDataOutputCollector());
        } else {
            group = groupSpec;
            DataCollector groupInput = group.setupPipe(new MapDataOutputCollector());
            evalPipe = evalSpec.setupPipe(groupInput);
        }
        
    }
    
    private OutputCollector getSplitCollector(final SplitSpec splitSpec) throws IOException{
    	 
    	PigOutputFormat outputFormat = (PigOutputFormat)job.getOutputFormat(); 
		
    	for (String name: splitSpec.tempFiles){
    		sideFileWriters.add( outputFormat.getRecordWriter(FileSystem.get(job), job, new Path(name), "split-" + getTaskId(), reporter));
    	}
    	return new OutputCollector(){
    		public void collect(WritableComparable key, Writable value) throws IOException {
    			Tuple t = (Tuple) value;
    			ArrayList<Cond> conditions = splitSpec.conditions;
    			for (int i=0; i< conditions.size(); i++){
    				Cond cond = conditions.get(i);
    				if (cond.eval(t)){
    					//System.out.println("Writing " + t + " to condition " + cond);
    					sideFileWriters.get(i).write(null, t);
    				}
    			}
    			
    		}
    	};
    }
    
    private void setupReducePipe() throws IOException {
        pigContext = (PigContext)ObjectSerializer.deserialize(job.get("pig.pigContext"));
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
    
        evalPipe = evalSpec.setupPipe(new ReduceDataOutputCollector());
        
        inputCount = ((ArrayList<FileSpec>)ObjectSerializer.deserialize(job.get("pig.inputs"))).size();

    }
    
    public void closeSideFiles(){
    	for (PigRecordWriter writer: sideFileWriters){
    		try{
    			writer.close(reporter);
    		}catch(IOException e){
    			e.printStackTrace();
    		}
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
        	closeSideFiles();
        	if (group != null)
        		group.finish();
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
