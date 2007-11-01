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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.io.File;
import java.io.FileOutputStream;

import org.apache.log4j.Logger;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.IndexedTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.Datum;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.io.PigFile;
import org.apache.pig.impl.physicalLayer.POMapreduce;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.PigLogger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob; 


/**
 * This is that Main-Class for the Pig Jar file. It will setup a Pig jar file to run with the proper
 * libraries. It will also provide a basic shell if - or -e is used as the name of the Jar file.
 * 
 * @author breed
 * 
 */
public class MapReduceLauncher {
    
    public static long totalHadoopTimeSpent = 0;
    public static int numMRJobs;
    public static int mrJobNumber;
    
    public static void initQueryStatus(int numMRJobsIn) {
        numMRJobs = numMRJobsIn;
        mrJobNumber = 0;
    }
        
    static Random rand = new Random();

    /**
     * Submit a Pig job to hadoop.
     * 
     * @param mapFuncs
     *            a list of map functions to apply to the inputs. The cardinality of the list should
     *            be the same as input's cardinality.
     * @param groupFuncs
     *            a list of grouping functions to apply to the inputs. The cardinality of the list
     *            should be the same as input's cardinality.
     * @param reduceFunc
     *            the reduce function.
     * @param mapTasks
     *            the number of map tasks to use.
     * @param reduceTasks
     *            the number of reduce tasks to use.
     * @param input
     *            a list of inputs
     * @param output
     *            the path of the output.
     * @return an indicator of success or failure.
     * @throws IOException
     */
    public boolean launchPig(POMapreduce pom) throws IOException {
		Logger log = PigLogger.getLogger();
        JobConf conf = new JobConf(pom.pigContext.getConf());
        conf.setJobName(pom.pigContext.getJobName());
        boolean success = false;
        List<String> funcs = new ArrayList<String>();
        if (pom.toMap != null){
              	for (EvalSpec es: pom.toMap)
             		funcs.addAll(es.getFuncs());
        }
        if (pom.groupFuncs != null){
               	for(EvalSpec es: pom.groupFuncs)
               		funcs.addAll(es.getFuncs());
        }
        if (pom.toReduce != null)
        	funcs.addAll(pom.toReduce.getFuncs());
	// create jobs.jar locally and pass it to hadoop
	File submitJarFile = File.createTempFile("Job", ".jar");	
	try
	{
		FileOutputStream fos = new FileOutputStream(submitJarFile);
               	JarManager.createJar(fos, funcs, pom.pigContext);
               	System.out.println("Job jar size = " + submitJarFile.length());
		conf.setJar(submitJarFile.getPath());
        	String user = System.getProperty("user.name");
        	conf.setUser(user != null ? user : "Pigster");

            	if (pom.reduceParallelism != -1)
               		conf.setNumReduceTasks(pom.reduceParallelism);
            	if (pom.toMap != null)
                	conf.set("pig.mapFuncs", ObjectSerializer.serialize(pom.toMap));
            	if (pom.toCombine != null)
                	conf.set("pig.combineFunc", ObjectSerializer.serialize(pom.toCombine));
            	if (pom.groupFuncs != null)
                	conf.set("pig.groupFuncs", ObjectSerializer.serialize(pom.groupFuncs));
            	if (pom.toReduce != null)
                	conf.set("pig.reduceFunc", ObjectSerializer.serialize(pom.toReduce));
            	if (pom.toSplit != null)
            		conf.set("pig.splitSpec", ObjectSerializer.serialize(pom.toSplit));
            	if (pom.pigContext != null)
            		conf.set("pig.pigContext", ObjectSerializer.serialize(pom.pigContext));
            
            	conf.setMapRunnerClass(PigMapReduce.class);
            	if (pom.toCombine != null)
                	conf.setCombinerClass(PigCombine.class);
            	if (pom.quantilesFile!=null){
            		conf.set("pig.quantilesFile", pom.quantilesFile);
            	}
            	if (pom.partitionFunction!=null){
            		conf.setPartitionerClass(SortPartitioner.class);
            	}
            	conf.setReducerClass(PigMapReduce.class);
            	conf.setInputFormat(PigInputFormat.class);
            	conf.setOutputFormat(PigOutputFormat.class);
            	conf.setInputKeyClass(UTF8.class);
            	conf.setInputValueClass(Tuple.class);
            	conf.setOutputKeyClass(Tuple.class);
            	conf.setOutputValueClass(IndexedTuple.class);
            	conf.set("pig.inputs", ObjectSerializer.serialize(pom.inputFileSpecs));
            
            	conf.setOutputPath(new Path(pom.outputFileSpec.getFileName()));
            	conf.set("pig.storeFunc", pom.outputFileSpec.getFuncSpec());

            	//
            	// Now, actually submit the job (using the submit name)
            	//
	    	JobClient jobClient = pom.pigContext.getJobClient();
	    	RunningJob status = jobClient.submitJob(conf);
            	log.debug("submitted job: " + status.getJobID());
            
            	long sleepTime = 1000;
		double lastQueryProgress = -1.0;
		int lastJobsQueued = -1;
		double lastMapProgress = -1.0;
		double lastReduceProgress = -1.0;
            	while (true) {
               		try {
                    	Thread.sleep(sleepTime); } catch (Exception e) {}
			if (status.isComplete())
			{
				success = status.isSuccessful();
                    		log.debug("Job finished " + (success ? "" : "un") + "successfully");
				if (success) mrJobNumber++;
                    		double queryProgress = ((double) mrJobNumber) / ((double) numMRJobs);
				if (queryProgress > lastQueryProgress) {
                       			log.info("Pig progress = " + ((int) (queryProgress * 100)) + "%");
					lastQueryProgress = queryProgress;
				}
                    		break;
			}
			else // still running
			{
                        	double mapProgress = status.mapProgress();
                        	double reduceProgress = status.reduceProgress();
				if (lastMapProgress != mapProgress || lastReduceProgress != reduceProgress) {
                            		log.debug("Hadoop job progress: Map=" + (int) (mapProgress * 100) + "% Reduce="
                                    			+ (int) (reduceProgress * 100) + "%");
			    		lastMapProgress = mapProgress;
			    		lastReduceProgress = reduceProgress;
				}
                        	double numJobsCompleted = mrJobNumber;
                        	double thisJobProgress = (mapProgress + reduceProgress) / 2.0;
                        	double queryProgress = (numJobsCompleted + thisJobProgress) / ((double) numMRJobs);
				if (queryProgress > lastQueryProgress) {
                        		log.info("Pig progress = " + ((int) (queryProgress * 100)) + "%");
					lastQueryProgress = queryProgress;
				}
			}

		}

            	// bug 1030028: if the input file is empty; hadoop doesn't create the output file!
            	Path outputFile = conf.getOutputPath();
            	String outputName = outputFile.getName();
            	int colon = outputName.indexOf(':');
            	if (colon != -1) {
                	outputFile = new Path(outputFile.getParent(), outputName.substring(0, colon));
            	}
            	if (success && !pom.pigContext.getDfs().exists(outputFile)) {
            	
            		// create an empty output file
                	PigFile f = new PigFile(outputFile.toString(), false);
                	f.store(new DataBag(Datum.DataType.TUPLE),
						new PigStorage(), pom.pigContext);
                
            	}

          /*
            if (!success && pom.pigContext.debug) {
                String host = conf.get("mapred.job.tracker").split(":")[0];
                int port = conf.getInt("mapred.job.tracker.info.port", 50030);
                URL logURL = new URL("http", host, port, "/logs/hadoop-crawler-jobtracker-" + host + ".log");
                BufferedReader br = new BufferedReader(new InputStreamReader(logURL.openStream()));
                String line;
                boolean jobLine = false;
                String jid = status.getJobId();
                int underScore = jid.indexOf('_');
                String tid = "task" + jid.substring(underScore);
                while ((line = br.readLine()) != null) {
                    // This whole log processing logic is embarrassing, but this
                    // part is the most
                    // embarrassing! New log entries start with the date, so we
                    // are looking for
                    // the year 200x. (Hopefully by 2010 this wretched code will
                    // be gone!)
                    if (line.startsWith("200")) {
                        jobLine = line.indexOf(jid) != -1 || line.indexOf(tid) != -1;
                    }
                    if (jobLine)
                        System.out.println(line);
                }
            }
            */
		if (!success) {
			// go find the error messages
			getErrorMessages(jobClient.getMapTaskReports(status.getJobID()),
					"map", log);
			getErrorMessages(jobClient.getReduceTaskReports(status.getJobID()),
					"reduce", log);
		}
		else {
                	long timeSpent = 0;
              
                	// NOTE: this call is crashing due to a bug in Hadoop; the bug is known and the patch has not been applied yet.
                	TaskReport[] mapReports = jobClient.getMapTaskReports(status.getJobID());
                	TaskReport[] reduceReports = jobClient.getReduceTaskReports(status.getJobID());
                	for (TaskReport r : mapReports) {
                    		timeSpent += (r.getFinishTime() - r.getStartTime());
                	}
                	for (TaskReport r : reduceReports) {
                    		timeSpent += (r.getFinishTime() - r.getStartTime());
                	}
                
                	totalHadoopTimeSpent += timeSpent;
            	}
	}catch (Exception e) {
		// Do we need different handling for different exceptions
                e.printStackTrace();
                throw new IOException(e.getMessage());
	}finally{
		submitJarFile.delete();
	}

        return success;
    }

private void getErrorMessages(TaskReport reports[], String type, Logger log)
{
	for (int i = 0; i < reports.length; i++) {
		String msgs[] = reports[i].getDiagnostics();
		StringBuilder sb = new StringBuilder("Error message from task (" + type + ") " +
			reports[i].getTaskId());
		for (int j = 0; j < msgs.length; j++) {
			sb.append(" " + msgs[j]);
		}
		log.error(sb.toString());
	}
}

}
