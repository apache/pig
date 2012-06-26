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

package org.apache.pig.test;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class TestJobStats {

	private static final TaskReport[] mapTaskReports = new TaskReport[5];
	private static final TaskReport[] reduceTaskReports = new TaskReport[5];
	private static final JobID jobID = new JobID("jobStat", Integer.MAX_VALUE);
	private static String ASSERT_STRING;
	
	private static final int ONE_THOUSAND = 1000;
	
	private static final long[][] MAP_START_FINISH_TIME_DATA = { { 0, 100},
		{ 200, 400 }, { 500, 800 }, { 900, 1300 }, { 1400, 1900 } };

	private static final long[][] REDUCE_START_FINISH_TIME_DATA = { { 0, 100 },
		{ 200, 400 }, { 500, 700 }, { 700, 900 }, { 1000, 1500 } };

	@BeforeClass
    public static void oneTimeSetup() throws Exception {
		
		// setting up TaskReport for map tasks
		for (int i = 0; i < mapTaskReports.length; i++) {
			mapTaskReports[i] = Mockito.mock(TaskReport.class);
			Mockito.when(mapTaskReports[i].getStartTime()).thenReturn(MAP_START_FINISH_TIME_DATA[i][0] * ONE_THOUSAND);
			Mockito.when(mapTaskReports[i].getFinishTime()).thenReturn(MAP_START_FINISH_TIME_DATA[i][1] * ONE_THOUSAND);
		}
		
		// setting up TaskReport for reduce tasks
		for (int i = 0; i < reduceTaskReports.length; i++) {
			reduceTaskReports[i] = Mockito.mock(TaskReport.class);
			Mockito.when(reduceTaskReports[i].getStartTime()).thenReturn(REDUCE_START_FINISH_TIME_DATA[i][0] * ONE_THOUSAND);
			Mockito.when(reduceTaskReports[i].getFinishTime()).thenReturn(REDUCE_START_FINISH_TIME_DATA[i][1] * ONE_THOUSAND);
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append(jobID.toString()).append("\t");
		sb.append(mapTaskReports.length).append("\t");
		sb.append(reduceTaskReports.length).append("\t");
		
		sb.append("500\t100\t300\t300\t500\t100\t240\t200");
		ASSERT_STRING = sb.toString();
    }
	
	JobStats createJobStats(String name, JobGraph plan) {
	    try {
    	    Constructor con = JobStats.class.getDeclaredConstructor(String.class, JobGraph.class);
    	    con.setAccessible(true);
    	    JobStats jobStats = (JobStats)con.newInstance(name, plan);
    	    return jobStats;
	    } catch (Exception e) {
	        return null;
	    }
	}
	
	Method getJobStatsMethod(String methodName, Class<?>... parameterTypes) throws Exception {
	    Method m = JobStats.class.getDeclaredMethod(methodName, parameterTypes);
        m.setAccessible(true);
        return m;
	}
	
	@Test
	public void testMedianMapReduceTime() throws Exception {

		JobConf jobConf = new JobConf();
		JobClient jobClient = Mockito.mock(JobClient.class);
		
		// mock methods to return the predefined map and reduce task reports
		Mockito.when(jobClient.getMapTaskReports(jobID)).thenReturn(mapTaskReports);
		Mockito.when(jobClient.getReduceTaskReports(jobID)).thenReturn(reduceTaskReports);

		PigStats.JobGraph jobGraph = new PigStats.JobGraph();
		JobStats jobStats = createJobStats("JobStatsTest", jobGraph);
		getJobStatsMethod("setId", JobID.class).invoke(jobStats, jobID);
		getJobStatsMethod("setSuccessful", boolean.class).invoke(jobStats, true);

		getJobStatsMethod("addMapReduceStatistics", JobClient.class, Configuration.class)
		    .invoke(jobStats, jobClient, jobConf);
		String msg = (String)getJobStatsMethod("getDisplayString", boolean.class)
		    .invoke(jobStats, false);
		
		System.out.println(JobStats.SUCCESS_HEADER);
		System.out.println(msg);
		
		assertTrue(msg.startsWith(ASSERT_STRING));
	}
	
	@Test
	public void testOneTaskReport() throws Exception {
		// setting up one map task report
		TaskReport[] mapTaskReports = new TaskReport[1];
		mapTaskReports[0] = Mockito.mock(TaskReport.class);
		Mockito.when(mapTaskReports[0].getStartTime()).thenReturn(300L * ONE_THOUSAND);
		Mockito.when(mapTaskReports[0].getFinishTime()).thenReturn(400L * ONE_THOUSAND);
		
		// setting up one reduce task report
		TaskReport[] reduceTaskReports = new TaskReport[1];
		reduceTaskReports[0] = Mockito.mock(TaskReport.class);
		Mockito.when(reduceTaskReports[0].getStartTime()).thenReturn(500L * ONE_THOUSAND);
		Mockito.when(reduceTaskReports[0].getFinishTime()).thenReturn(700L * ONE_THOUSAND);
		
		JobConf jobConf = new JobConf();
		JobClient jobClient = Mockito.mock(JobClient.class);

		Mockito.when(jobClient.getMapTaskReports(jobID)).thenReturn(mapTaskReports);
		Mockito.when(jobClient.getReduceTaskReports(jobID)).thenReturn(reduceTaskReports);
		
		PigStats.JobGraph jobGraph = new PigStats.JobGraph();
		JobStats jobStats = createJobStats("JobStatsTest", jobGraph);
		getJobStatsMethod("setId", JobID.class).invoke(jobStats, jobID);
		getJobStatsMethod("setSuccessful", boolean.class).invoke(jobStats, true);
		
		getJobStatsMethod("addMapReduceStatistics", JobClient.class, Configuration.class)
		    .invoke(jobStats, jobClient, jobConf);
		String msg = (String)getJobStatsMethod("getDisplayString", boolean.class)
		    .invoke(jobStats, false);
		System.out.println(JobStats.SUCCESS_HEADER);
		System.out.println(msg);
		
		StringBuilder sb = new StringBuilder();
		sb.append(jobID.toString()).append("\t");
		sb.append(mapTaskReports.length).append("\t");
		sb.append(reduceTaskReports.length).append("\t");
		sb.append("100\t100\t100\t100\t200\t200\t200\t200");
		
		System.out.println("assert msg: " + sb.toString());
		assertTrue(msg.startsWith(sb.toString()));
		
	}
}
