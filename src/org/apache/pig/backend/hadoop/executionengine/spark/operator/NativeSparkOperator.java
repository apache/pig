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
package org.apache.pig.backend.hadoop.executionengine.spark.operator;

import org.apache.hadoop.util.RunJar;
import org.apache.pig.backend.hadoop.executionengine.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.RunJarSecurityManager;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.spark.SparkStatsUtil;

/**
 * NativeSparkOperator:
 */
public class NativeSparkOperator extends SparkOperator {
    private static final long serialVersionUID = 1L;
    private static int countJobs = 0;
    private String nativeSparkJar;
    private String[] params;
    private String jobId;

    public NativeSparkOperator(OperatorKey k, String sparkJar, String[] parameters) {
        super(k);
        nativeSparkJar = sparkJar;
        params = parameters;
        jobId = sparkJar + "_" + getJobNumber();
    }

    private static int getJobNumber() {
        countJobs++;
        return countJobs;
    }

    public String getJobId() {
        return jobId;
    }

    public void runJob() throws JobCreationException {
        RunJarSecurityManager secMan = new RunJarSecurityManager();
        try {
            RunJar.main(getNativeMRParams());
            SparkStatsUtil.addNativeJobStats(PigStats.get(), this);
        } catch (SecurityException se) {   //java.lang.reflect.InvocationTargetException
            if (secMan.getExitInvoked()) {
                if (secMan.getExitCode() != 0) {
                    JobCreationException e = new JobCreationException("Native job returned with non-zero return code");
                    SparkStatsUtil.addFailedNativeJobStats(PigStats.get(), this, e);
                } else {
                    SparkStatsUtil.addNativeJobStats(PigStats.get(), this);
                }
            }
        } catch (Throwable t) {
            JobCreationException e = new JobCreationException(
                    "Cannot run native spark job " + t.getMessage(), t);
            SparkStatsUtil.addFailedNativeJobStats(PigStats.get(), this, e);
            throw e;
        } finally {
            secMan.retire();
        }
    }

    private String[] getNativeMRParams() {
        String[] paramArr = new String[params.length + 1];
        paramArr[0] = nativeSparkJar;
        for (int i = 0; i < params.length; i++) {
            paramArr[i + 1] = params[i];
        }
        return paramArr;
    }

    public String getCommandString() {
        StringBuilder sb = new StringBuilder("hadoop jar ");
        sb.append(nativeSparkJar);
        for (String pr : params) {
            sb.append(" ");
            sb.append(pr);
        }
        return sb.toString();
    }
}
