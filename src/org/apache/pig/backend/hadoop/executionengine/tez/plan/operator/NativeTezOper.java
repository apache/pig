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
package org.apache.pig.backend.hadoop.executionengine.tez.plan.operator;

import java.util.Arrays;

import org.apache.hadoop.util.RunJar;
import org.apache.pig.backend.hadoop.executionengine.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.RunJarSecurityManager;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.TezOperator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.tez.TezPigScriptStats;

public class NativeTezOper extends TezOperator {
    private static final long serialVersionUID = 1L;
    private static int countJobs = 0;
    private String nativeTezJar;
    private String[] params;
    private String jobId;

    public NativeTezOper(OperatorKey k, String tezJar, String[] parameters) {
        super(k);
        nativeTezJar = tezJar;
        params = parameters;
        jobId = nativeTezJar + "_" + getJobNumber();
    }

    private static int getJobNumber() {
        countJobs++;
        return countJobs;
    }

    public String getJobId() {
        return jobId;
    }

    public String getCommandString() {
        StringBuilder sb = new StringBuilder("hadoop jar ");
        sb.append(nativeTezJar);
        for(String pr: params) {
            sb.append(" ");
            sb.append(pr);
        }
        return sb.toString();
    }

    private String[] getNativeTezParams() {
        // may need more cooking
        String[] paramArr = new String[params.length + 1];
        paramArr[0] = nativeTezJar;
        for(int i=0; i< params.length; i++) {
            paramArr[i+1] = params[i];
        }
        return paramArr;
    }

    @Override
    public void visit(TezOpPlanVisitor v) throws VisitorException {
        v.visitTezOp(this);
    }

    public void runJob(String jobStatsKey) throws JobCreationException {
        RunJarSecurityManager secMan = new RunJarSecurityManager();
        try {
            RunJar.main(getNativeTezParams());
            ((TezPigScriptStats)PigStats.get()).addTezJobStatsForNative(jobStatsKey, this, true);
        } catch (SecurityException se) {
            if(secMan.getExitInvoked()) {
                if(secMan.getExitCode() != 0) {
                    throw new JobCreationException("Native job returned with non-zero return code");
                }
                else {
                    ((TezPigScriptStats)PigStats.get()).addTezJobStatsForNative(jobStatsKey, this, true);
                }
            }
        } catch (Throwable t) {
            JobCreationException e = new JobCreationException(
                    "Cannot run native tez job "+ t.getMessage(), t);
            ((TezPigScriptStats)PigStats.get()).addTezJobStatsForNative(jobStatsKey, this, false);
            throw e;
        } finally {
            secMan.retire();
        }
    }

    @Override
    public String name(){
        return "Tez - " + mKey.toString() + "\n"
        + " Native Tez - jar : " + nativeTezJar + ", params: " + Arrays.toString(params) ;


    }

}
