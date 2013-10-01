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

import java.util.Arrays;

import org.apache.hadoop.util.RunJar;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStatsUtil;

public class NativeMapReduceOper extends MapReduceOper {
    
    private static final long serialVersionUID = 1L;
    private static int countJobs = 0;
    private String nativeMRJar;
    private String[] params;
    
    public NativeMapReduceOper(OperatorKey k, String mrJar, String[] parameters) {
        super(k);
        nativeMRJar = mrJar;
        params = parameters;
    }
    
    public static int getJobNumber() {
        countJobs++;
        return countJobs;
    }
    
    public String getJobId() {
        return nativeMRJar + "_";
    }
    
    public String getCommandString() {
        StringBuilder sb = new StringBuilder("hadoop jar ");
        sb.append(nativeMRJar);
        for(String pr: params) {
            sb.append(" ");
            sb.append(pr);
        }
        return sb.toString(); 
    }
    
    private String[] getNativeMRParams() {
        // may need more cooking
        String[] paramArr = new String[params.length + 1];
        paramArr[0] = nativeMRJar;
        for(int i=0; i< params.length; i++) {
            paramArr[i+1] = params[i];
        }
        return paramArr;
    }
    
    @Override
    public void visit(MROpPlanVisitor v) throws VisitorException {
        v.visitMROp(this);
    }
    
    public void runJob() throws JobCreationException {
        RunJarSecurityManager secMan = new RunJarSecurityManager();
        try {
            RunJar.main(getNativeMRParams());
            PigStatsUtil.addNativeJobStats(PigStats.get(), this, true);
        } catch (SecurityException se) {
            if(secMan.getExitInvoked()) {
                if(secMan.getExitCode() != 0) {
                    throw new JobCreationException("Native job returned with non-zero return code");
                }
                else {
                    PigStatsUtil.addNativeJobStats(PigStats.get(), this, true);
                }
            }
        } catch (Throwable t) {
            JobCreationException e = new JobCreationException(
                    "Cannot run native mapreduce job "+ t.getMessage(), t);
            PigStatsUtil.addNativeJobStats(PigStats.get(), this, false, e);
            throw e;
        } finally {
            secMan.retire();
        }
    }
    
    @Override
    public String name(){
        return "MapReduce - " + mKey.toString() + "\n" 
        + " Native MapReduce - jar : " + nativeMRJar + ", params: " + Arrays.toString(params) ;
        
        		
    }
    
}
