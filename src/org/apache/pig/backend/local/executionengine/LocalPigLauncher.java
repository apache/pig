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

package org.apache.pig.backend.local.executionengine;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.Launcher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigHadoopLogger;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.UDFFinishVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.PigException;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;

public class LocalPigLauncher extends Launcher {
    private static final Tuple DUMMYTUPLE = null;
    private Log log = LogFactory.getLog(getClass());
    List<POStore> stores;

    @Override
    public void explain(PhysicalPlan pp, PigContext pc, PrintStream ps,
                        String format, boolean isVerbose)
        throws PlanException, VisitorException, IOException {
        pp.explain(ps, format, isVerbose);
        ps.append('\n');
    }

    @Override
    public PigStats launchPig(PhysicalPlan php, String grpName, PigContext pc)
            throws PlanException, VisitorException, IOException, ExecException,
            JobCreationException {

    	//Until a PigLocalLogger is implemented, setting up a PigHadoopLogger
    	PhysicalOperator.setPigLogger(PigHadoopLogger.getInstance());

        stores = PlanHelper.getStores(php);

        int noJobs = stores.size();
        int failedJobs = 0;
        
        PigStats stats = new PigStats();
        stats.setPhysicalPlan(php);
        stats.setExecType(pc.getExecType());

        for (POStore op : stores) {
            op.setStoreImpl(new LocalPOStoreImpl(pc));
            op.setUp();
        }

        // We need to handle stores that have loads as successors
        // first. PlanHelper's getStores has returned those in the
        // dependency order, so that's how we will run them.
        for (Iterator<POStore> it = stores.iterator(); it.hasNext(); ) {
            POStore op = it.next();

            List<PhysicalOperator> sucs = new ArrayList<PhysicalOperator>();
            if (php.getSuccessors(op) != null) {
                sucs.addAll(php.getSuccessors(op));
            }
            
            if (sucs.size() != 0) {
                log.info("running store with dependencies");
                POStore[] st = new POStore[1];
                st[0] = op;
                failedJobs += runPipeline(st, pc);
                for (PhysicalOperator suc: sucs) {
                    php.disconnect(op, suc);
                }
                it.remove();
            }
        }
                
        // The remaining stores can be run together.
        failedJobs += runPipeline(stores.toArray(new POStore[0]), pc);
        
        stats.accumulateStats();

        UDFFinishVisitor finisher = new UDFFinishVisitor(php, new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(php));
        finisher.visit();

        for (FileSpec spec: failedStores) {
            log.info("Failed to produce result in: \""+spec.getFileName()+"\"");
        }

        for (FileSpec spec: succeededStores) {
            log.info("Successfully stored result in: \""+spec.getFileName()+"\"");
        }

        if (failedJobs == 0) {
            log.info("Records written : " + stats.getRecordsWritten());
            log.info("Bytes written : " + stats.getBytesWritten());
            log.info("100% complete!");
            log.info("Success!!");
            return stats;
        } else {
            log.info("Failed jobs!!");
            log.info(failedJobs + " out of " + noJobs + " failed!");
        }
        return null;

    }

    private int runPipeline(POStore[] leaves, PigContext pc) throws IOException, ExecException {
        BitSet bs = new BitSet(leaves.length);
        int failed = 0;
        while(true) {
            if (bs.cardinality() == leaves.length) {
                break;
            }
            for(int i=bs.nextClearBit(0); i<leaves.length; i=bs.nextClearBit(i+1)) {
                Result res = leaves[i].getNext(DUMMYTUPLE);
                switch(res.returnStatus) {
                case POStatus.STATUS_NULL:
                    // good null from store means keep at it.
                    continue;
                case POStatus.STATUS_OK:
                    // ok shouldn't happen store should have consumed it.
                    // fallthrough
                case POStatus.STATUS_ERR:
                    leaves[i].cleanUp();
                    leaves[i].tearDown();
                    failed++;
                    failedStores.add(leaves[i].getSFile());
                    if ("true".equalsIgnoreCase(
                        pc.getProperties().getProperty("stop.on.failure","false"))) {
                        int errCode = 6017;
                        String msg = "Execution failed, while processing "
                            + leaves[i].getSFile().getFileName();
                        
                        throw new ExecException(msg, errCode, PigException.REMOTE_ENVIRONMENT);
                    }
                    bs.set(i);
                    break;
                case POStatus.STATUS_EOP:
                    leaves[i].tearDown();
                    succeededStores.add(leaves[i].getSFile());
                    // fallthrough
                default:
                    bs.set(i);
                    break;
                }
            }
        }
        return failed;
    }
}
