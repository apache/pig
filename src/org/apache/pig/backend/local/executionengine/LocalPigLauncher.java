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
import java.util.BitSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.Launcher;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
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
    public boolean launchPig(PhysicalPlan php, String grpName, PigContext pc)
            throws PlanException, VisitorException, IOException, ExecException,
            JobCreationException {
        // TODO Auto-generated method stub
        stores = PlanHelper.getStores(php);
        int noJobs = stores.size();
        int failedJobs = 0;

        for (POStore op : stores) {
            op.setStoreImpl(new LocalPOStoreImpl(pc));
            op.setUp();
        }

        failedJobs = runPipeline(stores.toArray(new POStore[0]));

        if (failedJobs == 0) {
            log.info("100% complete!");
            log.info("Success!!");
            return true;
        } else {
            log.info("Failed jobs!!");
            log.info(failedJobs + " out of " + noJobs + " failed!");
        }
        return false;

    }

    private int runPipeline(POStore[] leaves) throws IOException, ExecException {
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
                    // fallthrough
                case POStatus.STATUS_EOP:
                    leaves[i].tearDown();
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
