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
package org.apache.pig.backend.hadoop.executionengine.fetch;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.UDFFinishVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.EmptyPigStats;
import org.joda.time.DateTimeZone;

/**
 * This class is responsible for executing the fetch task, saving the result to disk
 * and do the necessary cleanup afterwards.
 *
 */
public class FetchLauncher {

    private final PigContext pigContext;
    private final Configuration conf;

    public FetchLauncher(PigContext pigContext) {
        this.pigContext = pigContext;
        this.conf = ConfigurationUtil.toConfiguration(pigContext.getProperties());
    }

    /**
     * Runs the fetch task by executing chain of calls on the PhysicalPlan from the leaf
     * up to the LoadFunc
     *
     * @param pp - Physical plan
     * @return SimpleFetchPigStats instance representing the fetched result
     * @throws IOException
     */
    public PigStats launchPig(PhysicalPlan pp) throws IOException {
        POStore poStore = (POStore) pp.getLeaves().get(0);
        init(pp, poStore);

        // run fetch
        runPipeline(poStore);

        UDFFinishVisitor udfFinisher = new UDFFinishVisitor(pp,
                new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(pp));
        udfFinisher.visit();

        return PigStats.start(new EmptyPigStats(pigContext, poStore));
    }

    /**
     * Creates an empty MR plan
     *
     * @param pp - Physical plan
     * @param pc - PigContext
     * @param ps - PrintStream to write the plan to
     * @param format format of the output plan
     * @throws PlanException
     * @throws VisitorException
     * @throws IOException
     */
    public void explain(PhysicalPlan pp, PigContext pc, PrintStream ps, String format)
            throws PlanException, VisitorException, IOException {
        if ("xml".equals(format)) {
            ps.println("<mapReducePlan>No MR jobs. Fetch only</mapReducePlan>");
        }
        else {
            ps.println("#--------------------------------------------------");
            ps.println("# Map Reduce Plan                                  ");
            ps.println("#--------------------------------------------------");
            ps.println("No MR jobs. Fetch only.");
        }
        return;
    }

    private void init(PhysicalPlan pp, POStore poStore) throws IOException {

        poStore.setStoreImpl(new FetchPOStoreImpl(pigContext));
        poStore.setUp();
        if (!PlanHelper.getPhysicalOperators(pp, POStream.class).isEmpty()) {
            MapRedUtil.setupStreamingDirsConfSingle(poStore, pigContext, conf);
        }

        PhysicalOperator.setReporter(new FetchProgressableReporter());
        SchemaTupleBackend.initialize(conf, pigContext);

        UDFContext udfContext = UDFContext.getUDFContext();
        udfContext.addJobConf(conf);
        udfContext.setClientSystemProps(pigContext.getProperties());
        udfContext.serialize(conf);

        PigMapReduce.sJobConfInternal.set(conf);
        String dtzStr = PigMapReduce.sJobConfInternal.get().get("pig.datetime.default.tz");
        if (dtzStr != null && dtzStr.length() > 0) {
            // ensure that the internal timezone is uniformly in UTC offset style
            DateTimeZone.setDefault(DateTimeZone.forOffsetMillis(DateTimeZone.forID(dtzStr).getOffset(null)));
        }
    }

    private void runPipeline(POStore posStore) throws IOException {
        while (true) {
            Result res = posStore.getNextTuple();
            if (res.returnStatus == POStatus.STATUS_OK)
                continue;

            if (res.returnStatus == POStatus.STATUS_EOP) {
                posStore.tearDown();
                return;
            }

            if (res.returnStatus == POStatus.STATUS_NULL)
                continue;

            if(res.returnStatus==POStatus.STATUS_ERR){
                String errMsg;
                if(res.result != null) {
                    errMsg = "Fetch failed. Couldn't retrieve result: " + res.result;
                } else {
                    errMsg = "Fetch failed. Couldn't retrieve result";
                }
                int errCode = 2088;
                ExecException ee = new ExecException(errMsg, errCode, PigException.BUG);
                throw ee;
            }
        }
    }

}
