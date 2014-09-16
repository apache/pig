/**
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
package org.apache.pig.backend.hadoop.executionengine.tez.optimizers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigConfiguration;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.tez.POSimpleTezLoad;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperPlan;
import org.apache.pig.backend.hadoop.executionengine.tez.TezOperator;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;

public class LoaderProcessor extends TezOpPlanVisitor {
    private Configuration conf;
    private PigContext pc;
    private static final Log log = LogFactory.getLog(LoaderProcessor.class);
    public LoaderProcessor(TezOperPlan plan, PigContext pigContext) {
        super(plan, new DependencyOrderWalker<TezOperator, TezOperPlan>(plan));
        this.pc = pigContext;
        this.conf = ConfigurationUtil.toConfiguration(pc.getProperties());;
    }

    /**
     * Do the final configuration of LoadFuncs and store what goes where. This
     * will need to be changed as the inputs get un-bundled
     *
     * @param tezOp
     * @param conf
     * @param job
     * @return true if any POLoads were found, else false.
     * @throws VisitorException
     * @throws IOException
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    private List<POLoad> processLoads(TezOperator tezOp
            ) throws VisitorException, IOException, ClassNotFoundException, InterruptedException {
        ArrayList<FileSpec> inp = new ArrayList<FileSpec>();
        ArrayList<List<OperatorKey>> inpTargets = new ArrayList<List<OperatorKey>>();
        ArrayList<String> inpSignatureLists = new ArrayList<String>();
        ArrayList<Long> inpLimits = new ArrayList<Long>();

        Job job = Job.getInstance(conf);
        conf = job.getConfiguration();
        conf.setBoolean("mapred.mapper.new-api", true);
        conf.setClass("mapreduce.inputformat.class",
                PigInputFormat.class, InputFormat.class);
        conf.set("pig.pigContext", ObjectSerializer.serialize(pc));
        List<POLoad> lds = PlanHelper.getPhysicalOperators(tezOp.plan,
                POLoad.class);

        if (lds != null && lds.size() > 0) {
            for (POLoad ld : lds) {
                LoadFunc lf = ld.getLoadFunc();
                lf.setLocation(ld.getLFile().getFileName(), job);

                // Store the inp filespecs
                inp.add(ld.getLFile());
            }
        }

        if (lds != null && lds.size() > 0) {
            for (POLoad ld : lds) {
                // Store the target operators for tuples read
                // from this input
                List<PhysicalOperator> ldSucs = new ArrayList<PhysicalOperator>(
                        tezOp.plan.getSuccessors(ld));
                List<OperatorKey> ldSucKeys = new ArrayList<OperatorKey>();
                if (ldSucs != null) {
                    for (PhysicalOperator operator2 : ldSucs) {
                        ldSucKeys.add(operator2.getOperatorKey());
                    }
                }
                inpTargets.add(ldSucKeys);
                inpSignatureLists.add(ld.getSignature());
                inpLimits.add(ld.getLimit());
                // Remove the POLoad from the plan
                tezOp.plan.remove(ld);
                // Now add the input handling operator for the Tez backend
                // TODO: Move this upstream to the PhysicalPlan generation
                POSimpleTezLoad tezLoad = new POSimpleTezLoad(ld.getOperatorKey(), ld.getLFile());
                tezLoad.setInputKey(ld.getOperatorKey().toString());
                tezLoad.copyAliasFrom(ld);
                tezLoad.setCacheFiles(ld.getCacheFiles());
                tezLoad.setShipFiles(ld.getShipFiles());
                tezOp.plan.add(tezLoad);
                for (PhysicalOperator sucs : ldSucs) {
                    tezOp.plan.connect(tezLoad, sucs);
                }
            }
            UDFContext.getUDFContext().serialize(conf);
            conf.set("udf.import.list",
                    ObjectSerializer.serialize(PigContext.getPackageImportList()));
            conf.set("pig.inputs", ObjectSerializer.serialize(inp));
            conf.set("pig.inpTargets", ObjectSerializer.serialize(inpTargets));
            conf.set("pig.inpSignatures", ObjectSerializer.serialize(inpSignatureLists));
            conf.set("pig.inpLimits", ObjectSerializer.serialize(inpLimits));
            String tmp;
            long maxCombinedSplitSize = 0;
            if (!tezOp.combineSmallSplits() || pc.getProperties().getProperty(PigConfiguration.PIG_SPLIT_COMBINATION, "true").equals("false"))
                conf.setBoolean(PigConfiguration.PIG_NO_SPLIT_COMBINATION, true);
            else if ((tmp = pc.getProperties().getProperty(PigConfiguration.PIG_MAX_COMBINED_SPLIT_SIZE, null)) != null) {
                try {
                    maxCombinedSplitSize = Long.parseLong(tmp);
                } catch (NumberFormatException e) {
                    log.warn("Invalid numeric format for pig.maxCombinedSplitSize; use the default maximum combined split size");
                }
            }
            if (maxCombinedSplitSize > 0)
                conf.setLong("pig.maxCombinedSplitSize", maxCombinedSplitSize);
            tezOp.getLoaderInfo().setInpSignatureLists(inpSignatureLists);
            tezOp.getLoaderInfo().setInp(inp);
            tezOp.getLoaderInfo().setInpLimits(inpLimits);
            // Not using MRInputAMSplitGenerator because delegation tokens are
            // fetched in FileInputFormat
            tezOp.getLoaderInfo().setInputSplitInfo(MRInputHelpers.generateInputSplitsToMem(conf, false, 0));
        }
        return lds;
    }

    @Override
    public void visitTezOp(TezOperator tezOp) throws VisitorException {
        try {
            tezOp.getLoaderInfo().setLoads(processLoads(tezOp));
        } catch (Exception e) {
            throw new VisitorException(e);
        }
    }
}
