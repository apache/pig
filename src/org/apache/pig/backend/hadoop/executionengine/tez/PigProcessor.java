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
package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigHadoopLogger;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.UDFFinishVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigProgressable;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.tools.pigstats.PigStatusReporter;
import org.apache.tez.common.TezUtils;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalIOProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.TezProcessorContext;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.api.KeyValueReader;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

public class PigProcessor implements LogicalIOProcessor {

    private static final Log LOG = LogFactory.getLog(PigProcessor.class);
    // Names of the properties that store serialized physical plans
    public static final String PLAN = "pig.exec.tez.plan";
    public static final String COMBINE_PLAN = "pig.exec.tez.combine.plan";
    // This flag is used in both order by and skewed job. This is a configuration
    // entry to instruct sample job to dynamically estimate parallelism
    public static final String ESTIMATE_PARALLELISM = "pig.exec.estimate.parallelism";
    // This flag is used in both order by and skewed job.
    // This is the key in sampleMap of estimated parallelism
    public static final String ESTIMATED_NUM_PARALLELISM = "pig.exec.estimated.num.parallelism";

    // The operator key for sample vertex, used by partition vertex to collect sample
    public static final String SAMPLE_VERTEX = "pig.sampleVertex";

    // The operator key for sort vertex, used by sample vertex to send parallelism event
    // if Pig need to estimate parallelism of sort vertex
    public static final String SORT_VERTEX = "pig.sortVertex";

    private PhysicalPlan execPlan;

    private Set<MROutput> fileOutputs = new HashSet<MROutput>();

    private PhysicalOperator leaf;

    private Configuration conf;
    private PigHadoopLogger pigHadoopLogger;

    private TezProcessorContext processorContext;

    public static String sampleVertex;
    public static Map<String, Object> sampleMap;

    @SuppressWarnings("unchecked")
    @Override
    public void initialize(TezProcessorContext processorContext)
            throws Exception {
        this.processorContext = processorContext;

        // Reset any static variables to avoid conflict in container-reuse.
        sampleVertex = null;
        sampleMap = null;

        // Reset static variables cleared for avoiding OOM.
        // TODO: Figure out a cleaner way to do this. ThreadLocals actually can be avoided all together
        // for mapreduce/tez mode and just used for Local mode.
        PhysicalOperator.reporter = new ThreadLocal<PigProgressable>();
        PigMapReduce.sJobConfInternal = new ThreadLocal<Configuration>();

        byte[] payload = processorContext.getUserPayload();
        conf = TezUtils.createConfFromUserPayload(payload);
        PigContext.setPackageImportList((ArrayList<String>) ObjectSerializer
                .deserialize(conf.get("udf.import.list")));
        PigContext pc = (PigContext) ObjectSerializer.deserialize(conf.get("pig.pigContext"));

        // To determine front-end in UDFContext
        conf.set("mapreduce.job.application.attempt.id", processorContext.getUniqueIdentifier());
        UDFContext.getUDFContext().addJobConf(conf);
        UDFContext.getUDFContext().deserialize();

        String execPlanString = conf.get(PLAN);
        execPlan = (PhysicalPlan) ObjectSerializer.deserialize(execPlanString);
        SchemaTupleBackend.initialize(conf, pc);
        PigMapReduce.sJobContext = HadoopShims.createJobContext(conf, new org.apache.hadoop.mapreduce.JobID());

        // Set the job conf as a thread-local member of PigMapReduce
        // for backwards compatibility with the existing code base.
        PigMapReduce.sJobConfInternal.set(conf);

        boolean aggregateWarning = "true".equalsIgnoreCase(pc.getProperties().getProperty("aggregate.warning"));
        PigStatusReporter pigStatusReporter = PigStatusReporter.getInstance();
        pigStatusReporter.setContext(new TezTaskContext(processorContext));
        pigHadoopLogger = PigHadoopLogger.getInstance();
        pigHadoopLogger.setReporter(pigStatusReporter);
        pigHadoopLogger.setAggregate(aggregateWarning);
        PhysicalOperator.setPigLogger(pigHadoopLogger);

        LinkedList<TezTaskConfigurable> tezTCs = PlanHelper.getPhysicalOperators(execPlan, TezTaskConfigurable.class);
        for (TezTaskConfigurable tezTC : tezTCs){
            tezTC.initialize(processorContext);
        }
    }

    @Override
    public void handleEvents(List<Event> processorEvents) {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() throws Exception {
        // Avoid memory leak. ThreadLocals especially leak a lot of memory.
        // The Reporter and Context objects hold TezProcessorContextImpl
        // which holds input and its sort buffers which are huge.
        PhysicalOperator.reporter = new ThreadLocal<PigProgressable>();
        PigMapReduce.sJobConfInternal = new ThreadLocal<Configuration>();
        PigMapReduce.sJobContext = null;
        PigContext.setPackageImportList(null);
        UDFContext.destroy();
        execPlan = null;
        fileOutputs = null;
        leaf = null;
        conf = null;
        sampleMap = null;
        sampleVertex = null;
        if (pigHadoopLogger != null) {
            pigHadoopLogger.destroy();
            pigHadoopLogger = null;
        }
    }

    @Override
    public void run(Map<String, LogicalInput> inputs,
            Map<String, LogicalOutput> outputs) throws Exception {

        try {
            initializeInputs(inputs);

            initializeOutputs(outputs);


            List<PhysicalOperator> leaves = null;

            if (!execPlan.isEmpty()) {
                leaves = execPlan.getLeaves();
                // TODO: Pull from all leaves when there are multiple leaves/outputs
                leaf = leaves.get(0);
            }

            runPipeline(leaf);

            // Calling EvalFunc.finish()
            UDFFinishVisitor finisher = new UDFFinishVisitor(execPlan,
                    new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(
                            execPlan));
            try {
                finisher.visit();
            } catch (VisitorException e) {
                int errCode = 2121;
                String msg = "Error while calling finish method on UDFs.";
                throw new VisitorException(msg, errCode, PigException.BUG, e);
            }

            for (MROutput fileOutput : fileOutputs){
                if (fileOutput.isCommitRequired()) {
                    fileOutput.commit();
                }
            }

            // send event containing parallelism to sorting job of order by / skewed join
            if (conf.getBoolean(ESTIMATE_PARALLELISM, false)) {
                int parallelism = 1;
                if (sampleMap!=null && sampleMap.containsKey(ESTIMATED_NUM_PARALLELISM)) {
                    parallelism = (Integer)sampleMap.get(ESTIMATED_NUM_PARALLELISM);
                }
                String sortingVertex = conf.get(SORT_VERTEX);
                // Should contain only 1 output for sampleAggregation job
                LOG.info("Sending numParallelism " + parallelism + " to " + sortingVertex);
                VertexManagerEvent vmEvent = new VertexManagerEvent(
                        sortingVertex, Ints.toByteArray(parallelism));
                List<Event> events = Lists.newArrayListWithCapacity(1);
                events.add(vmEvent);
                processorContext.sendEvents(events);
            }
        } catch (Exception e) {
            abortOutput();
            LOG.error("Encountered exception while processing: ", e);
            throw e;
        }
    }

    private void abortOutput() {
        for (MROutput fileOutput : fileOutputs){
            try {
                fileOutput.abort();
            } catch (IOException e) {
                LOG.error("Encountered exception while aborting output", e);
            }
        }
    }

    private void initializeInputs(Map<String, LogicalInput> inputs)
            throws Exception {

        Set<String> inputsToSkip = new HashSet<String>();

        sampleVertex = conf.get("pig.sampleVertex");
        if (sampleVertex != null) {
            collectSample(sampleVertex, inputs.get(sampleVertex));
            inputsToSkip.add(sampleVertex);
        }

        LinkedList<TezInput> tezInputs = PlanHelper.getPhysicalOperators(execPlan, TezInput.class);
        for (TezInput tezInput : tezInputs){
            tezInput.addInputsToSkip(inputsToSkip);
        }

        LinkedList<ReadScalarsTez> scalarInputs = new LinkedList<ReadScalarsTez>();
        for (POUserFunc userFunc : PlanHelper.getPhysicalOperators(execPlan, POUserFunc.class) ) {
            if (userFunc.getFunc() instanceof ReadScalarsTez) {
                scalarInputs.add((ReadScalarsTez)userFunc.getFunc());
            }
        }

        for (ReadScalarsTez scalarInput: scalarInputs) {
            scalarInput.addInputsToSkip(inputsToSkip);
        }

        for (Entry<String, LogicalInput> entry : inputs.entrySet()) {
            if (inputsToSkip.contains(entry.getKey())) {
                LOG.info("Skipping fetch of input " + entry.getValue() + " from vertex " + entry.getKey());
            } else {
                LOG.info("Starting fetch of input " + entry.getValue() + " from vertex " + entry.getKey());
                entry.getValue().start();
            }
        }

        for (TezInput tezInput : tezInputs){
            tezInput.attachInputs(inputs, conf);
        }

        for (ReadScalarsTez scalarInput: scalarInputs) {
            scalarInput.attachInputs(inputs, conf);
        }

    }

    private void initializeOutputs(Map<String, LogicalOutput> outputs) throws Exception {

        for (Entry<String, LogicalOutput> entry : outputs.entrySet()) {
            LogicalOutput output = entry.getValue();
            LOG.info("Starting output " + output + " to vertex " + entry.getKey());
            output.start();
            if (output instanceof MROutput){
                MROutput mrOut = (MROutput) output;
                fileOutputs.add(mrOut);
            }
        }
        LinkedList<TezOutput> tezOuts = PlanHelper.getPhysicalOperators(execPlan, TezOutput.class);
        for (TezOutput tezOut : tezOuts){
            tezOut.attachOutputs(outputs, conf);
        }
    }

    protected void runPipeline(PhysicalOperator leaf) throws IOException, InterruptedException {
        while(true){
            Result res = leaf.getNextTuple();
            if(res.returnStatus==POStatus.STATUS_OK){
                continue;
            }

            if(res.returnStatus==POStatus.STATUS_EOP) {
                return;
            }

            if(res.returnStatus==POStatus.STATUS_NULL)
                continue;

            if(res.returnStatus==POStatus.STATUS_ERR){
                String errMsg;
                if(res.result != null) {
                    errMsg = "Received Error while " +
                            "processing the map plan: " + res.result;
                } else {
                    errMsg = "Received Error while " +
                            "processing the map plan.";
                }

                int errCode = 2055;
                ExecException ee = new ExecException(errMsg, errCode, PigException.BUG);
                throw ee;
            }
        }
    }

    @SuppressWarnings({ "unchecked" })
    private void collectSample(String sampleVertex, LogicalInput logicalInput) throws Exception {
        String quantileMapCacheKey = "sample-" + sampleVertex  + ".cached";
        sampleMap =  (Map<String, Object>)ObjectCache.getInstance().retrieve(quantileMapCacheKey);
        if (sampleMap != null) {
            return;
        }
        LOG.info("Starting fetch of input " + logicalInput + " from vertex " + sampleVertex);
        logicalInput.start();
        KeyValueReader reader = (KeyValueReader) logicalInput.getReader();
        reader.next();
        Object val = reader.getCurrentValue();
        if (val != null) {
            // Sample is not empty
            Tuple t = (Tuple) val;
            sampleMap = (Map<String, Object>) t.get(0);
            ObjectCache.getInstance().cache(quantileMapCacheKey, sampleMap);
        } else {
            LOG.warn("Cannot fetch sample from " + sampleVertex);
        }
    }

}
