package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.tez.common.TezUtils;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalIOProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.TezProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.output.OnFileSortedOutput;

public class PigProcessor implements LogicalIOProcessor {
    // Names of the properties that store serialized physical plans
    public static final String PLAN = "pig.exec.tez.plan";
    public static final String COMBINE_PLAN = "pig.exec.tez.combine.plan";
    private PhysicalPlan execPlan;

    private Set<OnFileSortedOutput> shuffleOutputs = new HashSet<OnFileSortedOutput>();
    private Set<MROutput> fileOutputs = new HashSet<MROutput>();

    private Map<String,KeyValueWriter> writers = new HashMap<String, KeyValueWriter>();

    private PhysicalOperator leaf;

    private boolean shuffle;
    private byte keyType;

    private InputHandler input;

    private Configuration conf;

    @Override
    public void initialize(TezProcessorContext processorContext)
            throws Exception {
        byte[] payload = processorContext.getUserPayload();
        conf = TezUtils.createConfFromUserPayload(payload);
        PigContext pc = (PigContext) ObjectSerializer.deserialize(conf.get("pig.pigContext"));

        input = createInputHandler(conf);

        UDFContext.getUDFContext().addJobConf(conf);
        UDFContext.getUDFContext().deserialize();

        String execPlanString = conf.get(PLAN);
        execPlan = (PhysicalPlan) ObjectSerializer.deserialize(execPlanString);
        execPlan.explain(System.out);
        SchemaTupleBackend.initialize(conf, pc);
    }

    @Override
    public void handleEvents(List<Event> processorEvents) {
        // TODO Auto-generated method stub

    }
    @Override
    public void close() throws Exception {
        // TODO Auto-generated method stub

    }
    @Override
    public void run(Map<String, LogicalInput> inputs,
            Map<String, LogicalOutput> outputs) throws Exception {
        
        input.initialize(conf, inputs);
        
        initializeOutputs(outputs);

        List<PhysicalOperator> roots = execPlan.getRoots();
        List<PhysicalOperator> leaves = execPlan.getLeaves();
        // TODO: Pull from all leaves when there are multiple leaves/outputs
        leaf = leaves.get(0);
        // TODO: Remove in favor of maps/indexed arrays in a multi-output world
        if (shuffle){
            keyType = ((POLocalRearrange)leaf).getKeyType();
        }

        while (input.next()){
            Tuple inputTuple = input.getCurrentTuple();
            for (PhysicalOperator root : roots) {
                root.attachInput(inputTuple);
            }

            runPipeline(leaf);
        }

        for (MROutput fileOutput : fileOutputs){
            fileOutput.commit();
        }
    }

    private  InputHandler createInputHandler(Configuration conf) throws PigException {
        Class<? extends InputHandler> inputClass;
        try {
            inputClass = (Class<? extends InputHandler>)
                    Class.forName(conf.get("pig.input.handler.class"));
            Constructor<? extends InputHandler> constructor = inputClass.getConstructor();
            return constructor.newInstance();
        } catch (Exception e) {
            throw new PigException("Could not instantiate input handler", e);
        }

    }

    private void initializeOutputs(Map<String, LogicalOutput> outputs) throws IOException{
        for (Entry<String, LogicalOutput> entry : outputs.entrySet()){
            String name = entry.getKey();
            LogicalOutput logicalOutput = entry.getValue();
            if (logicalOutput instanceof MROutput){
                MROutput mrOut = (MROutput) logicalOutput;
                fileOutputs.add(mrOut);
                writers.put(name, mrOut.getWriter());
                // Since we only have one output, we can cheat here
                shuffle = false;
            } else if (logicalOutput instanceof OnFileSortedOutput){
                OnFileSortedOutput onFileOut = (OnFileSortedOutput) logicalOutput;
                shuffleOutputs.add(onFileOut);
                writers.put(name, onFileOut.getWriter());
                shuffle = true;
            }
        }
    }

    protected void runPipeline(PhysicalOperator leaf) throws IOException, InterruptedException {
        while(true){
            Result res = leaf.getNextTuple();
            if(res.returnStatus==POStatus.STATUS_OK){
                writeResult((Tuple)res.result);
                continue;
            }

            if(res.returnStatus==POStatus.STATUS_EOP) {
                return;
            }

            if(res.returnStatus==POStatus.STATUS_NULL)
                continue;

            if(res.returnStatus==POStatus.STATUS_ERR){
                // remember that we had an issue so that in 
                // close() we can do the right thing
                //    errorInMap  = true;
                // if there is an errmessage use it
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

    private void writeResult(Tuple result) throws IOException {
        // For now we'll just have one output.
        if (shuffle){
            Byte index = (Byte)result.get(0);
            PigNullableWritable key =
                HDataType.getWritableComparableTypes(result.get(1), keyType);
            NullableTuple val = new NullableTuple((Tuple)result.get(2));

            // Both the key and the value need the index.  The key needs it so
            // that it can be sorted on the index in addition to the key
            // value.  The value needs it so that POPackage can properly
            // assign the tuple to its slot in the projection.
            key.setIndex(index);
            val.setIndex(index);
            writers.values().iterator().next().write(key, val);
        } else {
            writers.values().iterator().next().write(null, result);
        }
    }
}

