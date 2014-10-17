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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.WeakHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.SelfSpillBag.MemoryLimits;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Spillable;
import org.apache.pig.impl.util.SpillableMemoryManager;

import com.google.common.collect.Maps;

/**
 * Do partial aggregation in map plan. Inputs are buffered up in
 * a hashmap until a threshold is reached; then the combiner functions
 * are fed these buffered up inputs, and results stored in a secondary
 * map. Once that map fills up or all input has been seen, results are
 * piped out into the next operator (caller of getNext()).
 */
public class POPartialAgg extends PhysicalOperator implements Spillable {
    private static final Log LOG = LogFactory.getLog(POPartialAgg.class);
    private static final long serialVersionUID = 1L;

    private static final Result EOP_RESULT = new Result(POStatus.STATUS_EOP,
            null);

    // number of records to sample to determine average size used by each
    // entry in hash map and average seen reduction
    private static final int NUM_RECS_TO_SAMPLE = 10000;

    // We want to avoid massive ArrayList copies as they get big.
    // Array Lists grow by prevSize + prevSize/2. Given default initial size of 10,
    // 9369 is the size of the array after 18 such resizings. This seems like a sufficiently
    // large value to trigger spilling/aggregation instead of paying for yet another data
    // copy.
    private static final int MAX_LIST_SIZE = 9368;

    private static final int DEFAULT_MIN_REDUCTION = 10;

    // TODO: these are temporary. The real thing should be using memory usage estimation.
    private static final int FIRST_TIER_THRESHOLD = 20000;
    private static final int SECOND_TIER_THRESHOLD = FIRST_TIER_THRESHOLD / DEFAULT_MIN_REDUCTION;

    private static final WeakHashMap<POPartialAgg, Byte> ALL_POPARTS = new WeakHashMap<POPartialAgg, Byte>();

    private static final TupleFactory TF = TupleFactory.getInstance();
    private static final BagFactory BG = BagFactory.getInstance();

    private PhysicalPlan keyPlan;
    private ExpressionOperator keyLeaf;

    private List<PhysicalPlan> valuePlans;
    private List<ExpressionOperator> valueLeaves;

    private int numRecsInRawMap = 0;
    private int numRecsInProcessedMap = 0;

    private Map<Object, List<Tuple>> rawInputMap = Maps.newHashMap();
    private Map<Object, List<Tuple>> processedInputMap = Maps.newHashMap();

    private boolean disableMapAgg = false;
    private boolean sizeReductionChecked = false;
    private boolean inputsExhausted = false;
    private volatile boolean doSpill = false;
    private transient MemoryLimits memLimits;

    private transient boolean initialized = false;
    private int firstTierThreshold = FIRST_TIER_THRESHOLD;
    private int secondTierThreshold = SECOND_TIER_THRESHOLD;
    private int sizeReduction = 1;
    private int avgTupleSize = 0;
    private Iterator<Entry<Object, List<Tuple>>> spillingIterator;
    private boolean estimatedMemThresholds = false;


    public POPartialAgg(OperatorKey k) {
        super(k);
    }

    private void init() throws ExecException {
        ALL_POPARTS.put(this, null);
        float percent = getPercentUsageFromProp();
        if (percent <= 0) {
            LOG.info("No memory allocated to intermediate memory buffers. Turning off partial aggregation.");
            disableMapAgg();
        }
        initialized = true;
        SpillableMemoryManager.getInstance().registerSpillable(this);
    }

    @Override
    public Result getNextTuple() throws ExecException {
        // accumulate tuples from processInput in rawInputMap.
        // when the maps grow to mem limit, go over each item in map, and call
        // combiner aggs on each collection.
        // Store the results into processedInputMap. Clear out rawInputMap.
        // Mem usage is updated every time we modify either of the maps.
        // When processedInputMap is >= 20% of allotted memory, run aggs on it,
        // and output the results as returns of successive calls of this method.
        // Then reset processedInputMap.
        // The fact that we are in the latter stage is communicated via the doSpill
        // flag.

        if (!initialized && !ALL_POPARTS.containsKey(this)) {
            init();
        }

        while (true) {
            if (!sizeReductionChecked && numRecsInRawMap >= NUM_RECS_TO_SAMPLE) {
                checkSizeReduction();
            }
            if (!estimatedMemThresholds && numRecsInRawMap >= NUM_RECS_TO_SAMPLE) {
                estimateMemThresholds();
            }
            if (doSpill) {
                startSpill();
                Result result = spillResult();
                if (result.returnStatus == POStatus.STATUS_EOP) {
                    doSpill = false;
                }
                if (result.returnStatus != POStatus.STATUS_EOP
                        || inputsExhausted) {
                    return result;
                }
            }
            if (mapAggDisabled()) {
                // disableMapAgg() sets doSpill, so we can't get here while there is still contents in the buffered maps.
                // if we get to this point, everything is flushed, so we can simply return the raw tuples from now on.
                return processInput();
            } else {
                Result inp = processInput();
                if (inp.returnStatus == POStatus.STATUS_ERR) {
                    return inp;
                } else if (inp.returnStatus == POStatus.STATUS_EOP) {
                    if (parentPlan.endOfAllInput) {
                        // parent input is over. flush what we have.
                        inputsExhausted = true;
                        startSpill();
                        LOG.info("Spilling last bits.");
                        continue;
                    } else {
                        return EOP_RESULT;
                    }
                } else if (inp.returnStatus == POStatus.STATUS_NULL) {
                    continue;
                } else {
                    // add this input to map.
                    Tuple inpTuple = (Tuple) inp.result;
                    keyPlan.attachInput(inpTuple);

                    // evaluate the key
                    Result keyRes = getResult(keyLeaf);
                    if (keyRes.returnStatus != POStatus.STATUS_OK) {
                        return keyRes;
                    }
                    Object key = keyRes.result;
                    keyPlan.detachInput();
                    numRecsInRawMap += 1;
                    addKeyValToMap(rawInputMap, key, inpTuple);

                    if (shouldAggregateFirstLevel()) {
                        aggregateFirstLevel();
                    }
                    if (shouldAggregateSecondLevel()) {
                        aggregateSecondLevel();
                    }
                    if (shouldSpill()) {
                        LOG.info("Starting spill.");
                        startSpill(); // next time around, we'll start emitting.
                    }
                }
            }
        }
    }

    private void estimateMemThresholds() {
        if (!mapAggDisabled()) {
            LOG.info("Getting mem limits; considering " + ALL_POPARTS.size() + " POPArtialAgg objects.");

            float percent = getPercentUsageFromProp();
            memLimits = new MemoryLimits(ALL_POPARTS.size(), percent);
            int estTotalMem = 0;
            int estTuples = 0;
            for (Map.Entry<Object, List<Tuple>> entry : rawInputMap.entrySet()) {
                for (Tuple t : entry.getValue()) {
                    estTuples += 1;
                    int mem = (int) t.getMemorySize();
                    estTotalMem += mem;
                    memLimits.addNewObjSize(mem);
                }
            }
            avgTupleSize = estTotalMem / estTuples;
            long totalTuples = memLimits.getCacheLimit();
            LOG.info("Estimated total tuples to buffer, based on " + estTuples + " tuples that took up " + estTotalMem + " bytes: " + totalTuples);
            firstTierThreshold = (int) (0.5 + totalTuples * (1f - (1f / sizeReduction)));
            secondTierThreshold = (int) (0.5 + totalTuples *  (1f / sizeReduction));
            LOG.info("Setting thresholds. Primary: " + firstTierThreshold + ". Secondary: " + secondTierThreshold);
        }
        estimatedMemThresholds = true;
    }

    private void checkSizeReduction() throws ExecException {
        int numBeforeReduction = numRecsInProcessedMap + numRecsInRawMap;
        aggregateFirstLevel();
        aggregateSecondLevel();
        int numAfterReduction = numRecsInProcessedMap + numRecsInRawMap;
        LOG.info("After reduction, processed map: " + numRecsInProcessedMap + "; raw map: " + numRecsInRawMap);
        int minReduction = getMinOutputReductionFromProp();
        LOG.info("Observed reduction factor: from " + numBeforeReduction +
                " to " + numAfterReduction +
                " => " + numBeforeReduction / numAfterReduction + ".");
        if ( numBeforeReduction / numAfterReduction < minReduction) {
            LOG.info("Disabling in-memory aggregation, since observed reduction is less than " + minReduction);
            disableMapAgg();
        }
        sizeReduction = numBeforeReduction / numAfterReduction;
        sizeReductionChecked = true;

    }
    private void disableMapAgg() throws ExecException {
        startSpill();
        disableMapAgg = true;
    }

    private boolean mapAggDisabled() {
        return disableMapAgg;
    }

    private boolean shouldAggregateFirstLevel() {
        if (LOG.isInfoEnabled() && numRecsInRawMap > firstTierThreshold) {
            LOG.info("Aggregating " + numRecsInRawMap + " raw records.");
        }
        return (numRecsInRawMap > firstTierThreshold);
    }

    private boolean shouldAggregateSecondLevel() {
        if (LOG.isInfoEnabled() && numRecsInProcessedMap > secondTierThreshold) {
            LOG.info("Aggregating " + numRecsInProcessedMap + " secondary records.");
        }
        return (numRecsInProcessedMap > secondTierThreshold);
    }

    private boolean shouldSpill() {
        // is this always the same as shouldAgg?
        return shouldAggregateSecondLevel();
    }

    private void addKeyValToMap(Map<Object, List<Tuple>> map,
            Object key, Tuple inpTuple) throws ExecException {
        List<Tuple> value = map.get(key);
        if (value == null) {
            value = new ArrayList<Tuple>();
            map.put(key, value);
        }
        value.add(inpTuple);
        if (value.size() >= MAX_LIST_SIZE) {
            boolean isFirst = (map == rawInputMap);
            if (LOG.isDebugEnabled()){
                LOG.debug("The cache for key " + key + " has grown too large. Aggregating " + ((isFirst) ? "first level." : "second level."));
            }
            if (isFirst) {
                aggregateRawRow(key);
            } else {
                aggregateSecondLevel();
            }
        }
    }

    private void startSpill() throws ExecException {
        // If spillingIterator is null, we are already spilling and don't need to set up.
        if (spillingIterator != null) return;

        if (!rawInputMap.isEmpty()) {
            if (LOG.isInfoEnabled()) {
                LOG.info("In startSpill(), aggregating raw inputs. " + numRecsInRawMap + " tuples.");
            }
            aggregateFirstLevel();
            if (LOG.isInfoEnabled()) {
                LOG.info("processed inputs: " + numRecsInProcessedMap + " tuples.");
            }
        }
        if (!processedInputMap.isEmpty()) {
            if (LOG.isInfoEnabled()) {
                LOG.info("In startSpill(), aggregating processed inputs. " + numRecsInProcessedMap + " tuples.");
            }
            aggregateSecondLevel();
            if (LOG.isInfoEnabled()) {
                LOG.info("processed inputs: " + numRecsInProcessedMap + " tuples.");
            }
        }
        doSpill = true;
        spillingIterator = processedInputMap.entrySet().iterator();
    }

    private Result spillResult() throws ExecException {
        // if no more to spill, return EOP_RESULT.
        if (processedInputMap.isEmpty()) {
            spillingIterator = null;
            LOG.info("In spillResults(), processed map is empty -- done spilling.");
            return EOP_RESULT;
        } else {
            Map.Entry<Object, List<Tuple>> entry = spillingIterator.next();
            Tuple valueTuple = createValueTuple(entry.getKey(), entry.getValue());
            numRecsInProcessedMap -= entry.getValue().size();
            spillingIterator.remove();
            Result res = getOutput(entry.getKey(), valueTuple);
            return res;
        }
    }

    private void aggregateRawRow(Object key) throws ExecException {
        List<Tuple> value = rawInputMap.get(key);
        Tuple valueTuple = createValueTuple(key, value);
        Result res = getOutput(key, valueTuple);
        rawInputMap.remove(key);
        addKeyValToMap(processedInputMap, key, getAggResultTuple(res.result));
        numRecsInProcessedMap++;
    }

    /**
     * For each entry in rawInputMap, feed the list of tuples into the aggregator funcs
     * and add the results to processedInputMap. Remove the entries from rawInputMap as we go.
     * @throws ExecException
     */
    private int aggregate(Map<Object, List<Tuple>> fromMap, Map<Object, List<Tuple>> toMap, int numEntriesInTarget) throws ExecException {
        Iterator<Map.Entry<Object, List<Tuple>>> iter = fromMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Object, List<Tuple>> entry = iter.next();
            Tuple valueTuple = createValueTuple(entry.getKey(), entry.getValue());
            Result res = getOutput(entry.getKey(), valueTuple);
            iter.remove();
            addKeyValToMap(toMap, entry.getKey(), getAggResultTuple(res.result));
            numEntriesInTarget++;
        }
        return numEntriesInTarget;
    }

    private void aggregateFirstLevel() throws ExecException {
        numRecsInProcessedMap = aggregate(rawInputMap, processedInputMap, numRecsInProcessedMap);
        numRecsInRawMap = 0;
    }

    private void aggregateSecondLevel() throws ExecException {
        Map<Object, List<Tuple>> newMap = Maps.newHashMapWithExpectedSize(processedInputMap.size());
        numRecsInProcessedMap = aggregate(processedInputMap, newMap, 0);
        processedInputMap = newMap;
    }

    private Tuple createValueTuple(Object key, List<Tuple> inpTuples) throws ExecException {
        Tuple valueTuple = TF.newTuple(valuePlans.size() + 1);
        valueTuple.set(0, key);

        for (int i = 0; i < valuePlans.size(); i++) {
            DataBag bag = BG.newDefaultBag();
            valueTuple.set(i + 1, bag);
        }
        for (Tuple t : inpTuples) {
            for (int i = 1; i < t.size(); i++) {
                DataBag bag = (DataBag) valueTuple.get(i);
                bag.add((Tuple) t.get(i));
            }
        }

        return valueTuple;
    }

    private Tuple getAggResultTuple(Object result) throws ExecException {
        try {
            return (Tuple) result;
        } catch (ClassCastException ex) {
            throw new ExecException("Intermediate Algebraic "
                    + "functions must implement EvalFunc<Tuple>");
        }
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        // combiner optimizer does not get invoked if the plan is being executed
        // under illustrate, so POPartialAgg should not get used in that case
        throw new UnsupportedOperationException();
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitPartialAgg(this);
    }

    private int getMinOutputReductionFromProp() {
        int minReduction = PigMapReduce.sJobConfInternal.get().getInt(
                PigConfiguration.PARTAGG_MINREDUCTION, DEFAULT_MIN_REDUCTION);
        if (minReduction <= 0) {
            LOG.info("Specified reduction is < 0 (" + minReduction + "). Using default " + DEFAULT_MIN_REDUCTION);
            minReduction = DEFAULT_MIN_REDUCTION;
        }
        return minReduction;
    }

    private float getPercentUsageFromProp() {
        float percent = 0.2F;
        if (PigMapReduce.sJobConfInternal.get() != null) {
            String usage = PigMapReduce.sJobConfInternal.get().get(
                    PigConfiguration.PROP_CACHEDBAG_MEMUSAGE);
            if (usage != null) {
                percent = Float.parseFloat(usage);
            }
        }
        return percent;
    }


    private Result getResult(ExpressionOperator op) throws ExecException {
        Result res;
        switch (op.getResultType()) {
        case DataType.BAG:
        case DataType.BOOLEAN:
        case DataType.BYTEARRAY:
        case DataType.CHARARRAY:
        case DataType.DOUBLE:
        case DataType.FLOAT:
        case DataType.INTEGER:
        case DataType.LONG:
        case DataType.BIGINTEGER:
        case DataType.BIGDECIMAL:
        case DataType.DATETIME:
        case DataType.MAP:
        case DataType.TUPLE:
            res = op.getNext(op.getResultType());
            break;
        default:
            String msg = "Invalid result type: "
                    + DataType.findType(op.getResultType());
            throw new ExecException(msg, 2270, PigException.BUG);
        }

        return res;
    }

    /**
     * Runs the provided key-value pair through the aggregator plans.
     * @param key
     * @param value
     * @return Result, containing a tuple of form (key, tupleReturnedByPlan1, tupleReturnedByPlan2, ...)
     * @throws ExecException
     */
    private Result getOutput(Object key, Tuple value) throws ExecException {
        Tuple output = TF.newTuple(valuePlans.size() + 1);
        output.set(0, key);

        for (int i = 0; i < valuePlans.size(); i++) {
            valuePlans.get(i).attachInput(value);
            Result valRes = getResult(valueLeaves.get(i));
            if (valRes.returnStatus == POStatus.STATUS_ERR) {
                return valRes;
            }
            output.set(i + 1, valRes.result);
        }
        return new Result(POStatus.STATUS_OK, output);
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    @Override
    public String name() {
        return getAliasString() + "Partial Agg" + "["
                + DataType.findTypeName(resultType) + "]" + mKey.toString();

    }

    public PhysicalPlan getKeyPlan() {
        return keyPlan;
    }

    public void setKeyPlan(PhysicalPlan keyPlan) {
        this.keyPlan = keyPlan;
        keyLeaf = (ExpressionOperator) keyPlan.getLeaves().get(0);
    }

    public List<PhysicalPlan> getValuePlans() {
        return valuePlans;
    }

    public void setValuePlans(List<PhysicalPlan> valuePlans) {
        this.valuePlans = valuePlans;
        valueLeaves = new ArrayList<ExpressionOperator>();
        for (PhysicalPlan plan : valuePlans) {
            valueLeaves.add((ExpressionOperator) plan.getLeaves().get(0));
        }
    }

    @Override
    public long spill() {
        LOG.info("Spill triggered by SpillableMemoryManager");
        doSpill = true;
        return 0;
    }

    @Override
    public long getMemorySize() {
        return avgTupleSize * (numRecsInProcessedMap + numRecsInRawMap);
    }

}
