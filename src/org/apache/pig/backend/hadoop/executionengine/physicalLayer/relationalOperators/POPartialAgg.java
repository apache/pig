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
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.InternalCachedBag;
import org.apache.pig.data.SelfSpillBag.MemoryLimits;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.GroupingSpillable;
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
public class POPartialAgg extends PhysicalOperator implements Spillable, GroupingSpillable {
    private static final Log LOG = LogFactory.getLog(POPartialAgg.class);
    private static final long serialVersionUID = 1L;

    private static final Result EOP_RESULT = new Result(POStatus.STATUS_EOP,
            null);

    // number of records to sample to determine average size used by each
    // entry in hash map and average seen reduction
    private static final int NUM_RECS_TO_SAMPLE = 10000;

    // We want to allow bigger list sizes for group all.
    // But still have a cap on it to avoid JVM finding it hard to allocate space
    // TODO: How high can we go without performance degradation??
    private static final int MAX_LIST_SIZE = 25000;

    // We want to avoid massive ArrayList copies as they get big.
    // Array Lists grow by prevSize + prevSize/2. Given default initial size of 10,
    // 9369 is the size of the array after 18 such resizings. This seems like a sufficiently
    // large value to trigger spilling/aggregation instead of paying for yet another data
    // copy.
    // For group all cases, we will set this to a higher value
    private int listSizeThreshold = 9367;

    // Using default min reduction 7 instead of 10 as processedInputMap size
    // will be 4096 (hashmap size is power of 2) for both 20000/10 and 20000/7.
    // So using the lower number 7 as even 7x reduction is worth using map side aggregation
    private static final int DEFAULT_MIN_REDUCTION = 7;

    // TODO: these are temporary. The real thing should be using memory usage estimation.
    private static final int FIRST_TIER_THRESHOLD = 20000;
    private static final int SECOND_TIER_THRESHOLD = FIRST_TIER_THRESHOLD / DEFAULT_MIN_REDUCTION;

    private static final WeakHashMap<POPartialAgg, Byte> ALL_POPARTS = new WeakHashMap<POPartialAgg, Byte>();

    private static final TupleFactory TF = TupleFactory.getInstance();

    private PhysicalPlan keyPlan;
    private ExpressionOperator keyLeaf;
    private List<PhysicalPlan> valuePlans;
    private List<ExpressionOperator> valueLeaves;
    private boolean isGroupAll;

    private transient int numRecsInRawMap;
    private transient int numRecsInProcessedMap;

    private transient Map<Object, List<Tuple>> rawInputMap;
    private transient Map<Object, List<Tuple>> processedInputMap;

    //Transient booleans always initialize to false
    private transient boolean initialized;
    private transient boolean disableMapAgg;
    private transient boolean sizeReductionChecked;
    private transient boolean inputsExhausted;
    private transient boolean estimatedMemThresholds;
    // The doSpill flag is set when spilling is running or needs to run.
    // It is set by POPartialAgg when its buffers are full after having run aggregations and
    // the records have to be emitted to the map output.
    // The doContingentSpill flag is set when the SpillableMemoryManager is notified
    // by GC that the runtime is low on memory and the SpillableMemoryManager identifies
    // the particular buffer as a good spill candidate because it is large. The contingent spill logic tries
    // to satisfy the memory manager's request for freeing memory by aggregating data
    // rather than just spilling records to disk.
    private transient volatile boolean doSpill;
    private transient volatile boolean doContingentSpill;
    private transient volatile Object spillLock;

    private transient int minOutputReduction;
    private transient float percentUsage;
    private transient int numRecordsToSample;
    private transient int firstTierThreshold;
    private transient int secondTierThreshold;
    private transient int sizeReduction;
    private transient int avgTupleSize;
    private transient Iterator<Entry<Object, List<Tuple>>> spillingIterator;

    public POPartialAgg(OperatorKey k) {
        this(k, false);
    }

    public POPartialAgg(OperatorKey k, boolean isGroupAll) {
        super(k);
        this.isGroupAll = isGroupAll;
    }

    private void init() throws ExecException {
        ALL_POPARTS.put(this, null);
        numRecsInRawMap = 0;
        numRecsInProcessedMap = 0;
        minOutputReduction = DEFAULT_MIN_REDUCTION;
        numRecordsToSample = NUM_RECS_TO_SAMPLE;
        firstTierThreshold = FIRST_TIER_THRESHOLD;
        secondTierThreshold = SECOND_TIER_THRESHOLD;
        sizeReduction = 1;
        avgTupleSize = 0;
        percentUsage = 0.2F;
        spillLock = new Object();
        if (PigMapReduce.sJobConfInternal.get() != null) {
            String usage = PigMapReduce.sJobConfInternal.get().get(
                    PigConfiguration.PIG_CACHEDBAG_MEMUSAGE);
            if (usage != null) {
                percentUsage = Float.parseFloat(usage);
            }
            minOutputReduction = PigMapReduce.sJobConfInternal.get().getInt(
                    PigConfiguration.PIG_EXEC_MAP_PARTAGG_MINREDUCTION, DEFAULT_MIN_REDUCTION);
            if (minOutputReduction <= 0) {
                LOG.info("Specified reduction is < 0 (" + minOutputReduction + "). Using default " +
                        DEFAULT_MIN_REDUCTION);
                minOutputReduction = DEFAULT_MIN_REDUCTION;
            }
        }
        if (percentUsage <= 0) {
            LOG.info("No memory allocated to intermediate memory buffers. Turning off partial aggregation.");
            disableMapAgg = true;
            // Set them to true instead of adding another check for !disableMapAgg
            sizeReductionChecked = true;
            estimatedMemThresholds = true;
        }
        // Avoid hashmap resizing. TODO: Investigate loadfactor of 0.90 or 1.0
        // newHashMapWithExpectedSize does new HashMap(expectedSize + expectedSize/3)
        // to factor in default load factor of 0.75.
        // For Hashmap, internally its size is always in power of 2.
        // So for NUM_RECS_TO_SAMPLE=10000, hashmap size will be 16384
        // With secondTierThreshold of 2857 (minReduction 7), hashmap size will be 4096
        if (!disableMapAgg) {
            rawInputMap = Maps.newHashMapWithExpectedSize(NUM_RECS_TO_SAMPLE);
            processedInputMap = Maps.newHashMapWithExpectedSize(SECOND_TIER_THRESHOLD);
        }
        if (isGroupAll) {
            listSizeThreshold = Math.min(numRecordsToSample, MAX_LIST_SIZE);
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
            if (!sizeReductionChecked && numRecsInRawMap >= numRecordsToSample) {
                checkSizeReduction();
                if (doContingentSpill && !doSpill) {
                    LOG.info("Avoided emitting records during spill memory call.");
                    doContingentSpill = false;
                }
            }
            if (!estimatedMemThresholds && numRecsInRawMap >= numRecordsToSample) {
                estimateMemThresholds();
            }
            if (doContingentSpill) {
                // Don't aggregate if spilling. Avoid concurrent update of spilling iterator.
                if (doSpill == false) {
                    // SpillableMemoryManager requested a spill to reduce memory
                    // consumption. See if we can avoid it.
                    aggregateBothLevels(false, false);
                    if (shouldSpill()) {
                        startSpill(false);
                    } else {
                        LOG.info("Avoided emitting records during spill memory call.");
                        doContingentSpill = false;
                    }
                }
            }
            if (doSpill) {
                startSpill(true);
                Result result = spillResult();
                if (result.returnStatus == POStatus.STATUS_EOP) {
                    doSpill = false;
                    doContingentSpill = false;
                }
                if (result.returnStatus != POStatus.STATUS_EOP
                        || inputsExhausted) {
                    return result;
                }
            }
            if (mapAggDisabled()) {
                // disableMapAgg() sets doSpill, so we can't get here while there is still contents in the buffered maps.
                // if we get to this point, everything is flushed, so we can simply return the raw tuples from now on.
                if (rawInputMap != null) {
                    // Free up the maps for garbage collection
                    rawInputMap = null;
                    processedInputMap = null;
                }
                return processInput();
            } else {
                Result inp = processInput();
                if (inp.returnStatus == POStatus.STATUS_ERR) {
                    return inp;
                } else if (inp.returnStatus == POStatus.STATUS_EOP) {
                    if (parentPlan.endOfAllInput) {
                        // parent input is over. flush what we have.
                        inputsExhausted = true;
                        LOG.info("Spilling last bits.");
                        startSpill(true);
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

                    aggregateBothLevels(true, true);
                    if (shouldSpill()) {
                        startSpill(false); // next time around, we'll start emitting.
                    }
                }
            }
        }
    }

    private void estimateMemThresholds() {
        if (!mapAggDisabled()) {
            LOG.info("Getting mem limits; considering " + ALL_POPARTS.size()
                    + " POPArtialAgg objects." + " with memory percentage "
                    + percentUsage);
            MemoryLimits memLimits = new MemoryLimits(ALL_POPARTS.size(), percentUsage);
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
            // The second tier should at least allow one tuple before it tries to aggregate.
            // This code retains the total number of tuples in the buffer while guaranteeing
            // the second tier has at least one tuple.
            if (secondTierThreshold == 0) {
                secondTierThreshold += 1;
                firstTierThreshold -= 1;
            }
            if (isGroupAll) {
                listSizeThreshold = Math.min(firstTierThreshold, MAX_LIST_SIZE);
            }
        }
        estimatedMemThresholds = true;
    }

    private void checkSizeReduction() throws ExecException {
        if (!mapAggDisabled()) {
            int numBeforeReduction = numRecsInProcessedMap + numRecsInRawMap;
            aggregateBothLevels(false, false);
            int numAfterReduction = numRecsInProcessedMap + numRecsInRawMap;
            LOG.info("After reduction, processed map: " + numRecsInProcessedMap + "; raw map: " + numRecsInRawMap);
            LOG.info("Observed reduction factor: from " + numBeforeReduction +
                    " to " + numAfterReduction +
                    " => " + numBeforeReduction / numAfterReduction + ".");
            if ( numBeforeReduction / numAfterReduction < minOutputReduction) {
                LOG.info("Disabling in-memory aggregation, since observed reduction is less than " + minOutputReduction);
                disableMapAgg();
            }
            sizeReduction = numBeforeReduction / numAfterReduction;
            sizeReductionChecked = true;
        }

    }
    private void disableMapAgg() throws ExecException {
        // Do not aggregate as when disableMapAgg is called aggregation is
        // called and size reduction checked
        startSpill(false);
        disableMapAgg = true;
    }

    private boolean mapAggDisabled() {
        return disableMapAgg;
    }

    private boolean shouldAggregateFirstLevel() {
        return (numRecsInRawMap > firstTierThreshold);
    }

    private boolean shouldAggregateSecondLevel() {
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
            if (isGroupAll) {
                // Set exact array initial size to avoid array copies
                // listSizeThreshold = numRecordsToSample before estimating memory
                // thresholds and firstTierThreshold after memory estimation
                int listSize = (map == rawInputMap) ? listSizeThreshold : Math.min(secondTierThreshold, MAX_LIST_SIZE);
                value = new ArrayList<Tuple>(listSize);
            } else {
                value = new ArrayList<Tuple>();
            }
            map.put(key, value);
        }
        value.add(inpTuple);
        if (value.size() > listSizeThreshold) {
            boolean isFirst = (map == rawInputMap);
            if (LOG.isDebugEnabled()){
                LOG.debug("The cache for key " + key + " has grown too large. Aggregating " + ((isFirst) ? "first level." : "second level."));
            }
            if (isFirst) {
                // Aggregate and remove just this key to keep size in check
                aggregateRawRow(key, value);
            } else {
                aggregateSecondLevel();
            }
        }
    }

    private void startSpill(boolean aggregate) throws ExecException {
        // If spillingIterator is null, we are already spilling and don't need to set up.
        if (spillingIterator != null) return;

        LOG.info("Starting spill.");
        if (aggregate) {
            aggregateBothLevels(false, true);
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

    private void aggregateRawRow(Object key, List<Tuple> value) throws ExecException {
        numRecsInRawMap -= value.size();
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

    private void aggregateBothLevels(boolean checkThresholdForFirst,
            boolean checkThresholdForSecond) throws ExecException {
        // When processed map is initially empty, just aggregate first level as
        // aggregating second level immediately would not yield anything
        boolean aggregateSecondLevel = !processedInputMap.isEmpty();
        if (!checkThresholdForFirst || shouldAggregateFirstLevel()) {
            aggregateFirstLevel();
        }
        if (aggregateSecondLevel && (!checkThresholdForSecond || shouldAggregateSecondLevel())) {
            aggregateSecondLevel();
        }
    }

    private void aggregateFirstLevel() throws ExecException {
        if (rawInputMap.isEmpty()) {
            return;
        }
        int rawTuples = numRecsInRawMap;
        int processedTuples = numRecsInProcessedMap;
        numRecsInProcessedMap = aggregate(rawInputMap, processedInputMap, numRecsInProcessedMap);
        numRecsInRawMap = 0;
        LOG.info("Aggregated " + rawTuples+ " raw tuples."
                + " Processed tuples before aggregation = " + processedTuples
                + ", after aggregation = " + numRecsInProcessedMap);
    }

    private void aggregateSecondLevel() throws ExecException {
        if (processedInputMap.isEmpty()) {
            return;
        }
        int processedTuples = numRecsInProcessedMap;
        Map<Object, List<Tuple>> newMap = Maps.newHashMapWithExpectedSize(processedInputMap.size());
        numRecsInProcessedMap = aggregate(processedInputMap, newMap, 0);
        processedInputMap = newMap;
        LOG.info("Aggregated " + processedTuples + " processed tuples to " + numRecsInProcessedMap + " tuples");
    }

    private Tuple createValueTuple(Object key, List<Tuple> inpTuples) throws ExecException {
        Tuple valueTuple = TF.newTuple(valuePlans.size() + 1);
        valueTuple.set(0, key);

        for (int i = 0; i < valuePlans.size(); i++) {
            DataBag bag = null;
            if (doContingentSpill) {
                // Don't use additional memory since we already have memory stress
                bag = new InternalCachedBag();
            } else {
                // Take 10% of memory, need fine tune later
                bag = new InternalCachedBag(1, 0.1F);
            }
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
        if (mapAggDisabled()) {
            return 0;
        } else {
            LOG.info("Spill triggered by SpillableMemoryManager");
            doContingentSpill = true;
            synchronized(spillLock) {
                if (!sizeReductionChecked) {
                    numRecordsToSample = numRecsInRawMap;
                }
                try {
                    while (doContingentSpill == true) {
                        Thread.sleep(50); //Keeping it on the lower side for now. Tune later
                    }
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted exception while waiting for spill to finish", e);
                }
                LOG.info("Finished spill for SpillableMemoryManager call");
                return 1;
            }
        }
    }

    @Override
    public long getMemorySize() {
        return avgTupleSize * (numRecsInProcessedMap + numRecsInRawMap);
    }

}
