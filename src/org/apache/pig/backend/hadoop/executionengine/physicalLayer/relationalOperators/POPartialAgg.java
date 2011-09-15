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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.SelfSpillBag.MemoryLimits;
import org.apache.pig.data.SizeUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

/**
 * Do partial aggregation in map plan. It uses a hash-map to aggregate. If
 * consecutive records have same key, it will aggregate those without adding
 * them to the hash-map. As future optimization, the use of hash-map could be
 * disabled when input data is sorted on group-by keys
 */
public class POPartialAgg extends PhysicalOperator {

    public static final String PROP_PARTAGG_MINREDUCTION = "pig.exec.mapPartAgg.minReduction";

    private static final Log log = LogFactory.getLog(POPartialAgg.class);
    private static final long serialVersionUID = 1L;

    private PhysicalPlan keyPlan;
    private ExpressionOperator keyLeaf;

    private List<PhysicalPlan> valuePlans;
    private List<ExpressionOperator> valueLeaves;
    private static final Result ERR_RESULT = new Result();
    private static final Result EOP_RESULT = new Result(POStatus.STATUS_EOP,
            null);

    // run time variables
    private transient Object currentKey = null;
    private transient Map<Object, Tuple> aggMap;
    // tuple of the format - (null(key),bag-val1,bag-val2,...)
    // attach this to the plans with algebraic udf before evaluating the plans
    private transient Tuple valueTuple = null;

    private boolean isFinished = false;

    private transient Iterator<Tuple> mapDumpIterator;
    private transient int numToDump;

    // maximum bag size of currentValues cached before aggregation is done
    private static final int MAX_SIZE_CURVAL_CACHE = 1024;

    // number of records to sample to determine average size used by each
    // entry in hash map
    private static final int NUM_RESRECS_TO_SAMPLE_SZ_ESTIMATE = 100;

    // params for auto disabling map aggregation
    private static final int NUM_INPRECS_TO_SAMPLE_SZ_REDUCTION = 1000;

    private static final int DEFAULT_MIN_REDUCTION = 10;

    private boolean disableMapAgg = false;
    private int num_inp_recs;
    private boolean sizeReductionChecked = false;

    private transient int maxHashMapSize;

    private transient TupleFactory tupleFact;
    private transient MemoryLimits memLimits;

    public POPartialAgg(OperatorKey k) {
        super(k);
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

    @Override
    public Result getNext(Tuple t) throws ExecException {

        if (disableMapAgg) {
            // map aggregation has been automatically disabled
            if (mapDumpIterator != null) {
                // there are some accumulated entries in map to be dumped
                return getNextResFromMap();
            } else {
                Result inp = processInput();
                if (disableMapAgg) {
                    // the in-map partial aggregation is an optional step, just
                    // like the combiner.
                    // act as if this operator was never there, by just 
                    // returning the input
                    return inp;
                }
            }
        }

        if (mapDumpIterator != null) {
            // if this iterator is not null, we are process of dumping records
            // from the map
            if (isFinished) {
                return getNextResFromMap();
            } else if (numToDump > 0) {
                // there are some tuples yet to be dumped, to free memory
                --numToDump;
                return getNextResFromMap();
            } else {
                mapDumpIterator = null;
            }
        }

        if (isFinished) {
            // done with dumping all records
            return new Result(POStatus.STATUS_EOP, null);
        }

        while (true) {
            //process each input until EOP
            Result inp = processInput();
            if (inp.returnStatus == POStatus.STATUS_ERR) {
                // error
                return inp;
            }
            if (inp.returnStatus == POStatus.STATUS_EOP) {
                if (parentPlan.endOfAllInput) {
                    // it is actually end of all input
                    // start dumping results
                    isFinished = true;
                    logCapacityOfAggMap();
                    // check if there was ANY input
                    if (valueTuple == null) {
                        return EOP_RESULT;
                    }

                    // first return agg for currentKey
                    Result output = getOutput();
                    aggMap.remove(currentKey);

                    mapDumpIterator = aggMap.values().iterator();

                    // free the variables not needed anymore
                    currentKey = null;
                    valueTuple = null;

                    return output;
                } else {
                    // return EOP
                    return inp;
                }
            }
            if (inp.returnStatus == POStatus.STATUS_NULL) {
                continue;
            }

            // check if this operator is doing a good job of reducing the number
            // of records going to output to justify the costs of itself
            // if not , disable map partial agg
            if ((!sizeReductionChecked)) {
                checkSizeReduction();

                if (disableMapAgg) {
                    // in-map partial aggregation just got disabled
                    // return the new input record, it has not been aggregated
                    return inp;
                }
            }

            // we have some real input data

            // setup input for keyplan
            Tuple inpTuple = (Tuple) inp.result;
            keyPlan.attachInput(inpTuple);

            // evaluate the key
            Result keyRes = getResult(keyLeaf);
            if (keyRes == ERR_RESULT) {
                return ERR_RESULT;
            }
            Object key = keyRes.result;
            keyPlan.detachInput();

            if (valueTuple == null) {
                // this is the first record the operator is seeing
                // do initializations
                init(key, inpTuple);
                continue;
            } else {
                // check if key changed
                boolean keyChanged = (currentKey != null && key == null)
                        || ((key != null) && (!key.equals(currentKey)));

                if (!keyChanged) {
                    addToCurrentValues(inpTuple);

                    // if there are enough number of values,
                    // aggregate the values accumulated in valueTuple
                    if (((DefaultDataBag) valueTuple.get(1)).size() >= MAX_SIZE_CURVAL_CACHE) {
                        // not a key change, so store the agg result back to bag
                        aggregateCurrentValues();
                    }
                    continue;
                } else {// new key

                    // compute aggregate for currentKey
                    Result output = getOutput();
                    if (output.returnStatus != POStatus.STATUS_OK) {
                        return ERR_RESULT;
                    }
                    
                    // set new current key, value
                    currentKey = key;
                    resetCurrentValues();
                    addToCurrentValues(inpTuple);

                    // get existing result from map (if any) and add it to
                    // current values
                    Tuple existingResult = aggMap.get(key);

                    // existingResult will be null only if key is absent in
                    // aggMap
                    if (existingResult != null) {
                        addToCurrentValues(existingResult);
                    }

                    // storing a new entry in the map, so update estimate of
                    // num of entries that will fit into the map
                    if (memLimits.getNumObjectsSizeAdded() < NUM_RESRECS_TO_SAMPLE_SZ_ESTIMATE) {
                        updateMaxMapSize(output.result);
                    }

                    // check if it is time to dump some aggs from the hashmap
                    if (aggMap.size() >= maxHashMapSize) {
                        // dump 10% of max hash size because dumping just one
                        // record at a time might result in most group key being
                        // dumped (depending on hashmap implementation)
                        // TODO: dump the least recently/frequently used entries
                        numToDump = maxHashMapSize / 10;
                        mapDumpIterator = aggMap.values().iterator();

                        return output;
                    } else {
                        // there is space available in the hashmap, store the
                        // output there
                        addOutputToAggMap(output);
                    }

                    continue;
                }
            }
        }
    }

    private void updateMaxMapSize(Object result) {
        long size = SizeUtil.getMapEntrySize(currentKey,
                result);
        memLimits.addNewObjSize(size);
        maxHashMapSize = memLimits.getCacheLimit();
    }

    /**
     * Aggregate values accumulated in
     * 
     * @throws ExecException
     */
    private void aggregateCurrentValues() throws ExecException {
        for (int i = 0; i < valuePlans.size(); i++) {
            valuePlans.get(i).attachInput(valueTuple);
            Result valRes = getResult(valueLeaves.get(i));
            if (valRes == ERR_RESULT) {
                throw new ExecException(
                        "Error computing aggregate during in-map partial aggregation");
            }

            Tuple aggVal = getAggResultTuple(valRes.result);

            // i'th plan should read only from i'th bag
            // so we are done with i'th bag, clear it and
            // add the new agg result to it
            DataBag valBag = (DataBag) valueTuple.get(i + 1);
            valBag.clear();
            valBag.add(aggVal);

            valuePlans.get(i).detachInput();
        }
    }

    private void init(Object key, Tuple inpTuple) throws ExecException {
        tupleFact = TupleFactory.getInstance();

        // value tuple has bags of values for currentKey
        valueTuple = tupleFact.newTuple(valuePlans.size() + 1);

        for (int i = 0; i < valuePlans.size(); i++) {
            valueTuple.set(i + 1, new DefaultDataBag(new ArrayList<Tuple>(
                    MAX_SIZE_CURVAL_CACHE)));
        }

        // set current key, add value
        currentKey = key;
        addToCurrentValues(inpTuple);
        aggMap = new HashMap<Object, Tuple>();

        // TODO: keep track of actual number of objects that share the
        // memory limit. For now using a default of 3, which is what is
        // used by InternalCachedBag
        memLimits = new MemoryLimits(3, -1);
        maxHashMapSize = Integer.MAX_VALUE;

    }

    private Tuple getAggResultTuple(Object result) throws ExecException {
        try {
            return (Tuple) result;
        } catch (ClassCastException ex) {
            throw new ExecException("Intermediate Algebraic "
                    + "functions must implement EvalFunc<Tuple>");
        }
    }

    private void checkSizeReduction() throws ExecException {

        num_inp_recs++;
        if (num_inp_recs == NUM_INPRECS_TO_SAMPLE_SZ_REDUCTION
                || (aggMap != null && aggMap.size() == maxHashMapSize - 1)) {
            // the above check for the hashmap current size is
            // done to avoid having to keep track of any dumps that
            // could
            // happen before NUM_INPRECS_TO_SAMPLE_SZ_REDUCTION is
            // reached

            sizeReductionChecked = true;

            // find out how many output records we have for this many
            // input records

            int outputReduction = aggMap.size() == 0 ? Integer.MAX_VALUE
                    : num_inp_recs / aggMap.size();
            int min_output_reduction = getMinOutputReductionFromProp();
            if (outputReduction < min_output_reduction) {
                disableMapAgg = true;
                log.info("Disabling in-map partial aggregation because the "
                        + "reduction in tuples (" + outputReduction
                        + ") is lower than threshold (" + min_output_reduction
                        + ")");
                logCapacityOfAggMap();
                // get current key vals output
                Result output = getOutput();

                // free the variables not needed anymore
                currentKey = null;
                valueTuple = null;

                // store the output into hash map for now
                addOutputToAggMap(output);

                mapDumpIterator = aggMap.values().iterator();
            }
        }

    }

    private void logCapacityOfAggMap() {
        log.info("Maximum capacity of hashmap used for map"
                + " partial aggregation was " + maxHashMapSize + " entries");
    }

    private void addOutputToAggMap(Result output) throws ExecException {
        aggMap.put(((Tuple) output.result).get(0), (Tuple) output.result);
    }

    private int getMinOutputReductionFromProp() {
        int minReduction = PigMapReduce.sJobConfInternal.get().getInt(
                PROP_PARTAGG_MINREDUCTION, 0);
     
        if (minReduction <= 0) {
            // the default minimum reduction is 10
            minReduction = DEFAULT_MIN_REDUCTION;
        }
        return minReduction;
    }

    private Result getNextResFromMap() {
        if (!mapDumpIterator.hasNext()) {
            mapDumpIterator = null;
            return EOP_RESULT;
        }
        Tuple outTuple = mapDumpIterator.next();
        mapDumpIterator.remove();
        return new Result(POStatus.STATUS_OK, outTuple);
    }

    private Result getOutput() throws ExecException {
        Tuple output = tupleFact.newTuple(valuePlans.size() + 1);
        output.set(0, currentKey);

        for (int i = 0; i < valuePlans.size(); i++) {
            valuePlans.get(i).attachInput(valueTuple);
            Result valRes = getResult(valueLeaves.get(i));
            if (valRes == ERR_RESULT) {
                return ERR_RESULT;
            }
            output.set(i + 1, valRes.result);
        }
        return new Result(POStatus.STATUS_OK, output);
    }

    private void resetCurrentValues() throws ExecException {
        for (int i = 1; i < valueTuple.size(); i++) {
            ((DataBag) valueTuple.get(i)).clear();
        }
    }

    private void addToCurrentValues(Tuple inpTuple) throws ExecException {
        for (int i = 1; i < inpTuple.size(); i++) {
            DataBag bag = (DataBag) valueTuple.get(i);
            bag.add((Tuple) inpTuple.get(i));
        }
    }

    private Result getResult(ExpressionOperator op) throws ExecException {
        Result res = ERR_RESULT;

        switch (op.getResultType()) {
        case DataType.BAG:
        case DataType.BOOLEAN:
        case DataType.BYTEARRAY:
        case DataType.CHARARRAY:
        case DataType.DOUBLE:
        case DataType.FLOAT:
        case DataType.INTEGER:
        case DataType.LONG:
        case DataType.MAP:
        case DataType.TUPLE:
            res = op.getNext(getDummy(op.getResultType()), op.getResultType());
            break;
        default:
            String msg = "Invalid result type: "
                    + DataType.findType(op.getResultType());
            throw new ExecException(msg, 2270, PigException.BUG);
        }

        // allow null as group by key
        if (res.returnStatus == POStatus.STATUS_OK
                || res.returnStatus == POStatus.STATUS_NULL) {
            return res;
        }
        return ERR_RESULT;
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

}
