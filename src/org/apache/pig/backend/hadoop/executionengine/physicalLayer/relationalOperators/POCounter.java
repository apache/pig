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

import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduceCounter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.pen.util.LineageTracer;

/**
 * This operator is part of the RANK operator implementation.
 * It adds a local counter and a unique task id to each tuple.
 * There are 2 modes of operations: regular and dense.
 * The local counter is depends on the mode of operation.
 * With regular rank is considered duplicate rows while assigning
 * numbers to distinct values groups.
 * With dense rank counts the number of distinct values, without
 * considering duplicate rows. Depending on if it is considered.
 * the entire tuple (row number) or a by a set of columns (rank by).
 *
 * This Physical Operator relies on some specific MR class,
 * available at PigMapReduceCounter.
 **/

public class POCounter extends PhysicalOperator {

    private static final long serialVersionUID = 1L;
    private static final Long ONE = 1L;

    private List<PhysicalPlan> counterPlans;
    private List<Boolean> mAscCols;

    /**
     * In case of RANK BY, it could by dense or not.
     * Being a dense rank means to assign consecutive ranks
     * to different values.
     **/
    private boolean isDenseRank = false;

    /**
     * In case of simple RANK, namely row number mode
     * which is a consecutive number assigned to each tuple.
     **/
    private boolean isRowNumber = false;

    protected static final TupleFactory mTupleFactory = TupleFactory.getInstance();

    /**
     * Local counter for tuples on the same task.
     **/
    private Long localCount = 1L;

    /**
     * Task ID to label each tuple analyzed by the corresponding task
     **/
    private String taskID = "-1";

    /**
     * Unique identifier that links POCounter and PORank,
     * through the global counter labeled with it.
     **/
    private String operationID;

    public POCounter(OperatorKey k) {
        this(k, -1, null);
    }

    public POCounter(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    public POCounter(OperatorKey k, List<PhysicalOperator> inputs) {
        this(k, -1, inputs);
    }

    public POCounter(OperatorKey k, int rp, List<PhysicalOperator> inputs) {
        super(k, rp, inputs);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public POCounter(OperatorKey operatorKey, int requestedParallelism,
            List inp, List<PhysicalPlan> counterPlans,
            List<Boolean> ascendingCol) {
        super(operatorKey, requestedParallelism, inp);
        this.setCounterPlans(counterPlans);
        this.setAscendingColumns(ascendingCol);
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        if(illustrator != null){
            return new ExampleTuple((Tuple)out);
        }
        return (Tuple) out;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitCounter(this);
    }

    @Override
    public Result getNextTuple() throws ExecException {
        Result inp = null;

        while (true) {
            inp = processInput();
            if (inp.returnStatus == POStatus.STATUS_EOP
                    || inp.returnStatus == POStatus.STATUS_ERR)
                break;
            if (inp.returnStatus == POStatus.STATUS_NULL) {
                continue;
            }

            return addCounterValue(inp);
        }
        return inp;
    }

    /**
     * Add current task id and local counter value.
     * @param input from the previous output
     * @return  a tuple within two values prepended to the tuple
     * the task identifier and the local counter value.
     * Local counter value could be incremented by one (is a row number or dense rank)
     * or, could be incremented by the size of the bag on the previous tuple processed
     **/
    protected Result addCounterValue(Result input) throws ExecException {
        Tuple in = (Tuple) input.result;
        Tuple out = mTupleFactory.newTuple(in.getAll().size() + 2);
        Long sizeBag = 0L;
        int positionBag, i = 2;

        // Tuples are added by two stamps before the tuple content:
        // 1.- At position 0: Current taskId
        out.set(0, getTaskId());

        // 2.- At position 1: counter value
        //On this case, each tuple is analyzed independently of the tuples grouped
        if(isRowNumber() || isDenseRank()) {

            //Only when is Dense Rank (attached to a reduce phase) it is incremented on this way
            //Otherwise, the increment is done at mapper automatically
            if(isDenseRank())
                PigMapReduceCounter.PigReduceCounter.incrementCounter(POCounter.ONE);

            out.set(1, getLocalCounter());

            //and the local incrementer is sequentially increased.
            incrementLocalCounter();

        } else if(!isDenseRank()) {
            //Standard rank: On this case is important the
            //number of tuples on the same group.
            positionBag = in.getAll().size()-1;
            if (in.getType(positionBag) == DataType.BAG) {
                sizeBag = ((org.apache.pig.data.DefaultAbstractBag)in.get(positionBag)).size();
            }

            //This value (the size of the tuples on the bag) is used to increment
            //the current global counter and
            PigMapReduceCounter.PigReduceCounter.incrementCounter(sizeBag);

            out.set(1, getLocalCounter());

            //the value for the next tuple on the current task
            addToLocalCounter(sizeBag);

        }

        for (Object o : in) {
            out.set(i++, o);
        }

        input.result = illustratorMarkup(in, out, 0);

        return input;
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
        return getAliasString() + "POCounter" + "["
        + DataType.findTypeName(resultType) + "]" + " - "
        + mKey.toString();
    }

    public void setCounterPlans(List<PhysicalPlan> counterPlans) {
        this.counterPlans = counterPlans;
    }

    public List<PhysicalPlan> getCounterPlans() {
        return counterPlans;
    }

    public void setAscendingColumns(List<Boolean> mAscCols) {
        this.mAscCols = mAscCols;
    }

    public List<Boolean> getAscendingColumns() {
        return mAscCols;
    }

    /**
     *  Initialization step into the POCounter is to set
     *  up local counter to 1.
     **/
    public void resetLocalCounter() {
        this.localCount = 1L;
    }

    /**
     *  Sequential counter used at ROW NUMBER and RANK BY DENSE mode
     **/
    public Long incrementLocalCounter() {
        return localCount++;
    }

    public void setLocalCounter(Long localCount) {
        this.localCount = localCount;
    }

    public Long getLocalCounter() {
        return this.localCount;
    }

    public void addToLocalCounter(Long sizeBag) {
        this.localCount += sizeBag;
    }

    /**
     *  Task ID: identifier of the task (map or reducer)
     **/
    public void setTaskId(String taskID) {
        this.taskID = taskID;
    }

    public String getTaskId() {
        return this.taskID;
    }

    /**
     *  Dense Rank flag
     **/
    public void setIsDenseRank(boolean isDenseRank) {
        this.isDenseRank = isDenseRank;
    }

    public boolean isDenseRank() {
        return isDenseRank;
    }

    /**
     *  Row number flag
     **/
    public void setIsRowNumber(boolean isRowNumber) {
        this.isRowNumber = isRowNumber;
    }

    public boolean isRowNumber() {
        return isRowNumber;
    }

    /**
     *  Operation ID: identifier shared within the corresponding PORank
     **/
    public void setOperationID(String operationID) {
        this.operationID = operationID;
    }

    public String getOperationID() {
        return operationID;
    }
}