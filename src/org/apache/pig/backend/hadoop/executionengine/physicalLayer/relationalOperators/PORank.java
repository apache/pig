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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.pen.util.LineageTracer;

/**
 * This operator is part of the RANK operator implementation.
 * Reads the output tuple from POCounter and the cumulative sum previously calculated.
 * Here is read the task identifier in order to get the corresponding cumulative sum,
 * and the local counter at the tuple. These values are summed and prepended to the tuple.
 **/

public class PORank extends PhysicalOperator {

    private static final Log log = LogFactory.getLog(PORank.class);

    private static final long serialVersionUID = 1L;

    private List<PhysicalPlan> rankPlans;
    private List<Boolean> mAscCols;
    private List<Byte> ExprOutputTypes;

    /**
     * Unique identifier that links POCounter and PORank,
     * through the global counter labeled with it.
     **/
    private String operationID;

    /**
     * Counter used to set tuples into the equivalence
     * classes.
     **/
    private int localCountIllustrator = 0;

    public PORank(OperatorKey k) {
        this(k, -1, null);
    }

    public PORank(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    public PORank(OperatorKey k, List<PhysicalOperator> inp) {
        this(k, -1, inp);
    }

    public PORank(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
    }

    public PORank(PORank copy) {
        super(copy);
        this.rankPlans = copy.rankPlans;
        this.mAscCols = copy.mAscCols;
        this.ExprOutputTypes = copy.ExprOutputTypes;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public PORank(OperatorKey operatorKey, int requestedParallelism, List inp,
            List<PhysicalPlan> rankPlans, List<Boolean> ascendingCol) {
        super(operatorKey, requestedParallelism, inp);
        this.setRankPlans(rankPlans);
        this.setAscendingCols(ascendingCol);

        ExprOutputTypes = new ArrayList<Byte>(rankPlans.size());

        for (PhysicalPlan plan : rankPlans) {
            ExprOutputTypes.add(plan.getLeaves().get(0).getResultType());
        }
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        if(illustrator != null){
            ExampleTuple tOut = new ExampleTuple((Tuple)out);
            illustrator.addData((Tuple)out);
            illustrator.getEquivalenceClasses().get(eqClassIndex).add((Tuple)in);

            LineageTracer lineageTracer = illustrator.getLineage();
            lineageTracer.insert(tOut);
            return tOut;
        }
        return (Tuple) out;
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitRank(this);
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
            return addRank(inp);
        }

        return inp;
    }

    /**
     * Reads the output tuple from POCounter and the cumulative sum previously calculated.
     * Here is read the task identifier in order to get the corresponding cumulative sum,
     * and the local counter at the tuple. These values are summed and prepended to the tuple.
     * @param input processed by POCounter
     * @return input as Result. The input.result tuple owns the prepend rank value
     **/
    public Result addRank(Result input) throws ExecException {
        Tuple in = (Tuple) input.result;

        Long localCounter = (Long) in.get(0);
        Integer taskId = (Integer) in.getAll().remove(in.getAll().size() - 1);

        Long rank = getRankCounterOffset(taskId);

        in.set(0, rank + localCounter);

        if(localCountIllustrator > 2)
            localCountIllustrator = 0;

        input.result = illustratorMarkup(in, in, localCountIllustrator);

        localCountIllustrator++;

        return input;
    }

    protected Long getRankCounterOffset(Integer taskId) {
        String nameCounter = JobControlCompiler.PIG_MAP_COUNTER + getOperationID() + JobControlCompiler.PIG_MAP_SEPARATOR + String.valueOf(taskId);
        Long rank = PigMapReduce.sJobConfInternal.get().getLong( nameCounter , -1L );

        if(illustrator != null) {
            rank = 0L;
        }

        if(rank == -1) {
            log.error("Error on reading counter "+ nameCounter);
            throw new RuntimeException("Unable to read counter "+ nameCounter);
        }
        return rank;
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
        return getAliasString() + "PORank" + "["
        + DataType.findTypeName(resultType) + "]" + " - "
        + mKey.toString();
    }

    public void setRankPlans(List<PhysicalPlan> rankPlans) {
        this.rankPlans = rankPlans;
    }

    public List<PhysicalPlan> getRankPlans() {
        return rankPlans;
    }

    public void setAscendingCols(List<Boolean> mAscCols) {
        this.mAscCols = mAscCols;
    }

    public List<Boolean> getAscendingCols() {
        return mAscCols;
    }

    /**
     * Operation ID: identifier shared within the corresponding POCounter
     * @param operationID
     **/
    public void setOperationID(String operationID) {
        this.operationID = operationID;
    }

    public String getOperationID() {
        return operationID;
    }

    @Override
    public PORank clone() throws CloneNotSupportedException {
        PORank clone = (PORank)super.clone();
        // rankPlans, mAscCols, ExprOutputTypes are unused. Not cloning them
        return clone;
    }
}
