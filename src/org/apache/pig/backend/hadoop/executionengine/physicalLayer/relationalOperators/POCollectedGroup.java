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
import java.util.Iterator;

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
import org.apache.pig.data.InternalCachedBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.pen.util.LineageTracer;
import org.apache.pig.impl.util.IdentityHashSet;

/**
 * The collected group operator is a special operator used when users give
 * the hint 'using "collected"' in a group by clause. It implements a map-side
 * group that collects all records for a given key into a buffer. When it sees
 * a key change it will emit the key and bag for records it had buffered.
 * It will assume that all keys for a given record are collected together
 * and thus there is not need to buffer across keys.
 *
 */
public class POCollectedGroup extends PhysicalOperator {

    private static final List<PhysicalPlan> EMPTY_PLAN_LIST = new ArrayList<PhysicalPlan>();

    protected static final long serialVersionUID = 1L;

    protected static final TupleFactory mTupleFactory = TupleFactory.getInstance();

    protected List<PhysicalPlan> plans;

    protected List<ExpressionOperator> leafOps;

    protected byte keyType;

    private final Tuple output;

    private DataBag outputBag = null;

    private Object prevKey = null;

    private boolean useDefaultBag = false;

    public POCollectedGroup(OperatorKey k) {
        this(k, -1, null);
    }

    public POCollectedGroup(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    public POCollectedGroup(OperatorKey k, List<PhysicalOperator> inp) {
        this(k, -1, inp);
    }

    public POCollectedGroup(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
        leafOps = new ArrayList<ExpressionOperator>();
        output = mTupleFactory.newTuple(2);
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitCollectedGroup(this);
    }

    @Override
    public String name() {
        return getAliasString() + "Map side group " + "["
                + DataType.findTypeName(resultType) + "]" + "{"
                + DataType.findTypeName(keyType) + "}" + " - "
                + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    /**
     * Overridden since the attachment of the new input should cause the old
     * processing to end.
     */
    @Override
    public void attachInput(Tuple t) {
        super.attachInput(t);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Result getNextTuple() throws ExecException {

        // Since the output is buffered, we need to flush the last
        // set of records when the close method is called by mapper.
        if (this.parentPlan.endOfAllInput) {
            if (outputBag != null) {
                Tuple tup = mTupleFactory.newTuple(2);
                tup.set(0, prevKey);
                tup.set(1, outputBag);
                outputBag = null;
                return new Result(POStatus.STATUS_OK, tup);
            }

            return new Result(POStatus.STATUS_EOP, null);
        }

        Result inp = null;
        Result res = null;

        while (true) {
            inp = processInput();
            if (inp.returnStatus == POStatus.STATUS_EOP ||
                    inp.returnStatus == POStatus.STATUS_ERR) {
                break;
            }

            if (inp.returnStatus == POStatus.STATUS_NULL) {
                continue;
            }

            for (PhysicalPlan ep : plans) {
                ep.attachInput((Tuple)inp.result);
            }

            List<Result> resLst = new ArrayList<Result>();
            for (ExpressionOperator op : leafOps) {
                res = op.getNext(op.getResultType());
                if (res.returnStatus != POStatus.STATUS_OK) {
                    return new Result();
                }
                resLst.add(res);
            }

            Tuple tup = constructOutput(resLst,(Tuple)inp.result);
            Object curKey = tup.get(0);

            // the first time, just create a new buffer and continue.
            if (prevKey == null && outputBag == null) {

                if (PigMapReduce.sJobConfInternal.get() != null) {
                    String bagType = PigMapReduce.sJobConfInternal.get().get("pig.cachedbag.type");
                    if (bagType != null && bagType.equalsIgnoreCase("default")) {
                        useDefaultBag = true;
                    }
                }
                prevKey = curKey;
                outputBag = useDefaultBag ? BagFactory.getInstance().newDefaultBag()
                // In a very rare case if there is a POStream after this
                // POCollectedGroup in the pipeline and is also blocking the pipeline;
                // constructor argument should be 2. But for one obscure
                // case we don't want to pay the penalty all the time.

                // Additionally, if there is a merge join(on a different key) following POCollectedGroup
                // default bags should be used. But since we don't allow anything
                // before Merge Join currently we are good.
                        : new InternalCachedBag(1);
                outputBag.add((Tuple)tup.get(1));
                continue;
            }

            // no key change
            if (prevKey == null && curKey == null) {
                outputBag.add((Tuple)tup.get(1));
                continue;
            }

            // no key change
            if (prevKey != null && curKey != null && ((Comparable)curKey).compareTo(prevKey) == 0) {
                outputBag.add((Tuple)tup.get(1));
                continue;
            }

            // key change
            Tuple tup2 = mTupleFactory.newTuple(2);
            tup2.set(0, prevKey);
            tup2.set(1, outputBag);
            res.result = tup2;

            prevKey = curKey;
            outputBag = useDefaultBag ? BagFactory.getInstance().newDefaultBag()
                    : new InternalCachedBag(1);
            outputBag.add((Tuple)tup.get(1));
            return res;
        }

        return inp;
    }

    protected Tuple constructOutput(List<Result> resLst, Tuple value) throws ExecException{

        // Construct key
        Object key;

        if (resLst.size() > 1) {
            Tuple t = mTupleFactory.newTuple(resLst.size());
            int i = -1;
            for (Result res : resLst) {
                t.set(++i, res.result);
            }
            key = t;
        }
        else {
            key = resLst.get(0).result;
        }

        // Put key and value in a tuple and return
        output.set(0, key);
        output.set(1, value);

        return output;
    }

    public byte getKeyType() {
        return keyType;
    }

    public void setKeyType(byte keyType) {
        this.keyType = keyType;
    }

    public List<PhysicalPlan> getPlans() {
        return (plans == null) ? EMPTY_PLAN_LIST : plans;
    }

    public void setPlans(List<PhysicalPlan> plans) throws PlanException {
        this.plans = plans;
        leafOps.clear();
        for (PhysicalPlan plan : plans) {
            ExpressionOperator leaf = (ExpressionOperator)plan.getLeaves().get(0);
            leafOps.add(leaf);
        }
   }
    
    private void setIllustratorEquivalenceClasses(Tuple tin) {
        if (illustrator != null) {
          illustrator.getEquivalenceClasses().get(0).add(tin);
        }
    }
    
    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        return null;
    }
}
