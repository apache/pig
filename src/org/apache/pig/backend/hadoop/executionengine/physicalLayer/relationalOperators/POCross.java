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

import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.pen.util.LineageTracer;

/**
 * Recover this class for nested cross operation.
 * 
 * 
 */
public class POCross extends PhysicalOperator {

    private static final long serialVersionUID = 1L;

    protected DataBag[] inputBags;

    protected Tuple[] data;

    protected transient Iterator<Tuple>[] its;

    public POCross(OperatorKey k) {
        super(k);
    }

    public POCross(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
    }

    public POCross(OperatorKey k, int rp) {
        super(k, rp);
    }

    public POCross(OperatorKey k, List<PhysicalOperator> inp) {
        super(k, inp);
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitCross(this);
    }

    @Override
    public String name() {
        return getAliasString() + "POCross" + "["
                + DataType.findTypeName(resultType) + "]" + " - "
                + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        if (illustrator != null) {
            ExampleTuple tOut = new ExampleTuple((Tuple) out);
            illustrator.addData(tOut);
            illustrator.getEquivalenceClasses().get(eqClassIndex).add(
                    (Tuple) out);
            LineageTracer lineageTracer = illustrator.getLineage();
            lineageTracer.insert(tOut);
            for (int i = 0; i < data.length; i++) {
                lineageTracer.union(tOut, data[i]);
            }
            return tOut;
        } else {
            return (Tuple) out;
        }
    }

    @Override
    public Result getNext(Tuple t) throws ExecException {
        Result res = new Result();
        int noItems = inputs.size();
        if (inputBags == null) {
            accumulateData();
        }

        if (its != null) {
            // we check if we are done with processing
            // we do that by checking if all the iterators are used up
            boolean finished = true;
            for (int i = 0; i < its.length; i++) {
                if (inputBags[i].size() == 0) {
                    finished = true;
                    break;
                }
                finished &= !its[i].hasNext();
            }
            if (finished) {
                res.returnStatus = POStatus.STATUS_EOP;
                // reset inputBags, its, data to null so that in the next round
                // of getNext, the new input data will be loaded.
                inputBags = null;
                its = null;
                data = null;
                return res;
            }

        }

        if (data == null) {
            // getNext being called for the first time or starting on new input
            // data we instantiate the template array and start populating it
            // with data
            data = new Tuple[noItems];
            for (int i = 0; i < noItems; ++i) {
                data[i] = its[i].next();

            }
            res.result = createTuple(data);
            res.returnStatus = POStatus.STATUS_OK;
            return res;
        } else {
            for (int index = noItems - 1; index >= 0; --index) {
                if (its[index].hasNext()) {
                    data[index] = its[index].next();
                    res.result = createTuple(data);
                    res.returnStatus = POStatus.STATUS_OK;
                    return res;
                } else {
                    // reset this index's iterator so cross product can be
                    // achieved we would be resetting this way only for the
                    // indexes from the end when the first index which needs to
                    // be flattened has reached the last element in its
                    // iterator, we won't come here - instead, we reset all
                    // iterators at the beginning of this method.
                    its[index] = (inputBags[index]).iterator();
                    data[index] = its[index].next();
                }

            }
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private void accumulateData() throws ExecException {
        int count = 0;
        inputBags = new DataBag[inputs.size()];

        its = new Iterator[inputs.size()];
        for (PhysicalOperator op : inputs) {
            DataBag bag = BagFactory.getInstance().newDefaultBag();
            inputBags[count] = bag;
            for (Result res = op.getNext(dummyTuple); res.returnStatus != POStatus.STATUS_EOP; res = op
                    .getNext(dummyTuple)) {
                if (res.returnStatus == POStatus.STATUS_NULL)
                    continue;
                if (res.returnStatus == POStatus.STATUS_ERR)
                    throw new ExecException(
                            "Error accumulating data in the local Cross operator");
                if (res.returnStatus == POStatus.STATUS_OK)
                    bag.add((Tuple) res.result);
            }
            its[count++] = bag.iterator();
        }
    }

    private Tuple createTuple(Tuple[] data) throws ExecException {
        Tuple out = TupleFactory.getInstance().newTuple();

        for (int i = 0; i < data.length; ++i) {
            Tuple t = data[i];
            int size = t.size();
            for (int j = 0; j < size; ++j) {
                out.append(t.get(j));
            }
        }

        return illustratorMarkup(out, out, 0);
    }

}
