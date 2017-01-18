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

/**
 *
 */
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.pen.util.LineageTracer;

/**
 * This package operator is a specialization
 * of POPackage operator used for the specific
 * case of the order by query. See JIRA 802
 * for more details.
 */
public class LitePackager extends Packager {

    private static final long serialVersionUID = 1L;
    private PigNullableWritable keyWritable;

    @Override
    public void attachInput(Object key, DataBag[] bags, boolean[] readOnce)
            throws ExecException {
        this.key = key;
        this.bags = bags;
        this.readOnce = readOnce;
        // Bag can be read directly and need not be materialized again
    }

    @Override
    public boolean[] getInner() {
        return null;
    }

    @Override
    public void setInner(boolean[] inner) {
    }

    /**
     * Make a deep copy of this operator.
     * @throws CloneNotSupportedException
     */
    @Override
    public LitePackager clone() throws CloneNotSupportedException {
        LitePackager clone = (LitePackager) super.clone();
        clone.inner = null;
        if (keyInfo != null) {
            clone.keyInfo = new HashMap<Integer, Pair<Boolean, Map<Integer, Integer>>>(keyInfo);
        }
        return clone;
    }

    /**
     * @return the distinct
     */
    @Override
    public boolean isDistinct() {
        return false;
    }

    /**
     * @param distinct the distinct to set
     */
    @Override
    public void setDistinct(boolean distinct) {
    }

    /**
     * Similar to POPackage.getNext except that
     * only one input is expected with index 0
     * and ReadOnceBag is used instead of
     * DefaultDataBag.
     */
    @Override
    public Result getNext() throws ExecException {
        if (bags == null) {
            return new Result(POStatus.STATUS_EOP, null);
        }

        Tuple res;

        //Construct the output tuple by appending
        //the key and all the above constructed bags
        //and return it.
        res = mTupleFactory.newTuple(numInputs+1);
        res.set(0,key);
        res.set(1, bags[0]);
        detachInput();
        Result r = new Result();
        r.returnStatus = POStatus.STATUS_OK;
        r.result = illustratorMarkup(null, res, 0);
        return r;
    }

    /**
     * Makes use of the superclass method, but this requires an additional
     * parameter key passed by ReadOnceBag. key of this instance will be set to
     * null in detachInput call, but an instance of ReadOnceBag may have the
     * original key that it uses. Therefore this extra argument is taken to
     * temporarily set it before the call to the superclass method and then
     * restore it.
     */
    @Override
    public Tuple getValueTuple(PigNullableWritable keyWritable,
            NullableTuple ntup, int index) throws ExecException {
        PigNullableWritable origKey = this.keyWritable;
        this.keyWritable = keyWritable;
        Tuple retTuple = super.getValueTuple(keyWritable, ntup, index);
        this.keyWritable = origKey;
        return retTuple;
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        if (illustrator != null) {
            ExampleTuple tOut = new ExampleTuple((Tuple) out);
            LineageTracer lineageTracer = illustrator.getLineage();
            lineageTracer.insert(tOut);
            if (illustrator.getEquivalenceClasses() == null) {
                LinkedList<IdentityHashSet<Tuple>> equivalenceClasses = new LinkedList<IdentityHashSet<Tuple>>();
                for (int i = 0; i < numInputs; ++i) {
                    IdentityHashSet<Tuple> equivalenceClass = new IdentityHashSet<Tuple>();
                    equivalenceClasses.add(equivalenceClass);
                }
                illustrator.setEquivalenceClasses(equivalenceClasses, parent);
            }
            illustrator.getEquivalenceClasses().get(eqClassIndex).add(tOut);
            tOut.synthetic = false; // not expect this to be really used
            illustrator.addData((Tuple) tOut);
            return tOut;
        } else
            return (Tuple) out;
    }
}

