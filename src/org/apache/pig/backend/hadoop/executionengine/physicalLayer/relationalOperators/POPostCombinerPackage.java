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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.VisitorException;
/**
 * The package operator that packages the globally rearranged tuples into
 * output format after the combiner stage.  It differs from POPackage in that
 * instead it does not use the index in the NullableTuple to find the bag to put a
 * tuple in.  Intead, the inputs are
 * put in a bag corresponding to their offset in the tuple.
 */
public class POPostCombinerPackage extends POPackage {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final Log log = LogFactory.getLog(getClass());

    private static BagFactory mBagFactory = BagFactory.getInstance();
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();

    private boolean[] mBags; // For each field, indicates whether or not it
                             // needs to be put in a bag.

    /**
     * A new POPostCombinePackage will be constructed as a near clone of the
     * provided POPackage.
     * @param pkg POPackage to clone.
     * @param bags for each field, indicates whether it should be a bag (true)
     * or a simple field (false).
     */
    public POPostCombinerPackage(POPackage pkg, boolean[] bags) {
        super(new OperatorKey(pkg.getOperatorKey().scope,
            NodeIdGenerator.getGenerator().getNextNodeId(pkg.getOperatorKey().scope)),
            pkg.getRequestedParallelism(), pkg.getInputs());
        resultType = pkg.getResultType();
        keyType = pkg.keyType;
        numInputs = 1;
        inner = new boolean[1];
        for (int i = 0; i < pkg.inner.length; i++) {
            inner[i] = true;
        }
        mBags = bags;
    }

    @Override
    public String name() {
        return "PostCombinerPackage" + "[" + DataType.findTypeName(resultType) + "]" + "{" + DataType.findTypeName(keyType) + "}" +" - " + mKey.toString();
    }

    @Override
    public Result getNext(Tuple t) throws ExecException {
        int keyField = -1;
        //Create numInputs bags
        Object[] fields = new Object[mBags.length];
        for (int i = 0; i < mBags.length; i++) {
            if (mBags[i]) fields[i] = mBagFactory.newDefaultBag();
        }
        
        // For each indexed tup in the inp, split them up and place their
        // fields into the proper bags.  If the given field isn't a bag, just
        // set the value as is.
        while (tupIter.hasNext()) {
            NullableTuple ntup = tupIter.next();
            Tuple tup = (Tuple)ntup.getValueAsPigType();
            for (int i = 0; i < tup.size(); i++) {
                if (mBags[i]) ((DataBag)fields[i]).add((Tuple)tup.get(i));
                else fields[i] = tup.get(i);
            }
        }
        
        //Construct the output tuple by appending
        //the key and all the above constructed bags
        //and return it.
        Tuple res;
        res = mTupleFactory.newTuple(mBags.length);
        for (int i = 0; i < mBags.length; i++) res.set(i, fields[i]);
        Result r = new Result();
        r.result = res;
        r.returnStatus = POStatus.STATUS_OK;
        return r;

    }

}
