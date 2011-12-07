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
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.Utils;


/**
 * The partition rearrange operator is a part of the skewed join
 * implementation. It has an embedded physical plan that
 * generates tuples of the form (inpKey,reducerIndex,(indxed inp Tuple)).
 *
 */
public class POPartitionRearrange extends POLocalRearrange {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private Integer totalReducers = -1;
    // ReducerMap will store the tuple, max reducer index & min reducer index
    private static Map<Object, Pair<Integer, Integer> > reducerMap = new HashMap<Object, Pair<Integer, Integer> >();
    private boolean loaded;

    protected static final BagFactory mBagFactory = BagFactory.getInstance();
    private PigContext pigContext;

    public POPartitionRearrange(OperatorKey k) {
        this(k, -1, null);
    }

    public POPartitionRearrange(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    public POPartitionRearrange(OperatorKey k, List<PhysicalOperator> inp) {
        this(k, -1, inp);
    }

    public POPartitionRearrange(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
        index = -1;
        leafOps = new ArrayList<ExpressionOperator>();
    }

    /* Loads the key distribution file obtained from the sampler */
    private void loadPartitionFile() throws RuntimeException {
        String keyDistFile = PigMapReduce.sJobConfInternal.get().get("pig.keyDistFile", "");
        if (keyDistFile.isEmpty()) {
            throw new RuntimeException(
            "Internal error: missing key distribution file property.");
        }

        boolean tmpFileCompression = Utils.tmpFileCompression(pigContext);
        if (tmpFileCompression) {
            PigMapReduce.sJobConfInternal.get().setBoolean("pig.tmpfilecompression", true);
            try {
                PigMapReduce.sJobConfInternal.get().set("pig.tmpfilecompression.codec", Utils.tmpFileCompressionCodec(pigContext));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        try {

            Integer [] redCnt = new Integer[1];

            reducerMap = MapRedUtil.loadPartitionFileFromLocalCache(
                    keyDistFile, redCnt, DataType.NULL, PigMapReduce.sJobConfInternal.get());

            // check if the partition file is empty
            totalReducers = (redCnt[0] == null) ? -1 : redCnt[0];
            loaded = true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String name() {
        return getAliasString() + "Partition rearrange " + "["
                + DataType.findTypeName(resultType) + "]" + "{"
                + DataType.findTypeName(keyType) + "}" + "(" + mIsDistinct
                + ") - " + mKey.toString();
    }

    /**
     * Calls getNext on the generate operator inside the nested
     * physical plan. Converts the generated tuple into the proper
     * format, i.e, (key,indexedTuple(value))
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {

        Result inp = null;
        Result res = null;

        // Load the skewed join key partitioning file
        if (!loaded) {
        	try {
        		loadPartitionFile();
        	} catch (Exception e) {
        		throw new RuntimeException(e);
        	}
        }

        while (true) {
            inp = processInput();
            if (inp.returnStatus == POStatus.STATUS_EOP || inp.returnStatus == POStatus.STATUS_ERR) {
                break;
            }
            if (inp.returnStatus == POStatus.STATUS_NULL) {
                continue;
            }

            for (PhysicalPlan ep : plans) {
                ep.attachInput((Tuple)inp.result);
            }
            List<Result> resLst = new ArrayList<Result>();
            for (ExpressionOperator op : leafOps){
                res = op.getNext(getDummy(op.getResultType()), op.getResultType());
                if(res.returnStatus!=POStatus.STATUS_OK) {
                    return new Result();
                }
                resLst.add(res);
            }
            res.result = constructPROutput(resLst,(Tuple)inp.result);

            return res;
        }
        return inp;
    }

	// Returns bag of tuples
    protected DataBag constructPROutput(List<Result> resLst, Tuple value) throws ExecException{
		Tuple t = super.constructLROutput(resLst, null, value);

        //Construct key
        Object key = t.get(1);

		// Construct an output bag and feed in the tuples
		DataBag opBag = mBagFactory.newDefaultBag();

		//Put the index, key, and value
		//in a tuple and return
		Pair <Integer, Integer> indexes = reducerMap.get(key);	// first -> min, second ->max

		// For non skewed keys, we set the partition index to be -1
		if (indexes == null) {
			indexes = new Pair <Integer, Integer>(-1,0);
		}

		for (Integer reducerIdx=indexes.first, cnt=0; cnt <= indexes.second; reducerIdx++, cnt++) {
			if (reducerIdx >= totalReducers) {
				reducerIdx = 0;
			}
			Tuple opTuple = mTupleFactory.newTuple(4);
			opTuple.set(0, t.get(0));
			// set the partition index
			opTuple.set(1, reducerIdx.intValue());
			opTuple.set(2, key);
			opTuple.set(3, t.get(2));

			opBag.add(opTuple);
		}

		return opBag;
    }

    /**
     * @param pigContext the pigContext to set
     */
    public void setPigContext(PigContext pigContext) {
        this.pigContext = pigContext;
    }

    /**
     * @return the pigContext
     */
    public PigContext getPigContext() {
        return pigContext;
    }

    /**
     * Make a deep copy of this operator.
     * @throws CloneNotSupportedException
     */
    @Override
    public POPartitionRearrange clone() throws CloneNotSupportedException {
		POPartitionRearrange clone = (POPartitionRearrange) super.clone();
	    //clone.reducerMap = (HashMap)reducerMap.clone();
        return clone;
    }

}
