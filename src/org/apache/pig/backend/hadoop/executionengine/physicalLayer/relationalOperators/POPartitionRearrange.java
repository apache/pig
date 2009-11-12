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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;


import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.builtin.BinStorage;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.TupleFactory;

import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;


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
	private String partitionFile;
	private Integer totalReducers = -1;
	// ReducerMap will store the tuple, max reducer index & min reducer index
	private static Map<Object, Pair<Integer, Integer> > reducerMap = new HashMap<Object, Pair<Integer, Integer> >();
	private boolean loaded;

	protected static final BagFactory mBagFactory = BagFactory.getInstance();

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

	/* Returns the name for the partition sampling file */
	public String getPartitionFile() {
		return partitionFile;
	}

	/* Set the partition sampling file */
	public void setPartitionFile(String file) {
		partitionFile = file;
	}

	/* Loads the key distribution file obtained from the sampler */
	private void loadPartitionFile() throws RuntimeException {
		try {
			Integer [] redCnt = new Integer[1]; 
			reducerMap = MapRedUtil.loadPartitionFile(partitionFile, redCnt, null, DataType.NULL);
			totalReducers = redCnt[0];
			loaded = true;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

    @Override
    public String name() {
        return "Partition rearrange " + "[" + DataType.findTypeName(resultType) +
            "]" + "{" + DataType.findTypeName(keyType) + "}" + "(" +
            mIsDistinct + ") - " + mKey.toString();
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
            if (inp.returnStatus == POStatus.STATUS_EOP || inp.returnStatus == POStatus.STATUS_ERR)
                break;
            if (inp.returnStatus == POStatus.STATUS_NULL)
                continue;
            
            for (PhysicalPlan ep : plans) {
                ep.attachInput((Tuple)inp.result);
            }
            List<Result> resLst = new ArrayList<Result>();
            for (ExpressionOperator op : leafOps){
                
                switch(op.getResultType()){
                case DataType.BAG:
                    res = op.getNext(dummyBag);
                    break;
                case DataType.BOOLEAN:
                    res = op.getNext(dummyBool);
                    break;
                case DataType.BYTEARRAY:
                    res = op.getNext(dummyDBA);
                    break;
                case DataType.CHARARRAY:
                    res = op.getNext(dummyString);
                    break;
                case DataType.DOUBLE:
                    res = op.getNext(dummyDouble);
                    break;
                case DataType.FLOAT:
                    res = op.getNext(dummyFloat);
                    break;
                case DataType.INTEGER:
                    res = op.getNext(dummyInt);
                    break;
                case DataType.LONG:
                    res = op.getNext(dummyLong);
                    break;
                case DataType.MAP:
                    res = op.getNext(dummyMap);
                    break;
                case DataType.TUPLE:
                    res = op.getNext(dummyTuple);
                    break;
                }
                if(res.returnStatus!=POStatus.STATUS_OK)
                    return new Result();
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
			opTuple.set(1, reducerIdx.byteValue());
			opTuple.set(2, key);
			opTuple.set(3, t.get(2));
			
			opBag.add(opTuple);
		}
		
		return opBag;
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
