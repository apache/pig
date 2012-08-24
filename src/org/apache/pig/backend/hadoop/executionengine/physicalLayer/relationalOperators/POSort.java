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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.SortInfo;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserComparisonFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.InternalSortedBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.pen.util.LineageTracer;

/**
 * This implementation is applicable for both the physical plan and for the
 * local backend, as the conversion of physical to mapreduce would see the SORT
 * operator and take necessary steps to convert it to a quantile and a sort job.
 *
 * This is a blocking operator. The sortedDataBag accumulates Tuples and sorts
 * them only when there an iterator is started. So all the tuples from the input
 * operator should be accumulated and filled into the dataBag. The attachInput
 * method is not applicable here.
 *
 *
 */

//We intentionally skip type checking in backend for performance reasons
@SuppressWarnings("unchecked")
public class POSort extends PhysicalOperator {
    private static final Log log = LogFactory.getLog(POSort.class);

	/**
     *
     */
    private static final long serialVersionUID = 1L;
    //private List<Integer> mSortCols;
	private List<PhysicalPlan> sortPlans;
	private List<Byte> ExprOutputTypes;
	private List<Boolean> mAscCols;
	private POUserComparisonFunc mSortFunc;
	private Comparator<Tuple> mComparator;

	private boolean inputsAccumulated = false;
	private long limit;
	public boolean isUDFComparatorUsed = false;
	private DataBag sortedBag;
	transient Iterator<Tuple> it;

	public POSort(
            OperatorKey k,
            int rp,
            List inp,
            List<PhysicalPlan> sortPlans,
			List<Boolean> mAscCols,
            POUserComparisonFunc mSortFunc) {
		super(k, rp, inp);
		//this.mSortCols = mSortCols;
		this.sortPlans = sortPlans;
		this.mAscCols = mAscCols;
        this.limit = -1;
		this.mSortFunc = mSortFunc;
		if (mSortFunc == null) {
            mComparator = new SortComparator();
			/*sortedBag = BagFactory.getInstance().newSortedBag(
					new SortComparator());*/
			ExprOutputTypes = new ArrayList<Byte>(sortPlans.size());

			for(PhysicalPlan plan : sortPlans) {
				ExprOutputTypes.add(plan.getLeaves().get(0).getResultType());
			}
		} else {
			/*sortedBag = BagFactory.getInstance().newSortedBag(
					new UDFSortComparator());*/
            mComparator = new UDFSortComparator();
			isUDFComparatorUsed = true;
		}
	}

	public POSort(OperatorKey k, int rp, List inp) {
		super(k, rp, inp);

	}

	public POSort(OperatorKey k, int rp) {
		super(k, rp);

	}

	public POSort(OperatorKey k, List inp) {
		super(k, inp);

	}

	public POSort(OperatorKey k) {
		super(k);

	}

	public class SortComparator implements Comparator<Tuple>,Serializable {
		/**
         *
         */
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Tuple o1, Tuple o2) {
			int count = 0;
			int ret = 0;
			if(sortPlans == null || sortPlans.size() == 0) {
                return 0;
            }
			for(PhysicalPlan plan : sortPlans) {
				try {
					plan.attachInput(o1);
					Result res1 = getResult(plan, ExprOutputTypes.get(count));
					plan.attachInput(o2);
					Result res2 = getResult(plan, ExprOutputTypes.get(count));
					if(res1.returnStatus != POStatus.STATUS_OK || res2.returnStatus != POStatus.STATUS_OK) {
						log.error("Error processing the input in the expression plan : " + plan.toString());
					} else {
						if(mAscCols.get(count++)) {
							ret = DataType.compare(res1.result, res2.result);
                            // If they are not equal, return
                            // Otherwise, keep comparing the next one
                            if (ret != 0) {
                                return ret ;
                            }
                        }
                        else {
                            ret = DataType.compare(res2.result, res1.result);
                            if (ret != 0) {
                                return ret ;
                            }

                        }

					}

				} catch (ExecException e) {
					log.error("Invalid result while executing the expression plan : " + plan.toString() + "\n" + e.getMessage());
				}
			}
			return ret;
		}

		private Result getResult(PhysicalPlan plan, byte resultType) throws ExecException {
			ExpressionOperator Op = (ExpressionOperator) plan.getLeaves().get(0);
			Result res = null;

			switch (resultType) {
            case DataType.BYTEARRAY:
            case DataType.CHARARRAY:
            case DataType.DOUBLE:
            case DataType.FLOAT:
            case DataType.BOOLEAN:
            case DataType.INTEGER:
            case DataType.LONG:
            case DataType.DATETIME:
            case DataType.TUPLE:
                res = Op.getNext(getDummy(resultType), resultType);
                break;

            default: {
                int errCode = 2082;
                String msg = "Did not expect result of type: " +
                        DataType.findTypeName(resultType);
                    throw new ExecException(msg, errCode, PigException.BUG);
            }

            }
			return res;
		}
	}

	public class UDFSortComparator implements Comparator<Tuple>,Serializable {

		/**
         *
         */
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Tuple t1, Tuple t2) {

			mSortFunc.attachInput(t1, t2);
			Integer i = null;
			Result res = null;
			try {
				res = mSortFunc.getNext(i);
			} catch (ExecException e) {

				log.error("Input not ready. Error on reading from input. "
						+ e.getMessage());
			}
			if (res != null) {
                return (Integer) res.result;
            } else {
                return 0;
            }
		}

	}

    @Override
    public String name() {
        return getAliasString() + "POSort" + "["
                + DataType.findTypeName(resultType) + "]" + "("
                + (mSortFunc != null ? mSortFunc.getFuncSpec() : "") + ")"
                + " - " + mKey.toString();
    }

	@Override
	public boolean isBlocking() {

		return true;
	}

	@Override
	public Result getNext(Tuple t) throws ExecException {
		Result res = new Result();

		if (!inputsAccumulated) {
			res = processInput();
			// by default, we create InternalSortedBag, unless user configures
			// explicitly to use old bag
			String bagType = null;
	        if (PigMapReduce.sJobConfInternal.get() != null) {
	   			bagType = PigMapReduce.sJobConfInternal.get().get("pig.cachedbag.sort.type");
	   	    }
            if (bagType != null && bagType.equalsIgnoreCase("default")) {
            	sortedBag = BagFactory.getInstance().newSortedBag(mComparator);
       	    } else {
    	    	sortedBag = new InternalSortedBag(3, mComparator);
    	    }

			while (res.returnStatus != POStatus.STATUS_EOP) {
				if (res.returnStatus == POStatus.STATUS_ERR) {
					log.error("Error in reading from the inputs");
					return res;
					//continue;
				} else if (res.returnStatus == POStatus.STATUS_NULL) {
                    // ignore the null, read the next tuple.
                    res = processInput();
					continue;
				}
				sortedBag.add((Tuple) res.result);
				res = processInput();

			}

			inputsAccumulated = true;

		}
		if (it == null) {
            it = sortedBag.iterator();
        }
        if (it.hasNext()) {
            res.result = it.next();
            illustratorMarkup(res.result, res.result, 0);
            res.returnStatus = POStatus.STATUS_OK;
        } else {
            res.returnStatus = POStatus.STATUS_EOP;
            reset();
        }
		return res;
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
	public void visit(PhyPlanVisitor v) throws VisitorException {

		v.visitSort(this);
	}

    @Override
    public void reset() {
        inputsAccumulated = false;
        sortedBag = null;
        it = null;
    }

    public List<PhysicalPlan> getSortPlans() {
        return sortPlans;
    }

    public void setSortPlans(List<PhysicalPlan> sortPlans) {
        this.sortPlans = sortPlans;
    }

    public POUserComparisonFunc getMSortFunc() {
        return mSortFunc;
    }

    public void setMSortFunc(POUserComparisonFunc sortFunc) {
        mSortFunc = sortFunc;
    }

    public List<Boolean> getMAscCols() {
        return mAscCols;
    }

    public void setLimit(long l)
    {
    	limit = l;
    }

    public long getLimit()
    {
    	return limit;
    }

    public boolean isLimited()
    {
    	return (limit!=-1);
    }

    @Override
    public POSort clone() throws CloneNotSupportedException {
        List<PhysicalPlan> clonePlans = new
            ArrayList<PhysicalPlan>(sortPlans.size());
        for (PhysicalPlan plan : sortPlans) {
            clonePlans.add(plan.clone());
        }
        List<Boolean> cloneAsc = new ArrayList<Boolean>(mAscCols.size());
        for (Boolean b : mAscCols) {
            cloneAsc.add(b);
        }
        POUserComparisonFunc cloneFunc = null;
        if (mSortFunc != null) {
            cloneFunc = mSortFunc.clone();
        }
        // Don't set inputs as PhysicalPlan.clone will take care of that
        return new POSort(new OperatorKey(mKey.scope,
            NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)),
            requestedParallelism, null, clonePlans, cloneAsc, cloneFunc);
    }

   
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        if(illustrator != null) {
          illustrator.getEquivalenceClasses().get(eqClassIndex).add((Tuple) in);
            illustrator.addData((Tuple) out);
        }
        return (Tuple) out;
    }
}
