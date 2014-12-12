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

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.PORelationToExprProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.AccumulativeBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.SchemaTupleClassGenerator.GenContext;
import org.apache.pig.data.SchemaTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.TupleMaker;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.pen.util.LineageTracer;

/**
 * This class provides a new ForEach physical operator to handle the ROLLUP with
 * hybrid IRG when the Rollup Optimizer is activated. This class contains almost
 * the same as POForEach class excepts some functions for the Hybrid IRG stuffs.
 */
// We intentionally skip type checking in backend for performance reasons
@SuppressWarnings("unchecked")
public class PORollupHIIForEach extends POForEach {
    private static final long serialVersionUID = 1L;

    protected List<PhysicalPlan> inputPlans;
    protected List<PhysicalOperator> opsToBeReset;

    protected static final TupleFactory mTupleFactory = TupleFactory.getInstance();

    // Since the plan has a generate, this needs to be maintained
    // as the generate can potentially return multiple tuples for
    // same call.
    protected boolean processingPlan = false;

    // its holds the iterators of the databags given by the input expressions
    // which need flattening.
    transient protected Iterator<Tuple>[] its = null;

    // This holds the outputs given out by the input expressions of any datatype
    protected Object[] bags = null;

    // This is the template whcih contains tuples and is flattened out in
    // createTuple() to generate the final output
    protected Object[] data = null;

    // store result types of the plan leaves
    protected byte[] resultTypes = null;

    // store whether or not an accumulative UDF has terminated early
    protected BitSet earlyTermination = null;

    // array version of isToBeFlattened - this is purely
    // for optimization - instead of calling isToBeFlattened.get(i)
    // we can do the quicker array access - isToBeFlattenedArray[i].
    // Also we can store "boolean" values rather than "Boolean" objects
    // so we can also save on the Boolean.booleanValue() calls
    protected boolean[] isToBeFlattenedArray;

    ExampleTuple tIn = null;
    protected int noItems;

    protected PhysicalOperator[] planLeafOps = null;

    protected transient AccumulativeTupleBuffer buffer;

    protected Tuple inpTuple;

    private Schema schema;

    // start adding new variables

    // The first tuple that stores the value of the previous Rollup Dimension
    // for the first IRG
    protected Tuple prevRollupDimension = null;

    // The second tuple that stores the value of the previous Rollup Dimension
    // for the second IRG
    protected Tuple prevRollupDimension2 = null;

    protected Tuple currentRollupDimension = null;

    // This holds the payload values for the first IRG
    protected DataBag[][] tmpResult;

    // This holds the payload values for the second IRG
    protected DataBag[][] tmpResult2;

    // This holds the result tuples for the first IRG
    protected Result[] returnRes;

    // This holds the result tuples for the second IRG
    protected Result[] returnRes2;

    // To check if we can work on the second IRG or not
    protected boolean secondPass = false;

    // The pivot position of the rollup operation
    protected int pivot = -1;

    // To check if we finished the first IRG or not
    protected boolean finIRG1 = false;

    // To check if we finished the second IRG or not
    protected boolean finIRG2 = false;

    protected int noUserFunc = 0;

    //the size of total fields that involve in CUBE clause
    protected int dimensionSize = 0;

    // These variables below are used in case the rollup operation has been
    // moved to the end of the operation list.

    //the index of the first field involves in ROLLUP
    protected int rollupFieldIndex = 0;

    //Use to check if we are using IRG or IRG+IRG
    protected boolean onlyIRG = false;

    //Use to check the field at pivot position
    protected int conditionPosition = 0;

    //Number of fields that involve in ROLLUP
    protected int rollupSize = 0;

    //the original index of the first field involves in ROLLUP in case it was moved to the end
    //(if we have the combination of cube and rollup)
    protected int rollupOldFieldIndex = 0;

    //This array stores the order of each fields in CUBE clause
    //before ROLLUP is moved (in case CUBE clause is the combination
    //of CUBE and ROLLUP.
    protected int outputIndex[] = null;

    //Check if we have already met the marker tuple or not
    protected boolean finished = false;

    protected static final BagFactory mBagFactory = BagFactory.getInstance();

    // finish adding new variables

    /**
     * We create a template for output the fields in a tuple in case the rollup
     * operation has been moved to the end of the operation list
     *
     * @param len
     */
    public void outputIndexInit(int len) {
        outputIndex = new int[len];
        for (int i = 0; i < len - this.rollupOldFieldIndex; i++)
            if (i < this.rollupOldFieldIndex)
                outputIndex[i] = i;
            else
                outputIndex[i] = i + rollupSize;

        int count = this.rollupOldFieldIndex;

        for (int i = len - this.rollupFieldIndex; i < len; i++)
            outputIndex[i] = count++;
    }

    public void setOnlyIRG() {
        onlyIRG = true;
    }

    /**
     * Set the original index of the first field of Rollup operation In case the
     * rollup operation has been moved to the end of the operation list
     *
     * @param rofi
     */
    public void setRollupOldFieldIndex(int rofi) {
        this.rollupOldFieldIndex = rofi;
    }

    public int getRollupOldFieldIndex() {
        return this.rollupOldFieldIndex;
    }

    public void setRollupSize(int rs) {
        this.rollupSize = rs;
    }

    public int getRollupSize() {
        return this.rollupSize;
    }

    public void setDimensionSize(int ds) {
        this.dimensionSize = ds;
    }

    public int getDimensionSize() {
        return this.dimensionSize;
    }

    /**
     * Set the updated index of the first field of Rollup operation and also
     * update the new pivot position due to the change of the rollup operation
     * position In case the rollup operation has been moved to the end of the
     * operation list
     *
     * @param rfi
     */
    public void setRollupFieldIndex(int rfi) {
        this.rollupFieldIndex = rfi;
        pivot = pivot + rollupFieldIndex;
        conditionPosition = pivot;
    }

    public int getRollupFieldIndex() {
        return this.rollupFieldIndex;
    }

    public void setPivot(int pvt) {
        this.pivot = pvt;
    }

    public int getPivot() {
        return this.pivot;
    }

    public PORollupHIIForEach(OperatorKey k) {
        this(k, -1, null, null);
    }

    public PORollupHIIForEach(OperatorKey k, int rp) {
        this(k, rp, null, null);
    }

    public PORollupHIIForEach(OperatorKey k, List inp) {
        this(k, -1, inp, null);
    }

    public PORollupHIIForEach(OperatorKey k, int rp, List<PhysicalPlan> inp, List<Boolean> isToBeFlattened) {
        super(k, rp);
        setUpFlattens(isToBeFlattened);
        this.inputPlans = inp;
        opsToBeReset = new ArrayList<PhysicalOperator>();
        getLeaves();
    }

    public PORollupHIIForEach(OperatorKey operatorKey, int requestedParallelism, List<PhysicalPlan> innerPlans,
            List<Boolean> flattenList, Schema schema) {
        this(operatorKey, requestedParallelism, innerPlans, flattenList);
        this.schema = schema;
    }

    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitPOForEach(this);
    }

    @Override
    public String name() {
        return getAliasString() + "New Rollup HII For Each" + " (" + getFlatStr() + ")" + "["
                + DataType.findTypeName(resultType) + "]" + " - " + mKey.toString();
    }

    String getFlatStr() {
        if (isToBeFlattenedArray == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Boolean b : isToBeFlattenedArray) {
            sb.append(b);
            sb.append(',');
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    @Override
    public void setAccumulative() {
        super.setAccumulative();
        for (PhysicalPlan p : inputPlans) {
            Iterator<PhysicalOperator> iter = p.iterator();
            while (iter.hasNext()) {
                PhysicalOperator po = iter.next();
                if (po instanceof ExpressionOperator || po instanceof PODistinct) {
                    po.setAccumulative();
                }
            }
        }
    }

    @Override
    public void setAccumStart() {
        super.setAccumStart();
        for (PhysicalPlan p : inputPlans) {
            Iterator<PhysicalOperator> iter = p.iterator();
            while (iter.hasNext()) {
                PhysicalOperator po = iter.next();
                if (po instanceof ExpressionOperator || po instanceof PODistinct) {
                    po.setAccumStart();
                }
            }
        }
    }

    @Override
    public void setAccumEnd() {
        super.setAccumEnd();
        for (PhysicalPlan p : inputPlans) {
            Iterator<PhysicalOperator> iter = p.iterator();
            while (iter.hasNext()) {
                PhysicalOperator po = iter.next();
                if (po instanceof ExpressionOperator || po instanceof PODistinct) {
                    po.setAccumEnd();
                }
            }
        }
    }

    /**
     * Compute the rollup operation for the first IRG
     *
     * @throws ExecException
     */
    protected void computeRollup() throws ExecException {
        int len = prevRollupDimension.size();
        int iMulti = -1;
        int index = 0;

        //Check if the ROLLUP has been moved to the end in case
        //there are CUBE and ROLLUP, update the index field that
        //we need to do ROLLUP.
        if (rollupFieldIndex != 0)
            index = rollupFieldIndex - 1;

        if (prevRollupDimension.get(len - 1) != null) {
            //find the maximum value that differs the currentRollupDimension and prevRollupDimension
            //in case there is a combination of CUBE(s) and ROLLUP(s).
            for (int i = 0; i < rollupFieldIndex; i++)
                if (DataType.compare(currentRollupDimension.get(i), prevRollupDimension.get(i)) != 0) {
                    iMulti = rollupFieldIndex - 1;
                    break;
                }

            //Calculate the rollup if the currentRollupDimension and preRollupDimension are different
            //in field that has index is in range of ROLLUP or there are multiple ROLLUP/CUBE or this is
            //the call from finish()
            for (int i = index; i < len - 1; i++) {
                if (DataType.compare(currentRollupDimension.get(i), prevRollupDimension.get(i)) != 0
                        || (rollupFieldIndex != 0 && finIRG1 == true) || iMulti != -1) {

                    // find the maximum value of the first index, which differs
                    // the currentRollupDimension and prevRollupDimension, and the pivot
                    int iTemp = Math.max(i, iMulti);
                    int x = Math.max(iTemp, pivot);

                    // create the missing tuples for rollup operation in the
                    // first IRG (in HII) or the IRG(in only IRG)
                    for (int j = len - 2; j >= x; j--) {
                        Tuple group = mTupleFactory.newTuple();
                        for (int k = 0; k < len; k++) {
                            if (k <= j) {
                                group.append(prevRollupDimension.get(k));
                            } else {
                                group.append(null);
                            }
                        }

                        //Store the value of this tuple in the tmpResult
                        Tuple out = mTupleFactory.newTuple();
                        out.append(group);

                        for (int k = 0; k < noUserFunc; k++)
                            out.append(tmpResult[j + 1][k]);

                        //call the processPlan for this new tuple to update
                        //the value in tmpResult.
                        attachInputToPlans(out);
                        returnRes[j + 1] = processPlan(j);
                        //After returning these missing tuples, clear them in the tmpResult.
                        for (int k = 0; k < noUserFunc; k++) {
                            tmpResult[j + 1][k].clear();
                        }
                    }
                    break;
                }
            }
        } else {
            for (int j = 0; j < len; j++)
                for (int k = 0; k < noUserFunc; k++) {
                    tmpResult[j][k].clear();
                }
        }
    }

    /**
     * Compute the rollup operation for the second IRG
     *
     * @throws ExecException
     */
    protected void computeRollup2() throws ExecException {
        int len = prevRollupDimension2.size();

        int index = 0;
        int iMulti = -1;

        //Check if the ROLLUP has been moved to the end in case
        //there are CUBE and ROLLUP, update the index field that
        //we need to do ROLLUP.
        if (rollupFieldIndex != 0)
            index = rollupFieldIndex - 1;

        //find the maximum value that differs the currentRollupDimension and prevRollupDimension
        //in case there is a combination of CUBE(s) and ROLLUP(s).
        for (int i = 0; i < rollupFieldIndex; i++)
            if (DataType.compare(currentRollupDimension.get(i), prevRollupDimension2.get(i)) != 0) {
                iMulti = rollupFieldIndex - 1;
                break;
            }

        //Calculate the rollup if the currentRollupDimension and preRollupDimension2 are different
        //in field that has index is in range of ROLLUP or there are multiple ROLLUP/CUBE or this is
        //the call from finish()
        for (int i = index; i < pivot; i++) {
            if (DataType.compare(currentRollupDimension.get(i), prevRollupDimension2.get(i)) != 0 || finIRG2 == true
                    || iMulti != -1) {
                int x = Math.max(iMulti, i);
                // create the missing tuples for rollup operation in the second
                // IRG
                for (int j = pivot - 2; j >= x; j--) {
                    Tuple group = mTupleFactory.newTuple();
                    for (int k = 0; k < len; k++) {
                        if (k <= j) {
                            group.append(prevRollupDimension2.get(k));
                        } else {
                            group.append(null);
                        }
                    }

                    //Store the value of this tuple in the tmpResult
                    Tuple out = mTupleFactory.newTuple();
                    out.append(group);
                    for (int k = 0; k < noUserFunc; k++)
                        out.append(tmpResult2[j + 1][k]);

                    //call the processPlan for this new tuple to update
                    //the value in tmpResult.
                    attachInputToPlans(out);
                    returnRes2[j + 1] = processPlan(j);
                    //After returning these missing tuples, clear them in the tmpResult.
                    for (int k = 0; k < noUserFunc; k++)
                        tmpResult2[j + 1][k].clear();
                }
                break;
            }
        }
    }

    /**
     * Call the final aggregation for the IRGs and return the results.
     *
     * @return returnRes
     * @throws ExecException
     */
    public Result[] finish() throws ExecException {
        if (prevRollupDimension != null) {

            if (rollupFieldIndex == 0)
                currentRollupDimension = mTupleFactory.newTuple(prevRollupDimension.size());

            finIRG1 = true;
            secondPass = false;
            computeRollup();
            secondPass = true;
        }

        //compute the final aggregation for the first IRG
        if (prevRollupDimension2 == null && prevRollupDimension != null) {
            //If there is only one ROLLUP, create new tuple
            //If there is multiple ROLLUP or combination of
            //ROLLUP and CUBE, we've already calculated the
            //last tuple ((,,,,) for example) when we compute rollup
            if (rollupFieldIndex == 0)
                computeFinalAggregation();
            return returnRes;
        }

        //if this is a IRG+IRG, we must compute the final aggregation for the
        //second IRG.
        if (secondPass) {
            //If there is only one ROLLUP, create new tuple
            //If there is multiple ROLLUP or combination of
            //ROLLUP and CUBE, we've already calculated the
            //last tuple ((,,,,) for example) when we compute rollup
            if (rollupFieldIndex == 0)
                currentRollupDimension = mTupleFactory.newTuple(prevRollupDimension2.size());
            finIRG2 = true;
            computeRollup2();
            if (pivot != 0)
                computeFinalAggregation2();
        }

        return returnRes;
    }

    /**
     * Compute the final aggregation for the second IRG
     *
     * @throws ExecException
     */
    protected void computeFinalAggregation2() throws ExecException {
        Tuple group = mTupleFactory.newTuple();
        //Create a tuple that all fields are null, do the rollup
        //for the rest values and tuples that are still stored in
        //returnRes2 and tmpResult2
        for (int k = 0; k < prevRollupDimension2.size(); k++)
            group.append(null);
        Tuple out = mTupleFactory.newTuple();
        out.append(group);
        for (int k = 0; k < noUserFunc; k++)
            out.append(tmpResult2[0][k]);
        attachInputToPlans(out);

        if(returnRes == null) {
            returnRes = new Result[dimensionSize];
            for (int i = 0; i < dimensionSize; i++) {
                returnRes[i] = null;
            }
        }

        if (rollupFieldIndex == 0)
            if(returnRes!=null)
                returnRes[0] = processPlan(-1);
            else
                returnRes2[0] = processPlan(-1);
        //Move all the not-null tuples from returnRes2 to returnRes,
        //so the returnRes will contain all the remaining tuples from two IRGs.
        for (int i = 0; i < prevRollupDimension2.size(); i++)
            if (returnRes[i] == null && returnRes2[i] != null)
                returnRes[i] = returnRes2[i];
    }

    /**
     * Compute the final aggregation for the first IRG
     *
     * @throws ExecException
     */
    protected void computeFinalAggregation() throws ExecException {
        Tuple group = mTupleFactory.newTuple();
        //Create a tuple that all fields are null, do the rollup
        //for the rest values and tuples that are still stored in
        //returnRes and tmpResult
        for (int k = 0; k < prevRollupDimension.size(); k++)
            group.append(null);
        Tuple out = mTupleFactory.newTuple();
        out.append(group);
        for (int k = 0; k < noUserFunc; k++)
            out.append(tmpResult[0][k]);
        attachInputToPlans(out);
        //-1 is passed to processPlan, this is the last tuple
        //that this PORollupHIIForEach will output
        returnRes[0] = processPlan(-1);
    }

    private boolean isEarlyTerminated() {
        return isEarlyTerminated;
    }

    private void earlyTerminate() {
        isEarlyTerminated = true;
    }

    /**
     * Calls getNext on the generate operator inside the nested physical plan
     * and returns it maintaining an additional state to denote the begin and
     * end of the nested plan processing.
     **/
    @Override
    public Result getNextTuple() throws ExecException {
        try {
            Result res = null;
            Result inp = null;
            // The nested plan is under processing
            // So return tuples that the generate oper
            // returns

            // Return the result if it's still also in the returnRes
            if (prevRollupDimension != null) {
                for (int i = prevRollupDimension.size() - 1; i >= 0; i--)
                    if (returnRes[i] != null) {
                        res = returnRes[i];
                        returnRes[i] = null;
                        return res;
                    }
            }

            // Return the result if it's still also in the returnRes2
            // We only go to the for loop if prevRollupDimension2 is not null
            // and we have not called yet the finish function.
            if (prevRollupDimension2 != null && !finished) {
                for (int i = prevRollupDimension2.size() - 1; i >= 0; i--)
                    if (returnRes2[i] != null) {
                        res = returnRes2[i];
                        returnRes2[i] = null;
                        return res;
                    }
            }

            if (processingPlan) {
                while (true) {
                    res = processPlan(currentRollupDimension.size() - 1);

                    if (res.returnStatus == POStatus.STATUS_OK) {
                        return res;
                    }
                    if (res.returnStatus == POStatus.STATUS_EOP) {
                        processingPlan = false;
                        for (PhysicalPlan plan : inputPlans) {
                            plan.detachInput();
                        }
                        break;
                    }
                    if (res.returnStatus == POStatus.STATUS_ERR) {
                        return res;
                    }
                    if (res.returnStatus == POStatus.STATUS_NULL) {
                        continue;
                    }
                }
            }
            // The nested plan processing is done or is
            // yet to begin. So process the input and start
            // nested plan processing on the input tuple
            // read
            while (true) {
                inp = processInput();
                if (inp.returnStatus == POStatus.STATUS_EOP || inp.returnStatus == POStatus.STATUS_ERR) {
                    return inp;
                }
                if (inp.returnStatus == POStatus.STATUS_NULL) {
                    continue;
                }

                inpTuple = (Tuple) inp.result;

                // Initiate the currentRollupDimension
                currentRollupDimension = null;

                if (inp.returnStatus == POStatus.STATUS_EOP) {
                    return inp;
                }

                int len = 0;
                if (inpTuple.getType(0) == DataType.TUPLE) {
                    currentRollupDimension = (Tuple) inpTuple.get(0);
                    len = currentRollupDimension.size();

                    boolean checkLast = false;

                    // The special record which has size larger than the default
                    // one to mark that we went through the last record, compute the
                    // final rollup aggregation by calling finish()
//                    if (prevRollupDimension != null)
//                        if (len > prevRollupDimension.size())
//                            checkLast = true;

                    if (len > dimensionSize)
                        checkLast = true;

                    if (checkLast) {
                        // Get the reducer's index
                        int checkFirstReducer = (Integer) currentRollupDimension.get(len - 1);
                        Result tmp[] = finish();

                        if (tmp != null) {
                            finished = true; // called finished
                            res = new Result();
                            // Change the NULL tuple to one of the remaining tuples
                            if(returnRes!=null) {
                                for(int i = dimensionSize - 1; i >=0; i--)
                                    if(returnRes[i]!=null) {
                                        res.result = returnRes[i].result;
                                        res.returnStatus = POStatus.STATUS_OK;
                                        returnRes[i] = null;
                                        break;
                                    }
                            } else {
                                for(int i = dimensionSize - 1; i >=0; i--)
                                    if(returnRes2[i]!=null) {
                                        res.result = returnRes2[i].result;
                                        res.returnStatus = POStatus.STATUS_OK;
                                        returnRes2[i] = null;
                                        break;
                                    }
                            }
                            // If this marker tuple is not for reducer0
                            // so we just output the remaining results from the
                            // pivot to the end of the length
                            if (checkFirstReducer != 0) {
                                for (int i = 0; i < pivot; i++)
                                    returnRes[i] = null;
                                if (pivot == 0)
                                    returnRes[0] = null;
                            }
                            return res;
                        }
                        //tmp is null so this reducer received only the marker tuple
                        else{
                            res = new Result();
                            res.result = null;
                            res.returnStatus = POStatus.STATUS_EOP;
                            return res;
                        }
                    }

                    // Initiate the output index template
                    if (outputIndex == null && rollupFieldIndex != 0)
                        this.outputIndexInit(len);

                }

                if (currentRollupDimension == null) {
                    res.returnStatus = POStatus.STATUS_ERR;
                    return res;
                }

                // Check if the field at the (updated - in case we has moved the
                // rollup operation to the end of the operation list) pivot
                // position
                // of the tuple is null or not. If it is not null, we compute
                // the
                // rollup using the first rollup, else, we compute the it using
                // the second IRG
                if (currentRollupDimension.get(conditionPosition) != null) {
                    secondPass = false;
                    if (prevRollupDimension != null) {
                        computeRollup();
                    } else {
                        noUserFunc = 0;
                        for (int i = 0; i < noItems; i++) {
                            if ((planLeafOps[i]) instanceof POUserFunc) {
                                noUserFunc++;
                            }
                        }
                        tmpResult = new DataBag[len][noUserFunc];
                        returnRes = new Result[len];

                        for (int i = 0; i < len; i++) {
                            for (int j = 0; j < noUserFunc; j++) {
                                tmpResult[i][j] = mBagFactory.newDefaultBag();
                            }
                            returnRes[i] = null;
                        }
                    }
                    prevRollupDimension = currentRollupDimension;
                } else {
                    secondPass = true;
                    if (prevRollupDimension2 != null) {
                        computeRollup2();
                    } else {
                        noUserFunc = 0;
                        for (int i = 0; i < noItems; i++) {
                            if ((planLeafOps[i]) instanceof POUserFunc) {
                                noUserFunc++;
                            }
                        }
                        tmpResult2 = new DataBag[len][noUserFunc];
                        returnRes2 = new Result[len];

                        for (int i = 0; i < len; i++) {
                            for (int j = 0; j < noUserFunc; j++) {
                                tmpResult2[i][j] = mBagFactory.newDefaultBag();
                            }
                            returnRes2[i] = null;
                        }
                    }
                    prevRollupDimension2 = currentRollupDimension;
                }
                attachInputToPlans((Tuple) inp.result);

                for (PhysicalOperator po : opsToBeReset) {
                    po.reset();
                }

                if (isAccumulative()) {
                    for (int i = 0; i < inpTuple.size(); i++) {
                        if (inpTuple.getType(i) == DataType.BAG) {
                            // we only need to check one bag, because all the
                            // bags
                            // share the same buffer
                            buffer = ((AccumulativeBag) inpTuple.get(i)).getTuplebuffer();
                            break;
                        }
                    }

                    setAccumStart();
                    while (true) {
                        if (!isEarlyTerminated() && buffer.hasNextBatch()) {
                            try {
                                buffer.nextBatch();
                            } catch (IOException e) {
                                throw new ExecException(e);
                            }
                        } else {
                            inpTuple = ((POPackage.POPackageTupleBuffer) buffer).illustratorMarkup(null, inpTuple, 0);
                            // buffer.clear();
                            setAccumEnd();
                        }

                        if (!secondPass)
                            returnRes[0] = processPlan(currentRollupDimension.size() - 1);
                        else if (pivot == 0)
                            returnRes2[0] = processPlan(pivot);
                        else
                            returnRes2[0] = processPlan(pivot - 1);

                        if (res.returnStatus == POStatus.STATUS_BATCH_OK) {
                            // attach same input again to process next batch
                            attachInputToPlans((Tuple) inp.result);
                        } else if (res.returnStatus == POStatus.STATUS_EARLY_TERMINATION) {
                            // if this bubbled up, then we just need to pass a
                            // null value through the pipe
                            // so that POUserFunc will properly return the
                            // values
                            attachInputToPlans(null);
                            earlyTerminate();
                        } else {
                            break;
                        }
                    }

                } else {
                    // if we are still in IRG1, we compute the rollup
                    // and store it in the returnRes
                    if (!secondPass)
                        returnRes[0] = processPlan(currentRollupDimension.size() - 1);
                    // else, we process the rollup and store it in the
                    // returnRes2
                    // if the pivot is zero, it's IRG, else, we process at the
                    // (pivot - 1)
                    // because the pivot position user specified is always
                    // larger than the
                    // index in the rollup fields by one.
                    else if (pivot == 0)
                        returnRes2[0] = processPlan(pivot);
                    else
                        returnRes2[0] = processPlan(pivot - 1);
                }

                processingPlan = true;

                // We return the result that we stored in returnRes or
                // returnRes2
                for (int i = currentRollupDimension.size() - 1; i >= 0; i--) {
                    if (!secondPass) {
                        if (returnRes[i] != null) {
                            res = returnRes[i];
                            returnRes[i] = null;
                            break;
                        }
                    } else {
                        if (returnRes2[i] != null) {
                            res = returnRes2[i];
                            returnRes2[i] = null;
                            break;
                        }
                    }
                }
                return res;
            }
        } catch (RuntimeException e) {
            throw new ExecException("Error while executing RollupHIIForEach at " + this.getOriginalLocations(), e);
        }
    }

    private boolean isEarlyTerminated = false;
    private TupleMaker<? extends Tuple> tupleMaker;

    private boolean knownSize = false;

    protected Result processPlan(int pos) throws ExecException {
        if (schema != null && tupleMaker == null) {
            // Note here that if SchemaTuple is currently turned on, then any
            // UDF's in the chain
            // must follow good practices. Namely, they should not append to the
            // Tuple that comes
            // out of an iterator (a practice which is fairly common, but is not
            // recommended).
            tupleMaker = SchemaTupleFactory.getInstance(schema, false, GenContext.FOREACH);
            if (tupleMaker != null) {
                knownSize = true;
            }
        }
        if (tupleMaker == null) {
            tupleMaker = TupleFactory.getInstance();
        }

        Result res = new Result();

        // We check if all the databags have exhausted the tuples. If so we
        // enforce the reading of new data by setting data and its to null
        if (its != null) {
            boolean restartIts = true;
            for (int i = 0; i < noItems; ++i) {
                if (its[i] != null && isToBeFlattenedArray[i] == true) {
                    restartIts &= !its[i].hasNext();
                }
            }
            // this means that all the databags have reached their last
            // elements. so we need to force reading of fresh databags
            if (restartIts) {
                its = null;
                data = null;
            }
        }

        if (its == null) {
            // getNext being called for the first time OR starting with a set of
            // new data from inputs
            its = new Iterator[noItems];
            bags = new Object[noItems];
            earlyTermination = new BitSet(noItems);

            int cnt = 0;

            for (int i = 0; i < noItems; ++i) {
                // Getting the iterators
                // populate the input data
                Result inputData = null;
                switch (resultTypes[i]) {
                case DataType.BAG:
                case DataType.TUPLE:
                case DataType.BYTEARRAY:
                case DataType.MAP:
                case DataType.BOOLEAN:
                case DataType.INTEGER:
                case DataType.DOUBLE:
                case DataType.LONG:
                case DataType.FLOAT:
                case DataType.BIGINTEGER:
                case DataType.BIGDECIMAL:
                case DataType.DATETIME:
                case DataType.CHARARRAY:
                    inputData = planLeafOps[i].getNext(resultTypes[i]);
                    // We stores the values that we want to compute the rollup
                    // in tmpResult for the first IRG and in tmpResult2 for the second IRG
                    if (((planLeafOps[i]) instanceof POUserFunc) && (inputData.result != null) && (pos != -1))
                        if (!secondPass) {
                            tmpResult[pos][cnt++].add(mTupleFactory.newTuple(inputData.result));
                        } else {
                            tmpResult2[pos][cnt++].add(mTupleFactory.newTuple(inputData.result));
                        }
                    break;
                default: {
                    int errCode = 2080;
                    String msg = "Foreach currently does not handle type " + DataType.findTypeName(resultTypes[i]);
                    throw new ExecException(msg, errCode, PigException.BUG);
                }

                }

                // we accrue information about what accumulators have early
                // terminated
                // in the case that they all do, we can finish
                if (inputData.returnStatus == POStatus.STATUS_EARLY_TERMINATION) {
                    if (!earlyTermination.get(i))
                        earlyTermination.set(i);

                    continue;
                }

                if (inputData.returnStatus == POStatus.STATUS_BATCH_OK) {
                    continue;
                }

                if (inputData.returnStatus == POStatus.STATUS_EOP) {
                    // we are done with all the elements. Time to return.
                    its = null;
                    bags = null;
                    return inputData;
                }
                // if we see a error just return it
                if (inputData.returnStatus == POStatus.STATUS_ERR) {
                    return inputData;
                }

                // Object input = null;

                bags[i] = inputData.result;

                if (inputData.result instanceof DataBag && isToBeFlattenedArray[i]) {
                    its[i] = ((DataBag) bags[i]).iterator();
                } else {
                    its[i] = null;
                }
            }
        }

        // if accumulating, we haven't got data yet for some fields, just return
        if (isAccumulative() && isAccumStarted()) {
            if (earlyTermination.cardinality() < noItems) {
                res.returnStatus = POStatus.STATUS_BATCH_OK;
            } else {
                res.returnStatus = POStatus.STATUS_EARLY_TERMINATION;
            }
            return res;
        }

        while (true) {
            if (data == null) {
                // getNext being called for the first time or starting on new
                // input data
                // we instantiate the template array and start populating it
                // with data
                data = new Object[noItems];
                for (int i = 0; i < noItems; ++i) {
                    if (isToBeFlattenedArray[i] && bags[i] instanceof DataBag) {
                        if (its[i].hasNext()) {
                            data[i] = its[i].next();
                        } else {
                            // the input set is null, so we return. This is
                            // caught above and this function recalled with
                            // new inputs.
                            its = null;
                            data = null;
                            res.returnStatus = POStatus.STATUS_NULL;
                            return res;
                        }
                    } else {
                        data[i] = bags[i];
                    }

                }
                if (getReporter() != null) {
                    getReporter().progress();
                }
                // createTuple(data);

                res.result = createTuple(data);

                res.returnStatus = POStatus.STATUS_OK;
                return res;
            } else {
                // we try to find the last expression which needs flattening and
                // start iterating over it
                // we also try to update the template array
                for (int index = noItems - 1; index >= 0; --index) {
                    if (its[index] != null && isToBeFlattenedArray[index]) {
                        if (its[index].hasNext()) {
                            data[index] = its[index].next();
                            res.result = createTuple(data);
                            res.returnStatus = POStatus.STATUS_OK;
                            return res;
                        } else {
                            its[index] = ((DataBag) bags[index]).iterator();
                            data[index] = its[index].next();
                        }
                    }
                }
            }
        }
    }

    /**
     * We create a new tuple for the final flattened tuple, in case the rollup
     * operation has been moved to the end of the operation list, we re-order
     * the fields as the same order as the input's by using the outputIndex we
     * initialized before.
     *
     * @param data: array that is the template for the final flattened tuple
     * @return the final flattened tuple
     */
    protected Tuple createTuple(Object[] data) throws ExecException {
        Tuple out = tupleMaker.newTuple();
        Tuple temp = mTupleFactory.newTuple();
        int idx = 0;
        for (int i = 0; i < data.length; ++i) {
            Object in = data[i];

            if ((isToBeFlattenedArray[i] || rollupFieldIndex != 0) && in instanceof Tuple) {
                Tuple t = (Tuple) in;
                int size = t.size();

                //Output the fields'order in the tuple due to
                //the outputIndex. In this case, the ROLLUP was
                //moved to the end so we need to re-order the field
                //inside a tuple to output the fields as the original orders.
                if (rollupFieldIndex != 0) {
                    if (!isToBeFlattenedArray[i]) {
                        for (int j = 0; j < size; j++)
                            temp.append(t.get(outputIndex[j]));
                        Object inn = temp;
                        out.append(inn);
                    } else {
                        for (int j = 0; j < size; j++)
                            out.append(t.get(outputIndex[j]));
                    }
                //There's no moving of ROLLUP to the end, we don't need
                //to re-order the fields'index.
                } else {
                    for (int j = 0; j < size; ++j) {
                        if (knownSize) {
                            out.set(idx++, t.get(j));
                        } else {
                            out.append(t.get(j));
                        }
                    }
                }
            } else {
                if (knownSize) {
                    out.set(idx++, in);
                } else {
                    out.append(in);
                }
            }
        }
        if (inpTuple != null) {
            return illustratorMarkup(inpTuple, out, 0);
        } else {
            return illustratorMarkup2(data, out);
        }
    }

    /**
     * Make a deep copy of this operator.
     *
     * @throws CloneNotSupportedException
     */
    @Override
    public PORollupHIIForEach clone() throws CloneNotSupportedException {
        List<PhysicalPlan> plans = new ArrayList<PhysicalPlan>(inputPlans.size());
        for (PhysicalPlan plan : inputPlans) {
            plans.add(plan.clone());
        }
        List<Boolean> flattens = null;
        if (isToBeFlattenedArray != null) {
            flattens = new ArrayList<Boolean>(isToBeFlattenedArray.length);
            for (boolean b : isToBeFlattenedArray) {
                flattens.add(b);
            }
        }

        List<PhysicalOperator> ops = new ArrayList<PhysicalOperator>(opsToBeReset.size());
        for (PhysicalOperator op : opsToBeReset) {
            ops.add(op);
        }
        PORollupHIIForEach clone = new PORollupHIIForEach(new OperatorKey(mKey.scope, NodeIdGenerator
                .getGenerator().getNextNodeId(mKey.scope)), requestedParallelism, plans, flattens);
        clone.setOpsToBeReset(ops);
        clone.setResultType(getResultType());
        clone.addOriginalLocation(alias, getOriginalLocations());
        return clone;
    }

    protected void attachInputToPlans(Tuple t) {
        // super.attachInput(t);
        for (PhysicalPlan p : inputPlans) {
            p.attachInput(t);
        }
    }

    public void getLeaves() {
        if (inputPlans != null) {
            int i = -1;
            if (isToBeFlattenedArray == null) {
                isToBeFlattenedArray = new boolean[inputPlans.size()];
            }
            planLeafOps = new PhysicalOperator[inputPlans.size()];
            for (PhysicalPlan p : inputPlans) {
                ++i;
                PhysicalOperator leaf = p.getLeaves().get(0);
                planLeafOps[i] = leaf;
                if (leaf instanceof POProject && leaf.getResultType() == DataType.TUPLE
                        && ((POProject) leaf).isProjectToEnd()) {
                    isToBeFlattenedArray[i] = true;
                }
            }
        }
        // we are calculating plan leaves
        // so lets reinitialize
        reInitialize();
    }

    private void reInitialize() {
        if (planLeafOps != null) {
            noItems = planLeafOps.length;
            resultTypes = new byte[noItems];
            for (int i = 0; i < resultTypes.length; i++) {
                resultTypes[i] = planLeafOps[i].getResultType();
            }
        } else {
            noItems = 0;
            resultTypes = null;
        }

        if (inputPlans != null) {
            for (PhysicalPlan pp : inputPlans) {
                try {
                    ResetFinder lf = new ResetFinder(pp, opsToBeReset);
                    lf.visit();
                } catch (VisitorException ve) {
                    String errMsg = "Internal Error:  Unexpected error looking for nested operators which need to be reset in FOREACH";
                    throw new RuntimeException(errMsg, ve);
                }
            }
        }
    }

    public List<PhysicalPlan> getInputPlans() {
        return inputPlans;
    }

    public void setInputPlans(List<PhysicalPlan> plans) {
        inputPlans = plans;
        planLeafOps = null;
        getLeaves();
    }

    public void addInputPlan(PhysicalPlan plan, boolean flatten) {
        inputPlans.add(plan);
        // add to planLeafOps
        // copy existing leaves
        PhysicalOperator[] newPlanLeafOps = new PhysicalOperator[planLeafOps.length + 1];
        for (int i = 0; i < planLeafOps.length; i++) {
            newPlanLeafOps[i] = planLeafOps[i];
        }
        // add to the end
        newPlanLeafOps[planLeafOps.length] = plan.getLeaves().get(0);
        planLeafOps = newPlanLeafOps;

        // add to isToBeFlattenedArray
        // copy existing values
        boolean[] newIsToBeFlattenedArray = new boolean[isToBeFlattenedArray.length + 1];
        for (int i = 0; i < isToBeFlattenedArray.length; i++) {
            newIsToBeFlattenedArray[i] = isToBeFlattenedArray[i];
        }
        // add to end
        newIsToBeFlattenedArray[isToBeFlattenedArray.length] = flatten;
        isToBeFlattenedArray = newIsToBeFlattenedArray;

        // we just added a leaf - reinitialize
        reInitialize();
    }

    public void setToBeFlattened(List<Boolean> flattens) {
        setUpFlattens(flattens);
    }

    public List<Boolean> getToBeFlattened() {
        List<Boolean> result = null;
        if (isToBeFlattenedArray != null) {
            result = new ArrayList<Boolean>();
            for (int i = 0; i < isToBeFlattenedArray.length; i++) {
                result.add(isToBeFlattenedArray[i]);
            }
        }
        return result;
    }

    public boolean inProcessing() {
        return processingPlan;
    }

    protected void setUpFlattens(List<Boolean> isToBeFlattened) {
        if (isToBeFlattened == null) {
            isToBeFlattenedArray = null;
        } else {
            isToBeFlattenedArray = new boolean[isToBeFlattened.size()];
            int i = 0;
            for (Iterator<Boolean> it = isToBeFlattened.iterator(); it.hasNext();) {
                isToBeFlattenedArray[i++] = it.next();
            }
        }
    }

    /**
     * Visits a pipeline and calls reset on all the nodes. Currently only pays
     * attention to limit nodes, each of which need to be told to reset their
     * limit.
     */
    protected class ResetFinder extends PhyPlanVisitor {

        ResetFinder(PhysicalPlan plan, List<PhysicalOperator> toBeReset) {
            super(plan, new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan));
        }

        @Override
        public void visitDistinct(PODistinct d) throws VisitorException {
            // FIXME: add only if limit is present
            opsToBeReset.add(d);
        }

        @Override
        public void visitLimit(POLimit limit) throws VisitorException {
            opsToBeReset.add(limit);
        }

        @Override
        public void visitSort(POSort sort) throws VisitorException {
            // FIXME: add only if limit is present
            opsToBeReset.add(sort);
        }

        /*
         * (non-Javadoc)
         *
         * @see
         * org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans
         * .PhyPlanVisitor
         * #visitProject(org.apache.pig.backend.hadoop.executionengine
         * .physicalLayer.expressionOperators.POProject)
         */
        @Override
        public void visitProject(POProject proj) throws VisitorException {
            if (proj instanceof PORelationToExprProject) {
                opsToBeReset.add(proj);
            }
        }
    }

    /**
     * @return the opsToBeReset
     */
    public List<PhysicalOperator> getOpsToBeReset() {
        return opsToBeReset;
    }

    /**
     * @param opsToBeReset
     *            the opsToBeReset to set
     */
    public void setOpsToBeReset(List<PhysicalOperator> opsToBeReset) {
        this.opsToBeReset = opsToBeReset;
    }

    protected Tuple illustratorMarkup2(Object[] in, Object out) {
        if (illustrator != null) {
            ExampleTuple tOut = new ExampleTuple((Tuple) out);
            illustrator.getLineage().insert(tOut);
            boolean synthetic = false;
            for (Object tIn : in) {
                synthetic |= ((ExampleTuple) tIn).synthetic;
                illustrator.getLineage().union(tOut, (Tuple) tIn);
            }
            illustrator.addData(tOut);
            int i;
            for (i = 0; i < noItems; ++i) {
                if (((DataBag) bags[i]).size() < 2) {
                    break;
                }
            }
            if (i >= noItems && !illustrator.getEqClassesShared()) {
                illustrator.getEquivalenceClasses().get(0).add(tOut);
            }
            tOut.synthetic = synthetic;
            return tOut;
        } else {
            return (Tuple) out;
        }
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        if (illustrator != null) {
            ExampleTuple tOut = new ExampleTuple((Tuple) out);
            illustrator.addData(tOut);
            if (!illustrator.getEqClassesShared()) {
                illustrator.getEquivalenceClasses().get(0).add(tOut);
            }
            LineageTracer lineageTracer = illustrator.getLineage();
            lineageTracer.insert(tOut);
            tOut.synthetic = ((ExampleTuple) in).synthetic;
            lineageTracer.union((ExampleTuple) in, tOut);
            return tOut;
        } else {
            return (Tuple) out;
        }
    }

}
