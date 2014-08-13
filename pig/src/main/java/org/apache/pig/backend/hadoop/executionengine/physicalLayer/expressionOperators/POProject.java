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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.SingleTupleBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

/**
 * Implements the overloaded form of the project operator.
 * Projects the specified column from the input tuple.
 * However, if asked for tuples when the input is a bag,
 * the overloaded form is invoked and the project streams
 * the tuples through instead of the bag.
 */
public class POProject extends ExpressionOperator {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private static TupleFactory tupleFactory = TupleFactory.getInstance();

    protected static final BagFactory bagFactory = BagFactory.getInstance();

    private boolean resultSingleTupleBag = false;

    //The column to project
    protected ArrayList<Integer> columns;

    //True if we are in the middle of streaming tuples
    //in a bag
    boolean processingBagOfTuples = false;

    //The bag iterator used while straeming tuple
    transient Iterator<Tuple> bagIterator = null;

    //Represents the fact that this instance of POProject
    //is overloaded to stream tuples in the bag rather
    //than passing the entire bag. It is the responsibility
    //of the translator to set this.
    boolean overloaded = false;


    private boolean isProjectToEnd = false;
    private int startCol;

    public POProject(OperatorKey k) {
        this(k,-1,0);
    }

    public POProject(OperatorKey k, int rp) {
        this(k, rp, 0);
    }

    public POProject(OperatorKey k, int rp, int col) {
        super(k, rp);
        columns = new ArrayList<Integer>();
        columns.add(col);
    }

    public POProject(OperatorKey k, int rp, ArrayList<Integer> cols) {
        super(k, rp);
        columns = cols;
    }

    public void setProjectToEnd(int startCol){
        this.isProjectToEnd = true;
        this.startCol = startCol;
        columns = new ArrayList<Integer>();
    }

    @Override
    public String name() {

        String str = "Project" + "[" + DataType.findTypeName(resultType) + "]";

        if(isStar()){
            str += "[*]";
        }else if(isProjectToEnd){
            str += "[" + startCol + " .. " + "]";
        }else{
            str += columns;
        }

        str += " - " + mKey.toString();
        return str;

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
        v.visitProject(this);
    }


    /**
     * Overridden since the attachment of the new input
     * should cause the old processing to end.
     */
    @Override
    public void attachInput(Tuple t) {
        super.attachInput(t);
        processingBagOfTuples = false;
    }

    /**
     * Fetches the input tuple and returns the requested
     * column
     * @return next value.
     * @throws ExecException
     */
    public Result getNext() throws ExecException{
        Result res = processInput();
        Tuple inpValue = (Tuple)res.result;

        Object ret;
        if(res.returnStatus != POStatus.STATUS_OK){
            return res;
        }
        if (isStar()) {
            illustratorMarkup(inpValue, res.result, -1);
            return res;
        } else if(columns.size() == 1) {
            try {
                ret = inpValue.get(columns.get(0));
            } catch (IndexOutOfBoundsException ie) {
                if(pigLogger != null) {
                    pigLogger.warn(this,"Attempt to access field " +
                            "which was not found in the input", PigWarning.ACCESSING_NON_EXISTENT_FIELD);
                }
                res.returnStatus = POStatus.STATUS_OK;
                ret = null;
            } catch (NullPointerException npe) {
                // the tuple is null, so a dereference should also produce a null
                // there is a slight danger here that the Tuple implementation
                // may have given the exception for a different reason but if we
                // don't catch it, we will die and the most common case for the
                // exception would be because the tuple is null
                res.returnStatus = POStatus.STATUS_OK;
                ret = null;
            }
        } else if(isProjectToEnd){
            ret = getRangeTuple(inpValue);
        }
        else {
            ArrayList<Object> objList = new ArrayList<Object>(columns.size());

            for(int col : columns) {
                addColumn(objList, inpValue, col);
            }
            ret = tupleFactory.newTupleNoCopy(objList);
        }
        res.result = ret;
        illustratorMarkup(inpValue, res.result, -1);
        return res;
    }

    private boolean isRangeInvalid(int lastColIdx) {
        if(startCol > lastColIdx){
            // this must be happening because tuple is smaller than startCol
            if(pigLogger != null) {
                pigLogger.warn(this, "Invalid range being projected," +
                        " startCol postition" + startCol + " greater than tuple size",
                        PigWarning.PROJECTION_INVALID_RANGE
                );
            }
            return true;
        }
        return false;
    }

    /**
     * Add i'th column from inpValue to objList
     * @param objList
     * @param inpValue
     * @param i
     * @throws ExecException
     */
    private void addColumn(ArrayList<Object> objList, Tuple inpValue, int i)
    throws ExecException {
        try {
            objList.add(inpValue.get(i));
        } catch (IndexOutOfBoundsException ie) {
            if(pigLogger != null) {
                pigLogger.warn(this,"Attempt to access field " + i +
                        " which was not found in the input", PigWarning.ACCESSING_NON_EXISTENT_FIELD);
            }
            objList.add(null);
        }
        catch (NullPointerException npe) {
            // the tuple is null, so a dereference should also produce a null
            // there is a slight danger here that the Tuple implementation
            // may have given the exception for a different reason but if we
            // don't catch it, we will die and the most common case for the
            // exception would be because the tuple is null
            objList.add(null);
        }
    }

    @Override
    public Result getNextDataBag() throws ExecException {

        Result res = processInputBag();
        if(res.returnStatus!=POStatus.STATUS_OK)
            return res;
        return(consumeInputBag(res));
    }

    /**
     * @param input
     * @throws ExecException
     */
    protected Result consumeInputBag(Result input) throws ExecException {
        if(isInputAttached() || isStar()){
            Result retVal = new Result();
            retVal.result = input.result;
            retVal.returnStatus = POStatus.STATUS_OK;
            detachInput();
            return retVal;
        }

        if (input.result instanceof DataBag) {
            DataBag inpBag = (DataBag) input.result;
            Result retVal = new Result();

            DataBag outBag;
            if(resultSingleTupleBag) {
                // we have only one tuple in a bag - so create
                // A SingleTupleBag for the result and fill it
                // appropriately from the input bag
                Tuple tuple = inpBag.iterator().next();
                if(!isProjectToEnd){
                    ArrayList<Object> objList = new ArrayList<Object>(columns.size());
                    for (int col : columns) {
                        addColumn(objList, tuple, col);
                    }
                    outBag = new SingleTupleBag( tupleFactory.newTupleNoCopy(objList) );
                }else {
                    Tuple tmpTuple = getRangeTuple(tuple);
                    outBag = new SingleTupleBag(tmpTuple);
                }
            } else {
                outBag = bagFactory.newDefaultBag();
                for (Tuple tuple : inpBag) {
                    if(!isProjectToEnd){
                        ArrayList<Object> objList = new ArrayList<Object>(columns.size());
                        for (int col : columns) {
                            addColumn(objList, tuple, col);
                        }
                        outBag.add( tupleFactory.newTupleNoCopy(objList) );
                    }else{
                        Tuple outTuple = getRangeTuple(tuple);
                        outBag.add(outTuple);
                    }
                }
            }
            retVal.result = outBag;
            retVal.returnStatus = POStatus.STATUS_OK;
            return retVal;
        } else if (input.result instanceof Tuple) {
            // if input is tuple, columns should only have one item
            Result retVal = new Result();
            retVal.result = ((Tuple)input.result).get(columns.get(0));
            retVal.returnStatus = POStatus.STATUS_OK;
            return retVal;
        } else if (input.result==null) {
            Result retVal = new Result();
            retVal.result = null;
            retVal.returnStatus = POStatus.STATUS_OK;
            return retVal;
        } else {
            throw new ExecException("Cannot dereference a bag from " + input.result.getClass().getName(), 1129);
        }
    }

    private Tuple getRangeTuple(Tuple tuple) throws ExecException {
        int lastColIdx = tuple.size() - 1;
        Tuple outTuple;
        if(isRangeInvalid(lastColIdx)){
            //invalid range - return empty tuple
            outTuple = tupleFactory.newTuple();
        }
        else {
            ArrayList<Object> objList = new ArrayList<Object>(lastColIdx - startCol + 1);
            for(int i = startCol; i <= lastColIdx ; i++){
                addColumn(objList, tuple, i);
            }
            outTuple = tupleFactory.newTupleNoCopy(objList);
        }
        return outTuple;
    }

    @Override
    public Result getNextDataByteArray() throws ExecException {
        return getNext();
    }

    @Override
    public Result getNextDouble() throws ExecException {
        return getNext();
    }

    @Override
    public Result getNextFloat() throws ExecException {
        return getNext();
    }

    @Override
    public Result getNextInteger() throws ExecException {
        return getNext();
    }

    @Override
    public Result getNextLong() throws ExecException {
        return getNext();
    }



    @Override
    public Result getNextBoolean() throws ExecException {
        return getNext();
    }

    @Override
    public Result getNextDateTime() throws ExecException {
        return getNext();
    }

    @Override
    public Result getNextMap() throws ExecException {
        return getNext();
    }

    @Override
    public Result getNextString() throws ExecException {
        return getNext();
    }

    @Override
    public Result getNextBigInteger() throws ExecException {
        return getNext();
    }

    @Override
    public Result getNextBigDecimal() throws ExecException {
        return getNext();
    }

    /**
     * Asked for Tuples. Check if the input is a bag.
     * If so, stream the tuples in the bag instead of
     * the entire bag.
     */
    @Override
    public Result getNextTuple() throws ExecException {
        Result res = new Result();
        if(!processingBagOfTuples){
            Tuple inpValue = null;
            res = processInput();
            if(res.returnStatus!=POStatus.STATUS_OK)
                return res;
            if(isStar())
                return res;

            inpValue = (Tuple)res.result;
            res.result = null;

            Object ret;

            if(columns.size() == 1) {
                try{
                    ret = inpValue.get(columns.get(0));
                } catch (IndexOutOfBoundsException ie) {
                    if(pigLogger != null) {
                        pigLogger.warn(this,"Attempt to access field " +
                                "which was not found in the input", PigWarning.ACCESSING_NON_EXISTENT_FIELD);
                    }
                    ret = null;
                } catch (NullPointerException npe) {
                    // the tuple is null, so a dereference should also produce a null
                    // there is a slight danger here that the Tuple implementation
                    // may have given the exception for a different reason but if we
                    // don't catch it, we will die and the most common case for the
                    // exception would be because the tuple is null
                    ret = null;
                }
            } else if(isProjectToEnd) {
                ret = getRangeTuple(inpValue);
            } else {
                ArrayList<Object> objList = new ArrayList<Object>(columns.size());

                for(int col: columns) {
                    try {
                        objList.add(inpValue.get(col));
                    } catch (IndexOutOfBoundsException ie) {
                        if(pigLogger != null) {
                            pigLogger.warn(this,"Attempt to access field " +
                                    "which was not found in the input", PigWarning.ACCESSING_NON_EXISTENT_FIELD);
                        }
                        objList.add(null);
                    } catch (NullPointerException npe) {
                        // the tuple is null, so a dereference should also produce a null
                        // there is a slight danger here that the Tuple implementation
                        // may have given the exception for a different reason but if we
                        // don't catch it, we will die and the most common case for the
                        // exception would be because the tuple is null
                        objList.add(null);
                    }
                }
                ret = tupleFactory.newTuple(objList);
                res.result = (Tuple)ret;
                return res;
            }

            if(overloaded){
                if (ret!=null) {
                    DataBag retBag = (DataBag)ret;
                    bagIterator = retBag.iterator();
                    if(bagIterator.hasNext()){
                        processingBagOfTuples = true;
                        res.result = bagIterator.next();
                    }
                    // If the bag contains no tuple, set the returnStatus to STATUS_EOP
                    if (!processingBagOfTuples)
                        res.returnStatus = POStatus.STATUS_EOP;
                } else {
                    res.returnStatus = POStatus.STATUS_EOP;
                }
            }
            else {
                res.result = (Tuple)ret;
            }
            return res;
        }
        if(bagIterator.hasNext()){
            res.result = bagIterator.next();
            res.returnStatus = POStatus.STATUS_OK;
            return res;
        }
        else{
            //done processing the bag of tuples
            processingBagOfTuples = false;
            return getNextTuple();
        }
    }

    public ArrayList<Integer> getColumns(){
        if(isProjectToEnd) {
            throw new AssertionError("Internal error. Improper use of method getColumns() in "
                + POProject.class.getSimpleName());
        }
        return columns;
    }

    public int getColumn() throws ExecException {
        if(columns.size() != 1 || isProjectToEnd) {
            int errCode = 2068;
            String msg = "Internal error. Improper use of method getColumn() in "
                + POProject.class.getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG);
        }
        return columns.get(0);
    }

    public int getStartCol(){
        return startCol;
    }

    public void setColumns(ArrayList<Integer> cols) {
        if(isProjectToEnd){
            throw new AssertionError("Columns should not be set for range projection");
        }
        this.columns = cols;
    }

    public void setColumn(int col) {
        isProjectToEnd = false;
        if(null == columns) {
            columns = new ArrayList<Integer>();
        } else {
            columns.clear();
        }
        columns.add(col);
    }

    public boolean isOverloaded() {
        return overloaded;
    }

    public void setOverloaded(boolean overloaded) {
        this.overloaded = overloaded;
    }

    public boolean isStar() {
        return isProjectToEnd && startCol == 0;
    }

    public boolean isProjectToEnd(){
        return isProjectToEnd;
    }

    public void setStar(boolean star) {
        if(star){
            isProjectToEnd = true;
            startCol = 0;
        }else{
            isProjectToEnd = false;
        }
    }

    @Override
    public POProject clone() throws CloneNotSupportedException {
        ArrayList<Integer> cols = new ArrayList<Integer>(columns.size());
        // Can resuse the same Integer objects, as they are immutable
        for (Integer i : columns) {
            cols.add(i);
        }
        POProject clone = new POProject(new OperatorKey(mKey.scope,
            NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)),
            requestedParallelism, cols);
        clone.cloneHelper(this);
        clone.overloaded = overloaded;
        clone.startCol = startCol;
        clone.isProjectToEnd = isProjectToEnd;
        clone.resultType = resultType;
        return clone;
    }

    protected Result processInputBag() throws ExecException {

        Result res = new Result();
        if (input==null && (inputs == null || inputs.size()==0)) {
//            log.warn("No inputs found. Signaling End of Processing.");
            res.returnStatus = POStatus.STATUS_EOP;
            return res;
        }

        //Should be removed once the model is clear
        if(getReporter()!=null) {
            getReporter().progress();
        }

        if(!isInputAttached()) {
            if (inputs.get(0).getResultType()==DataType.BAG)
                return inputs.get(0).getNextDataBag();
            else
                return inputs.get(0).getNextTuple();
        }
        else{
            res.result = (DataBag)input.get(columns.get(0));
            res.returnStatus = POStatus.STATUS_OK;
            return res;
        }
    }

    public void setResultSingleTupleBag(boolean resultSingleTupleBag) {
        this.resultSingleTupleBag = resultSingleTupleBag;
    }

    @Override
    public List<ExpressionOperator> getChildExpressions() {
        return null;
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        if(illustrator != null) {

        }
        return null;
    }
}
