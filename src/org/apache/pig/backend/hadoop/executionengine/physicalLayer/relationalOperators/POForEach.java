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
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.AccumulativeBag;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;
import org.apache.pig.pen.util.LineageTracer;

//We intentionally skip type checking in backend for performance reasons
@SuppressWarnings("unchecked")
public class POForEach extends PhysicalOperator {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    protected List<PhysicalPlan> inputPlans;
    protected List<PhysicalOperator> opsToBeReset;
    protected static final TupleFactory mTupleFactory = TupleFactory.getInstance();
    //Since the plan has a generate, this needs to be maintained
    //as the generate can potentially return multiple tuples for
    //same call.
    protected boolean processingPlan = false;

    //its holds the iterators of the databags given by the input expressions which need flattening.
    transient protected Iterator<Tuple> [] its = null;

    //This holds the outputs given out by the input expressions of any datatype
    protected Object [] bags = null;

    //This is the template whcih contains tuples and is flattened out in createTuple() to generate the final output
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

    public POForEach(OperatorKey k) {
        this(k,-1,null,null);
    }

    public POForEach(OperatorKey k, int rp, List inp) {
        this(k,rp,inp,null);
    }

    public POForEach(OperatorKey k, int rp) {
        this(k,rp,null,null);
    }

    public POForEach(OperatorKey k, List inp) {
        this(k,-1,inp,null);
    }

    public POForEach(OperatorKey k, int rp, List<PhysicalPlan> inp, List<Boolean>  isToBeFlattened){
        super(k, rp);
        setUpFlattens(isToBeFlattened);
        this.inputPlans = inp;
        opsToBeReset = new ArrayList<PhysicalOperator>();
        getLeaves();
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitPOForEach(this);
    }

    @Override
    public String name() {
        return getAliasString() + "New For Each" + "(" + getFlatStr() + ")" + "["
                + DataType.findTypeName(resultType) + "]" + " - "
                + mKey.toString();
    }

    String getFlatStr() {
        if(isToBeFlattenedArray ==null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Boolean b : isToBeFlattenedArray) {
            sb.append(b);
            sb.append(',');
        }
        if(sb.length()>0){
            sb.deleteCharAt(sb.length()-1);
        }
        return sb.toString();
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
    public void setAccumulative() {
        super.setAccumulative();
        for(PhysicalPlan p : inputPlans) {
            Iterator<PhysicalOperator> iter = p.iterator();
            while(iter.hasNext()) {
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
        for(PhysicalPlan p : inputPlans) {
            Iterator<PhysicalOperator> iter = p.iterator();
            while(iter.hasNext()) {
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
        for(PhysicalPlan p : inputPlans) {
            Iterator<PhysicalOperator> iter = p.iterator();
            while(iter.hasNext()) {
                PhysicalOperator po = iter.next();
                if (po instanceof ExpressionOperator || po instanceof PODistinct) {
                    po.setAccumEnd();
                }
            }
        }
    }

    /**
     * Calls getNext on the generate operator inside the nested
     * physical plan and returns it maintaining an additional state
     * to denote the begin and end of the nested plan processing.
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
        Result res = null;
        Result inp = null;
        //The nested plan is under processing
        //So return tuples that the generate oper
        //returns
        if(processingPlan){
            while(true) {
                res = processPlan();

                if(res.returnStatus==POStatus.STATUS_OK) {
                    return res;
                }
                if(res.returnStatus==POStatus.STATUS_EOP) {
                    processingPlan = false;
                    for(PhysicalPlan plan : inputPlans) {
                        plan.detachInput();
                    }
                    break;
                }
                if(res.returnStatus==POStatus.STATUS_ERR) {
                    return res;
                }
                if(res.returnStatus==POStatus.STATUS_NULL) {
                    continue;
                }
            }
        }
        //The nested plan processing is done or is
        //yet to begin. So process the input and start
        //nested plan processing on the input tuple
        //read
        while (true) {
            inp = processInput();
            if (inp.returnStatus == POStatus.STATUS_EOP ||
                    inp.returnStatus == POStatus.STATUS_ERR) {
                return inp;
            }
            if (inp.returnStatus == POStatus.STATUS_NULL) {
                continue;
            }

            attachInputToPlans((Tuple) inp.result);
            inpTuple = (Tuple)inp.result;

            for (PhysicalOperator po : opsToBeReset) {
                po.reset();
            }

            if (isAccumulative()) {
                for(int i=0; i<inpTuple.size(); i++) {
                    if (inpTuple.getType(i) == DataType.BAG) {
                        // we only need to check one bag, because all the bags
                        // share the same buffer
                        buffer = ((AccumulativeBag)inpTuple.get(i)).getTuplebuffer();
                        break;
                    }
                }

                setAccumStart();
                while(true) {
                    if (!isEarlyTerminated() && buffer.hasNextBatch()) {
                        try {
                            buffer.nextBatch();
                        }catch(IOException e) {
                            throw new ExecException(e);
                        }
                    }else{
                        inpTuple = ((POPackage.POPackageTupleBuffer) buffer).illustratorMarkup(null, inpTuple, 0);
 //                       buffer.clear();
                        setAccumEnd();
                    }

                    res = processPlan();

                    if (res.returnStatus == POStatus.STATUS_BATCH_OK) {
                        // attach same input again to process next batch
                        attachInputToPlans((Tuple) inp.result);
                    } else if (res.returnStatus == POStatus.STATUS_EARLY_TERMINATION) {
                        //if this bubbled up, then we just need to pass a null value through the pipe
                        //so that POUserFunc will properly return the values
                        attachInputToPlans(null);
                        earlyTerminate();
                    } else {
                        break;
                    }
                }

            } else {
                res = processPlan();
            }

            processingPlan = true;

            return res;
        }
    }

    private boolean isEarlyTerminated = false;

    private boolean isEarlyTerminated() {
        return isEarlyTerminated;
    }

    private void earlyTerminate() {
        isEarlyTerminated = true;
    }

    protected Result processPlan() throws ExecException{
        Result res = new Result();

        //We check if all the databags have exhausted the tuples. If so we enforce the reading of new data by setting data and its to null
        if(its != null) {
            boolean restartIts = true;
            for(int i = 0; i < noItems; ++i) {
                if(its[i] != null && isToBeFlattenedArray[i] == true) {
                    restartIts &= !its[i].hasNext();
                }
            }
            //this means that all the databags have reached their last elements. so we need to force reading of fresh databags
            if(restartIts) {
                its = null;
                data = null;
            }
        }


        if(its == null) {
            //getNext being called for the first time OR starting with a set of new data from inputs
            its = new Iterator[noItems];
            bags = new Object[noItems];
            earlyTermination = new BitSet(noItems);

            for(int i = 0; i < noItems; ++i) {
                //Getting the iterators
                //populate the input data
                Result inputData = null;
                switch(resultTypes[i]) {
                case DataType.BAG:
                case DataType.TUPLE :
                case DataType.BYTEARRAY :
                case DataType.MAP :
                case DataType.BOOLEAN :
                case DataType.INTEGER :
                case DataType.DOUBLE :
                case DataType.LONG :
                case DataType.FLOAT :
                case DataType.DATETIME :
                case DataType.CHARARRAY :
                    inputData = planLeafOps[i].getNext(getDummy(resultTypes[i]), resultTypes[i]);
                    break;
                default: {
                    int errCode = 2080;
                    String msg = "Foreach currently does not handle type " + DataType.findTypeName(resultTypes[i]);
                    throw new ExecException(msg, errCode, PigException.BUG);
                }

                }

                //we accrue information about what accumulators have early terminated
                //in the case that they all do, we can finish
                if (inputData.returnStatus == POStatus.STATUS_EARLY_TERMINATION) {
                    if (!earlyTermination.get(i))
                        earlyTermination.set(i);

                    continue;
                }

                if (inputData.returnStatus == POStatus.STATUS_BATCH_OK) {
                    continue;
                }

                if(inputData.returnStatus == POStatus.STATUS_EOP) {
                    //we are done with all the elements. Time to return.
                    its = null;
                    bags = null;
                    return inputData;
                }
                // if we see a error just return it
                if(inputData.returnStatus == POStatus.STATUS_ERR) {
                    return inputData;
                }

//                Object input = null;

                bags[i] = inputData.result;

                if(inputData.result instanceof DataBag && isToBeFlattenedArray[i]) {
                    its[i] = ((DataBag)bags[i]).iterator();
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

        while(true) {
            if(data == null) {
                //getNext being called for the first time or starting on new input data
                //we instantiate the template array and start populating it with data
                data = new Object[noItems];
                for(int i = 0; i < noItems; ++i) {
                    if(isToBeFlattenedArray[i] && bags[i] instanceof DataBag) {
                        if(its[i].hasNext()) {
                            data[i] = its[i].next();
                        } else {
                            //the input set is null, so we return.  This is
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
                if(reporter!=null) {
                    reporter.progress();
                }
                //createTuple(data);
                res.result = createTuple(data);
                res.returnStatus = POStatus.STATUS_OK;
                return res;
            } else {
                //we try to find the last expression which needs flattening and start iterating over it
                //we also try to update the template array
                for(int index = noItems - 1; index >= 0; --index) {
                    if(its[index] != null && isToBeFlattenedArray[index]) {
                        if(its[index].hasNext()) {
                            data[index] =  its[index].next();
                            res.result = createTuple(data);
                            res.returnStatus = POStatus.STATUS_OK;
                            return res;
                        }
                        else{
                            // reset this index's iterator so cross product can be achieved
                            // we would be resetting this way only for the indexes from the end
                            // when the first index which needs to be flattened has reached the
                            // last element in its iterator, we won't come here - instead, we reset
                            // all iterators at the beginning of this method.
                            its[index] = ((DataBag)bags[index]).iterator();
                            data[index] = its[index].next();
                        }
                    }
                }
            }
        }

        //return null;
    }

    /**
     *
     * @param data array that is the template for the final flattened tuple
     * @return the final flattened tuple
     */
    protected Tuple createTuple(Object[] data) throws ExecException {
        Tuple out =  mTupleFactory.newTuple();
        for(int i = 0; i < data.length; ++i) {
            Object in = data[i];

            if(isToBeFlattenedArray[i] && in instanceof Tuple) {
                Tuple t = (Tuple)in;
                int size = t.size();
                for(int j = 0; j < size; ++j) {
                    out.append(t.get(j));
                }
            } else {
                out.append(in);
            }
        }
        if (inpTuple != null) {
            return illustratorMarkup(inpTuple, out, 0);
        } else {
            return illustratorMarkup2(data, out);
        }
    }


    protected void attachInputToPlans(Tuple t) {
        //super.attachInput(t);
        for(PhysicalPlan p : inputPlans) {
            p.attachInput(t);
        }
    }

    public void getLeaves() {
        if (inputPlans != null) {
            int i=-1;
            if(isToBeFlattenedArray == null) {
                isToBeFlattenedArray = new boolean[inputPlans.size()];
            }
            planLeafOps = new PhysicalOperator[inputPlans.size()];
            for(PhysicalPlan p : inputPlans) {
                ++i;
                PhysicalOperator leaf = p.getLeaves().get(0);
                planLeafOps[i] = leaf;
                if(leaf instanceof POProject &&
                        leaf.getResultType() == DataType.TUPLE &&
                         ((POProject)leaf).isProjectToEnd() ) {
                    isToBeFlattenedArray[i] = true;
                }
            }
        }
        // we are calculating plan leaves
        // so lets reinitialize
        reInitialize();
    }

    private void reInitialize() {
        if(planLeafOps != null) {
            noItems = planLeafOps.length;
            resultTypes = new byte[noItems];
            for (int i = 0; i < resultTypes.length; i++) {
                resultTypes[i] = planLeafOps[i].getResultType();
            }
        } else {
            noItems = 0;
            resultTypes = null;
        }

        if(inputPlans != null) {
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
        for(int i = 0; i < isToBeFlattenedArray.length; i++) {
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
        if(isToBeFlattenedArray != null) {
            result = new ArrayList<Boolean>();
            for (int i = 0; i < isToBeFlattenedArray.length; i++) {
                result.add(isToBeFlattenedArray[i]);
            }
        }
        return result;
    }

    /**
     * Make a deep copy of this operator.
     * @throws CloneNotSupportedException
     */
    @Override
    public POForEach clone() throws CloneNotSupportedException {
        List<PhysicalPlan> plans = new
            ArrayList<PhysicalPlan>(inputPlans.size());
        for (PhysicalPlan plan : inputPlans) {
            plans.add(plan.clone());
        }
        List<Boolean> flattens = null;
        if(isToBeFlattenedArray != null) {
            flattens = new
                ArrayList<Boolean>(isToBeFlattenedArray.length);
            for (boolean b : isToBeFlattenedArray) {
                flattens.add(b);
            }
        }

        List<PhysicalOperator> ops = new ArrayList<PhysicalOperator>(opsToBeReset.size());
        for (PhysicalOperator op : opsToBeReset) {
            ops.add(op);
        }
        POForEach clone = new POForEach(new OperatorKey(mKey.scope,
                NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)),
                requestedParallelism, plans, flattens);
        clone.setOpsToBeReset(ops);
        clone.setResultType(getResultType());
        clone.addOriginalLocation(alias, getOriginalLocations());
        return clone;
    }

    public boolean inProcessing()
    {
        return processingPlan;
    }

    protected void setUpFlattens(List<Boolean> isToBeFlattened) {
        if(isToBeFlattened == null) {
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
     * Visits a pipeline and calls reset on all the nodes.  Currently only
     * pays attention to limit nodes, each of which need to be told to reset
     * their limit.
     */
    private class ResetFinder extends PhyPlanVisitor {

        ResetFinder(PhysicalPlan plan, List<PhysicalOperator> toBeReset) {
            super(plan,
                new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan));
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

        /* (non-Javadoc)
         * @see org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor#visitProject(org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject)
         */
        @Override
        public void visitProject(POProject proj) throws VisitorException {
            if(proj instanceof PORelationToExprProject) {
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
     * @param opsToBeReset the opsToBeReset to set
     */
    public void setOpsToBeReset(List<PhysicalOperator> opsToBeReset) {
        this.opsToBeReset = opsToBeReset;
    }

    private Tuple illustratorMarkup2(Object[] in, Object out) {
      if(illustrator != null) {
          ExampleTuple tOut = new ExampleTuple((Tuple) out);
          illustrator.getLineage().insert(tOut);
          boolean synthetic = false;
          for (Object tIn : in)
          {
              synthetic |= ((ExampleTuple) tIn).synthetic;
              illustrator.getLineage().union(tOut, (Tuple) tIn);
          }
          illustrator.addData(tOut);
          int i;
          for (i = 0; i < noItems; ++i) {
            if (((DataBag)bags[i]).size() < 2) {
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
        if(illustrator != null) {
            ExampleTuple tOut = new ExampleTuple((Tuple) out);
            illustrator.addData(tOut);
            if (!illustrator.getEqClassesShared()) {
                illustrator.getEquivalenceClasses().get(0).add(tOut);
            }
            LineageTracer lineageTracer = illustrator.getLineage();
            lineageTracer.insert(tOut);
            tOut.synthetic = ((ExampleTuple) in).synthetic;
            lineageTracer.union((ExampleTuple) in , tOut);
            return tOut;
        } else {
            return (Tuple) out;
        }
    }
}
