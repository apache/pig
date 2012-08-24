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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.pen.util.ExampleTuple;

/**
 * The local rearrange operator is a part of the co-group
 * implementation. It has an embedded physical plan that
 * generates tuples of the form (grpKey,(indxed inp Tuple)).
 *
 */
public class POLocalRearrange extends PhysicalOperator {
    private static final Log log = LogFactory.getLog(POLocalRearrange.class);

    /**
     *
     */
    protected static final long serialVersionUID = 1L;

    protected static final TupleFactory mTupleFactory = TupleFactory.getInstance();

    private static final Result ERR_RESULT = new Result();

    protected List<PhysicalPlan> plans;

    protected List<PhysicalPlan> secondaryPlans;

    protected List<ExpressionOperator> leafOps;

    protected List<ExpressionOperator> secondaryLeafOps;

    // The position of this LR in the package operator
    protected byte index;

    protected byte keyType;

    protected byte mainKeyType;

    protected byte secondaryKeyType;

    protected boolean mIsDistinct = false;

    protected boolean isCross = false;

    // map to store mapping of projected columns to
    // the position in the "Key" where these will be projected to.
    // We use this information to strip off these columns
    // from the "Value" and in POPackage stitch the right "Value"
    // tuple back by getting these columns from the "key". The goal
    // is to reduce the amount of the data sent to Hadoop in the map.
    // Example: a  = load 'bla'; b = load 'bla'; c = cogroup a by ($2, $3), b by ($, $2)
    // For the first input (a), the map would contain following key:value
    // 2:0 (2 corresponds to $2 in cogroup a by ($2, $3) and 0 corresponds to 1st index in key)
    // 3:1 (3 corresponds to $3 in cogroup a by ($2, $3) and 0 corresponds to 2nd index in key)
    private final Map<Integer, Integer> mProjectedColsMap;
    private final Map<Integer, Integer> mSecondaryProjectedColsMap;

    // A place holder Tuple used in distinct case where we really don't
    // have any value to pass through.  But hadoop gets cranky if we pass a
    // null, so we'll just create one instance of this empty tuple and
    // pass it for every row.  We only get around to actually creating it if
    // mIsDistinct is set to true.
    protected Tuple mFakeTuple = null;

	// indicator whether the project in the inner plans
	// is a project(*) - we set this ONLY when the project(*)
	// is the ONLY thing in the cogroup by ..
	private boolean mProjectStar = false;
	private boolean mSecondaryProjectStar = false;

    // marker to note that the "key" is a tuple
    // this is required by POPackage to pick things
    // off the "key" correctly to stitch together the
    // "value"
    private boolean isKeyTuple = false;
    // marker to note that the tuple "key" is compound
    // in nature. For example:
    // group a by (a0, a1);
    // The group key is a tuple of two fields, isKeyCompound is on
    // group a by a0; -- a0 is a tuple
    // The group key is a tuple of one field, isKeyCompound is off
    private boolean isKeyCompound = false;
    private boolean isSecondaryKeyTuple = false;

    private int mProjectedColsMapSize = 0;
    private int mSecondaryProjectedColsMapSize = 0;


    private boolean useSecondaryKey = false;

    // By default, we strip keys from the value.
    private boolean stripKeyFromValue = true;

    public POLocalRearrange(OperatorKey k) {
        this(k, -1, null);
    }

    public POLocalRearrange(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    public POLocalRearrange(OperatorKey k, List<PhysicalOperator> inp) {
        this(k, -1, inp);
    }

    public POLocalRearrange(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
        index = -1;
        leafOps = new ArrayList<ExpressionOperator>();
        secondaryLeafOps = new ArrayList<ExpressionOperator>();
        mProjectedColsMap = new HashMap<Integer, Integer>();
        mSecondaryProjectedColsMap = new HashMap<Integer, Integer>();
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitLocalRearrange(this);
    }

    @Override
    public String name() {
        return getAliasString() + "Local Rearrange" + "["
                + DataType.findTypeName(resultType) + "]" + "{"
                + DataType.findTypeName(keyType) + "}" + "(" + mIsDistinct
                + ") - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    public byte getIndex() {
        return index;
    }

    /**
     * Sets the co-group index of this operator
     *
     * @param index the position of this operator in
     * a co-group operation
     * @throws ExecException if the index value is bigger then 0x7F
     */
    public void setIndex(int index) throws ExecException {
        setIndex(index, false);
    }

    /**
     * Sets the multi-query index of this operator
     *
     * @param index the position of the parent plan of this operator
     * in the enclosed split operator
     * @throws ExecException if the index value is bigger then 0x7F
     */
    public void setMultiQueryIndex(int index) throws ExecException {
        setIndex(index, true);
    }

    private void setIndex(int index, boolean multiQuery) throws ExecException {
        if (index > PigNullableWritable.idxSpace) {
            // indices in group and cogroup should only
            // be in the range 0x00 to 0x7F (only 127 possible
            // inputs)
            int errCode = 1082;
            String msg = multiQuery?
                    "Merge more than 127 map-reduce jobs not supported."
                  : "Cogroups with more than 127 inputs not supported.";
            throw new ExecException(msg, errCode, PigException.INPUT);
        } else {
            // We could potentially be sending the (key, value) relating to
            // multiple "group by" statements through one map reduce job
            // in  multiquery optimized execution. In this case, we want
            // two keys which have the same content but coming from different
            // group by operations to be treated differently so that they
            // go to different invocations of the reduce(). To achieve this
            // we let the index be outside the regular index space - 0x00 to 0x7F
            // by ORing with the mqFlag bitmask which will put the index above
            // the 0x7F value. In PigNullableWritable.compareTo if the index is
            // in this "multiquery" space, we also consider the index when comparing
            // two PigNullableWritables and not just the contents. Keys with same
            // contents coming from different "group by" operations would have different
            // indices and hence would go to different invocation of reduce()
            this.index = multiQuery ? (byte)(index | PigNullableWritable.mqFlag) : (byte)index;
        }
    }

    public boolean isDistinct() {
        return mIsDistinct;
    }

    public void setDistinct(boolean isDistinct) {
        mIsDistinct = isDistinct;
        if (mIsDistinct) {
            mFakeTuple = mTupleFactory.newTuple();
        }
    }

    /**
     * Overridden since the attachment of the new input should cause the old
     * processing to end.
     */
    @Override
    public void attachInput(Tuple t) {
        super.attachInput(t);
    }

    /**
     * Calls getNext on the generate operator inside the nested
     * physical plan. Converts the generated tuple into the proper
     * format, i.e, (key,indexedTuple(value))
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {

        Result inp = null;
        Result res = ERR_RESULT;
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

            if (secondaryPlans!=null) {
                for (PhysicalPlan ep : secondaryPlans) {
                    ep.attachInput((Tuple)inp.result);
                }
            }

            List<Result> secondaryResLst = null;
            if (secondaryLeafOps!=null) {
                secondaryResLst = new ArrayList<Result>();
            }

            for (ExpressionOperator op : leafOps){

                switch(op.getResultType()){
                case DataType.BAG:
                case DataType.BOOLEAN:
                case DataType.BYTEARRAY:
                case DataType.CHARARRAY:
                case DataType.DOUBLE:
                case DataType.FLOAT:
                case DataType.INTEGER:
                case DataType.LONG:
                case DataType.DATETIME:
                case DataType.MAP:
                case DataType.TUPLE:
                    res = op.getNext(getDummy(op.getResultType()), op.getResultType());
                    break;
                default:
                    log.error("Invalid result type: " + DataType.findType(op.getResultType()));
                    break;
                }

                // allow null as group by key
                if (res.returnStatus != POStatus.STATUS_OK && res.returnStatus != POStatus.STATUS_NULL) {
                    return new Result();
                }

                resLst.add(res);
            }

            if (secondaryLeafOps!=null)
            {
                for (ExpressionOperator op : secondaryLeafOps){

                    switch(op.getResultType()){
                    case DataType.BAG:
                    case DataType.BOOLEAN:
                    case DataType.BYTEARRAY:
                    case DataType.CHARARRAY:
                    case DataType.DOUBLE:
                    case DataType.FLOAT:
                    case DataType.INTEGER:
                    case DataType.LONG:
                    case DataType.DATETIME:
                    case DataType.MAP:
                    case DataType.TUPLE:
                        res = op.getNext(getDummy(op.getResultType()), op.getResultType());
                        break;
                    default:
                        log.error("Invalid result type: " + DataType.findType(op.getResultType()));
                        break;
                    }

                    // allow null as group by key
                    if (res.returnStatus != POStatus.STATUS_OK && res.returnStatus != POStatus.STATUS_NULL) {
                        return new Result();
                    }

                    secondaryResLst.add(res);
                }
            }

            // If we are using secondary sort key, our new key is:
            // (nullable, index, (key, secondary key), value)
            res.result = constructLROutput(resLst,secondaryResLst,(Tuple)inp.result);
            res.returnStatus = POStatus.STATUS_OK;

            detachPlans(plans);

            if(secondaryPlans != null) {
                detachPlans(secondaryPlans);
            }

            res.result = illustratorMarkup(inp.result, res.result, 0);
            return res;
        }
        return inp;
    }


    private void detachPlans(List<PhysicalPlan> plans) {
        for (PhysicalPlan ep : plans) {
            ep.detachInput();
        }
    }

    protected Object getKeyFromResult(List<Result> resLst, byte type) throws ExecException {
        Object key;
        if(resLst.size()>1){
            Tuple t = mTupleFactory.newTuple(resLst.size());
            int i=-1;
            for(Result res : resLst) {
                t.set(++i, res.result);
            }
            key = t;
        } else if (resLst.size() == 1 && type == DataType.TUPLE) {

            // We get here after merging multiple jobs that have different
            // map key types into a single job during multi-query optimization.
            // If the key isn't a tuple, it must be wrapped in a tuple.
            Object obj = resLst.get(0).result;
            if (obj instanceof Tuple) {
                key = obj;
            } else {
                Tuple t = mTupleFactory.newTuple(1);
                t.set(0, resLst.get(0).result);
                key = t;
            }
        }
        else{
            key = resLst.get(0).result;
        }
        return key;
    }

    protected Tuple constructLROutput(List<Result> resLst, List<Result> secondaryResLst, Tuple value) throws ExecException{
        Tuple lrOutput = mTupleFactory.newTuple(3);
        lrOutput.set(0, Byte.valueOf(this.index));
        //Construct key
        Object key;
        Object secondaryKey=null;

        if (secondaryResLst!=null && secondaryResLst.size()>0)
        {
            key = getKeyFromResult(resLst, mainKeyType);
            secondaryKey = getKeyFromResult(secondaryResLst, secondaryKeyType);
        } else {
            key = getKeyFromResult(resLst, keyType);
        }


        if(!stripKeyFromValue){
            lrOutput.set(1, key);
            lrOutput.set(2, value);
            return lrOutput;
        }

        if (mIsDistinct) {

            //Put the key and the indexed tuple
            //in a tuple and return
            lrOutput.set(1, key);
            if (illustrator != null)
                lrOutput.set(2, key);
            else
                lrOutput.set(2, mFakeTuple);
            return lrOutput;
        } else if(isCross){

            for(int i=0;i<plans.size();i++) {
                value.getAll().remove(0);
            }
            //Put the index, key, and value
            //in a tuple and return
            lrOutput.set(1, key);
            lrOutput.set(2, value);
            return lrOutput;
        } else {

            //Put the index, key, and value
            //in a tuple and return
            if (useSecondaryKey)
            {
                Tuple compoundKey = mTupleFactory.newTuple(2);
                compoundKey.set(0, key);
                compoundKey.set(1, secondaryKey);
                lrOutput.set(1, compoundKey);
            } else {
                lrOutput.set(1, key);
            }

            // strip off the columns in the "value" which
            // are present in the "key"
            if(mProjectedColsMapSize != 0 || mProjectStar == true) {

                Tuple minimalValue = null;
                if(!mProjectStar) {
                    minimalValue = mTupleFactory.newTuple();
                    // look for individual columns that we are
                    // projecting
                    for (int i = 0; i < value.size(); i++) {
                        if(mProjectedColsMap.get(i) == null) {
                            // this column was not found in the "key"
                            // so send it in the "value"
                            minimalValue.append(value.get(i));
                        }
                    }
                    minimalValue = illustratorMarkup(value, minimalValue, -1);
                } else {
                    // for the project star case
                    // we would send out an empty tuple as
                    // the "value" since all elements are in the
                    // "key"
                    minimalValue = mTupleFactory.newTuple(0);

                }
                lrOutput.set(2, minimalValue);

            } else {

                // there were no columns in the "key"
                // which we can strip off from the "value"
                // so just send the value we got
                lrOutput.set(2, value);

            }
            return lrOutput;
        }
    }

    public byte getKeyType() {
        return keyType;
    }

    public void setKeyType(byte keyType) {
        if (useSecondaryKey) {
            this.mainKeyType = keyType;
        } else {
            this.keyType = keyType;
        }
    }

    public List<PhysicalPlan> getPlans() {
        return plans;
    }

    public void setUseSecondaryKey(boolean useSecondaryKey) {
        this.useSecondaryKey = useSecondaryKey;
        mainKeyType = keyType;
    }

    public void setPlans(List<PhysicalPlan> plans) throws PlanException {
        this.plans = plans;
        leafOps.clear();
        int keyIndex = 0; // zero based index for fields in the key
        for (PhysicalPlan plan : plans) {
            ExpressionOperator leaf = (ExpressionOperator)plan.getLeaves().get(0);
            leafOps.add(leaf);

            // don't optimize CROSS
            if(!isCross) {
                // Look for the leaf Ops which are POProject operators - get the
                // the columns that these POProject Operators are projecting.
                // They MUST be projecting either a column or '*'.
                // Keep track of the columns which are being projected and
                // the position in the "Key" where these will be projected to.
                // Then we can use this information to strip off these columns
                // from the "Value" and in POPackage stitch the right "Value"
                // tuple back by getting these columns from the "key". The goal
                // is reduce the amount of the data sent to Hadoop in the map.
                if(leaf instanceof POProject) {
                    POProject project = (POProject) leaf;
                    if(project.isStar()) {
                        // note that we have a project *
                        mProjectStar  = true;
                        // key will be a tuple in this case
                        isKeyTuple = true;

                        //The number of columns from the project * is unkown
                        // so position of remaining colums in key can't be determined.
                        //stop optimizing here
                        break;
                    } else if(project.isProjectToEnd()){
                        List<PhysicalOperator> preds = plan.getPredecessors(project);
                        if(preds != null && preds.size() != 0){
                            //a sanity check - should never come here
                            throw new AssertionError("project-range has predecessors");
                        }
                        //The number of columns from the project-to-end is unkown
                        // so position of remaining colums in key can't be determined.
                        //stop optimizing here
                        break;

                    }
                    else {
                        try {
                            List<PhysicalOperator> preds = plan.getPredecessors(leaf);
                            if (preds==null || !(preds.get(0) instanceof POProject)) {
                                mProjectedColsMap.put(project.getColumn(), keyIndex);
                            }
                        } catch (ExecException e) {
                            int errCode = 2070;
                            String msg = "Problem in accessing column from project operator.";
                            throw new PlanException(msg, errCode, PigException.BUG);
                        }
                    }
                    if(project.getResultType() == DataType.TUPLE) {
                        isKeyTuple = true;
                    }
                }
                keyIndex++;
            }
        }
        if(keyIndex > 1) {
            // make a note that the "key" is a tuple
            // this is required by POPackage to pick things
            // off the "key" correctly to stitch together the
            // "value"
            isKeyTuple  = true;
            isKeyCompound = true;
        }
        mProjectedColsMapSize = mProjectedColsMap.size();
    }

    public void setSecondaryPlans(List<PhysicalPlan> plans) throws PlanException {
        this.secondaryPlans = plans;
        secondaryLeafOps.clear();
        int keyIndex = 0; // zero based index for fields in the key
        for (PhysicalPlan plan : plans) {
            ExpressionOperator leaf = (ExpressionOperator)plan.getLeaves().get(0);
            secondaryLeafOps.add(leaf);

            // don't optimize CROSS
            if(!isCross) {
                // Look for the leaf Ops which are POProject operators - get the
                // the columns that these POProject Operators are projecting.
                // They MUST be projecting either a column or '*'.
                // Keep track of the columns which are being projected and
                // the position in the "Key" where these will be projected to.
                // Then we can use this information to strip off these columns
                // from the "Value" and in POPackage stitch the right "Value"
                // tuple back by getting these columns from the "key". The goal
                // is reduce the amount of the data sent to Hadoop in the map.
                if(leaf instanceof POProject) {
                    POProject project = (POProject) leaf;
                    if(project.isStar()) {
                        // note that we have a project *
                        mSecondaryProjectStar  = true;
                        // key will be a tuple in this case
                        isSecondaryKeyTuple = true;
                        //The number of columns from the project * is unknown
                        // so position of remaining columns in key can't be determined.
                        //stop optimizing here
                        break;
                    } else if(project.isProjectToEnd()){
                        List<PhysicalOperator> preds = plan.getPredecessors(project);
                        if(preds != null && preds.size() != 0){
                            //a sanity check - should never come here
                            throw new AssertionError("project-range has predecessors");
                        }
                        //The number of columns from the project-to-end is unknown
                        // so position of remaining columns in key can't be determined.
                        //stop optimizing here
                        break;

                    }
                    else {
                        try {
                            List<PhysicalOperator> preds = plan.getPredecessors(leaf);
                            if (preds==null || !(preds.get(0) instanceof POProject)) {
                                mSecondaryProjectedColsMap.put(project.getColumn(), keyIndex);
                            }
                        } catch (ExecException e) {
                            int errCode = 2070;
                            String msg = "Problem in accessing column from project operator.";
                            throw new PlanException(msg, errCode, PigException.BUG);
                        }
                    }
                    if(project.getResultType() == DataType.TUPLE) {
                        isSecondaryKeyTuple = true;
                    }
                }
                keyIndex++;
            }
        }
        if(keyIndex > 1) {
            // make a note that the "key" is a tuple
            // this is required by POPackage to pick things
            // off the "key" correctly to stitch together the
            // "value"
            isSecondaryKeyTuple  = true;
        }
        mainKeyType = keyType;
        keyType = DataType.TUPLE;
        if (plans.size()>1) {
            secondaryKeyType = DataType.TUPLE;
        } else
        {
            secondaryKeyType = plans.get(0).getLeaves().get(0).getResultType();
        }
        mSecondaryProjectedColsMapSize = mSecondaryProjectedColsMap.size();
    }

    /**
     * Make a deep copy of this operator.
     * @throws CloneNotSupportedException
     */
    @Override
    public POLocalRearrange clone() throws CloneNotSupportedException {
        List<PhysicalPlan> clonePlans = new
            ArrayList<PhysicalPlan>(plans.size());
        for (PhysicalPlan plan : plans) {
            clonePlans.add(plan.clone());
        }
        POLocalRearrange clone = new POLocalRearrange(new OperatorKey(
            mKey.scope,
            NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)),
            requestedParallelism);
        try {
            clone.setPlans(clonePlans);
        } catch (PlanException pe) {
            CloneNotSupportedException cnse = new CloneNotSupportedException("Problem with setting plans of " + this.getClass().getSimpleName());
            cnse.initCause(pe);
            throw cnse;
        }
        clone.keyType = keyType;
        clone.mainKeyType = mainKeyType;
        clone.secondaryKeyType = secondaryKeyType;
        clone.useSecondaryKey = useSecondaryKey;
        clone.index = index;
        // Needs to be called as setDistinct so that the fake index tuple gets
        // created.
        clone.setDistinct(mIsDistinct);
        clone.addOriginalLocation(alias, getOriginalLocations());
        return clone;
    }

    public boolean isCross() {
        return isCross;
    }

    public void setCross(boolean isCross) {
        this.isCross = isCross;
    }

    /**
     * @return the mProjectedColsMap
     */
    public Map<Integer, Integer> getProjectedColsMap() {
        return mProjectedColsMap;
    }

    /**
     * @return the mProjectedColsMap
     */
    public Map<Integer, Integer> getSecondaryProjectedColsMap() {
        return mSecondaryProjectedColsMap;
    }

    /**
     * @return the mProjectStar
     */
    public boolean isProjectStar() {
        return mProjectStar;
    }

    /**
     * @return the mProjectStar
     */
    public boolean isSecondaryProjectStar() {
        return mSecondaryProjectStar;
    }

    /**
     * @return the keyTuple
     */
    public boolean isKeyTuple() {
        return isKeyTuple;
    }
    
    /**
     * @return the isKeyCompound
     */
    public boolean isKeyCompound() {
        return isKeyCompound;
    }

    /**
     * @return the keyTuple
     */
    public boolean isSecondaryKeyTuple() {
        return isSecondaryKeyTuple;
    }

    /**
     * @param plans
     * @throws ExecException
     */
    public void setPlansFromCombiner(List<PhysicalPlan> plans) throws PlanException {
        this.plans = plans;
        leafOps.clear();
        mProjectedColsMap.clear();
        int keyIndex = 0; // zero based index for fields in the key
        for (PhysicalPlan plan : plans) {
            ExpressionOperator leaf = (ExpressionOperator)plan.getLeaves().get(0);
            leafOps.add(leaf);

            // don't optimize CROSS
            if(!isCross) {
                // Look for the leaf Ops which are POProject operators - get the
                // the columns that these POProject Operators are projecting.
                // Keep track of the columns which are being projected and
                // the position in the "Key" where these will be projected to.
                // Then we can use this information to strip off these columns
                // from the "Value" and in POPostCombinerPackage stitch the right "Value"
                // tuple back by getting these columns from the "key". The goal
                // is reduce the amount of the data sent to Hadoop in the map.
                if(leaf instanceof POProject) {
                    POProject project = (POProject) leaf;
                    if(project.isProjectToEnd()) {
                        int errCode = 2021;
                        String msg = "Internal error. Unexpected operator project(*) " +
                        		"or (..) in local rearrange inner plan.";
                        throw new PlanException(msg, errCode, PigException.BUG);
                    } else {
                        try {
                            mProjectedColsMap.put(project.getColumn(), keyIndex);
                        } catch (ExecException e) {
                            int errCode = 2070;
                            String msg = "Problem in accessing column from project operator.";
                            throw new PlanException(msg, errCode, PigException.BUG);
                        }
                    }
                    if(project.getResultType() == DataType.TUPLE) {
                        isKeyTuple = true;
                    }
                }
                keyIndex++;
            }
        }
        if(keyIndex > 1) {
            // make a note that the "key" is a tuple
            // this is required by POPackage to pick things
            // off the "key" correctly to stitch together the
            // "value"
            isKeyTuple  = true;
        }
        mProjectedColsMapSize  = mProjectedColsMap.size();

    }

    protected void setStripKeyFromValue(boolean stripKeyFromValue) {
        this.stripKeyFromValue = stripKeyFromValue;
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        if (illustrator != null) {
            if (!(out instanceof ExampleTuple))
            {
                ExampleTuple tOut = new ExampleTuple((Tuple) out);
                illustrator.getLineage().insert(tOut);
                illustrator.addData(tOut);
                illustrator.getLineage().union(tOut, (Tuple) in);
                tOut.synthetic = ((ExampleTuple) in).synthetic;
                return tOut;
            }
        }
        return (Tuple) out;
    }
}
