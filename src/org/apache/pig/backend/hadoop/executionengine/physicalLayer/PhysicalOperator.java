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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.parser.SourceLocation;
import org.apache.pig.pen.Illustrable;
import org.apache.pig.pen.Illustrator;
import org.apache.pig.pen.util.LineageTracer;

/**
 *
 * This is the base class for all operators. This supports a generic way of
 * processing inputs which can be overridden by operators extending this class.
 * The input model assumes that it can either be taken from an operator or can
 * be attached directly to this operator. Also it is assumed that inputs to an
 * operator are always in the form of a tuple.
 *
 * For this pipeline rework, we assume a pull based model, i.e, the root
 * operator is going to call getNext with the appropriate type which initiates a
 * cascade of getNext calls that unroll to create input for the root operator to
 * work on.
 *
 * Any operator that extends the PhysicalOperator, supports a getNext with all
 * the different types of parameter types. The concrete implementation should
 * use the result type of its input operator to decide the type of getNext's
 * parameter. This is done to avoid switch/case based on the type as much as
 * possible. The default is assumed to return an erroneus Result corresponding
 * to an unsupported operation on that type. So the operators need to implement
 * only those types that are supported.
 *
 */
public abstract class PhysicalOperator extends Operator<PhyPlanVisitor> implements Illustrable, Cloneable {
    private static final Log log = LogFactory.getLog(PhysicalOperator.class);

    protected static final long serialVersionUID = 1L;

    // The degree of parallelism requested
    protected int requestedParallelism;

    // The inputs that this operator will read data from
    protected List<PhysicalOperator> inputs;

    // The outputs that this operator will write data to
    // Will be used to create Targeted tuples
    protected List<PhysicalOperator> outputs;

    // The data type for the results of this operator
    protected byte resultType = DataType.TUPLE;

    // The physical plan this operator is part of
    protected PhysicalPlan parentPlan;

    // Specifies if the input has been directly attached
    protected boolean inputAttached = false;

    // If inputAttached is true, input is set to the input tuple
    protected Tuple input = null;

    // The result of performing the operation along with the output
    protected Result res = null;


    // alias associated with this PhysicalOperator
    protected String alias = null;

    // Will be used by operators to report status or transmit heartbeat
    // Should be set by the backends to appropriate implementations that
    // wrap their own version of a reporter.
    public static PigProgressable reporter;

    // Will be used by operators to aggregate warning messages
    // Should be set by the backends to appropriate implementations that
    // wrap their own version of a logger.
    protected static PigLogger pigLogger;

    // Dummy types used to access the getNext of appropriate
    // type. These will be null
    static final protected DataByteArray dummyDBA = null;

    static final protected String dummyString = null;

    static final protected Double dummyDouble = null;

    static final protected Float dummyFloat = null;

    static final protected Integer dummyInt = null;

    static final protected Long dummyLong = null;

    static final protected Boolean dummyBool = null;

    static final protected DateTime dummyDateTime = null;

    static final protected Tuple dummyTuple = null;

    static final protected DataBag dummyBag = null;

    static final protected Map dummyMap = null;

    // TODO: This is not needed. But a lot of tests check serialized physical plans
    // that are sensitive to the serialized image of the contained physical operators.
    // So for now, just keep it. Later it'll be cleansed along with those test golden
    // files
    protected LineageTracer lineageTracer;

    protected transient Illustrator illustrator = null;

    private boolean accum;
    private transient boolean accumStart;

    private List<OriginalLocation> originalLocations =  new ArrayList<OriginalLocation>();

    public PhysicalOperator(OperatorKey k) {
        this(k, -1, null);
    }

    public PhysicalOperator(OperatorKey k, int rp) {
        this(k, rp, null);
    }

    public PhysicalOperator(OperatorKey k, List<PhysicalOperator> inp) {
        this(k, -1, inp);
    }

    public PhysicalOperator(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k);
        requestedParallelism = rp;
        inputs = inp;
        res = new Result();
    }

    @Override
    public void setIllustrator(Illustrator illustrator) {
	      this.illustrator = illustrator;
    }

    public Illustrator getIllustrator() {
        return illustrator;
    }

    public int getRequestedParallelism() {
        return requestedParallelism;
    }

    public void setRequestedParallelism(int requestedParallelism) {
        this.requestedParallelism = requestedParallelism;
    }

    public byte getResultType() {
        return resultType;
    }

    public String getAlias() {
        return alias;
    }

    protected String getAliasString() {
        return (alias == null) ? "" : (alias + ": ");
    }

    public void addOriginalLocation(String alias, SourceLocation sourceLocation) {
        this.alias = alias;
        this.originalLocations.add(new OriginalLocation(alias, sourceLocation.line(), sourceLocation.offset()));
    }

    public void addOriginalLocation(String alias, List<OriginalLocation> originalLocations) {
        this.alias = alias;
        this.originalLocations.addAll(originalLocations);
    }

    public List<OriginalLocation> getOriginalLocations() {
        return Collections.unmodifiableList(originalLocations);
    }

    public void setAccumulative() {
        accum = true;
    }

    public boolean isAccumulative() {
       return accum;
    }

    public void setAccumStart() {
       if (!accum) {
               throw new IllegalStateException("Accumulative is not turned on.");
       }
       accumStart = true;
    }

    public boolean isAccumStarted() {
    	return accumStart;
    }

    public void setAccumEnd() {
       if (!accum){
    	   throw new IllegalStateException("Accumulative is not turned on.");
       }
       accumStart = false;
    }

    public void setResultType(byte resultType) {
        this.resultType = resultType;
    }

    public List<PhysicalOperator> getInputs() {
        return inputs;
    }

    public void setInputs(List<PhysicalOperator> inputs) {
        this.inputs = inputs;
    }

    public boolean isInputAttached() {
        return inputAttached;
    }

    /**
     * Shorts the input path of this operator by providing the input tuple
     * directly
     *
     * @param t -
     *            The tuple that should be used as input
     */
    public void attachInput(Tuple t) {
        input = t;
        this.inputAttached = true;
    }

    /**
     * Detaches any tuples that are attached
     *
     */
    public void detachInput() {
        input = null;
        this.inputAttached = false;
    }

    /**
     * A blocking operator should override this to return true. Blocking
     * operators are those that need the full bag before operate on the tuples
     * inside the bag. Example is the Global Rearrange. Non-blocking or pipeline
     * operators are those that work on a tuple by tuple basis.
     *
     * @return true if blocking and false otherwise
     */
    public boolean isBlocking() {
        return false;
    }

    /**
     * A generic method for parsing input that either returns the attached input
     * if it exists or fetches it from its predecessor. If special processing is
     * required, this method should be overridden.
     *
     * @return The Result object that results from processing the input
     * @throws ExecException
     */
    public Result processInput() throws ExecException {

        Result res = new Result();
        if (input == null && (inputs == null || inputs.size()==0)) {
//            log.warn("No inputs found. Signaling End of Processing.");
            res.returnStatus = POStatus.STATUS_EOP;
            return res;
        }

        //Should be removed once the model is clear
        if(reporter!=null) {
            reporter.progress();
        }

        if (!isInputAttached()) {
            return inputs.get(0).getNext(dummyTuple);
        } else {
            res.result = input;
            res.returnStatus = (res.result == null ? POStatus.STATUS_NULL: POStatus.STATUS_OK);
            detachInput();
            return res;
        }
    }

    @Override
    public abstract void visit(PhyPlanVisitor v) throws VisitorException;

    /**
     * Implementations that call into the different versions of getNext are often
     * identical, differing only in the signature of the getNext() call they make.
     * This method allows to cut down on some of the copy-and-paste.
     *
     * @param obj The object we are working with. Its class should correspond to DataType
     * @param dataType Describes the type of obj; a byte from DataType.
     * @return result Result of applying this Operator to the Object.
     * @throws ExecException
     */
    @SuppressWarnings("rawtypes")  // For legacy use of untemplatized Map.
    public Result getNext(Object obj, byte dataType) throws ExecException {
        switch (dataType) {
        case DataType.BAG:
            return getNext((DataBag) obj);
        case DataType.BOOLEAN:
            return getNext((Boolean) obj);
        case DataType.BYTEARRAY:
            return getNext((DataByteArray) obj);
        case DataType.CHARARRAY:
            return getNext((String) obj);
        case DataType.DOUBLE:
            return getNext((Double) obj);
        case DataType.FLOAT:
            return getNext((Float) obj);
        case DataType.INTEGER:
            return getNext((Integer) obj);
        case DataType.LONG:
            return getNext((Long) obj);
        case DataType.DATETIME:
            return getNext((DateTime) obj);
        case DataType.MAP:
            return getNext((Map) obj);
        case DataType.TUPLE:
            return getNext((Tuple) obj);
        default:
            throw new ExecException("Unsupported type for getNext: " + DataType.findTypeName(dataType));
        }
    }

    public static Object getDummy(byte dataType) throws ExecException {
        switch (dataType) {
        case DataType.BAG:
            return dummyBag;
        case DataType.BOOLEAN:
            return dummyBool;
        case DataType.BYTEARRAY:
            return dummyDBA;
        case DataType.CHARARRAY:
            return dummyString;
        case DataType.DOUBLE:
            return dummyDouble;
        case DataType.FLOAT:
            return dummyFloat;
        case DataType.INTEGER:
            return dummyFloat;
        case DataType.LONG:
            return dummyLong;
        case DataType.DATETIME:
            return dummyDateTime;
        case DataType.MAP:
            return dummyMap;
        case DataType.TUPLE:
            return dummyTuple;
        default:
            throw new ExecException("Unsupported type for getDummy: " + DataType.findTypeName(dataType));
        }
    }

    public Result getNext(Integer i) throws ExecException {
        return res;
    }

    public Result getNext(Long l) throws ExecException {
        return res;
    }

    public Result getNext(Double d) throws ExecException {
        return res;
    }

    public Result getNext(Float f) throws ExecException {
        return res;
    }
    
    public Result getNext(DateTime dt) throws ExecException {
        return res;
    }

    public Result getNext(String s) throws ExecException {
        return res;
    }

    public Result getNext(DataByteArray ba) throws ExecException {
        return res;
    }

    public Result getNext(Map m) throws ExecException {
        return res;
    }

    public Result getNext(Boolean b) throws ExecException {
        return res;
    }

    public Result getNext(Tuple t) throws ExecException {
        return res;
    }

    public Result getNext(DataBag db) throws ExecException {
        Result ret = null;
        DataBag tmpBag = BagFactory.getInstance().newDefaultBag();
        for(ret = getNext(dummyTuple);ret.returnStatus!=POStatus.STATUS_EOP;ret=getNext(dummyTuple)){
            if(ret.returnStatus == POStatus.STATUS_ERR) {
                return ret;
            }
            tmpBag.add((Tuple)ret.result);
        }
        ret.result = tmpBag;
        ret.returnStatus = (tmpBag.size() == 0)? POStatus.STATUS_EOP : POStatus.STATUS_OK;
        return ret;
    }

    /**
     * Reset internal state in an operator.  For use in nested pipelines
     * where operators like limit and sort may need to reset their state.
     * Limit needs it because it needs to know it's seeing a fresh set of
     * input.  Blocking operators like sort and distinct need it because they
     * may not have drained their previous input due to a limit and thus need
     * to be told to drop their old input and start over.
     */
    public void reset() {
    }

    public static void setReporter(PigProgressable reporter) {
        PhysicalOperator.reporter = reporter;
    }

    /**
     * Make a deep copy of this operator. This function is blank, however,
     * we should leave a place holder so that the subclasses can clone
     * @throws CloneNotSupportedException
     */
    @Override
    public PhysicalOperator clone() throws CloneNotSupportedException {
        return (PhysicalOperator)super.clone();
    }

    protected void cloneHelper(PhysicalOperator op) {
        resultType = op.resultType;
        originalLocations.addAll(op.originalLocations);
    }

    /**
     * @param physicalPlan
     */
    public void setParentPlan(PhysicalPlan physicalPlan) {
       parentPlan = physicalPlan;
    }

    public Log getLogger() {
    	return log;
    }

    public static void setPigLogger(PigLogger logger) {
    	pigLogger = logger;
    }

    public static PigLogger getPigLogger() {
    	return pigLogger;
    }

    public static class OriginalLocation implements Serializable {
        private String alias;
        private int line;
        private int offset;

        public OriginalLocation(String alias, int line, int offset) {
            super();
            this.alias = alias;
            this.line = line;
            this.offset = offset;
}

        public String getAlias() {
            return alias;
        }

        public int getLine() {
            return line;
        }

        public int getOffset() {
            return offset;
        }

        @Override
        public String toString() {
            return alias+"["+line+","+offset+"]";
        }
    }
}
