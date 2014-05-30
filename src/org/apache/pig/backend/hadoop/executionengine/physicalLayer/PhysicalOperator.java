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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
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
    protected static final Result RESULT_EMPTY = new Result(POStatus.STATUS_NULL, null);
    protected static final Result RESULT_EOP = new Result(POStatus.STATUS_EOP, null);

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
    public static ThreadLocal<PigProgressable> reporter = new ThreadLocal<PigProgressable>();

    // Will be used by operators to aggregate warning messages
    // Should be set by the backends to appropriate implementations that
    // wrap their own version of a logger.
    protected static PigLogger pigLogger;

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

    public PhysicalOperator(PhysicalOperator copy) {
        super (copy.getOperatorKey());
        this.res = new Result();
        this.requestedParallelism = copy.requestedParallelism;
        this.inputs = copy.inputs;
        this.outputs = copy.outputs;
        this.resultType = copy.resultType;
        this.parentPlan = copy.parentPlan;
        this.inputAttached = copy.inputAttached;
        this.alias = copy.alias;
        this.lineageTracer = copy.lineageTracer;
        this.accum = copy.accum;
        this.originalLocations = copy.originalLocations;
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

    public void setAlias(String alias) {
        this.alias = alias;
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
        try {
            if (input == null && (inputs == null || inputs.size() == 0)) {
                // log.warn("No inputs found. Signaling End of Processing.");
                return new Result(POStatus.STATUS_EOP, null);
            }

            // Should be removed once the model is clear
            if (getReporter() != null) {
                getReporter().progress();
            }

            if (!isInputAttached()) {
                return inputs.get(0).getNextTuple();
            } else {
                Result res = new Result();
                res.result = input;
                res.returnStatus = POStatus.STATUS_OK;
                detachInput();
                return res;
            }
        } catch (ExecException e) {
            throw new ExecException("Exception while executing "
                    + this.toString() + ": " + e.toString(), e);
        }
    }

    @Override
    public abstract void visit(PhyPlanVisitor v) throws VisitorException;

    /**
     * Implementations that call into the different versions of getNext are often
     * identical, differing only in the signature of the getNext() call they make.
     * This method allows to cut down on some of the copy-and-paste.
     * @param dataType Describes the type of obj; a byte from DataType.
     *
     * @return result Result of applying this Operator to the Object.
     * @throws ExecException
     */
    public Result getNext(byte dataType) throws ExecException {
        try {
            switch (dataType) {
            case DataType.BAG:
                return getNextDataBag();
            case DataType.BOOLEAN:
                return getNextBoolean();
            case DataType.BYTEARRAY:
                return getNextDataByteArray();
            case DataType.CHARARRAY:
                return getNextString();
            case DataType.DOUBLE:
                return getNextDouble();
            case DataType.FLOAT:
                return getNextFloat();
            case DataType.INTEGER:
                return getNextInteger();
            case DataType.LONG:
                return getNextLong();
            case DataType.BIGINTEGER:
                return getNextBigInteger();
            case DataType.BIGDECIMAL:
                return getNextBigDecimal();
            case DataType.DATETIME:
                return getNextDateTime();
            case DataType.MAP:
                return getNextMap();
            case DataType.TUPLE:
                return getNextTuple();
            default:
                throw new ExecException("Unsupported type for getNext: " + DataType.findTypeName(dataType));
            }
        } catch (RuntimeException e) {
            throw new ExecException("Exception while executing " + this.toString() + ": " + e.toString(), e);
        }
    }

    public Result getNextInteger() throws ExecException {
        return res;
    }

    public Result getNextLong() throws ExecException {
        return res;
    }

    public Result getNextDouble() throws ExecException {
        return res;
    }

    public Result getNextFloat() throws ExecException {
        return res;
    }

    public Result getNextDateTime() throws ExecException {
        return res;
    }

    public Result getNextString() throws ExecException {
        return res;
    }

    public Result getNextDataByteArray() throws ExecException {
        return res;
    }

    public Result getNextMap() throws ExecException {
        return res;
    }

    public Result getNextBoolean() throws ExecException {
        return res;
    }

    public Result getNextTuple() throws ExecException {
        return res;
    }

    public Result getNextDataBag() throws ExecException {
        Result ret = null;
        DataBag tmpBag = BagFactory.getInstance().newDefaultBag();
        for (ret = getNextTuple(); ret.returnStatus != POStatus.STATUS_EOP; ret = getNextTuple()) {
            if (ret.returnStatus == POStatus.STATUS_ERR) {
                return ret;
            } else if (ret.returnStatus == POStatus.STATUS_NULL) {
                continue;
            } else {
                tmpBag.add((Tuple) ret.result);
            }
        }
        ret.result = tmpBag;
        ret.returnStatus = (tmpBag.size() == 0)? POStatus.STATUS_EOP : POStatus.STATUS_OK;
        return ret;
    }

    public Result getNextBigInteger() throws ExecException {
        return res;
    }

    public Result getNextBigDecimal() throws ExecException {
        return res;
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

    /**
     * @return PigProgressable stored in threadlocal
     */
    public static PigProgressable getReporter() {
        return PhysicalOperator.reporter.get();
    }

    /**
     * @param reporter PigProgressable to be stored in threadlocal
     */
    public static void setReporter(PigProgressable reporter) {
        PhysicalOperator.reporter.set(reporter);
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
