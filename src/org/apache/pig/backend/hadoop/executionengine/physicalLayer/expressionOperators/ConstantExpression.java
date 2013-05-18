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

import java.util.List;


import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;


/**
 * This class implements a Constant of any type.
 * Its value can be set using the setValue method.
 *
 */
public class ConstantExpression extends ExpressionOperator {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

//    private Log log = LogFactory.getLog(getClass());

    //The value that this constant represents
    Object value;

    //The result of calling getNext
    Result res = new Result();

    public ConstantExpression(OperatorKey k) {
        this(k,-1);
    }

    public ConstantExpression(OperatorKey k, int rp) {
        super(k, rp);
    }

    @Override
    public String name() {
        if(value!=null) {
            return "Constant(" + value.toString() +") - " + mKey.toString();
        } else {
            return "Constant(" + "DummyVal" +") - " + mKey.toString();
        }
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
        v.visitConstant(this);
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
        Tuple dummyTuple = TupleFactory.getInstance().newTuple(1);
        attachInput(dummyTuple);
    }

    private Result genericGetNext(byte dataType) throws ExecException {
        res = processInput();
        if(res.returnStatus != POStatus.STATUS_OK) {
            return res;
        }
        res.result = value;
        return res;
    }

    @Override
    public Result getNextDataBag() throws ExecException {
        return genericGetNext(DataType.BAG);
    }

    @Override
    public Result getNextDataByteArray() throws ExecException {
        return genericGetNext(DataType.BYTEARRAY);
    }

    @Override
    public Result getNextDouble() throws ExecException {
        return genericGetNext(DataType.DOUBLE);
    }

    @Override
    public Result getNextFloat() throws ExecException {
        return genericGetNext(DataType.FLOAT);

    }

    @Override
    public Result getNextInteger() throws ExecException {
        return genericGetNext(DataType.INTEGER);

    }

    @Override
    public Result getNextLong() throws ExecException {
        return genericGetNext(DataType.LONG);

    }

    @Override
    public Result getNextDateTime() throws ExecException {
        return genericGetNext(DataType.DATETIME);

    }

    @Override
    public Result getNextString() throws ExecException {
        return genericGetNext(DataType.CHARARRAY);

    }

    @Override
    public Result getNextTuple() throws ExecException {
        return genericGetNext(DataType.TUPLE);
    }

    @Override
    public Result getNextBoolean() throws ExecException {
        return genericGetNext(DataType.BOOLEAN);
    }

    @Override
    public Result getNextMap() throws ExecException {
        return genericGetNext(DataType.MAP);

    }

    @Override
    public Result getNextBigInteger() throws ExecException {
        return genericGetNext(DataType.BIGINTEGER);
    }

    @Override
    public Result getNextBigDecimal() throws ExecException {
        return genericGetNext(DataType.BIGDECIMAL);
    }

    @Override
    public ConstantExpression clone() throws CloneNotSupportedException {
        ConstantExpression clone =
            new ConstantExpression(new OperatorKey(mKey.scope,
            NodeIdGenerator.getGenerator().getNextNodeId(mKey.scope)));
        clone.value = value;
        clone.cloneHelper(this);
        return clone;
    }

    /**
     * Get the child expressions of this expression
     */
    @Override
    public List<ExpressionOperator> getChildExpressions() {
        return null;
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        return (Tuple) out;
    }
}
