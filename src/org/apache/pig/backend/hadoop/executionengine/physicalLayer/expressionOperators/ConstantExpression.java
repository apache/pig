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
import java.util.Map;

import org.joda.time.DateTime;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
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

    protected Result genericGetNext(Object obj, byte dataType) throws ExecException {
        res = processInput();
        if(res.returnStatus != POStatus.STATUS_OK) {
            return res;
        }
        res.result = value;
        return res;
    }

    @Override
    public Result getNext(DataBag db) throws ExecException {
        return genericGetNext(db, DataType.BAG);
    }

    @Override
    public Result getNext(DataByteArray ba) throws ExecException {
        return genericGetNext(ba, DataType.BYTEARRAY);
    }

    @Override
    public Result getNext(Double d) throws ExecException {
        return genericGetNext(d, DataType.DOUBLE);
    }

    @Override
    public Result getNext(Float f) throws ExecException {
        return genericGetNext(f, DataType.FLOAT);

    }

    @Override
    public Result getNext(Integer i) throws ExecException {
        return genericGetNext(i, DataType.INTEGER);

    }

    @Override
    public Result getNext(Long l) throws ExecException {
        return genericGetNext(l, DataType.LONG);

    }

    @Override
    public Result getNext(DateTime dt) throws ExecException {
        return genericGetNext(dt, DataType.DATETIME);

    }

    @Override
    public Result getNext(String s) throws ExecException {
        return genericGetNext(s, DataType.CHARARRAY);

    }

    @Override
    public Result getNext(Tuple t) throws ExecException {
        return genericGetNext(t, DataType.TUPLE);

    }

    @Override
    public Result getNext(Boolean b) throws ExecException {
        return genericGetNext(b, DataType.BOOLEAN);
    }

    @Override
    public Result getNext(Map m) throws ExecException {
        return genericGetNext(m, DataType.MAP);

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
