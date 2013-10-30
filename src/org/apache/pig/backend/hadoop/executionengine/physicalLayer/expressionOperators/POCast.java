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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StreamToPig;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.builtin.ToDate;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.CastUtils;
import org.apache.pig.impl.util.LogUtils;
import org.joda.time.DateTime;

/**
 * This is just a cast that converts DataByteArray into either String or
 * Integer. Just added it for testing the POUnion. Need the full operator
 * implementation.
 */
public class POCast extends ExpressionOperator {
    private final static Log log = LogFactory.getLog(POCast.class);
    private static final String unknownByteArrayErrorMessage = "Received a bytearray from the UDF or Union from two different Loaders. Cannot determine how to convert the bytearray to "; 
    private FuncSpec funcSpec = null;
    transient private LoadCaster caster;
    private boolean castNotNeeded = false;
    private Byte realType = null;
    private transient List<ExpressionOperator> child;
    private ResourceFieldSchema fieldSchema = null;

    private static final long serialVersionUID = 1L;

    public POCast(OperatorKey k) {
        super(k);
    }

    public POCast(OperatorKey k, int rp) {
        super(k, rp);
    }

    private void instantiateFunc() throws IOException {
        if (caster != null) return;

        if (funcSpec != null) {
            Object obj = PigContext
                    .instantiateFuncFromSpec(funcSpec);
            if (obj instanceof LoadFunc) {
                caster = ((LoadFunc)obj).getLoadCaster();
            } else if (obj instanceof StreamToPig) {
                caster = ((StreamToPig)obj).getLoadCaster();
            } else {
                throw new IOException("Invalid class type "
                        + funcSpec.getClassName());
            }
        }
    }

    private Result error() {
        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
    }

    public void setFuncSpec(FuncSpec lf) throws IOException {
        this.funcSpec = lf;
        instantiateFunc();
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitCast(this);
    }

    @Override
    public String name() {
        if (DataType.isSchemaType(resultType))
            return "Cast" + "[" + DataType.findTypeName(resultType)+":"
            + fieldSchema.calcCastString() + "]" + " - "
            + mKey.toString();
        else
            return "Cast" + "[" + DataType.findTypeName(resultType) + "]" + " - "
                + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public Result getNextBigInteger() throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte resultType = in.getResultType();
        switch (resultType) {
        case DataType.BAG:
        case DataType.TUPLE:
        case DataType.MAP:
        case DataType.DATETIME:
            return error();

        case DataType.BYTEARRAY: {
            DataByteArray dba;
            Result res = in.getNextDataByteArray();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                try {
                    dba = (DataByteArray) res.result;
                } catch (ClassCastException e) {
                    // res.result is not of type ByteArray. But it can be one of the types from which cast is still possible.
                    if (realType == null) {
                        // Find the type and cache it.
                        realType = DataType.findType(res.result);
                    }
                    try {
                        res.result = DataType.toBigInteger(res.result, realType);
                    } catch (ClassCastException cce) {
                        // Type has changed. Need to find type again and try casting it again.
                        realType = DataType.findType(res.result);
                        res.result = DataType.toBigInteger(res.result, realType);
                    }
                    return res;
                }
                try {
                    if (null != caster) {
                        res.result = caster.bytesToBigInteger(dba.get());
                    } else {
                        int errCode = 1075;
                        String msg = unknownByteArrayErrorMessage + "BigInteger.";
                        throw new ExecException(msg, errCode, PigException.INPUT);
                    }
                } catch (ExecException ee) {
                    throw ee;
                } catch (IOException e) {
                    log.error("Error while casting from ByteArray to BigInteger");
                }
            }
            return res;
        }

        case DataType.BOOLEAN: {
            Result res = in.getNextBoolean();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                if ((Boolean) res.result) {
                    res.result = BigInteger.ONE;
                } else {
                    res.result = BigInteger.ZERO;
                }
            }
            return res;
        }
        case DataType.INTEGER: {
            Result res = in.getNextInteger();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = BigInteger.valueOf(((Integer) res.result).longValue());
            }
            return res;
        }

        case DataType.DOUBLE: {
            Result res = in.getNextDouble();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = BigInteger.valueOf(((Double) res.result).longValue());
            }
            return res;
        }

        case DataType.LONG: {
            Result res = in.getNextLong();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = BigInteger.valueOf(((Long) res.result).longValue());
            }
            return res;
        }

        case DataType.FLOAT: {
            Result res = in.getNextFloat();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = BigInteger.valueOf(((Float) res.result).longValue());
            }
            return res;
        }

        case DataType.CHARARRAY: {
            Result res = in.getNextString();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new BigInteger((String)res.result);
            }
            return res;
        }

        case DataType.BIGINTEGER: {
            return in.getNextBigInteger();
        }

        case DataType.BIGDECIMAL: {
            Result res = in.getNextBigDecimal();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = ((BigDecimal)res.result).toBigInteger();
            }
            return res;
        }

        }

        return error();
    }

    @Override
    public Result getNextBigDecimal() throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte resultType = in.getResultType();
        switch (resultType) {
        case DataType.BAG:
        case DataType.TUPLE:
        case DataType.MAP:
        case DataType.DATETIME:
            return error();

        case DataType.BYTEARRAY: {
            DataByteArray dba;
            Result res = in.getNextDataByteArray();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                try {
                    dba = (DataByteArray) res.result;
                } catch (ClassCastException e) {
                    // res.result is not of type ByteArray. But it can be one of the types from which cast is still possible.
                    if (realType == null)
                        // Find the type and cache it.
                        realType = DataType.findType(res.result);
                    try {
                        res.result = DataType.toBigDecimal(res.result, realType);
                    } catch (ClassCastException cce) {
                        // Type has changed. Need to find type again and try casting it again.
                        realType = DataType.findType(res.result);
                        res.result = DataType.toBigDecimal(res.result, realType);
                    }
                    return res;
                }
                try {
                    if (null != caster) {
                        res.result = caster.bytesToBigDecimal(dba.get());
                    } else {
                        int errCode = 1075;
                        String msg = unknownByteArrayErrorMessage + "BigDecimal.";
                        throw new ExecException(msg, errCode, PigException.INPUT);
                    }
                } catch (ExecException ee) {
                    throw ee;
                } catch (IOException e) {
                    log.error("Error while casting from ByteArray to BigDecimal");
                }
            }
            return res;
        }

        case DataType.BOOLEAN: {
            Result res = in.getNextBoolean();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                if ((Boolean) res.result) {
                    res.result = BigDecimal.ONE;
                } else {
                    res.result = BigDecimal.ZERO;
                }
            }
            return res;
        }
        case DataType.INTEGER: {
            Result res = in.getNextInteger();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = BigDecimal.valueOf(((Integer) res.result).longValue());
            }
            return res;
        }

        case DataType.DOUBLE: {
            Result res = in.getNextDouble();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = BigDecimal.valueOf(((Double) res.result).doubleValue());
            }
            return res;
        }

        case DataType.LONG: {
            Result res = in.getNextLong();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = BigDecimal.valueOf(((Long) res.result).longValue());
            }
            return res;
        }

        case DataType.FLOAT: {
            Result res = in.getNextFloat();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = BigDecimal.valueOf(((Float) res.result).doubleValue());
            }
            return res;
        }

        case DataType.CHARARRAY: {
            Result res = in.getNextString();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new BigDecimal((String)res.result);
            }
            return res;
        }

        case DataType.BIGINTEGER: {
            Result res = in.getNextBigInteger();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new BigDecimal((BigInteger)res.result);
            }
            return res;
        }

        case DataType.BIGDECIMAL:
            return in.getNextBigDecimal();

        }

        return error();
    }

    @Override
    public Result getNextBoolean() throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte resultType = in.getResultType();
        switch (resultType) {
        case DataType.BAG:
        case DataType.TUPLE:
        case DataType.MAP:
        case DataType.DATETIME:
            return error();

        case DataType.BYTEARRAY: {
            DataByteArray dba;
            Result res = in.getNextDataByteArray();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                try {
                    dba = (DataByteArray) res.result;
                } catch (ClassCastException e) {
                    // res.result is not of type ByteArray. But it can be one of the types from which cast is still possible.
                    if (realType == null)
                        // Find the type and cache it.
                        realType = DataType.findType(res.result);
                    try {
                        res.result = DataType.toBoolean(res.result, realType);
                    } catch (ClassCastException cce) {
                        // Type has changed. Need to find type again and try casting it again.
                        realType = DataType.findType(res.result);
                        res.result = DataType.toBoolean(res.result, realType);
                    }
                    return res;
                }
                try {
                    if (null != caster) {
                        res.result = caster.bytesToBoolean(dba.get());
                    } else {
                        int errCode = 1075;
                        String msg = unknownByteArrayErrorMessage + "boolean.";
                        throw new ExecException(msg, errCode, PigException.INPUT);
                    }
                } catch (ExecException ee) {
                    throw ee;
                } catch (IOException e) {
                    log.error("Error while casting from ByteArray to Boolean");
                }
            }
            return res;
        }

        case DataType.CHARARRAY: {
            Result res = in.getNextString();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = CastUtils.stringToBoolean((String)res.result);
            }
            return res;
        }

        case DataType.BOOLEAN:
            return in.getNextBoolean();

        case DataType.INTEGER: {
            Integer i = null;
            Result res = in.getNextInteger();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Boolean.valueOf(((Integer) res.result).intValue() != 0);
            }
            return res;
        }

        case DataType.LONG: {
            Result res = in.getNextLong();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Boolean.valueOf(((Long) res.result).longValue() != 0L);
            }
            return res;
        }

        case DataType.FLOAT: {
            Result res = in.getNextFloat();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Boolean.valueOf(((Float) res.result).floatValue() != 0.0F);
            }
            return res;
        }

        case DataType.DOUBLE: {
            Result res = in.getNextDouble();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Boolean.valueOf(((Double) res.result).doubleValue() != 0.0);
            }
            return res;
        }

        case DataType.BIGINTEGER: {
            BigInteger bi = null;
            Result res = in.getNextBigInteger();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Boolean.valueOf(!BigInteger.ZERO.equals((BigInteger)res.result));
            }
            return res;
        }

        case DataType.BIGDECIMAL: {
            BigDecimal bd = null;
            Result res = in.getNextBigDecimal();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Boolean.valueOf(!BigDecimal.ZERO.equals((BigDecimal)res.result));
            }
            return res;
        }

        }

        return error();
    }

    @Override
    public Result getNextInteger() throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte resultType = in.getResultType();
        switch (resultType) {
        case DataType.BAG:
        case DataType.TUPLE:
        case DataType.MAP:
            return error();

        case DataType.BYTEARRAY: {
            DataByteArray dba;
            Result res = in.getNextDataByteArray();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                try {
                    dba = (DataByteArray) res.result;
                } catch (ClassCastException e) {
                    // res.result is not of type ByteArray. But it can be one of the types from which cast is still possible.
                    if (realType == null)
                        // Find the type and cache it.
                        realType = DataType.findType(res.result);
                    try {
                        res.result = DataType.toInteger(res.result, realType);
                    } catch (ClassCastException cce) {
                        // Type has changed. Need to find type again and try casting it again.
                        realType = DataType.findType(res.result);
                        res.result = DataType.toInteger(res.result, realType);
                    }
                    return res;
                }
                try {
                    if (null != caster) {
                        res.result = caster.bytesToInteger(dba.get());
                    } else {
                        int errCode = 1075;
                        String msg = unknownByteArrayErrorMessage + "int.";
                        throw new ExecException(msg, errCode, PigException.INPUT);
                    }
                } catch (ExecException ee) {
                    throw ee;
                } catch (IOException e) {
                    log.error("Error while casting from ByteArray to Integer");
                }
            }
            return res;
        }

        case DataType.BOOLEAN: {
            Result res = in.getNextBoolean();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                if ((Boolean) res.result) {
                    res.result = Integer.valueOf(1);
                } else {
                    res.result = Integer.valueOf(0);
                }
            }
            return res;
        }
        case DataType.INTEGER: {
            Result res = in.getNextInteger();
            return res;
        }

        case DataType.DOUBLE: {
            Double d = null;
            Result res = in.getNextDouble();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                // res.result = DataType.toInteger(res.result);
                res.result = Integer.valueOf(((Double) res.result).intValue());
            }
            return res;
        }

        case DataType.LONG: {
            Result res = in.getNextLong();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Integer.valueOf(((Long) res.result).intValue());
            }
            return res;
        }

        case DataType.FLOAT: {
            Result res = in.getNextFloat();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Integer.valueOf(((Float) res.result).intValue());
            }
            return res;
        }

        case DataType.DATETIME: {
            Result res = in.getNextDateTime();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Integer.valueOf(Long.valueOf(((DateTime) res.result).getMillis()).intValue());
            }
            return res;
        }

        case DataType.CHARARRAY: {
            Result res = in.getNextString();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = CastUtils.stringToInteger((String)res.result);
            }
            return res;
        }

        case DataType.BIGINTEGER: {
            Result res = in.getNextBigInteger();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Integer.valueOf(((BigInteger)res.result).intValue());
            }
            return res;
        }

        case DataType.BIGDECIMAL: {
            Result res = in.getNextBigDecimal();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Integer.valueOf(((BigDecimal)res.result).intValue());
            }
            return res;
        }

        }

        return error();
    }

    @Override
    public Result getNextLong() throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte resultType = in.getResultType();
        switch (resultType) {
        case DataType.BAG:
        case DataType.TUPLE:
        case DataType.MAP:
            return error();

        case DataType.BYTEARRAY: {
            DataByteArray dba;
            Result res = in.getNextDataByteArray();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                try {
                    dba = (DataByteArray) res.result;
                } catch (ClassCastException e) {
                    // res.result is not of type ByteArray. But it can be one of the types from which cast is still possible.
                    if (realType == null)
                        // Find the type in first call and cache it.
                        realType = DataType.findType(res.result);
                    try {
                        res.result = DataType.toLong(res.result, realType);
                    } catch (ClassCastException cce) {
                        // Type has changed. Need to find type again and try casting it again.
                        realType = DataType.findType(res.result);
                        res.result = DataType.toLong(res.result, realType);
                    }
                    return res;
                }
                try {
                    if (null != caster) {
                        res.result = caster.bytesToLong(dba.get());
                    } else {
                        int errCode = 1075;
                        String msg = unknownByteArrayErrorMessage + "long.";
                        throw new ExecException(msg, errCode, PigException.INPUT);
                    }
                } catch (ExecException ee) {
                    throw ee;
                } catch (IOException e) {
                    log.error("Error while casting from ByteArray to Long");
                }
            }
            return res;
        }

        case DataType.BOOLEAN: {
            Result res = in.getNextBoolean();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                if ((Boolean) res.result) {
                    res.result = Long.valueOf(1);
                } else {
                    res.result = Long.valueOf(0);
                }
            }
            return res;
        }
        case DataType.INTEGER: {
            Result res = in.getNextInteger();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Long.valueOf(((Integer) res.result).longValue());
            }
            return res;
        }

        case DataType.DOUBLE: {
            Result res = in.getNextDouble();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                // res.result = DataType.toInteger(res.result);
                res.result = Long.valueOf(((Double) res.result).longValue());
            }
            return res;
        }

        case DataType.LONG:
            return in.getNextLong();

        case DataType.FLOAT: {
            Result res = in.getNextFloat();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Long.valueOf(((Float) res.result).longValue());
            }
            return res;
        }

        case DataType.DATETIME: {
            Result res = in.getNextDateTime();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Long.valueOf(((DateTime) res.result).getMillis());
            }
            return res;
        }

        case DataType.CHARARRAY: {
            Result res = in.getNextString();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = CastUtils.stringToLong((String)res.result);
            }
            return res;
        }

        case DataType.BIGINTEGER: {
            Result res = in.getNextBigInteger();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Long.valueOf(((BigInteger)res.result).longValue());
            }
            return res;
        }

        case DataType.BIGDECIMAL: {
            Result res = in.getNextBigDecimal();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Long.valueOf(((BigDecimal)res.result).longValue());
            }
            return res;
        }

        }

        return error();
    }

    @Override
    public Result getNextDouble() throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte resultType = in.getResultType();
        switch (resultType) {
        case DataType.BAG:
        case DataType.TUPLE:
        case DataType.MAP:
            return error();

        case DataType.BYTEARRAY: {
            DataByteArray dba;
            Result res = in.getNextDataByteArray();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                try {
                    dba = (DataByteArray) res.result;
                } catch (ClassCastException e) {
                    // res.result is not of type ByteArray. But it can be one of the types from which cast is still possible.
                    if (realType == null)
                        // Find the type in first call and cache it.
                        realType = DataType.findType(res.result);
                    try {
                        res.result = DataType.toDouble(res.result, realType);
                    } catch (ClassCastException cce) {
                        // Type has changed. Need to find type again and try casting it again.
                        realType = DataType.findType(res.result);
                        res.result = DataType.toDouble(res.result, realType);
                    }
                    return res;
                }
                try {
                    if (null != caster) {
                        res.result = caster.bytesToDouble(dba.get());
                    } else {
                        int errCode = 1075;
                        String msg = unknownByteArrayErrorMessage + "double.";
                        throw new ExecException(msg, errCode, PigException.INPUT);
                    }
                } catch (ExecException ee) {
                    throw ee;
                } catch (IOException e) {
                    log.error("Error while casting from ByteArray to Double");
                }
            }
            return res;
        }

        case DataType.BOOLEAN: {
            Result res = in.getNextBoolean();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                if ((Boolean) res.result) {
                    res.result = new Double(1);
                } else {
                    res.result = new Double(0);
                }
            }
            return res;
        }
        case DataType.INTEGER: {
            Result res = in.getNextInteger();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new Double(((Integer) res.result).doubleValue());
            }
            return res;
        }

        case DataType.DOUBLE:
            return in.getNextDouble();

        case DataType.LONG: {
            Result res = in.getNextLong();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new Double(((Long) res.result).doubleValue());
            }
            return res;
        }

        case DataType.FLOAT: {
            Result res = in.getNextFloat();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new Double(((Float) res.result).doubleValue());
            }
            return res;
        }

        case DataType.DATETIME: {
            Result res = in.getNextDateTime();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new Double(Long.valueOf(((DateTime) res.result).getMillis()).doubleValue());
            }
            return res;
        }

        case DataType.CHARARRAY: {
            Result res = in.getNextString();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = CastUtils.stringToDouble((String)res.result);
            }
            return res;
        }

        case DataType.BIGINTEGER: {
            Result res = in.getNextBigInteger();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Double.valueOf(((BigInteger)res.result).doubleValue());
            }
            return res;
        }

        case DataType.BIGDECIMAL: {
            Result res = in.getNextBigDecimal();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Double.valueOf(((BigDecimal)res.result).doubleValue());
            }
            return res;
        }

        }

        return error();
    }

    @Override
    public Result getNextFloat() throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte resultType = in.getResultType();
        switch (resultType) {
        case DataType.BAG:
        case DataType.TUPLE:
        case DataType.MAP:
            return error();

        case DataType.BYTEARRAY: {
            DataByteArray dba;
            Result res = in.getNextDataByteArray();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                try {
                    dba = (DataByteArray) res.result;
                } catch (ClassCastException e) {
                    // res.result is not of type ByteArray. But it can be one of the types from which cast is still possible.
                    if (realType == null)
                        // Find the type in first call and cache it.
                        realType = DataType.findType(res.result);
                    try {
                        res.result = DataType.toFloat(res.result, realType);
                    } catch (ClassCastException cce) {
                        // Type has changed. Need to find type again and try casting it again.
                        realType = DataType.findType(res.result);
                        res.result = DataType.toFloat(res.result, realType);
                    }
                    return res;
                }
                try {
                    if (null != caster) {
                        res.result = caster.bytesToFloat(dba.get());
                    } else {
                        int errCode = 1075;
                        String msg = unknownByteArrayErrorMessage + "float.";
                        throw new ExecException(msg, errCode, PigException.INPUT);
                    }
                } catch (ExecException ee) {
                    throw ee;
                } catch (IOException e) {
                    log.error("Error while casting from ByteArray to Float");
                }
            }
            return res;
        }

        case DataType.BOOLEAN: {
            Result res = in.getNextBoolean();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                if ((Boolean) res.result) {
                    res.result = new Float(1);
                } else {
                    res.result = new Float(0);
                }
            }
            return res;
        }
        case DataType.INTEGER: {
            Result res = in.getNextInteger();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new Float(((Integer) res.result).floatValue());
            }
            return res;
        }

        case DataType.DOUBLE: {
            Result res = in.getNextDouble();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                // res.result = DataType.toInteger(res.result);
                res.result = new Float(((Double) res.result).floatValue());
            }
            return res;
        }

        case DataType.LONG: {
            Result res = in.getNextLong();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new Float(((Long) res.result).floatValue());
            }
            return res;
        }

        case DataType.FLOAT: {
            return in.getNextFloat();
        }

        case DataType.DATETIME: {
            DateTime dt = null;
            Result res = in.getNextDateTime();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new Float(Long.valueOf(((DateTime) res.result).getMillis()).floatValue());
            }
            return res;
        }

        case DataType.CHARARRAY: {
            Result res = in.getNextString();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = CastUtils.stringToFloat((String)res.result);
            }
            return res;
        }

        case DataType.BIGINTEGER: {
            Result res = in.getNextBigInteger();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Float.valueOf(((BigInteger)res.result).floatValue());
            }
            return res;
        }

        case DataType.BIGDECIMAL: {
            Result res = in.getNextBigDecimal();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Float.valueOf(((BigDecimal)res.result).floatValue());
            }
            return res;
        }

        }

        return error();
    }

    @Override
    public Result getNextDateTime() throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte resultType = in.getResultType();
        switch (resultType) {
        case DataType.BAG:
        case DataType.TUPLE:
        case DataType.MAP:
            return error();

        case DataType.BYTEARRAY: {
            DataByteArray dba;
            Result res = in.getNextDataByteArray();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                try {
                    dba = (DataByteArray) res.result;
                } catch (ClassCastException e) {
                    // res.result is not of type ByteArray. But it can be one of the types from which cast is still possible.
                    if (realType == null) {
                        // Find the type in first call and cache it.
                        realType = DataType.findType(res.result);
                    }
                    try {
                        res.result = DataType.toDateTime(res.result, realType);
                    } catch (ClassCastException cce) {
                        // Type has changed. Need to find type again and try casting it again.
                        realType = DataType.findType(res.result);
                        res.result = DataType.toDateTime(res.result, realType);
                    }
                    return res;
                }
                try {
                    if (null != caster) {
                        res.result = caster.bytesToDateTime(dba.get());
                    } else {
                        int errCode = 1075;
                        String msg = unknownByteArrayErrorMessage + "datetime.";
                        throw new ExecException(msg, errCode, PigException.INPUT);
                    }
                } catch (ExecException ee) {
                    throw ee;
                } catch (IOException e) {
                    log.error("Error while casting from ByteArray to DateTime");
                }
            }
            return res;
        }

        case DataType.INTEGER: {
            Result res = in.getNextInteger();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new DateTime(((Integer) res.result).longValue());
            }
            return res;
        }

        case DataType.DOUBLE: {
            Result res = in.getNextDouble();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new DateTime(((Double) res.result).longValue());
            }
            return res;
        }

        case DataType.LONG: {
            Result res = in.getNextLong();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new DateTime(((Long) res.result).longValue());
            }
            return res;
        }

        case DataType.FLOAT: {
            Result res = in.getNextFloat();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new DateTime(((Float) res.result).longValue());
            }
            return res;
        }

        case DataType.BIGINTEGER: {
            Result res = in.getNextBigInteger();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new DateTime(((BigInteger) res.result).longValue());
            }
            return res;
        }

        case DataType.BIGDECIMAL: {
            Result res = in.getNextBigDecimal();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new DateTime(((BigDecimal) res.result).longValue());
            }
            return res;
        }

        case DataType.DATETIME:
            return in.getNextDateTime();

        case DataType.CHARARRAY: {
            Result res = in.getNextString();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = ToDate.extractDateTime((String) res.result);
            }
            return res;
        }

        }

        return error();
    }

    @Override
    public Result getNextString() throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte resultType = in.getResultType();
        switch (resultType) {
        case DataType.BAG:
        case DataType.TUPLE:
        case DataType.MAP:
            return error();

        case DataType.BYTEARRAY: {
            DataByteArray dba;
            Result res = in.getNextDataByteArray();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                try {
                    dba = (DataByteArray) res.result;
                } catch (ClassCastException e) {
                    // res.result is not of type ByteArray. But it can be one of the types from which cast is still possible.
                    if (realType == null)
                        // Find the type in first call and cache it.
                        realType = DataType.findType(res.result);
                    try {
                        res.result = DataType.toString(res.result, realType);
                    } catch (ClassCastException cce) {
                        // Type has changed. Need to find type again and try casting it again.
                        realType = DataType.findType(res.result);
                        res.result = DataType.toString(res.result, realType);
                    }
                    return res;
                }
                try {
                    if (null != caster) {
                        res.result = caster.bytesToCharArray(dba.get());
                    } else {
                        int errCode = 1075;
                        String msg = unknownByteArrayErrorMessage + "string.";
                        throw new ExecException(msg, errCode, PigException.INPUT);
                    }
                } catch (ExecException ee) {
                    throw ee;
                } catch (IOException e) {
                    log
                            .error("Error while casting from ByteArray to CharArray");
                }
            }
            return res;
        }

        case DataType.BOOLEAN: {
            Result res = in.getNextBoolean();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                if ((Boolean) res.result) {
                    //res.result = "1";
                    res.result = Boolean.TRUE.toString();
                } else {
                    //res.result = "0";
                    res.result = Boolean.FALSE.toString();
                }
            }
            return res;
        }
        case DataType.INTEGER: {
            Result res = in.getNextInteger();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = ((Integer) res.result).toString();
            }
            return res;
        }

        case DataType.DOUBLE: {
            Result res = in.getNextDouble();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                // res.result = DataType.toInteger(res.result);
                res.result = ((Double) res.result).toString();
            }
            return res;
        }

        case DataType.LONG: {
            Result res = in.getNextLong();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = ((Long) res.result).toString();
            }
            return res;
        }

        case DataType.FLOAT: {
            Result res = in.getNextFloat();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = ((Float) res.result).toString();
            }
            return res;
        }

        case DataType.DATETIME: {
            Result res = in.getNextDateTime();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = ((DateTime) res.result).toString();
            }
            return res;
        }

        case DataType.CHARARRAY:
            return in.getNextString();

        case DataType.BIGINTEGER: {
            BigInteger bi = null;
            Result res = in.getNextBigInteger();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = ((BigInteger)res.result).toString();
            }
            return res;
        }

        case DataType.BIGDECIMAL: {
            Result res = in.getNextBigDecimal();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = ((BigDecimal)res.result).toString();
            }
            return res;
        }

        }

        return error();
    }

    @Override
    public Result getNextTuple() throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte castToType = DataType.TUPLE;
        Byte resultType = in.getResultType();
        switch (resultType) {

        case DataType.TUPLE: {
            Result res = in.getNextTuple();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                try {
                    res.result = convertWithSchema(res.result, fieldSchema);
                } catch (IOException e) {
                    LogUtils.warn(this, "Unable to interpret value " + res.result + " in field being " +
                            "converted to type tuple, caught ParseException <" +
                            e.getMessage() + "> field discarded",
                            PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, log);
                    res.result = null;
                }
            }
            return res;
        }

        case DataType.BYTEARRAY: {
            DataByteArray dba;
            Result res = in.getNextDataByteArray();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                // res.result = new
                // String(((DataByteArray)res.result).toString());
                if (castNotNeeded) {
                    // we examined the data once before and
                    // determined that the input is the same
                    // type as the type we are casting to
                    // so just send the input out as output
                    return res;
                }
                try {
                    dba = (DataByteArray) res.result;
                } catch (ClassCastException e) {
                    // check if the type of res.result is
                    // same as the type we are trying to cast to
                    if (DataType.findType(res.result) == castToType) {
                        // remember this for future calls
                        castNotNeeded = true;
                        // just return the output
                        return res;
                    } else {
                        // the input is a differen type
                        // rethrow the exception
                        int errCode = 1081;
                        String msg = "Cannot cast to tuple. Expected bytearray but received: " + DataType.findTypeName(res.result);
                        throw new ExecException(msg, errCode, PigException.INPUT, e);
                    }

                }
                try {
                    if (null != caster) {
                        res.result = caster.bytesToTuple(dba.get(), fieldSchema);
                    } else {
                        int errCode = 1075;
                        String msg = unknownByteArrayErrorMessage + "tuple.";
                        throw new ExecException(msg, errCode, PigException.INPUT);
                    }
                } catch (ExecException ee) {
                    throw ee;
                } catch (IOException e) {
                    log.error("Error while casting from ByteArray to Tuple");
                }
            }
            return res;
        }

        case DataType.BAG:
        case DataType.MAP:
        case DataType.INTEGER:
        case DataType.DOUBLE:
        case DataType.LONG:
        case DataType.FLOAT:
        case DataType.CHARARRAY:
        case DataType.BOOLEAN:
        case DataType.BIGINTEGER:
        case DataType.BIGDECIMAL:
        case DataType.DATETIME:
            return error();
        }

        return error();
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    private Object convertWithSchema(Object obj, ResourceFieldSchema fs) throws IOException {
        Object result = null;

        if (fs == null) {
            return obj;
        }

        if (obj == null) {
            // handle DataType.NULL
            return null;
        }

        switch (fs.getType()) {
        case DataType.BAG:
            if (obj instanceof DataBag) {
                DataBag db = (DataBag)obj;
                // Get inner schema of a bag
                if (fs.getSchema()!=null) {
                    ResourceFieldSchema tupleFs = fs.getSchema().getFields()[0];
                    Iterator<Tuple> iter = db.iterator();

                    while (iter.hasNext()) {
                        Tuple t = iter.next();
                        convertWithSchema(t, tupleFs);
                    }
                }
                result = db;
            } else if (obj instanceof DataByteArray) {
                if (null != caster) {
                    result = caster.bytesToBag(((DataByteArray)obj).get(), fs);
                } else {
                    int errCode = 1075;
                    String msg = unknownByteArrayErrorMessage + "bag.";
                    throw new ExecException(msg, errCode, PigException.INPUT);
                }
            } else {
                throw new ExecException("Cannot cast " + obj + " to bag.", 1120, PigException.INPUT);
            }
            break;
        case DataType.TUPLE:
            if (obj instanceof Tuple) {
                try {
                    Tuple t = (Tuple)obj;
                    ResourceSchema innerSchema = fs.getSchema();
                    if (innerSchema==null)
                        return t;
                    if (innerSchema.getFields().length!=t.size())
                        return null;
                    int i=0;
                    for (ResourceFieldSchema fieldSchema : innerSchema.getFields()) {
                        Object field = convertWithSchema(t.get(i), fieldSchema);
                        t.set(i, field);
                        i++;
                    }
                    result = t;
                } catch (Exception e) {
                    throw new ExecException("Cannot convert "+ obj + " to " + fs);
                }
            } else if (obj instanceof DataByteArray) {
                if (null != caster) {
                    result = caster.bytesToTuple(((DataByteArray)obj).get(), fs);
                } else {
                    int errCode = 1075;
                    String msg = unknownByteArrayErrorMessage + "tuple.";
                    throw new ExecException(msg, errCode, PigException.INPUT);
                }
            } else {
                throw new ExecException("Cannot cast " + obj + " to tuple.", 1120, PigException.INPUT);
            }
            break;
        case DataType.MAP:
            if (obj instanceof Map) {
                if (fs!=null && fs.getSchema()!=null) {
                    ResourceFieldSchema innerFieldSchema = fs.getSchema().getFields()[0];
                    Map m = (Map)obj;
                    for (Object entry : m.entrySet()) {
                        Object newValue = convertWithSchema(((Map.Entry)entry).getValue(), innerFieldSchema);
                        m.put(((Map.Entry)entry).getKey(), newValue);
                    }
                    result = m;
                }
                else
                    result = obj;
            } else if (obj instanceof DataByteArray) {
                if (null != caster) {
                    result = caster.bytesToMap(((DataByteArray)obj).get(), fs);
                } else {
                    int errCode = 1075;
                    String msg = unknownByteArrayErrorMessage + "tuple.";
                    throw new ExecException(msg, errCode, PigException.INPUT);
                }
            } else {
                throw new ExecException("Cannot cast " + obj + " to map.", 1120, PigException.INPUT);
            }
            break;
        case DataType.BOOLEAN:
            switch (DataType.findType(obj)) {
            case DataType.BYTEARRAY:
                if (null != caster) {
                    result = caster.bytesToBoolean(((DataByteArray) obj).get());
                } else {
                    int errCode = 1075;
                    String msg = unknownByteArrayErrorMessage + "int.";
                    throw new ExecException(msg, errCode, PigException.INPUT);
                }
                break;
            case DataType.BOOLEAN:
                result = obj;
                break;
            case DataType.INTEGER:
                result = Boolean.valueOf(((Integer) obj).intValue() != 0);;
                break;
            case DataType.DOUBLE:
                result = Boolean.valueOf(((Double) obj).doubleValue() != 0.0D);
                break;
            case DataType.LONG:
                result = Boolean.valueOf(((Long) obj).longValue() != 0L);
                break;
            case DataType.FLOAT:
                result = Boolean.valueOf(((Float) obj).floatValue() != 0.0F);
                break;
            case DataType.CHARARRAY:
                result = CastUtils.stringToBoolean((String)obj);
                break;
            case DataType.BIGINTEGER:
                result = Boolean.valueOf(!BigInteger.ZERO.equals((BigInteger)obj));
                break;
            case DataType.BIGDECIMAL:
                result = Boolean.valueOf(!BigDecimal.ZERO.equals((BigDecimal)obj));
                break;
            default:
                throw new ExecException("Cannot convert "+ obj + " to " + fs, 1120, PigException.INPUT);
            }
            break;
        case DataType.INTEGER:
            switch (DataType.findType(obj)) {
            case DataType.BYTEARRAY:
                if (null != caster) {
                    result = caster.bytesToInteger(((DataByteArray) obj).get());
                } else {
                    int errCode = 1075;
                    String msg = unknownByteArrayErrorMessage + "int.";
                    throw new ExecException(msg, errCode, PigException.INPUT);
                }
                break;
            case DataType.BOOLEAN:
                if ((Boolean) obj) {
                    result = Integer.valueOf(1);
                } else {
                    result = Integer.valueOf(0);
                }
                break;
            case DataType.INTEGER:
                result = obj;
                break;
            case DataType.DOUBLE:
                result = Integer.valueOf(((Double)obj).intValue());
                break;
            case DataType.LONG:
                result = Integer.valueOf(((Long)obj).intValue());
                break;
            case DataType.FLOAT:
                result = Integer.valueOf(((Float)obj).intValue());
                break;
            case DataType.DATETIME:
                result = Integer.valueOf(Long.valueOf(((DateTime)obj).getMillis()).intValue());
                break;
            case DataType.CHARARRAY:
                result = CastUtils.stringToInteger((String)obj);
                break;
            case DataType.BIGINTEGER:
                result = Integer.valueOf(((BigInteger)obj).intValue());
                break;
            case DataType.BIGDECIMAL:
                result = Integer.valueOf(((BigDecimal)obj).intValue());
                break;
            default:
                throw new ExecException("Cannot convert "+ obj + " to " + fs, 1120, PigException.INPUT);
            }
            break;
        case DataType.DOUBLE:
            switch (DataType.findType(obj)) {
            case DataType.BYTEARRAY:
                if (null != caster) {
                    result = caster.bytesToDouble(((DataByteArray) obj).get());
                } else {
                    int errCode = 1075;
                    String msg = unknownByteArrayErrorMessage + "double.";
                    throw new ExecException(msg, errCode, PigException.INPUT);
                }
                break;
            case DataType.BOOLEAN:
                if ((Boolean) obj) {
                    result = new Double(1);
                } else {
                    result = new Double(1);
                }
                break;
            case DataType.INTEGER:
                result = new Double(((Integer)obj).doubleValue());
                break;
            case DataType.DOUBLE:
                result = (Double)obj;
                break;
            case DataType.LONG:
                result = new Double(((Long)obj).doubleValue());
                break;
            case DataType.FLOAT:
                result = new Double(((Float)obj).doubleValue());
                break;
            case DataType.DATETIME:
                result = new Double(Long.valueOf(((DateTime)obj).getMillis()).doubleValue());
                break;
            case DataType.CHARARRAY:
                result = CastUtils.stringToDouble((String)obj);
                break;
            case DataType.BIGINTEGER:
                result = Double.valueOf(((BigInteger)obj).doubleValue());
                break;
            case DataType.BIGDECIMAL:
                result = Double.valueOf(((BigDecimal)obj).doubleValue());
                break;
            default:
                throw new ExecException("Cannot convert "+ obj + " to " + fs, 1120, PigException.INPUT);
            }
            break;
        case DataType.LONG:
            switch (DataType.findType(obj)) {
            case DataType.BYTEARRAY:
                if (null != caster) {
                    result = caster.bytesToLong(((DataByteArray)obj).get());
                } else {
                    int errCode = 1075;
                    String msg = unknownByteArrayErrorMessage + "long.";
                    throw new ExecException(msg, errCode, PigException.INPUT);
                }
                break;
            case DataType.BOOLEAN:
                if ((Boolean) obj) {
                    result = Long.valueOf(1);
                } else {
                    result = Long.valueOf(0);
                }
                break;
            case DataType.INTEGER:
                result = Long.valueOf(((Integer)obj).longValue());
                break;
            case DataType.DOUBLE:
                result = Long.valueOf(((Double)obj).longValue());
                break;
            case DataType.LONG:
                result = (Long)obj;
                break;
            case DataType.FLOAT:
                result = Long.valueOf(((Float)obj).longValue());
                break;
            case DataType.DATETIME:
                result = Long.valueOf(((DateTime)obj).getMillis());
                break;
            case DataType.CHARARRAY:
                result = CastUtils.stringToLong((String)obj);
                break;
            case DataType.BIGINTEGER:
                result = Long.valueOf(((BigInteger)obj).longValue());
                break;
            case DataType.BIGDECIMAL:
                result = Long.valueOf(((BigDecimal)obj).longValue());
                break;
            default:
                throw new ExecException("Cannot convert "+ obj + " to " + fs, 1120, PigException.INPUT);
            }
            break;
        case DataType.FLOAT:
            switch (DataType.findType(obj)) {
            case DataType.BYTEARRAY:
                if (null != caster) {
                    result = caster.bytesToFloat(((DataByteArray)obj).get());
                } else {
                    int errCode = 1075;
                    String msg = unknownByteArrayErrorMessage + "float.";
                    throw new ExecException(msg, errCode, PigException.INPUT);
                }
                break;
            case DataType.BOOLEAN:
                if ((Boolean) obj) {
                    result = new Float(1);
                } else {
                    result = new Float(0);
                }
                break;
            case DataType.INTEGER:
                result = new Float(((Integer) obj).floatValue());
                break;
            case DataType.DOUBLE:
                result = new Float(((Double)obj).floatValue());
                break;
            case DataType.LONG:
                result = new Float(((Long)obj).floatValue());
                break;
            case DataType.FLOAT:
                result = obj;
                break;
            case DataType.DATETIME:
                result = new Float(Long.valueOf(((DateTime)obj).getMillis()).floatValue());
                break;
            case DataType.CHARARRAY:
                result = CastUtils.stringToFloat((String)obj);
                break;
            case DataType.BIGINTEGER:
                result = Float.valueOf(((BigInteger)obj).floatValue());
                break;
            case DataType.BIGDECIMAL:
                result = Float.valueOf(((BigDecimal)obj).floatValue());
                break;
            default:
                throw new ExecException("Cannot convert "+ obj + " to " + fs, 1120, PigException.INPUT);
            }
            break;
        case DataType.DATETIME:
            switch (DataType.findType(obj)) {
            case DataType.BYTEARRAY:
                if (null != caster) {
                    result = caster.bytesToDateTime(((DataByteArray)obj).get());
                } else {
                    int errCode = 1075;
                    String msg = unknownByteArrayErrorMessage + "datetime.";
                    throw new ExecException(msg, errCode, PigException.INPUT);
                }
                break;
            case DataType.INTEGER:
                result = new DateTime(((Integer)obj).longValue());
                break;
            case DataType.DOUBLE:
                result = new DateTime(((Double)obj).longValue());
                break;
            case DataType.LONG:
                result = new DateTime(((Long)obj).longValue());
                break;
            case DataType.FLOAT:
                result = new DateTime(((Float)obj).longValue());
                break;
            case DataType.DATETIME:
                result = (DateTime)obj;
                break;
            case DataType.CHARARRAY:
                result = ToDate.extractDateTime((String) obj);
                break;
            case DataType.BIGINTEGER:
                result = new DateTime(((BigInteger)obj).longValue());
                break;
            case DataType.BIGDECIMAL:
                result = new DateTime(((BigDecimal)obj).longValue());
                break;
            default:
                throw new ExecException("Cannot convert "+ obj + " to " + fs, 1120, PigException.INPUT);
            }
            break;
        case DataType.CHARARRAY:
            switch (DataType.findType(obj)) {
            case DataType.BYTEARRAY:
                if (null != caster) {
                    result = caster.bytesToCharArray(((DataByteArray)obj).get());
                } else {
                    int errCode = 1075;
                    String msg = unknownByteArrayErrorMessage + "float.";
                    throw new ExecException(msg, errCode, PigException.INPUT);
                }
                break;
            case DataType.BOOLEAN:
                if ((Boolean) obj) {
                    //result = "1";
                    result = Boolean.TRUE.toString();
                } else {
                    //result = "0";
                    result = Boolean.FALSE.toString();
                }
                break;
            case DataType.INTEGER:
                result = ((Integer) obj).toString();
                break;
            case DataType.DOUBLE:
                result = ((Double) obj).toString();
                break;
            case DataType.LONG:
                result = ((Long) obj).toString();
                break;
            case DataType.FLOAT:
                result = ((Float) obj).toString();
                break;
            case DataType.DATETIME:
                result = ((DateTime)obj).toString();
                break;
            case DataType.CHARARRAY:
                result = obj;
                break;
            case DataType.BIGINTEGER:
                result = ((BigInteger)obj).toString();
                break;
            case DataType.BIGDECIMAL:
                result = ((BigDecimal)obj).toString();
                break;
            default:
                throw new ExecException("Cannot convert "+ obj + " to " + fs, 1120, PigException.INPUT);
            }
            break;
        case DataType.BIGINTEGER:
            switch (DataType.findType(obj)) {
            case DataType.BYTEARRAY:
                if (null != caster) {
                    result = caster.bytesToBigInteger(((DataByteArray)obj).get());
                } else {
                    int errCode = 1075;
                    String msg = unknownByteArrayErrorMessage + "BigInteger.";
                    throw new ExecException(msg, errCode, PigException.INPUT);
                }
                break;
            case DataType.BOOLEAN:
                if ((Boolean) obj) {
                    result = BigInteger.ONE;
                } else {
                    result = BigInteger.ZERO;
                }
                break;
            case DataType.INTEGER:
                result = BigInteger.valueOf(((Integer)obj).longValue());
                break;
            case DataType.DOUBLE:
                result = BigInteger.valueOf(((Double)obj).longValue());
                break;
            case DataType.LONG:
                result = BigInteger.valueOf(((Long)obj).longValue());
                break;
            case DataType.FLOAT:
                result = BigInteger.valueOf(((Float)obj).longValue());
                break;
            case DataType.CHARARRAY:
                result = new BigInteger((String)obj);
                break;
            case DataType.BIGINTEGER:
                result = (BigInteger)obj;
                break;
            case DataType.BIGDECIMAL:
                result = ((BigDecimal)obj).toBigInteger();
                break;
            case DataType.DATETIME:
                result = BigInteger.valueOf(((DateTime)obj).getMillis());
                break;
            default:
                throw new ExecException("Cannot convert "+ obj + " to " + fs, 1120, PigException.INPUT);
            }
        case DataType.BIGDECIMAL:
            switch (DataType.findType(obj)) {
            case DataType.BYTEARRAY:
                if (null != caster) {
                    result = caster.bytesToBigDecimal(((DataByteArray)obj).get());
                } else {
                    int errCode = 1075;
                    String msg = unknownByteArrayErrorMessage + "BigDecimal.";
                    throw new ExecException(msg, errCode, PigException.INPUT);
                }
                break;
            case DataType.BOOLEAN:
                if ((Boolean) obj) {
                    result = BigDecimal.ONE;
                } else {
                    result = BigDecimal.ZERO;
                }
                break;
            case DataType.INTEGER:
                result = BigDecimal.valueOf(((Integer)obj).longValue());
                break;
            case DataType.DOUBLE:
                result = BigDecimal.valueOf(((Double)obj).doubleValue());
                break;
            case DataType.LONG:
                result = BigDecimal.valueOf(((Long)obj).longValue());
                break;
            case DataType.FLOAT:
                result = BigDecimal.valueOf(((Float)obj).doubleValue());
                break;
            case DataType.CHARARRAY:
                result = new BigDecimal((String)obj);
                break;
            case DataType.BIGINTEGER:
                result = new BigDecimal((BigInteger)obj);
                break;
            case DataType.BIGDECIMAL:
                result = (BigDecimal)obj;
                break;
            case DataType.DATETIME:
                result = BigDecimal.valueOf(((DateTime)obj).getMillis());
                break;
            default:
                throw new ExecException("Cannot convert "+ obj + " to " + fs, 1120, PigException.INPUT);
            }
        default:
            throw new ExecException("Don't know how to convert "+ obj + " to " + fs, 1120, PigException.INPUT);
        }
        return result;
    }

    @Override
    public Result getNextDataBag() throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte castToType = DataType.BAG;
        Byte resultType = in.getResultType();
        switch (resultType) {

        case DataType.BAG: {
            res = in.getNextDataBag();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                try {
                    res.result = convertWithSchema(res.result, fieldSchema);
                } catch (IOException e) {
                    LogUtils.warn(this, "Unable to interpret value " + res.result + " in field being " +
                            "converted to type bag, caught ParseException <" +
                            e.getMessage() + "> field discarded",
                            PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, log);
                    res.result = null;
                }
            }
            return res;
        }

        case DataType.BYTEARRAY: {
            DataByteArray dba;
            Result res = in.getNextDataByteArray();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                // res.result = new
                // String(((DataByteArray)res.result).toString());
                if (castNotNeeded) {
                    // we examined the data once before and
                    // determined that the input is the same
                    // type as the type we are casting to
                    // so just send the input out as output
                    return res;
                }
                try {
                    dba = (DataByteArray) res.result;
                } catch (ClassCastException e) {
                    // check if the type of res.result is
                    // same as the type we are trying to cast to
                    if (DataType.findType(res.result) == castToType) {
                        // remember this for future calls
                        castNotNeeded = true;
                        // just return the output
                        return res;
                    } else {
                        // the input is a differen type
                        // rethrow the exception
                        int errCode = 1081;
                        String msg = "Cannot cast to bag. Expected bytearray but received: " + DataType.findTypeName(res.result);
                        throw new ExecException(msg, errCode, PigException.INPUT, e);
                    }

                }
                try {
                    if (null != caster) {
                        res.result = caster.bytesToBag(dba.get(), fieldSchema);
                    } else {
                        int errCode = 1075;
                        String msg = unknownByteArrayErrorMessage + "bag.";
                        throw new ExecException(msg, errCode, PigException.INPUT);
                    }
                } catch (ExecException ee) {
                    throw ee;
                } catch (IOException e) {
                    log.error("Error while casting from ByteArray to DataBag");
                }
            }
            return res;
        }

        case DataType.TUPLE:
        case DataType.MAP:
        case DataType.INTEGER:
        case DataType.DOUBLE:
        case DataType.LONG:
        case DataType.FLOAT:
        case DataType.DATETIME:
        case DataType.CHARARRAY:
        case DataType.BOOLEAN:
        case DataType.BIGINTEGER:
        case DataType.BIGDECIMAL:
            return error();

        }

        return error();
    }

    @SuppressWarnings("deprecation")
    @Override
    public Result getNextMap() throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte castToType = DataType.MAP;
        Byte resultType = in.getResultType();
        switch (resultType) {

        case DataType.MAP: {
            Result res = in.getNextMap();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                try {
                    res.result = convertWithSchema(res.result, fieldSchema);
                } catch (IOException e) {
                    LogUtils.warn(this, "Unable to interpret value " + res.result + " in field being " +
                            "converted to type map, caught ParseException <" +
                            e.getMessage() + "> field discarded",
                            PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, log);
                    res.result = null;
                }
            }
            return res;
        }

        case DataType.BYTEARRAY: {
            DataByteArray dba;
            Result res = in.getNextDataByteArray();
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                // res.result = new
                // String(((DataByteArray)res.result).toString());
                if (castNotNeeded) {
                    // we examined the data once before and
                    // determined that the input is the same
                    // type as the type we are casting to
                    // so just send the input out as output
                    return res;
                }
                try {
                    dba = (DataByteArray) res.result;
                } catch (ClassCastException e) {
                    // check if the type of res.result is
                    // same as the type we are trying to cast to
                    if (DataType.findType(res.result) == castToType) {
                        // remember this for future calls
                        castNotNeeded = true;
                        // just return the output
                        return res;
                    } else {
                        // the input is a differen type
                        // rethrow the exception
                        int errCode = 1081;
                        String msg = "Cannot cast to map. Expected bytearray but received: " + DataType.findTypeName(res.result);
                        throw new ExecException(msg, errCode, PigException.INPUT, e);
                    }

                }
                try {
                    if (null != caster) {
                        res.result = caster.bytesToMap(dba.get(), fieldSchema);
                    } else {
                        int errCode = 1075;
                        String msg = unknownByteArrayErrorMessage + "map.";
                        throw new ExecException(msg, errCode, PigException.INPUT);
                    }
                } catch (ExecException ee) {
                    throw ee;
                } catch (IOException e) {
                    log.error("Error while casting from ByteArray to Map");
                }
            }
            return res;
        }

        case DataType.TUPLE:
        case DataType.BAG:
        case DataType.INTEGER:
        case DataType.DOUBLE:
        case DataType.LONG:
        case DataType.DATETIME:
        case DataType.FLOAT:
        case DataType.CHARARRAY:
        case DataType.BOOLEAN:
        case DataType.BIGINTEGER:
        case DataType.BIGDECIMAL:
            return error();

        }

        return error();
    }

    @Override
    public Result getNextDataByteArray() throws ExecException {
        return error();
    }

    private void readObject(ObjectInputStream is) throws IOException,
            ClassNotFoundException {
        is.defaultReadObject();
        instantiateFunc();
    }

    @Override
    public POCast clone() throws CloneNotSupportedException {
        POCast clone = new POCast(new OperatorKey(mKey.scope, NodeIdGenerator
                .getGenerator().getNextNodeId(mKey.scope)));
        clone.cloneHelper(this);
        clone.funcSpec = funcSpec;
        clone.fieldSchema = fieldSchema;
        try {
            clone.instantiateFunc();
        } catch (IOException e) {
            CloneNotSupportedException cnse = new CloneNotSupportedException();
            cnse.initCause(e);
            throw cnse;
        }
        return clone;
    }

    /**
     * Get child expression of this expression
     */
    @Override
    public List<ExpressionOperator> getChildExpressions() {
        if (child == null) {
            child = new ArrayList<ExpressionOperator>();
            if (inputs.get(0) instanceof ExpressionOperator) {
                child.add( (ExpressionOperator)inputs.get(0));
            }
        }

        return child;
    }

    public void setFieldSchema(ResourceFieldSchema s) {
        fieldSchema = s;
    }

    public FuncSpec getFuncSpec() {
        return funcSpec;
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
      return (Tuple) out;
    }
}
