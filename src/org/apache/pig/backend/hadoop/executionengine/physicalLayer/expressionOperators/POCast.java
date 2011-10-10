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

/**
 * This is just a cast that converts DataByteArray into either String or
 * Integer. Just added it for testing the POUnion. Need the full operator
 * implementation.
 */
public class POCast extends ExpressionOperator {
    private final static Log log = LogFactory.getLog(POCast.class);
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
    public Result getNext(Boolean b) throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte resultType = in.getResultType();
        switch (resultType) {
        case DataType.BAG: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        case DataType.TUPLE: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        case DataType.MAP: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }
        
        case DataType.BYTEARRAY: {
            DataByteArray dba = null;
            Result res = in.getNext(dba);
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
                        String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to boolean.";
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
            String str = null;
            Result res = in.getNext(str);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = CastUtils.stringToBoolean((String)res.result);
            }
            return res;
        }
        
        case DataType.BOOLEAN: {
            Result res = in.getNext(b);
            return res;
        }
        
        case DataType.INTEGER: {
            Integer i = null;
            Result res = in.getNext(i);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Boolean.valueOf(((Integer) res.result).intValue() != 0);
            }
            return res;
        }

        case DataType.LONG: {
            Long l = null;
            Result res = in.getNext(l);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Boolean.valueOf(((Long) res.result).longValue() != 0L);
            }
            return res;
        }

        case DataType.FLOAT: {
            Float f = null;
            Result res = in.getNext(f);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Boolean.valueOf(((Float) res.result).floatValue() != 0.0F);
            }
            return res;
        }

        case DataType.DOUBLE: {
            Double d = null;
            Result res = in.getNext(d);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Boolean.valueOf(((Double) res.result).doubleValue() != 0.0);
            }
            return res;
        }
        }
        
        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
    }

    @Override
    public Result getNext(Integer i) throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte resultType = in.getResultType();
        switch (resultType) {
        case DataType.BAG: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        case DataType.TUPLE: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        case DataType.BYTEARRAY: {
            DataByteArray dba = null;
            Result res = in.getNext(dba);
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
                        String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to int.";
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

        case DataType.MAP: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        case DataType.BOOLEAN: {
            Boolean b = null;
            Result res = in.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                if (((Boolean) res.result) == true)
                    res.result = Integer.valueOf(1);
                else
                    res.result = Integer.valueOf(0);
            }
            return res;
        }
        case DataType.INTEGER: {

            Result res = in.getNext(i);
            return res;
        }

        case DataType.DOUBLE: {
            Double d = null;
            Result res = in.getNext(d);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                // res.result = DataType.toInteger(res.result);
                res.result = Integer.valueOf(((Double) res.result).intValue());
            }
            return res;
        }

        case DataType.LONG: {
            Long l = null;
            Result res = in.getNext(l);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Integer.valueOf(((Long) res.result).intValue());
            }
            return res;
        }

        case DataType.FLOAT: {
            Float f = null;
            Result res = in.getNext(f);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Integer.valueOf(((Float) res.result).intValue());
            }
            return res;
        }

        case DataType.CHARARRAY: {
            String str = null;
            Result res = in.getNext(str);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = CastUtils.stringToInteger((String)res.result);
            }
            return res;
        }

        }

        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
    }

    @Override
    public Result getNext(Long l) throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte resultType = in.getResultType();
        switch (resultType) {
        case DataType.BAG: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        case DataType.TUPLE: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        case DataType.MAP: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        case DataType.BYTEARRAY: {
            DataByteArray dba = null;
            Result res = in.getNext(dba);
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
                        String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to long.";
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
            Boolean b = null;
            Result res = in.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                if (((Boolean) res.result) == true)
                    res.result = Long.valueOf(1);
                else
                    res.result = Long.valueOf(0);
            }
            return res;
        }
        case DataType.INTEGER: {
            Integer dummyI = null;
            Result res = in.getNext(dummyI);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Long.valueOf(((Integer) res.result).longValue());
            }
            return res;
        }

        case DataType.DOUBLE: {
            Double d = null;
            Result res = in.getNext(d);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                // res.result = DataType.toInteger(res.result);
                res.result = Long.valueOf(((Double) res.result).longValue());
            }
            return res;
        }

        case DataType.LONG: {

            Result res = in.getNext(l);

            return res;
        }

        case DataType.FLOAT: {
            Float f = null;
            Result res = in.getNext(f);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = Long.valueOf(((Float) res.result).longValue());
            }
            return res;
        }

        case DataType.CHARARRAY: {
            String str = null;
            Result res = in.getNext(str);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = CastUtils.stringToLong((String)res.result);
            }
            return res;
        }

        }

        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
    }

    @Override
    public Result getNext(Double d) throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte resultType = in.getResultType();
        switch (resultType) {
        case DataType.BAG: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        case DataType.TUPLE: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        case DataType.MAP: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        case DataType.BYTEARRAY: {
            DataByteArray dba = null;
            Result res = in.getNext(dba);
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
                        String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to double.";
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
            Boolean b = null;
            Result res = in.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                if (((Boolean) res.result) == true)
                    res.result = new Double(1);
                else
                    res.result = new Double(0);
            }
            return res;
        }
        case DataType.INTEGER: {
            Integer dummyI = null;
            Result res = in.getNext(dummyI);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new Double(((Integer) res.result).doubleValue());
            }
            return res;
        }

        case DataType.DOUBLE: {

            Result res = in.getNext(d);

            return res;
        }

        case DataType.LONG: {
            Long l = null;
            Result res = in.getNext(l);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new Double(((Long) res.result).doubleValue());
            }
            return res;
        }

        case DataType.FLOAT: {
            Float f = null;
            Result res = in.getNext(f);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new Double(((Float) res.result).doubleValue());
            }
            return res;
        }

        case DataType.CHARARRAY: {
            String str = null;
            Result res = in.getNext(str);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = CastUtils.stringToDouble((String)res.result);
            }
            return res;
        }

        }

        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
    }

    @Override
    public Result getNext(Float f) throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte resultType = in.getResultType();
        switch (resultType) {
        case DataType.BAG: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        case DataType.TUPLE: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        case DataType.MAP: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        case DataType.BYTEARRAY: {
            DataByteArray dba = null;
            Result res = in.getNext(dba);
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
                        String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to float.";
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
            Boolean b = null;
            Result res = in.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                if (((Boolean) res.result) == true)
                    res.result = new Float(1);
                else
                    res.result = new Float(0);
            }
            return res;
        }
        case DataType.INTEGER: {
            Integer dummyI = null;
            Result res = in.getNext(dummyI);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new Float(((Integer) res.result).floatValue());
            }
            return res;
        }

        case DataType.DOUBLE: {
            Double d = null;
            Result res = in.getNext(d);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                // res.result = DataType.toInteger(res.result);
                res.result = new Float(((Double) res.result).floatValue());
            }
            return res;
        }

        case DataType.LONG: {

            Long l = null;
            Result res = in.getNext(l);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new Float(((Long) res.result).floatValue());
            }
            return res;
        }

        case DataType.FLOAT: {

            Result res = in.getNext(f);

            return res;
        }

        case DataType.CHARARRAY: {
            String str = null;
            Result res = in.getNext(str);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = CastUtils.stringToFloat((String)res.result);
            }
            return res;
        }

        }

        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
    }

    @Override
    public Result getNext(String str) throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte resultType = in.getResultType();
        switch (resultType) {
        case DataType.BAG: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        case DataType.TUPLE: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        case DataType.MAP: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        case DataType.BYTEARRAY: {
            DataByteArray dba = null;
            Result res = in.getNext(dba);
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
                        String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to string.";
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
            Boolean b = null;
            Result res = in.getNext(b);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                if (((Boolean) res.result) == true)
                    //res.result = "1";
                    res.result = Boolean.TRUE.toString();
                else
                    //res.result = "0";
                    res.result = Boolean.FALSE.toString();
            }
            return res;
        }
        case DataType.INTEGER: {
            Integer dummyI = null;
            Result res = in.getNext(dummyI);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = ((Integer) res.result).toString();
            }
            return res;
        }

        case DataType.DOUBLE: {
            Double d = null;
            Result res = in.getNext(d);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                // res.result = DataType.toInteger(res.result);
                res.result = ((Double) res.result).toString();
            }
            return res;
        }

        case DataType.LONG: {

            Long l = null;
            Result res = in.getNext(l);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = ((Long) res.result).toString();
            }
            return res;
        }

        case DataType.FLOAT: {
            Float f = null;
            Result res = in.getNext(f);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = ((Float) res.result).toString();
            }
            return res;
        }

        case DataType.CHARARRAY: {
            Result res = in.getNext(str);

            return res;
        }

        }

        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
    }

    @Override
    public Result getNext(Tuple t) throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte castToType = DataType.TUPLE;
        Byte resultType = in.getResultType();
        switch (resultType) {

        case DataType.TUPLE: {
            Result res = in.getNext(t);
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
            DataByteArray dba = null;
            Result res = in.getNext(dba);
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
                        String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to tuple.";
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

        case DataType.BOOLEAN: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        }

        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
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
                    String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to bag.";
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
                    String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to tuple.";
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
                    try {
                        result = caster.bytesToMap(((DataByteArray)obj).get(), fs);
                    }  catch(AbstractMethodError e) {
                        // this is for backward compatibility wherein some old LoadCaster
                        // which does not implement bytesToMap(byte[] b, ResourceFieldSchema fieldSchema)
                        // In this case, we only cast bytes to map, but leave the value as bytearray
                        result = caster.bytesToMap(((DataByteArray)obj).get());
                    }
                } else {
                    int errCode = 1075;
                    String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to tuple.";
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
                    String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to int.";
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
                    String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to int.";
                    throw new ExecException(msg, errCode, PigException.INPUT);
                }
                break;
            case DataType.BOOLEAN:
                if (((Boolean) obj) == true)
                    result = Integer.valueOf(1);
                else
                    result = Integer.valueOf(0);
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
            case DataType.CHARARRAY:
                result = CastUtils.stringToInteger((String)obj);
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
                    String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to double.";
                    throw new ExecException(msg, errCode, PigException.INPUT);
                }
                break;
            case DataType.BOOLEAN:
                if (((Boolean) obj) == true)
                    result = new Double(1);
                else
                    result = new Double(1);
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
            case DataType.CHARARRAY:
                result = CastUtils.stringToDouble((String)obj);
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
                    String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to long.";
                    throw new ExecException(msg, errCode, PigException.INPUT);
                }
                break;
            case DataType.BOOLEAN:
                if (((Boolean) obj) == true)
                    result = Long.valueOf(1);
                else
                    result = Long.valueOf(0);
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
            case DataType.CHARARRAY:
                result = CastUtils.stringToLong((String)obj);
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
                    String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to float.";
                    throw new ExecException(msg, errCode, PigException.INPUT);
                }
                break;
            case DataType.BOOLEAN:
                if (((Boolean) obj) == true)
                    result = new Float(1);
                else
                    result = new Float(0);
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
            case DataType.CHARARRAY:
                result = CastUtils.stringToFloat((String)obj);
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
                    String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to float.";
                    throw new ExecException(msg, errCode, PigException.INPUT);
                }
                break;
            case DataType.BOOLEAN:
                if (((Boolean) obj) == true)
                    //result = "1";
                    result = Boolean.TRUE.toString();
                else
                    //result = "0";
                    result = Boolean.FALSE.toString();
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
            case DataType.CHARARRAY:
                result = obj;
                break;
            default:
                throw new ExecException("Cannot convert "+ obj + " to " + fs, 1120, PigException.INPUT);
            }
            break;
        default:
            throw new ExecException("Don't know how to convert "+ obj + " to " + fs, 1120, PigException.INPUT);
        }
        return result;
    }
    
    @Override
    public Result getNext(DataBag bag) throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte castToType = DataType.BAG;
        Byte resultType = in.getResultType();
        switch (resultType) {

        case DataType.BAG: {
            res = in.getNext(bag);
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
            DataByteArray dba = null;
            Result res = in.getNext(dba);
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
                        String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to bag.";
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

        case DataType.CHARARRAY:

        case DataType.BOOLEAN: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        }

        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
    }

    @SuppressWarnings("deprecation")
    @Override
    public Result getNext(Map m) throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte castToType = DataType.MAP;
        Byte resultType = in.getResultType();
        switch (resultType) {

        case DataType.MAP: {
            Result res = in.getNext(m);
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
            DataByteArray dba = null;
            Result res = in.getNext(dba);
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
                        try {
                            res.result = caster.bytesToMap(dba.get(), fieldSchema);
                        } catch(AbstractMethodError e) {
                            // this is for backward compatibility wherein some old LoadCaster
                            // which does not implement bytesToMap(byte[] b, ResourceFieldSchema fieldSchema)
                            // In this case, we only cast bytes to map, but leave the value as bytearray
                            res.result = caster.bytesToMap(dba.get());
                        }
                    } else {
                        int errCode = 1075;
                        String msg = "Received a bytearray from the UDF. Cannot determine how to convert the bytearray to map.";
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

        case DataType.FLOAT:

        case DataType.CHARARRAY:

        case DataType.BOOLEAN: {
            Result res = new Result();
            res.returnStatus = POStatus.STATUS_ERR;
            return res;
        }

        }

        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
    }

    @Override
    public Result getNext(DataByteArray dba) throws ExecException {
        Result res = new Result();
        res.returnStatus = POStatus.STATUS_ERR;
        return res;
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
