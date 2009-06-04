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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.VisitorException;

/**
 * This is just a cast that converts DataByteArray into either String or
 * Integer. Just added it for testing the POUnion. Need the full operator
 * implementation.
 */
public class POCast extends ExpressionOperator {
    private FuncSpec loadFSpec = null;
    transient private LoadFunc load;
    private Log log = LogFactory.getLog(getClass());
    private boolean castNotNeeded = false;
    private Byte realType = null;

    private static final long serialVersionUID = 1L;

    public POCast(OperatorKey k) {
        super(k);
        // TODO Auto-generated constructor stub
    }

    public POCast(OperatorKey k, int rp) {
        super(k, rp);
        // TODO Auto-generated constructor stub
    }

    private void instantiateFunc() {
        if (load != null)
            return;
        if (this.loadFSpec != null) {
            this.load = (LoadFunc) PigContext
                    .instantiateFuncFromSpec(this.loadFSpec);
        }
    }

    public void setLoadFSpec(FuncSpec lf) {
        this.loadFSpec = lf;
        instantiateFunc();
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitCast(this);

    }

    @Override
    public String name() {
        return "Cast" + "[" + DataType.findTypeName(resultType) + "]" + " - "
                + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        // TODO Auto-generated method stub
        return false;
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
                    if (null != load) {
                        res.result = load.bytesToInteger(dba.get());
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
                    res.result = new Integer(1);
                else
                    res.result = new Integer(0);
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
                res.result = new Integer(((Double) res.result).intValue());
            }
            return res;
        }

        case DataType.LONG: {
            Long l = null;
            Result res = in.getNext(l);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new Integer(((Long) res.result).intValue());
            }
            return res;
        }

        case DataType.FLOAT: {
            Float f = null;
            Result res = in.getNext(f);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new Integer(((Float) res.result).intValue());
            }
            return res;
        }

        case DataType.CHARARRAY: {
            String str = null;
            Result res = in.getNext(str);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new Integer(Integer.valueOf((String) res.result));
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
                    if (null != load) {
                        res.result = load.bytesToLong(dba.get());
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
                    res.result = new Long(1);
                else
                    res.result = new Long(0);
            }
            return res;
        }
        case DataType.INTEGER: {
            Integer dummyI = null;
            Result res = in.getNext(dummyI);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new Long(((Integer) res.result).longValue());
            }
            return res;
        }

        case DataType.DOUBLE: {
            Double d = null;
            Result res = in.getNext(d);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                // res.result = DataType.toInteger(res.result);
                res.result = new Long(((Double) res.result).longValue());
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
                res.result = new Long(((Float) res.result).longValue());
            }
            return res;
        }

        case DataType.CHARARRAY: {
            String str = null;
            Result res = in.getNext(str);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new Long(Long.valueOf((String) res.result));
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
                    if (null != load) {
                        res.result = load.bytesToDouble(dba.get());
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
                res.result = new Double(Double.valueOf((String) res.result));
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
                    if (null != load) {
                        res.result = load.bytesToFloat(dba.get());
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
                res.result = new Float(Float.valueOf((String) res.result));
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
                    if (null != load) {
                        res.result = load.bytesToCharArray(dba.get());
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
                    res.result = new String("1");
                else
                    res.result = new String("1");
            }
            return res;
        }
        case DataType.INTEGER: {
            Integer dummyI = null;
            Result res = in.getNext(dummyI);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new String(((Integer) res.result).toString());
            }
            return res;
        }

        case DataType.DOUBLE: {
            Double d = null;
            Result res = in.getNext(d);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                // res.result = DataType.toInteger(res.result);
                res.result = new String(((Double) res.result).toString());
            }
            return res;
        }

        case DataType.LONG: {

            Long l = null;
            Result res = in.getNext(l);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new String(((Long) res.result).toString());
            }
            return res;
        }

        case DataType.FLOAT: {
            Float f = null;
            Result res = in.getNext(f);
            if (res.returnStatus == POStatus.STATUS_OK && res.result != null) {
                res.result = new String(((Float) res.result).toString());
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
                    if (null != load) {
                        res.result = load.bytesToTuple(dba.get());
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

    @Override
    public Result getNext(DataBag bag) throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte castToType = DataType.BAG;
        Byte resultType = in.getResultType();
        switch (resultType) {

        case DataType.BAG: {
            Result res = in.getNext(bag);
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
                    if (null != load) {
                        res.result = load.bytesToBag(dba.get());
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

    @Override
    public Result getNext(Map m) throws ExecException {
        PhysicalOperator in = inputs.get(0);
        Byte castToType = DataType.MAP;
        Byte resultType = in.getResultType();
        switch (resultType) {

        case DataType.MAP: {
            Result res = in.getNext(m);
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
                    if (null != load) {
                        res.result = load.bytesToMap(dba.get());
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
        clone.loadFSpec = loadFSpec;
        clone.instantiateFunc();
        return clone;
    }

}
