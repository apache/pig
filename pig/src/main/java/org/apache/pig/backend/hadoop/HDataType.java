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
package org.apache.pig.backend.hadoop;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableBag;
import org.apache.pig.impl.io.NullableBigDecimalWritable;
import org.apache.pig.impl.io.NullableBigIntegerWritable;
import org.apache.pig.impl.io.NullableBooleanWritable;
import org.apache.pig.impl.io.NullableBytesWritable;
import org.apache.pig.impl.io.NullableDateTimeWritable;
import org.apache.pig.impl.io.NullableDoubleWritable;
import org.apache.pig.impl.io.NullableFloatWritable;
import org.apache.pig.impl.io.NullableIntWritable;
import org.apache.pig.impl.io.NullableLongWritable;
import org.apache.pig.impl.io.NullableText;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.joda.time.DateTime;

/**
 * A class of helper methods for converting from pig data types to hadoop
 * data types, and vice versa.
 */
public class HDataType {
    static NullableBooleanWritable boolWrit = new NullableBooleanWritable();
    static NullableBytesWritable bytesWrit = new NullableBytesWritable();
    static NullableText stringWrit = new NullableText();
    static NullableFloatWritable floatWrit = new NullableFloatWritable();
    static NullableDoubleWritable doubleWrit = new NullableDoubleWritable();
    static NullableIntWritable intWrit = new NullableIntWritable();
    static NullableLongWritable longWrit = new NullableLongWritable();
    static NullableBigIntegerWritable bigIntWrit = new NullableBigIntegerWritable();
    static NullableBigDecimalWritable bigDecWrit = new NullableBigDecimalWritable();
    static NullableDateTimeWritable dtWrit = new NullableDateTimeWritable();
    static NullableBag defDB = new NullableBag();
    static NullableTuple defTup = new NullableTuple();
    static Map<Byte, String> typeToName = null;

    private static final HashMap<String, Byte> classToTypeMap = new HashMap<String, Byte>();
    static {
        classToTypeMap.put("org.apache.pig.impl.io.NullableBag", DataType.BAG);
        classToTypeMap.put("org.apache.pig.impl.io.NullableBigDecimalWritable", DataType.BIGDECIMAL);
        classToTypeMap.put("org.apache.pig.impl.io.NullableBigIntegerWritable", DataType.BIGINTEGER);
        classToTypeMap.put("org.apache.pig.impl.io.NullableBooleanWritable", DataType.BOOLEAN);
        classToTypeMap.put("org.apache.pig.impl.io.NullableBytesWritable", DataType.BYTEARRAY);
        classToTypeMap.put("org.apache.pig.impl.io.NullableDateTimeWritable", DataType.DATETIME);
        classToTypeMap.put("org.apache.pig.impl.io.NullableDoubleWritable", DataType.DOUBLE);
        classToTypeMap.put("org.apache.pig.impl.io.NullableFloatWritable", DataType.FLOAT);
        classToTypeMap.put("org.apache.pig.impl.io.NullableIntWritable", DataType.INTEGER);
        classToTypeMap.put("org.apache.pig.impl.io.NullableLongWritable", DataType.LONG);
        classToTypeMap.put("org.apache.pig.impl.io.NullableText", DataType.CHARARRAY);
        classToTypeMap.put("org.apache.pig.impl.io.NullableTuple", DataType.TUPLE);
    }

    public static PigNullableWritable getWritableComparable(String className) throws ExecException {
        if (classToTypeMap.containsKey(className)) {
            return getWritableComparableTypes(null, classToTypeMap.get(className));
        } else {
            throw new ExecException("Unable to map " + className + " to known types." + Arrays.toString(classToTypeMap.keySet().toArray()));
        }
    }

    public static PigNullableWritable getWritableComparableTypes(Object o, byte keyType) throws ExecException{

        byte newKeyType = keyType;
        if (o==null)
            newKeyType = DataType.NULL;

        switch (newKeyType) {
        case DataType.BAG:
            return new NullableBag((DataBag)o);

        case DataType.BOOLEAN:
            return new NullableBooleanWritable((Boolean)o);

        case DataType.BYTEARRAY:
            return new NullableBytesWritable(o);

        case DataType.CHARARRAY:
            return new NullableText((String)o);

        case DataType.DOUBLE:
            return new NullableDoubleWritable((Double)o);

        case DataType.FLOAT:
            return new NullableFloatWritable((Float)o);

        case DataType.INTEGER:
            return new NullableIntWritable((Integer)o);

        case DataType.LONG:
            return new NullableLongWritable((Long)o);

        case DataType.BIGINTEGER:
            return new NullableBigIntegerWritable((BigInteger)o);

        case DataType.BIGDECIMAL:
            return new NullableBigDecimalWritable((BigDecimal)o);

        case DataType.DATETIME:
            return new NullableDateTimeWritable((DateTime)o);

        case DataType.TUPLE:
            return new NullableTuple((Tuple)o);

        case DataType.MAP: {
            int errCode = 1068;
            String msg = "Using Map as key not supported.";
            throw new ExecException(msg, errCode, PigException.INPUT);
        }

        case DataType.NULL:
            switch (keyType) {
            case DataType.BAG:
                NullableBag nbag = new NullableBag();
                nbag.setNull(true);
                return nbag;
            case DataType.BOOLEAN:
                NullableBooleanWritable nboolWrit = new NullableBooleanWritable();
                nboolWrit.setNull(true);
                return nboolWrit;
            case DataType.BYTEARRAY:
                NullableBytesWritable nBytesWrit = new NullableBytesWritable();
                nBytesWrit.setNull(true);
                return nBytesWrit;
            case DataType.CHARARRAY:
                NullableText nStringWrit = new NullableText();
                nStringWrit.setNull(true);
                return nStringWrit;
            case DataType.DOUBLE:
                NullableDoubleWritable nDoubleWrit = new NullableDoubleWritable();
                nDoubleWrit.setNull(true);
                return nDoubleWrit;
            case DataType.FLOAT:
                NullableFloatWritable nFloatWrit = new NullableFloatWritable();
                nFloatWrit.setNull(true);
                return nFloatWrit;
            case DataType.INTEGER:
                NullableIntWritable nIntWrit = new NullableIntWritable();
                nIntWrit.setNull(true);
                return nIntWrit;
            case DataType.BIGINTEGER:
                NullableBigIntegerWritable nBigIntWrit = new NullableBigIntegerWritable();
                nBigIntWrit.setNull(true);
                return nBigIntWrit;
            case DataType.BIGDECIMAL:
                NullableBigDecimalWritable nBigDecWrit = new NullableBigDecimalWritable();
                nBigDecWrit.setNull(true);
                return nBigDecWrit;
            case DataType.LONG:
                NullableLongWritable nLongWrit = new NullableLongWritable();
                nLongWrit.setNull(true);
                return nLongWrit;
            case DataType.DATETIME:
                NullableDateTimeWritable nDateTimeWrit = new NullableDateTimeWritable();
                nDateTimeWrit.setNull(true);
                return nDateTimeWrit;
            case DataType.TUPLE:
                NullableTuple ntuple = new NullableTuple();
                ntuple.setNull(true);
                return ntuple;
            case DataType.MAP: {
                int errCode = 1068;
                String msg = "Using Map as key not supported.";
                throw new ExecException(msg, errCode, PigException.INPUT);
            }

            }
            break;
        default:
            if (typeToName == null) typeToName = DataType.genTypeToNameMap();
            int errCode = 2044;
            String msg = "The type "
                + typeToName.get(keyType)
                + " cannot be collected as a Key type";
            throw new ExecException(msg, errCode, PigException.BUG);

        }
        // should never come here
        return null;
    }

    public static PigNullableWritable getWritableComparableTypes(byte type) throws ExecException{
        PigNullableWritable wcKey = null;
         switch (type) {
        case DataType.BAG:
            wcKey = defDB;
            break;
        case DataType.BOOLEAN:
            wcKey = boolWrit;
            break;
        case DataType.BYTEARRAY:
            wcKey = bytesWrit;
            break;
        case DataType.CHARARRAY:
            wcKey = stringWrit;
            break;
        case DataType.DOUBLE:
            wcKey = doubleWrit;
            break;
        case DataType.FLOAT:
            wcKey = floatWrit;
            break;
        case DataType.INTEGER:
            wcKey = intWrit;
            break;
        case DataType.LONG:
            wcKey = longWrit;
            break;
        case DataType.BIGINTEGER:
            wcKey = bigIntWrit;
            break;
        case DataType.BIGDECIMAL:
            wcKey = bigDecWrit;
            break;
        case DataType.DATETIME:
            wcKey = dtWrit;
            break;
        case DataType.TUPLE:
            wcKey = defTup;
            break;
        case DataType.MAP: {
            int errCode = 1068;
            String msg = "Using Map as key not supported.";
            throw new ExecException(msg, errCode, PigException.INPUT);
        }
        default:
            if (typeToName == null) typeToName = DataType.genTypeToNameMap();
            int errCode = 2044;
            String msg = "The type "
                + typeToName.get(type)
                + " cannot be collected as a Key type";
            throw new ExecException(msg, errCode, PigException.BUG);
        }
        return wcKey;
    }

    public static byte findTypeFromNullableWritable(PigNullableWritable o) throws ExecException {
        if (o instanceof NullableBooleanWritable)
            return DataType.BOOLEAN;
        else if (o instanceof NullableBytesWritable)
            return DataType.BYTEARRAY;
        else if (o instanceof NullableText)
            return DataType.CHARARRAY;
        else if (o instanceof NullableFloatWritable)
            return DataType.FLOAT;
        else if (o instanceof NullableDoubleWritable)
            return DataType.DOUBLE;
        else if (o instanceof NullableIntWritable)
            return DataType.INTEGER;
        else if (o instanceof NullableLongWritable)
            return DataType.LONG;
        else if (o instanceof NullableBigIntegerWritable)
            return DataType.BIGINTEGER;
        else if (o instanceof NullableBigDecimalWritable)
            return DataType.BIGDECIMAL;
        else if (o instanceof NullableDateTimeWritable)
            return DataType.DATETIME;
        else if (o instanceof NullableBag)
            return DataType.BAG;
        else if (o instanceof NullableTuple)
            return DataType.TUPLE;
        else {
            int errCode = 2044;
            String msg = "Cannot find Pig type for " + o.getClass().getName();
            throw new ExecException(msg, errCode, PigException.BUG);
        }
    }
}
