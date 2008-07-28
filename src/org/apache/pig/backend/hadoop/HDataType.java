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

import java.util.Map;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.DoubleWritable;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * A class of helper methods for converting from pig data types to hadoop
 * data types, and vice versa.
 */
public class HDataType {
    static BooleanWritable boolWrit = new BooleanWritable();
    static BytesWritable bytesWrit = new BytesWritable();
    static Text stringWrit = new Text();
    static FloatWritable floatWrit = new FloatWritable();
    static DoubleWritable doubleWrit = new DoubleWritable();
    static IntWritable intWrit = new IntWritable();
    static LongWritable longWrit = new LongWritable();
    static DataBag defDB = BagFactory.getInstance().newDefaultBag();
    static Tuple defTup = TupleFactory.getInstance().newTuple();
    static Map<Byte, String> typeToName = null;

    public static WritableComparable getWritableComparableTypes(Object o) throws ExecException{
        WritableComparable wcKey = null;
        if (typeToName == null) typeToName = DataType.genTypeToNameMap();
        byte type = DataType.findType(o);
        switch (type) {
        case DataType.BAG:
            wcKey = (DataBag) o;
            break;
        case DataType.BOOLEAN:
            boolWrit.set((Boolean)o);
            wcKey = boolWrit;
            break;
        case DataType.BYTEARRAY:
            byte[] dbaBytes = ((DataByteArray) o).get();
            bytesWrit.set(dbaBytes,0,dbaBytes.length);
            wcKey = bytesWrit;
            break;
        case DataType.CHARARRAY:
            stringWrit.set((String) o);
            wcKey = stringWrit;
            break;
        case DataType.DOUBLE:
            doubleWrit.set((Double) o);
            wcKey = doubleWrit;
            break;
        case DataType.FLOAT:
            floatWrit.set((Float) o);
            wcKey = floatWrit;
            break;
        case DataType.INTEGER:
            intWrit.set((Integer) o);
            wcKey = intWrit;
            break;
        case DataType.LONG:
            longWrit.set((Long) o);
            wcKey = longWrit;
            break;
        case DataType.TUPLE:
            wcKey = (Tuple) o;
            break;
//        case DataType.MAP:
            // Hmm, This is problematic
            // Need a deep clone to convert a Map into
            // MapWritable
            // wcKey = new MapWritable();
//            break;
        default:
            throw new ExecException("The type "
                    + typeToName.get(type)
                    + " cannot be collected as a Key type");
        }
        return wcKey;
    }
    
    public static WritableComparable getWritableComparableTypes(byte type) throws ExecException{
        WritableComparable wcKey = null;
        if (typeToName == null) typeToName = DataType.genTypeToNameMap();
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
        case DataType.TUPLE:
            wcKey = defTup;
            break;
//        case DataType.MAP:
            // Hmm, This is problematic
            // Need a deep clone to convert a Map into
            // MapWritable
            // wcKey = new MapWritable();
//            break;
        default:
            throw new ExecException("The type "
                    + typeToName.get(type)
                    + " cannot be collected as a Key type");
        }
        return wcKey;
    }
    
    public static Object convertToPigType(WritableComparable key) {
        if ((key instanceof DataBag) || (key instanceof Tuple))
            return key;
        if (key instanceof BooleanWritable)
            return ((BooleanWritable) key).get();
        if (key instanceof BytesWritable) {
            return new DataByteArray(((BytesWritable) key).get(), 0,
                ((BytesWritable)key).getSize());
        }
        if (key instanceof Text)
            return ((Text) key).toString();
        if (key instanceof FloatWritable)
            return ((FloatWritable) key).get();
        if (key instanceof DoubleWritable)
            return ((DoubleWritable) key).get();
        if (key instanceof IntWritable)
            return ((IntWritable) key).get();
        if (key instanceof LongWritable)
            return ((LongWritable) key).get();
        return null;
    }
}
