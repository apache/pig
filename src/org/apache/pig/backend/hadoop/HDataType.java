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
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableBooleanWritable;
import org.apache.pig.impl.io.NullableBytesWritable;
import org.apache.pig.impl.io.NullableDoubleWritable;
import org.apache.pig.impl.io.NullableFloatWritable;
import org.apache.pig.impl.io.NullableIntWritable;
import org.apache.pig.impl.io.NullableLongWritable;
import org.apache.pig.impl.io.NullableText;

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
    static DataBag defDB = BagFactory.getInstance().newDefaultBag();
    static Tuple defTup = TupleFactory.getInstance().newTuple();
    static Map<Byte, String> typeToName = null;

    public static WritableComparable getWritableComparableTypes(Object o, byte keyType) throws ExecException{
        byte type = DataType.findType(o);
        switch (type) {
        case DataType.BAG:
            return (DataBag)o;

        case DataType.BOOLEAN:
            return new NullableBooleanWritable((Boolean)o);

        case DataType.BYTEARRAY:
            return new NullableBytesWritable(((DataByteArray)o).get());
            
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
          
        case DataType.TUPLE:
            return (Tuple) o;
         
//        case DataType.MAP:
            // Hmm, This is problematic
            // Need a deep clone to convert a Map into
            // MapWritable
            // wcKey = new MapWritable();
//            break;
        case DataType.NULL:
            switch (keyType) {
            case DataType.BAG:
                //TODO: create a null data bag
                break;
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
            case DataType.LONG:
                NullableLongWritable nLongWrit = new NullableLongWritable();
                nLongWrit.setNull(true);
                return nLongWrit;
            case DataType.TUPLE:
                Tuple t = DefaultTupleFactory.getInstance().newTuple();
                t.setNull(true);
                return t;
//            case DataType.MAP:
                // Hmm, This is problematic
                // Need a deep clone to convert a Map into
                // MapWritable
                // wcKey = new MapWritable();
//                break;
            }
            break;
        default:
            if (typeToName == null) typeToName = DataType.genTypeToNameMap();
            throw new ExecException("The type "
                    + typeToName.get(type)
                    + " cannot be collected as a Key type");
        }
        // should never come here
        return null;
    }
    
    public static WritableComparable getWritableComparableTypes(byte type) throws ExecException{
        WritableComparable wcKey = null;
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
            if (typeToName == null) typeToName = DataType.genTypeToNameMap();
            throw new ExecException("The type "
                    + typeToName.get(type)
                    + " cannot be collected as a Key type");
        }
        return wcKey;
    }
    
    public static Object convertToPigType(WritableComparable key) {
        if ((key instanceof DataBag) )
            return key;
        if(key instanceof Tuple)
           return ((Tuple)key).isNull() ? null : key;     
        if (key instanceof NullableBooleanWritable) {
            NullableBooleanWritable bWrit = (NullableBooleanWritable)key;   
            return bWrit.isNull() ? null :bWrit.get();
        }
        if (key instanceof NullableBytesWritable) {
            NullableBytesWritable byWrit = (NullableBytesWritable) key; 
            return byWrit.isNull() ? null : new DataByteArray(byWrit.get(), 0, byWrit.getSize());
        }
        if (key instanceof NullableText) {
            NullableText tWrit =  (NullableText) key;
            return tWrit.isNull() ? null :tWrit.toString();
        }
        if (key instanceof NullableFloatWritable) {
            NullableFloatWritable fWrit = (NullableFloatWritable) key;
            return fWrit == null ? null : fWrit.get();
        }
        if (key instanceof NullableDoubleWritable) {
            NullableDoubleWritable dWrit = (NullableDoubleWritable) key;
            return dWrit.isNull() ? null : dWrit.get();
        }
        if (key instanceof NullableIntWritable) {
            NullableIntWritable iWrit = (NullableIntWritable) key;
            return iWrit.isNull() ? null : iWrit.get();
        }
        if (key instanceof NullableLongWritable) {
            NullableLongWritable lWrit = (NullableLongWritable) key;
            return lWrit.isNull() ? null : lWrit.get();
        }
        return null;
    }
}
