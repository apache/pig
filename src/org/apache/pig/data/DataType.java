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
package org.apache.pig.data;

import java.lang.Class;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaMergeException;

/**
 * A class of static final values used to encode data type and a number of
 * static helper funcitons for manipulating data objects.  The data type
 * values could be
 * done as an enumeration, but it is done as byte codes instead to save
 * creating objects.
 */
public class DataType {
    // IMPORTANT! This list can be used to record values of data on disk,
    // so do not change the values.  You may strand user data.
    // IMPORTANT! Order matters here, as compare() below uses the order to
    // order unlike datatypes.  Don't change this ordering.
    // Spaced unevenly to leave room for new entries without changing
    // values or creating order issues.  
    public static final byte UNKNOWN   =   0;
    public static final byte NULL      =   1;
    public static final byte BOOLEAN   =   5; // internal use only
    public static final byte BYTE      =   6; // internal use only
    public static final byte INTEGER   =  10;
    public static final byte LONG      =  15;
    public static final byte FLOAT     =  20;
    public static final byte DOUBLE    =  25;
    public static final byte BYTEARRAY =  50;
    public static final byte CHARARRAY =  55;
    public static final byte BIGCHARARRAY =  60; //internal use only; for storing/loading chararray bigger than 64K characters in BinStorage
    public static final byte MAP       = 100;
    public static final byte TUPLE     = 110;
    public static final byte BAG       = 120;
    public static final byte ERROR     =  -1;

    /**
     * Determine the datatype of an object.
     * @param o Object to test.
     * @return byte code of the type, or ERROR if we don't know.
     */
    public static byte findType(Object o) {
        if (o == null) return NULL;

        // Try to put the most common first
        if (o instanceof DataByteArray) return BYTEARRAY;
        else if (o instanceof String) return CHARARRAY;
        else if (o instanceof Tuple) return TUPLE;
        else if (o instanceof DataBag) return BAG;
        else if (o instanceof Integer) return INTEGER;
        else if (o instanceof Long) return LONG;
        else if (o instanceof Map) return MAP;
        else if (o instanceof Float) return FLOAT;
        else if (o instanceof Double) return DOUBLE;
        else if (o instanceof Boolean) return BOOLEAN;
        else if (o instanceof Byte) return BYTE;
        else return ERROR;
    }

    /**
     * Given a Type object determine the data type it represents.  This isn't
     * cheap, as it uses reflection, so use sparingly.
     * @param t Type to examine
     * @return byte code of the type, or ERROR if we don't know.
     */
    public static byte findType(Type t) {
        if (t == null) return NULL;

        // Try to put the most common first
        if (t == DataByteArray.class) return BYTEARRAY;
        else if (t == String.class) return CHARARRAY;
        else if (t == Integer.class) return INTEGER;
        else if (t == Long.class) return LONG;
        else if (t == Float.class) return FLOAT;
        else if (t == Double.class) return DOUBLE;
        else if (t == Boolean.class) return BOOLEAN;
        else if (t == Byte.class) return BYTE;
        else {
            // Might be a tuple or a bag, need to check the interfaces it
            // implements
            if (t instanceof Class) {
                Class c = (Class)t;
                Class[] ioeInterfaces = c.getInterfaces();
                Class[] interfaces = null;
                if(c.isInterface()){
                    interfaces = new Class[ioeInterfaces.length+1];
                    interfaces[0] = c;
                    for (int i = 1; i < interfaces.length; i++) {
                     interfaces[i] = ioeInterfaces[i-1];
                    }
                }  else {
                    interfaces = ioeInterfaces;
                }
                for (int i = 0; i < interfaces.length; i++) {
                    if (interfaces[i].getName().equals("org.apache.pig.data.Tuple")) {
                        return TUPLE;
                    } else if (interfaces[i].getName().equals("org.apache.pig.data.DataBag")) {
                        return BAG;
                    } else if (interfaces[i].getName().equals("java.util.Map")) {
                        return MAP;
                    }
                }
            }
            return ERROR;
        }
    }
    
    public static int numTypes(){
        byte[] types = genAllTypes();
        return types.length;
    }
    public static byte[] genAllTypes(){
        byte[] types = { DataType.BAG, DataType.BIGCHARARRAY, DataType.BOOLEAN, DataType.BYTE, DataType.BYTEARRAY, DataType.CHARARRAY, 
                DataType.DOUBLE, DataType.FLOAT, DataType.INTEGER, DataType.LONG, DataType.MAP, DataType.TUPLE};
        return types;
    }
    
    private static String[] genAllTypeNames(){
        String[] names = { "BAG", "BIGCHARARRAY", "BOOLEAN", "BYTE", "BYTEARRAY", "CHARARRAY", "DOUBLE", "FLOAT", "INTEGER", "LONG", 
                "MAP", "TUPLE" };
        return names;
    }
    
    public static Map<Byte, String> genTypeToNameMap(){
        byte[] types = genAllTypes();
        String[] names = genAllTypeNames();
        Map<Byte,String> ret = new HashMap<Byte, String>();
        for(int i=0;i<types.length;i++){
            ret.put(types[i], names[i]);
        }
        return ret;
    }

    public static Map<String, Byte> genNameToTypeMap(){
        byte[] types = genAllTypes();
        String[] names = genAllTypeNames();
        Map<String, Byte> ret = new HashMap<String, Byte>();
        for(int i=0;i<types.length;i++){
            ret.put(names[i], types[i]);
        }
        return ret;
    }

    /**
     * Get the type name.
     * @param o Object to test.
     * @return type name, as a String.
     */
    public static String findTypeName(Object o) {
        return findTypeName(findType(o));
    }
    
    /**
     * Get the type name from the type byte code
     * @param dt Type byte code
     * @return type name, as a String.
     */
    public static String findTypeName(byte dt) {
        switch (dt) {
        case NULL:      return "NULL";
        case BOOLEAN:   return "boolean";
        case BYTE:      return "byte";
        case INTEGER:   return "int";
        case LONG:      return "long";
        case FLOAT:     return "float";
        case DOUBLE:    return "double";
        case BYTEARRAY: return "bytearray";
        case BIGCHARARRAY: return "bigchararray";
        case CHARARRAY: return "chararray";
        case MAP:       return "map";
        case TUPLE:     return "tuple";
        case BAG:       return "bag";
        default: return "Unknown";
        }
    }

    /**
     * Determine whether the this data type is complex.
     * @param dataType Data type code to test.
     * @return true if dataType is bag, tuple, or map.
     */
    public static boolean isComplex(byte dataType) {
        return ((dataType == BAG) || (dataType == TUPLE) ||
            (dataType == MAP));
    }

    /**
     * Determine whether the object is complex or atomic.
     * @param o Object to determine type of.
     * @return true if dataType is bag, tuple, or map.
     */
    public static boolean isComplex(Object o) {
        return isComplex(findType(o));
    }

    /**
     * Determine whether the this data type is atomic.
     * @param dataType Data type code to test.
     * @return true if dataType is bytearray, bigchararray, chararray, integer, long,
     * float, or boolean.
     */
    public static boolean isAtomic(byte dataType) {
        return ((dataType == BYTEARRAY) ||
                (dataType == CHARARRAY) ||
                (dataType == BIGCHARARRAY) ||
                (dataType == INTEGER) ||
                (dataType == LONG) ||
                (dataType == FLOAT) ||
                (dataType == DOUBLE) ||
                (dataType == BOOLEAN) ||
                (dataType == BYTE));
    }

    /**
     * Determine whether the this data type is atomic.
     * @param o Object to determine type of.
     * @return true if dataType is bytearray, chararray, integer, long,
     * float, or boolean.
     */
    public static boolean isAtomic(Object o) {
        return isAtomic(findType(o));
    }

    /**
     * Determine whether the this object can have a schema.
     * @param o Object to determine if it has a schema
     * @return true if the type can have a valid schema (i.e., bag or tuple)
     */
    public static boolean isSchemaType(Object o) {
        return isSchemaType(findType(o));
    }

    /**
     * Determine whether the this data type can have a schema.
     * @param dataType dataType to determine if it has a schema
     * @return true if the type can have a valid schema (i.e., bag or tuple)
     */
    public static boolean isSchemaType(byte dataType) {
        return ((dataType == BAG) || (dataType == TUPLE)); 
    }

    /**
    /**
     * Compare two objects to each other.  This function is necessary
     * because there's no super class that implements compareTo.  This
     * function provides an (arbitrary) ordering of objects of different
     * types as follows:  NULL &lt; BOOLEAN &lt; BYTE &lt; INTEGER &lt; LONG &lt;
     * FLOAT &lt; DOUBLE * &lt; BYTEARRAY &lt; STRING &lt; MAP &lt;
     * TUPLE &lt; BAG.  No other functions should implement this cross
     * object logic.  They should call this function for it instead.
     * @param o1 First object
     * @param o2 Second object
     * @return -1 if o1 is less, 0 if they are equal, 1 if o2 is less.
     */
    public static int compare(Object o1, Object o2) {
        byte dt1 = findType(o1);
        byte dt2 = findType(o2);

        if (dt1 == dt2) {
            switch (dt1) {
            case NULL:
                return 0;

            case BOOLEAN:
                return ((Boolean)o1).compareTo((Boolean)o2);

            case BYTE:
                return ((Byte)o1).compareTo((Byte)o2);

            case INTEGER:
                return ((Integer)o1).compareTo((Integer)o2);

            case LONG:
                return ((Long)o1).compareTo((Long)o2);

            case FLOAT:
                return ((Float)o1).compareTo((Float)o2);

            case DOUBLE:
                return ((Double)o1).compareTo((Double)o2);

            case BYTEARRAY:
                return ((DataByteArray)o1).compareTo((DataByteArray)o2);

            case CHARARRAY:
                return ((String)o1).compareTo((String)o2);

            case MAP: {
                Map<Object, Object> m1 = (Map<Object, Object>)o1;
                Map<Object, Object> m2 = (Map<Object, Object>)o2;
                int sz1 = m1.size();
                int sz2 = m2.size();
                if (sz1 < sz2) {
                    return -1;
                } else if (sz1 > sz2) { 
                    return 1;
                } else {
                    // This is bad, but we have to sort the keys of the maps in order
                    // to be commutative.
                    TreeMap<Object, Object> tm1 = new TreeMap<Object, Object>(m1);
                    TreeMap<Object, Object> tm2 = new TreeMap<Object, Object>(m2);
                    Iterator<Map.Entry<Object, Object> > i1 =
                        tm1.entrySet().iterator();
                    Iterator<Map.Entry<Object, Object> > i2 =
                        tm2.entrySet().iterator();
                    while (i1.hasNext()) {
                        Map.Entry<Object, Object> entry1 = i1.next();
                        Map.Entry<Object, Object> entry2 = i2.next();
                        int c = compare(entry1.getKey(), entry2.getKey());
                        if (c != 0) {
                            return c;
                        } else {
                            c = compare(entry1.getValue(), entry2.getValue());
                            if (c != 0) {
                                return c;
                            }
                        } 
                    }
                    return 0;
                }
                      }

            case TUPLE:
                return ((Tuple)o1).compareTo((Tuple)o2);

            case BAG:
                return ((DataBag)o1).compareTo((DataBag)o2);

            default:
                throw new RuntimeException("Unkown type " + dt1 +
                    " in compare");
            }
        } else if (dt1 < dt2) {
            return -1;
        } else {
            return 1;
        }
    }

    /**
     * Force a data object to an Integer, if possible.  Any numeric type
     * can be forced to an Integer (though precision may be lost), as well
     * as CharArray, ByteArray, or Boolean.  Complex types cannot be
     * forced to an Integer.  This isn't particularly efficient, so if you
     * already <b>know</b> that the object you have is an Integer you
     * should just cast it.
     * @return The object as a Integer.
     * @throws ExecException if the type can't be forced to an Integer.
     */
    public static Integer toInteger(Object o,byte type) throws ExecException {
        try {
			switch (type) {
			case BOOLEAN:
			    if (((Boolean)o) == true) return new Integer(1);
			    else return new Integer(0);

			case BYTE:
			    return new Integer(((Byte)o).intValue());

			case INTEGER:
			    return (Integer)o;

			case LONG:
			    return new Integer(((Long)o).intValue());

			case FLOAT:
			    return new Integer(((Float)o).intValue());

			case DOUBLE:
			    return new Integer(((Double)o).intValue());

			case BYTEARRAY:
			    return Integer.valueOf(((DataByteArray)o).toString());

			case CHARARRAY:
			    return Integer.valueOf((String)o);

			case NULL:
			    return null;

			case MAP:
			case TUPLE:
			case BAG:
			case UNKNOWN:
			default:
			    int errCode = 1071;
			    String msg = "Cannot convert a " + findTypeName(o) +
			    " to an Integer";
			    throw new ExecException(msg, errCode, PigException.INPUT);
			}
		} catch (ClassCastException cce) {
            throw cce;
        } catch (ExecException ee) {
			throw ee;
		} catch (NumberFormatException nfe) {
			int errCode = 1074;
			String msg = "Problem with formatting. Could not convert " + o + " to Integer.";
			throw new ExecException(msg, errCode, PigException.INPUT, nfe);
		} catch (Exception e) {
			int errCode = 2054;
			String msg = "Internal error. Could not convert " + o + " to Integer.";
			throw new ExecException(msg, errCode, PigException.BUG);
		}
    }
    /**
     * If type of object is not known, use this method, which internally calls
     * toInteger(object,type)
     * 
     * @param o
     * @return Object as Integer.
     * @throws ExecException
     */
    public static Integer toInteger(Object o) throws ExecException {
        return toInteger(o, findType(o));
    }
    /**
     * Force a data object to a Long, if possible.  Any numeric type
     * can be forced to a Long (though precision may be lost), as well
     * as CharArray, ByteArray, or Boolean.  Complex types cannot be
     * forced to a Long.  This isn't particularly efficient, so if you
     * already <b>know</b> that the object you have is a Long you
     * should just cast it.
     * @return The object as a Long.
     * @throws ExecException if the type can't be forced to a Long.
     */
    public static Long toLong(Object o,byte type) throws ExecException {
        try {
			switch (type) {
			case BOOLEAN:
			    if (((Boolean)o) == true) return new Long(1);
			    else return new Long(0);

			case BYTE:
			    return new Long(((Byte)o).longValue());

			case INTEGER:
			    return new Long(((Integer)o).longValue());

			case LONG:
			    return (Long)o;

			case FLOAT:
			    return new Long(((Float)o).longValue());

			case DOUBLE:
			    return new Long(((Double)o).longValue());

			case BYTEARRAY:
			    return Long.valueOf(((DataByteArray)o).toString());

			case CHARARRAY:
			    return Long.valueOf((String)o);

			case NULL:
			    return null;

			case MAP:
			case TUPLE:
			case BAG:
			case UNKNOWN:
			default:
			    int errCode = 1071;
			    String msg = "Cannot convert a " + findTypeName(o) +
			    " to a Long";
			    throw new ExecException(msg, errCode, PigException.INPUT);
			}
		} catch (ClassCastException cce) {
            throw cce;
        } catch (ExecException ee) {
			throw ee;
		} catch (NumberFormatException nfe) {
			int errCode = 1074;
			String msg = "Problem with formatting. Could not convert " + o + " to Long.";
			throw new ExecException(msg, errCode, PigException.INPUT, nfe);
		} catch (Exception e) {
			int errCode = 2054;
			String msg = "Internal error. Could not convert " + o + " to Long.";
			throw new ExecException(msg, errCode, PigException.BUG);
		}

    }
    /**
     * If type of object is not known, use this method which in turns call
     * toLong(object,type) after finding type.
     * 
     * @param o
     * @return Object as Long.
     * @throws ExecException
     */
    public static Long toLong(Object o) throws ExecException {
        return toLong(o, findType(o));
    }

    /**
     * Force a data object to a Float, if possible.  Any numeric type
     * can be forced to a Float (though precision may be lost), as well
     * as CharArray, ByteArray.  Complex types cannot be
     * forced to a Float.  This isn't particularly efficient, so if you
     * already <b>know</b> that the object you have is a Float you
     * should just cast it.
     * @return The object as a Float.
     * @throws ExecException if the type can't be forced to a Float.
     */
    public static Float toFloat(Object o,byte type) throws ExecException {
        try {
			switch (type) {
			case INTEGER:
			    return new Float(((Integer)o).floatValue());

			case LONG:
			    return new Float(((Long)o).floatValue());

			case FLOAT:
			    return (Float)o;

			case DOUBLE:
			    return new Float(((Double)o).floatValue());

			case BYTEARRAY:
			    return Float.valueOf(((DataByteArray)o).toString());

			case CHARARRAY:
			    return Float.valueOf((String)o);

			case NULL:
			    return null;

			case BOOLEAN:
			case BYTE:
			case MAP:
			case TUPLE:
			case BAG:
			case UNKNOWN:
			default:
			    int errCode = 1071;
			    String msg = "Cannot convert a " + findTypeName(o) +
			    " to a Float";
			    throw new ExecException(msg, errCode, PigException.INPUT);
			}
		} catch (ClassCastException cce) {
            throw cce;
        } catch (ExecException ee) {
			throw ee;
		} catch (NumberFormatException nfe) {
			int errCode = 1074;
			String msg = "Problem with formatting. Could not convert " + o + " to Float.";
			throw new ExecException(msg, errCode, PigException.INPUT, nfe);
		} catch (Exception e) {
			int errCode = 2054;
			String msg = "Internal error. Could not convert " + o + " to Float.";
			throw new ExecException(msg, errCode, PigException.BUG);
		}
    }
    /**
     * If type of object is not known, use this method which in turns call
     * toFloat(object,type) after finding type.
     * 
     * @param o
     * @return Object as Float.
     * @throws ExecException
     */
    public static Float toFloat(Object o) throws ExecException {
        return toFloat(o, findType(o));
    }

    /**
     * Force a data object to a Double, if possible.  Any numeric type
     * can be forced to a Double, as well
     * as CharArray, ByteArray.  Complex types cannot be
     * forced to a Double.  This isn't particularly efficient, so if you
     * already <b>know</b> that the object you have is a Double you
     * should just cast it.
     * @return The object as a Double.
     * @throws ExecException if the type can't be forced to a Double.
     */
    public static Double toDouble(Object o,byte type) throws ExecException {
        try {
			switch (type) {
			case INTEGER:
			    return new Double(((Integer)o).doubleValue());

			case LONG:
			    return new Double(((Long)o).doubleValue());

			case FLOAT:
			    return new Double(((Float)o).doubleValue());

			case DOUBLE:
			    return (Double)o;

			case BYTEARRAY:
			    return Double.valueOf(((DataByteArray)o).toString());

			case CHARARRAY:
			    return Double.valueOf((String)o);

			case NULL:
			    return null;

			case BOOLEAN:
			case BYTE:
			case MAP:
			case TUPLE:
			case BAG:
			case UNKNOWN:
			default:
			    int errCode = 1071;
			    String msg = "Cannot convert a " + findTypeName(o) +
			    " to a Double";
			    throw new ExecException(msg, errCode, PigException.INPUT);
			}
		} catch (ClassCastException cce) {
            throw cce;
        } catch (ExecException ee) {
			throw ee;
		} catch (NumberFormatException nfe) {
			int errCode = 1074;
			String msg = "Problem with formatting. Could not convert " + o + " to Double.";
			throw new ExecException(msg, errCode, PigException.INPUT, nfe);
		} catch (Exception e) {
			int errCode = 2054;
			String msg = "Internal error. Could not convert " + o + " to Double.";
			throw new ExecException(msg, errCode, PigException.BUG);
		}
    }
    /**
     * If type of object is not known, use this method which in turns call
     * toLong(object,type) after finding type.
     * 
     * @param o
     * @return Object as Double.
     * @throws ExecException
     */
    public static Double toDouble(Object o) throws ExecException {
        return toDouble(o, findType(o));
    }

    /**
     * Force a data object to a String, if possible.  Any simple (atomic) type
     * can be forced to a String including ByteArray.  Complex types cannot be
     * forced to a String.  This isn't particularly efficient, so if you
     * already <b>know</b> that the object you have is a String you
     * should just cast it.
     * @return The object as a String.
     * @throws ExecException if the type can't be forced to a String.
     */
    public static String toString(Object o,byte type) throws ExecException {
        try {
			switch (type) {
			case INTEGER:
			    return ((Integer)o).toString();

			case LONG:
			    return ((Long)o).toString();

			case FLOAT:
			    return ((Float)o).toString();

			case DOUBLE:
			    return ((Double)o).toString();

			case BYTEARRAY:
			    return ((DataByteArray)o).toString();

			case CHARARRAY:
			    return ((String)o);

			case NULL:
			    return null;

			case BOOLEAN:
			    return ((Boolean)o).toString();

			case BYTE:
			    return ((Byte)o).toString();

			case MAP:
			case TUPLE:
			case BAG:
			case UNKNOWN:
			default:
			    int errCode = 1071;
			    String msg = "Cannot convert a " + findTypeName(o) +
			    " to a String";
			    throw new ExecException(msg, errCode, PigException.INPUT);
			}
		} catch (ClassCastException cce) {
            throw cce;
        } catch (ExecException ee) {
			throw ee;
		} catch (Exception e) {
			int errCode = 2054;
			String msg = "Internal error. Could not convert " + o + " to String.";
			throw new ExecException(msg, errCode, PigException.BUG);
		}
    }
    /**
     * If type of object is not known, use this method which in turns call
     * toString(object,type) after finding type.
     * 
     * @param o
     * @return Object as String.
     * @throws ExecException
     */
    public static String toString(Object o) throws ExecException {
        return toString(o, findType(o));
    }
    /**
     * If this object is a map, return it as a map.
     * This isn't particularly efficient, so if you
     * already <b>know</b> that the object you have is a Map you
     * should just cast it.
     * @return The object as a Double.
     * @throws ExecException if the type can't be forced to a Double.
     */
    public static Map<Object, Object> toMap(Object o) throws ExecException {
        if (o == null) return null;

        if (o instanceof Map) {
            try {
				return (Map<Object, Object>)o;
    		} catch (Exception e) {
    			int errCode = 2054;
    			String msg = "Internal error. Could not convert " + o + " to Map.";
    			throw new ExecException(msg, errCode, PigException.BUG);
    		}
        } else {
            int errCode = 1071;
            String msg = "Cannot convert a " + findTypeName(o) +
            " to a Map";
            throw new ExecException(msg, errCode, PigException.INPUT);
        }
    }

    /**
     * If this object is a tuple, return it as a tuple.
     * This isn't particularly efficient, so if you
     * already <b>know</b> that the object you have is a Tuple you
     * should just cast it.
     * @return The object as a Double.
     * @throws ExecException if the type can't be forced to a Double.
     */
    public static Tuple toTuple(Object o) throws ExecException {
        if (o == null) return null;

        if (o instanceof Tuple) {
            try {
				return (Tuple)o;
    		} catch (Exception e) {
    			int errCode = 2054;
    			String msg = "Internal error. Could not convert " + o + " to Tuple.";
    			throw new ExecException(msg, errCode, PigException.BUG);
    		}
        } else {
            int errCode = 1071;
            String msg = "Cannot convert a " + findTypeName(o) +
            " to a Tuple";
            throw new ExecException(msg, errCode, PigException.INPUT);
        }
    }

    /**
     * If this object is a bag, return it as a bag.
     * This isn't particularly efficient, so if you
     * already <b>know</b> that the object you have is a bag you
     * should just cast it.
     * @return The object as a Double.
     * @throws ExecException if the type can't be forced to a Double.
     */
    public static DataBag toBag(Object o) throws ExecException {
        if (o == null) return null;

        if (o instanceof DataBag) {
            try {
				return (DataBag)o;
    		} catch (Exception e) {
    			int errCode = 2054;
    			String msg = "Internal error. Could not convert " + o + " to Bag.";
    			throw new ExecException(msg, errCode, PigException.BUG);
    		}
        } else {
            int errCode = 1071;
            String msg = "Cannot convert a " + findTypeName(o) +
            " to a DataBag";
            throw new ExecException(msg, errCode, PigException.INPUT);
        }
    }

    /**
     * Purely for debugging
     */
    public static void spillTupleContents(Tuple t, String label) {
        System.out.print("Tuple " + label + " ");
        Iterator<Object> i = t.getAll().iterator();
        for (int j = 0; i.hasNext(); j++) {
            System.out.print(j + ":" + i.next().getClass().getName() + " ");
        }
        System.out.println(t.toString());
    }
    
    public static boolean isNumberType(byte t) {
        switch (t) {
            case INTEGER:   return true ;
            case LONG:      return true ;
            case FLOAT:     return true ;
            case DOUBLE:    return true ;
            default: return false ;
        }        
    }
    
    public static boolean isUsableType(byte t) {
        switch (t) {
            case UNKNOWN:    return false ;
            case NULL:       return false ;
            case ERROR:      return false ;
            default :return true ;
        }
    }

        /***
     * Merge types if possible
     * @param type1
     * @param type2
     * @return the merged type, or DataType.ERROR if not successful
     */
    public static byte mergeType(byte type1, byte type2) {
        // Only legal types can be merged
        if ( (!DataType.isUsableType(type1)) ||
             (!DataType.isUsableType(type2)) ) {
            return DataType.ERROR ;
        }

        // Same type is OK
        if (type1==type2) {
            return type1 ;
        }

        // Both are number so we return the bigger type
        if ( (DataType.isNumberType(type1)) &&
             (DataType.isNumberType(type2)) ) {
            return type1>type2 ? type1:type2 ;
        }

        // One is bytearray and the other is (number or chararray)
        if (type1 == DataType.BYTEARRAY) {
            return type2 ;
        }

        if (type2 == DataType.BYTEARRAY) {
            return type1 ;
        }

        // else return just ERROR
        return DataType.ERROR ;
    }
    
    public static String mapToString(Map<Object, Object> m) {
        boolean hasNext = false;
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for(Object o: m.keySet()) {
            if(hasNext) {
                sb.append(",");
            } else {
                hasNext = true;
            }
            sb.append(o.toString());
            sb.append("#");
            if(m.get(o) != null) {
                sb.append(m.get(o).toString());
            }
        }
        sb.append("]");
        return sb.toString();
    }

    public static boolean equalByteArrays(byte[] lhs, byte[] rhs) {
        if(lhs == null && rhs == null) return true;
        if(lhs == null || rhs == null) return false;
        if(lhs.length != rhs.length) return false;
        for(int i = 0; i < lhs.length; ++i) {
            if(lhs[i] != rhs[i]) return false;
        }
        return true;
    }
        
    /***
     * Determine the field schema of an object
     * @param o the object whose field schema is to be determined
     * @return the field schema corresponding to the object
     * @throws ExecException,FrontendException,SchemaMergeException
     */
    public static Schema.FieldSchema determineFieldSchema(Object o) 
        throws ExecException, FrontendException, SchemaMergeException {
        byte dt = findType(o);

        switch (dt) {
        case NULL:
            return new Schema.FieldSchema(null, BYTEARRAY);

        case BOOLEAN:
            return new Schema.FieldSchema(null, BOOLEAN);

        case INTEGER:
            return new Schema.FieldSchema(null, INTEGER);

        case LONG:
            return new Schema.FieldSchema(null, LONG);

        case FLOAT:
            return new Schema.FieldSchema(null, FLOAT);

        case DOUBLE:
            return new Schema.FieldSchema(null, DOUBLE);

        case BYTEARRAY:
            return new Schema.FieldSchema(null, BYTEARRAY);

        case CHARARRAY:
            return new Schema.FieldSchema(null, CHARARRAY);

        case MAP: 
            return new Schema.FieldSchema(null, MAP);
        
        case TUPLE: {
	            Tuple t = (Tuple)o;
                long tupleSize = t.size();
                Schema schema = null;

                if(tupleSize != 0) {
	                schema = new Schema();
	                for(int i = 0; i < t.size(); ++i) {
	                    schema.add(determineFieldSchema(t.get(i))); 
	                }
                }
	            return new Schema.FieldSchema(null, schema, TUPLE);
            }
        
        case BAG: {
                DataBag b = (DataBag)o;
                long bagSize = b.size();
                Schema schema = null;

                if(bagSize != 0) {
                    Iterator<Tuple> it = b.iterator();
                    ArrayList<Schema> schemas = new ArrayList<Schema>();
                    while(it.hasNext()) {
                        schemas.add(determineFieldSchema((Object)it.next()).schema);
                    }
                    schema = schemas.get(0);
                    if(null == schema) {
                        Schema.FieldSchema tupleFs = new Schema.FieldSchema(null, null, TUPLE);
                        Schema bagSchema = new Schema(tupleFs);
                        return new Schema.FieldSchema(null, null, BAG);
                    }
                    int schemaSize = schema.size();

                    for(int i = 1; i < schemas.size(); ++i) {
                        Schema currSchema = schemas.get(i);
                        if((null == currSchema) || (currSchema.size() != schemaSize)) {
                            Schema.FieldSchema tupleFs = new Schema.FieldSchema(null, null, TUPLE);
                            Schema bagSchema = new Schema(tupleFs);
                            bagSchema.setTwoLevelAccessRequired(true);
                            return new Schema.FieldSchema(null, bagSchema, BAG);
                        }
                        schema = Schema.mergeSchema(schema, currSchema, false, false, false); 
                    }
                }
                Schema.FieldSchema tupleFs = new Schema.FieldSchema(null, schema, TUPLE);
                Schema bagSchema = new Schema(tupleFs);
                // since this schema has tuple field schema which internally
                // has a list of field schemas for the actual items in the bag
                // an access to any field in the bag is a  two level access
                bagSchema.setTwoLevelAccessRequired(true);
                return new Schema.FieldSchema(null, bagSchema, BAG);
            }
        default: {
                int errCode = 1073;
                String msg = "Cannot determine field schema for " + o;
                throw new ExecException(msg, errCode, PigException.INPUT);
            }
        }
    }
}
