/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.pig.piggybank.storage.hiverc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.lazy.LazyBoolean;
import org.apache.hadoop.hive.serde2.lazy.LazyByte;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.lazy.LazyFloat;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.LazyShort;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.pig.data.DataType;
import org.apache.pig.data.InternalMap;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * 
 * Implements helper methods for:<br/>
 * <ul>
 * <li>Parsing the hive table schema string.</li>
 * <li>Converting from hive to pig types</li>
 * <li>Converting from pig to hive types</li>
 * </ul>
 */
public class HiveRCSchemaUtil {

    private static final TupleFactory tupleFactory = TupleFactory.getInstance();

    /**
     * Regex to filter out column types
     */
    protected static final Pattern ptypes = Pattern
	    .compile("([ ][a-zA-Z0-9]*)|([a-zA-Z_0-9]*[<][a-zA-Z,_0-9]*[>])");

    /**
     * General schema parsing method, is used to parse the column names.
     * 
     * @param pattern
     *            String
     * @param schema
     *            String
     * @return List of String
     */
    public static List<String> parseSchema(Pattern pattern, String schema) {
	List<String> types = new ArrayList<String>();
	Matcher m = pattern.matcher(schema);
	String item = null;
	while (m.find()) {
	    item = m.group().trim();
	    if (item.length() > 0)
		types.add(item);
	}
	return types;
    }

    /**
     * Parses the schema types and returns a List of these.
     * 
     * @param schema
     * @return List of String
     */
    public static List<String> parseSchemaTypes(String schema) {
	List<String> types = new ArrayList<String>();
	Matcher m = ptypes.matcher(schema);
	String item = null;

	while (m.find()) {
	    item = m.group().trim();
	    if (item.length() > 0) {
		if (item.equalsIgnoreCase("map")) {
		    // if generic type
		    if (m.find()) {
			types.add(item + m.group().trim());
		    } else {
			throw new RuntimeException(
				"Map must have generic types specified");
		    }
		} else if (item.equalsIgnoreCase("array")) {
		    // if generic type
		    if (m.find()) {
			types.add(item + m.group().trim());
		    } else {
			throw new RuntimeException(
				"Array must have generic types specified");
		    }
		} else {
		    types.add(item);
		}
	    }
	}
	return types;
    }

    /**
     * Trims items in the list.
     * 
     * @param list
     * @return String
     */
    public static final String listToString(List<String> list) {
	StringBuilder buff = new StringBuilder();

	for (String item : list) {
	    buff.append(item.trim()).append(",");
	}
	int len = buff.length() - 1;
	buff.delete(len, len);
	return buff.toString();
    }

    /**
     * Extract the date from the hive file names e.g
     * /user/hive/warehouse/table/daydate=2009-10-01/upload001/0002.dat<br/>
     * This method will extract the 2009-10-01 from this name.
     * 
     * @param fileName
     * @return String
     */
    public static final String extractDayDate(String fileName) {
	int index = fileName.indexOf("daydate=");
	String dateStr = null;
	if (index == 0)
	    dateStr = fileName.substring(8, fileName.length());
	else if (index > 0)
	    dateStr = fileName.substring(index + 8,
		    fileName.indexOf('/', index));

	return dateStr;
    }

    /**
     * Returns a set of columns, with the column names strimmed
     * 
     * @param columnsToRead
     * @return Set
     */
    public static final Set<String> compileSet(String columnsToRead) {

	String[] columnsArr = columnsToRead.split(",");
	int len = columnsArr.length;

	Set<String> columnsSet = new TreeSet<String>();

	for (int i = 0; i < len; i++) {
	    columnsSet.add(columnsArr[i].trim());
	}

	return columnsSet;
    }

    /**
     * Returns the pig DataType for the hive type
     * 
     * @param hiveType
     * @return byte from DataType
     */
    public static byte findPigDataType(String hiveType) {
	hiveType = hiveType.toLowerCase();

	if (hiveType.equals("string"))
	    return DataType.CHARARRAY;
	else if (hiveType.equals("int"))
	    return DataType.INTEGER;
	else if (hiveType.equals("bigint") || hiveType.equals("long"))
	    return DataType.LONG;
	else if (hiveType.equals("float"))
	    return DataType.FLOAT;
	else if (hiveType.equals("double"))
	    return DataType.DOUBLE;
	else if (hiveType.equals("boolean"))
	    return DataType.BOOLEAN;
	else if (hiveType.equals("byte"))
	    return DataType.INTEGER;
	else if (hiveType.contains("array"))
	    return DataType.TUPLE;
	else if (hiveType.contains("map"))
	    return DataType.MAP;
	else
	    return DataType.ERROR;
    }

    /**
     * Converts from a hive type to a pig type
     * 
     * @param value
     *            Object hive type
     * @return Object pig type
     */
    public static Object extractPigTypeFromHiveType(Object value) {

	if (value instanceof org.apache.hadoop.hive.serde2.lazy.LazyArray) {
	    value = parseLazyArrayToPigArray((org.apache.hadoop.hive.serde2.lazy.LazyArray) value);
	} else if (value instanceof org.apache.hadoop.hive.serde2.lazy.LazyMap) {
	    value = parseLazyMapToPigMap((org.apache.hadoop.hive.serde2.lazy.LazyMap) value);
	} else {

	    if (value instanceof LazyString) {
		value = ((LazyString) value).getWritableObject().toString();
	    } else if (value instanceof LazyInteger) {
		value = ((LazyInteger) value).getWritableObject().get();
	    } else if (value instanceof LazyLong) {
		value = ((LazyLong) value).getWritableObject().get();
	    } else if (value instanceof LazyFloat) {
		value = ((LazyFloat) value).getWritableObject().get();
	    } else if (value instanceof LazyDouble) {
		value = ((LazyDouble) value).getWritableObject().get();
	    } else if (value instanceof LazyBoolean) {
		boolean boolvalue = ((LazyBoolean) value).getWritableObject()
			.get();
		value = (boolvalue) ? 1 : 0;
	    } else if (value instanceof LazyByte) {
		value = (int) ((LazyByte) value).getWritableObject().get();
	    } else if (value instanceof LazyShort) {
		value = ((LazyShort) value).getWritableObject().get();
	    }

	}

	return value;
    }

    /**
     * Converts the LazyMap to a InternalMap.
     * 
     * @param map
     *            LazyMap
     * @return InternalMap
     */
    public static InternalMap parseLazyMapToPigMap(LazyMap map) {
	InternalMap pigmap = new InternalMap();

	Map<Object, Object> javamap = map.getMap();

	if (javamap != null) {

	    // for each item in the map extract the java primitive type
	    for (Entry<Object, Object> entry : javamap.entrySet()) {
		pigmap.put(extractPigTypeFromHiveType(entry.getKey()),
			extractPigTypeFromHiveType(entry.getValue()));
	    }

	}

	return pigmap;
    }

    /**
     * Converts the LazyArray to a Tuple.<br/>
     * 
     * @param arr
     *            LazyArray
     * @return Tuple
     */
    public static Tuple parseLazyArrayToPigArray(LazyArray arr) {
	List<Object> list = new ArrayList<Object>();

	// each item inside the LazyArray must be converted to its java
	// primitive type
	List<Object> hivedataList = arr.getList();

	for (Object item : hivedataList) {
	    list.add(extractPigTypeFromHiveType(item));
	}

	return tupleFactory.newTuple(list);
    }

}
