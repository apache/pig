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
package org.apache.pig.impl.util;

import java.io.ByteArrayInputStream;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.parser.QueryParser;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Class with utility static methods
 */
public class Utils {
  
    /**
     * This method is a helper for classes to implement {@link java.lang.Object#equals(java.lang.Object)}
     * checks if two objects are equals - two levels of checks are
     * made - first if both are null or not null. If either is null,
     * check is made whether both are null.
     * If both are non null, equality also is checked if so indicated
     * @param obj1 first object to be compared
     * @param obj2 second object to be compared
     * @param checkEquality flag to indicate whether object equality should
     * be checked if obj1 and obj2 are non-null
     * @return true if the two objects are equal
     * false otherwise
     */
    public static boolean checkNullEquals(Object obj1, Object obj2, boolean checkEquality) {
        if(obj1 == null || obj2 == null) {
            return obj1 == obj2;
        }
        if(checkEquality) {
            if(!obj1.equals(obj2)) {
                return false;
            }
        }
        return true;
    }
    
    
    /**
     * This method is a helper for classes to implement {@link java.lang.Object#equals(java.lang.Object)}
     * The method checks whether the two arguments are both null or both not null and 
     * whether they are of the same class
     * @param obj1 first object to compare
     * @param obj2 second object to compare
     * @return true if both objects are null or both are not null
     * and if both are of the same class if not null
     * false otherwise
     */
    public static boolean checkNullAndClass(Object obj1, Object obj2) {
        if(checkNullEquals(obj1, obj2, false)) {
            if(obj1 != null) {
                return obj1.getClass() == obj2.getClass();
            } else {
                return true; // both obj1 and obj2 should be null
            }
        } else {
            return false;
        }
    }
    
    public static Schema getSchemaFromString(String schemaString) throws ParseException {
        return Utils.getSchemaFromString(schemaString, DataType.BYTEARRAY);
    }

    public static Schema getSchemaFromString(String schemaString, byte defaultType) throws ParseException {
        ByteArrayInputStream stream = new ByteArrayInputStream(schemaString.getBytes()) ;
        QueryParser queryParser = new QueryParser(stream) ;
        Schema schema = queryParser.TupleSchema() ;
        Schema.setSchemaDefaultType(schema, defaultType);
        return schema;
    }
    
    public static String getStringFromArray(String[] arr) {
        StringBuilder str = new StringBuilder();
        for(String s: arr) {
            str.append(s);
            str.append(" ");
        }
        return str.toString();
    }
    
}
