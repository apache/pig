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
package org.apache.hadoop.owl.testdriver;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.protocol.OwlColumnSchema;
import org.apache.hadoop.owl.protocol.OwlDatabase;
import org.apache.hadoop.owl.protocol.OwlDataElement;
import org.apache.hadoop.owl.protocol.OwlKeyListValue;
import org.apache.hadoop.owl.protocol.OwlKeyValue;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlPartition;
import org.apache.hadoop.owl.protocol.OwlPartitionKey;
import org.apache.hadoop.owl.protocol.OwlPartitionProperty;
import org.apache.hadoop.owl.protocol.OwlPropertyKey;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.protocol.OwlTableName;

/** Class which does the comparison of two OwlObjects */
public class CompareOwlObjects {

    /**
     * Gets the fields which can uniquely identify every owlObject type
     * 
     * @param owlClass the owl class
     * @return the identifier fields
     * @throws Exception the test failure exception
     */
    public static String[] getIdentifierFields(Class<? extends OwlObject> owlClass) throws Exception {

        if( owlClass.equals(OwlDatabase.class)) {
            return new String[] { "name" };
        } else if( owlClass.equals(OwlTableName.class)) {
            return new String[] { "tableName", "databaseName" };
        } else if( owlClass.equals(OwlTable.class)) {
            return new String[] { "name" };
        } else if( owlClass.equals(OwlKeyValue.class)) {
            return new String[] { "keyName"};
        } else if( owlClass.equals(OwlKeyListValue.class)) {
            return new String[] { "intValue", "stringValue"};
        } else if( owlClass.equals(OwlDataElement.class)) {
            return new String[] { "storageLocation"};
        } else if( owlClass.equals(OwlSchema.class)) {
            return new String[] { "columnSchema"};
        } else if( owlClass.equals(OwlColumnSchema.class)) {
            return new String[] { "name"};
        } else if( owlClass.equals(OwlPartitionKey.class) || owlClass.equals(OwlPropertyKey.class) ) {
            return new String[] { "name"};
        }  else if( owlClass.equals(OwlPartition.class)) {
            return new String[] { "keyValues" }; 
        } else if( owlClass.equals(OwlPartitionProperty.class)) {
            return new String[] {}; //partition property list not handled currently (since they don't have any unique identifier)
        } else {
            throw new Exception("Error matching results for class" + owlClass);
        }
        //GlobalKey not currently exposed to client, so not listed
    }


    /**
     * Compare owl objects for equality, main entry point to this class.
     * 
     * @param resultObject
     *            the result object
     * @param expectedObject
     *            the expected object
     * 
     * @throws Exception the test failure exception
     */
    @SuppressWarnings("unchecked")
    public static void compareObjects(OwlObject resultObject,
            OwlObject expectedObject) throws Exception {

        try {

            //EqualsBuilder.reflectionEquals or something like that cannot be used since they cannot handle order mismatch
            Method[] methods = resultObject.getClass().getMethods();

            for(Method method : methods ) {

                if( (! method.getName().startsWith("get" )) &&
                        (! method.getName().startsWith("is" )) ) {
                    continue;
                }
                if( method.getName().equals("getCreationTime") ||
                        method.getName().equals("getModificationTime") ) {
                    continue;
                }

                if( method.getParameterTypes().length > 0 ) {
                    //Ignore methods which take paramters
                    continue;
                }

                //Match all getter methods except modification and creation time
                Object resultValue =  method.invoke(resultObject);
                Object expectedValue = method.invoke(expectedObject);

                if( resultValue == null ) {
                    if( expectedValue != null ) {
                        throw new Exception("Expected non null value for " + method.getName());
                    }
                } else if( expectedValue == null ) {
                    throw new Exception("Expected null value for " + method.getName());
                }
                else if( resultValue instanceof OwlObject ) {
                    compareObjects((OwlObject) resultValue, (OwlObject) expectedValue);
                } else if( resultValue instanceof List) {
                    matchObjects(
                            (List<? extends OwlObject>) resultValue, (List<? extends OwlObject>) expectedValue);
                } else {
                    if( ! resultValue.equals(expectedValue) ) {
                        throw new Exception("Expected <" + expectedValue + "> for " + method.getName() +
                                ", got <" + resultValue + ">");
                    }
                }
            }
        }catch(Exception e) {
            System.out.println("Error comparing output " + OwlUtil.getJSONFromObject(resultObject) 
                    + " with expected result " + OwlUtil.getJSONFromObject(expectedObject));
            throw e;
        }
    }


    /**
     * Match the results with the expected results. Allows list comparison where the order is different to
     * allow for cases where the result order is different from expected result due to database difference.
     * 
     * @param resultObjects
     *            the result objects
     * @param expectedObjects
     *            the expected objects
     * 
     * @throws Exception
     *             the exception
     */
    public static void matchObjects(List<? extends OwlObject> resultObjects,
            List<? extends OwlObject> expectedObjects) throws Exception {

        for(OwlObject resultObject : resultObjects) {
            OwlObject expectedObject = getMatchingObject(resultObject, expectedObjects);

            compareObjects(resultObject, expectedObject);
        }

        //This check is kept after the loop so as to give more detailed error for some cases
        if( resultObjects.size() != expectedObjects.size() ) {
            throw new Exception("Expected " + expectedObjects.size() + " entries, found " + resultObjects.size());
        }
    }

    /**
     * Gets the matching object, to handle order difference within list.
     * 
     * @param resultObject
     *            the result object
     * @param expectedObjects
     *            the expected objects
     * 
     * @return the matching object
     * 
     * @throws Exception
     *             the exception
     */
    @SuppressWarnings("unchecked")
    private static OwlObject getMatchingObject(OwlObject resultObject,
            List<? extends OwlObject> expectedObjects) throws Exception {

        String[] identifiedFields = getIdentifierFields(resultObject.getClass());

        List<Method> methods = new ArrayList<Method>();
        for(String field : identifiedFields) {

            String getter = "get" + field.substring(0, 1).toUpperCase() + field.substring(1, field.length());
            methods.add(resultObject.getClass().getMethod(getter));
        }

        for(OwlObject expectedObject : expectedObjects) {
            if( identifiedFields.length == 0 ) {
                //No unique identifier, must match first entry
                return expectedObject;
            }

            boolean matched = true;

            for(Method method : methods) {
                Object resultValue =  method.invoke(resultObject);
                Object expectedValue = method.invoke(expectedObject);

                if( resultValue == null ) {
                    if( expectedValue != null ) {
                        matched = false;
                        break;
                    }
                } else if( resultValue instanceof OwlObject ) {
                    try {
                        compareObjects((OwlObject) resultValue, (OwlObject) expectedValue);
                    } catch(Exception e) {
                        matched = false;
                        break;
                    }
                } else if( resultValue instanceof List ) {
                    try {
                        matchObjects((List<? extends OwlObject>) resultValue, (List<? extends OwlObject>) expectedValue);
                    } catch(Exception e) {
                        matched = false;
                        break;
                    }
                } 
                else {
                    if( ! resultValue.equals(expectedValue) ) {
                        matched = false;
                        break;
                    }
                }
            }

            if( matched ) {
                return expectedObject;
            }
        }

        throw new Exception("No matching entry in expected results for output entry " + OwlUtil.getJSONFromObject(resultObject));
    }



}
