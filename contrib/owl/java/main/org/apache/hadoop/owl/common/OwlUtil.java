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
package org.apache.hadoop.owl.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.owl.protocol.ColumnType;
import org.apache.hadoop.owl.protocol.OwlColumnSchema;
import org.apache.hadoop.owl.protocol.OwlKeyListValue;
import org.apache.hadoop.owl.protocol.OwlKeyValue;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlPartitionKey;
import org.apache.hadoop.owl.protocol.OwlPropertyKey;
import org.apache.hadoop.owl.protocol.OwlSchema;
import org.apache.hadoop.owl.protocol.OwlKey.DataType;
import org.apache.hadoop.owl.protocol.OwlKey.KeyType;
import org.apache.hadoop.owl.protocol.OwlPartitionKey.IntervalFrequencyUnit;
import org.apache.hadoop.owl.protocol.OwlPartitionKey.PartitioningType;
import org.apache.hadoop.owl.protocol.OwlResultObject.Status;

import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import net.sf.json.JsonConfig;
import net.sf.json.util.EnumMorpher;
import net.sf.json.util.JSONUtils;
import net.sf.json.util.NewBeanInstanceStrategy;
import net.sf.json.util.PropertyFilter;

/**
 * Class used to define some utility functions used from both client and server
 */
public class OwlUtil {

    static {
        JSONUtils.getMorpherRegistry().registerMorpher(new EnumMorpher(PartitioningType.class));
        JSONUtils.getMorpherRegistry().registerMorpher(new EnumMorpher(ColumnType.class));
        JSONUtils.getMorpherRegistry().registerMorpher(new EnumMorpher(DataType.class));
        JSONUtils.getMorpherRegistry().registerMorpher(new EnumMorpher(KeyType.class));
        JSONUtils.getMorpherRegistry().registerMorpher(new EnumMorpher(IntervalFrequencyUnit.class));
        JSONUtils.getMorpherRegistry().registerMorpher(new EnumMorpher(Status.class));
        JSONUtils.getMorpherRegistry().registerMorpher(new EnumMorpher(ErrorType.class));
    }

    /** The date format supported by Owl. */
    public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss z";  

    /** The variable length */
    public static final int IDENTIFIER_LIMIT = 64;
    public static final int LOCATION_LIMIT = 750;

    /** Enum for type of operation being done */
    public enum Verb {
        CREATE("PUT"),
        READ("GET"),
        UPDATE("POST"),
        DELETE("DELETE");

        private String httpRequestType;

        private Verb(String httpRequestType){
            this.httpRequestType = httpRequestType;
        }

        public String getHttpRequestType() {
            return httpRequestType;
        }
    }

    /** Mapping between the operation types and the corresponding verb types */
    @SuppressWarnings("serial")
    public static final Map<String, Verb> verbMap = new HashMap<String, Verb>() {
        {
            put("CREATE", Verb.CREATE);
            put("PUBLISH", Verb.CREATE);
            put("DESCRIBE", Verb.READ);
            put("SELECT", Verb.READ);
            put("DROP", Verb.DELETE);
            put("ALTER", Verb.UPDATE);
        }
    };

    /**
     * Gets the verb type for the specified command.
     * @param command the command
     * @return the verb for command
     * @throws OwlException the owl exception
     */
    public static Verb getVerbForCommand(String command) throws OwlException {
        Verb verb = verbMap.get(command.toUpperCase());
        if( verb == null ) {
            throw new OwlException(ErrorType.ERROR_UNRECOGNIZED_COMMAND, command);
        }

        return verb;
    }

    /**
     * Gets the command token from command string.
     * @param commandString the command string
     * @return the command token
     * @throws OwlException the owl exception
     */
    public static String getCommandFromString(String commandString) throws OwlException {
        String[] tokens = commandString.trim().split("[ \t\r\n]");
        if( (tokens.length == 0) || (tokens[0].length() == 0) ) {
            throw new OwlException(ErrorType.ERROR_UNRECOGNIZED_COMMAND, commandString);
        }

        return tokens[0].toUpperCase();
    }


    /**
     * Get JSON string representation of passed Java object.
     * 
     * @param inputObject the input Java object to serialize
     * @return the serialized JSON string
     * 
     * @throws OwlException the owl exception
     */

    public static String getJSONFromObject(Object inputObject) throws OwlException {

        try {
            JsonConfig jsonConfig = new JsonConfig();  
            jsonConfig.setHandleJettisonEmptyElement(false);
            // Refer to http://www.jroller.com/aalmiray/entry/json_lib_filtering_properties 
            // and http://json-lib.sourceforge.net/apidocs/jdk15/net/sf/json/util/PropertyFilter.html
            // This bit turns off transcoding from Object to json string if values are null.
            // Very important/needed for update partial deltas to not break.
            setJsonPropertyFilter(jsonConfig);

            JSONObject jsonObject = (JSONObject) JSONSerializer.toJSON(inputObject, jsonConfig);
            return jsonObject.toString();
        } catch(Exception e) {
            throw new OwlException(ErrorType.ERROR_JSON_SERIALIZATION, e);
        }
    }


    /**
     * Translates a string into <code>x-www-form-urlencoded</code>
     * format. This method uses UTF-8 as the encoding format.
     * 
     * In addition, force-changes spaces from plus(after encoding from space) 
     * to "%20" to allow for servers that do not decode that.
     *
     * @param input <code>String</code> to be translated.
     * @return  the translated <code>String</code>.
     * @throws OwlException
     * { @link http://tools.ietf.org/html/rfc3986 }
     * { @link http://en.wikipedia.org/wiki/Percent-encoding }
     */
    public static String encodeURICompatible(String input) throws OwlException{
        String encoded = null;
        try {
            encoded = java.net.URLEncoder.encode(input,"UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new OwlException(ErrorType.ERROR_UNSUPPORTED_ENCODING, e); 
        }
        encoded = encoded.replaceAll("\\+", "%20");
        return encoded;
    }


    /**
     * Gets the object type class, reads the request message and finds the class corresponding to the response object type.
     * 
     * @param inputMessage
     *            the request message passed in
     * 
     * @return the object type class
     * 
     * @throws OwlException
     *             the owl exception
     */
    public static Class<? extends OwlObject> getObjectType(String inputMessage) throws OwlException {
        try {
            JSONObject restMessage = JSONObject.fromObject(inputMessage);

            if( restMessage == null ) {
                throw new OwlException(ErrorType.ERROR_READING_OBJECT_TYPE, inputMessage);
            }
            JSONObject header = restMessage.getJSONObject("h");


            if( header == null ) {
                throw new OwlException(ErrorType.ERROR_READING_OBJECT_TYPE, inputMessage);
            }

            String objectType = header.getString("objectType");

            @SuppressWarnings("unchecked") Class<? extends OwlObject> owlClass = 
                (Class<? extends OwlObject>) Class.forName(OwlObject.class.getPackage().getName() + "." + objectType);
            return owlClass;
        }catch(Exception e) {
            if( e instanceof OwlException ) {
                throw (OwlException) e;
            }
            throw new OwlException(ErrorType.ERROR_READING_OBJECT_TYPE, inputMessage, e);
        }
    }

    /**
     * Gets the Java object of Type classType corresponding to passed JSON string
     * 
     * @param baseClass the class being deserialized
     * @param owlClass the class to which any OwlObject instances should be resolved
     * @param jsonString the JSON string to deserialized
     * 
     * @return the Java object 
     * 
     * @throws OwlException
     *             the meta data exception
     */
    public static Object getObjectFromJSON(
            Class<?> baseClass,
            final Class<? extends OwlObject> owlClass,
            String jsonString) throws OwlException {

        try {
            JsonConfig jsonConfig = new JsonConfig();  
            jsonConfig.setRootClass(baseClass);
            jsonConfig.setHandleJettisonEmptyElement(false);

            setJsonPropertyFilter(jsonConfig);

            //Override the default new bean strategy to handle generic OwlObject types being passed in the message body
            jsonConfig.setNewBeanInstanceStrategy( new NewBeanInstanceStrategy() {

                @SuppressWarnings("unchecked")
                @Override
                /** Overrides the default newInstance function to handle generic resource types */ 
                public Object newInstance( Class target, JSONObject source ) throws InstantiationException, IllegalAccessException,
                SecurityException, NoSuchMethodException, InvocationTargetException {

                    if( target == OwlObject.class ) {
                        //Return an object of the actual OwlObject type 
                        return NewBeanInstanceStrategy.DEFAULT.newInstance(owlClass, source);
                    }
                    else {
                        //For non OwlObject objects, use the default strategy
                        return NewBeanInstanceStrategy.DEFAULT.newInstance(target, source);
                    }
                }
            } );


            //Map for mapping list values to proper types. Any new members of List type added as bean members
            //need to be added in this map so that the proper resolution happens during conversion from
            //JSON string to Java object.
            Map<String, Class<?>> classMap = new HashMap<String, Class<?>>();

            classMap.put("propertyKeys", OwlPropertyKey.class);
            classMap.put("partitionKeys", OwlPartitionKey.class);
            classMap.put("keyValues", OwlKeyValue.class);
            classMap.put("propertyValues", OwlKeyValue.class);
            classMap.put("listValues", OwlKeyListValue.class);
            // columnSchema is the field name in OwlSchema protocol class
            classMap.put("columnSchema", OwlColumnSchema.class);

            //Set the class map in the config
            jsonConfig.setClassMap(classMap);

            JSONObject jsonObject = JSONObject.fromObject(jsonString, jsonConfig);
            Object outputObject = JSONSerializer.toJava(jsonObject, jsonConfig);

            return outputObject;

        } catch(Exception e) {
            throw new OwlException(ErrorType.ERROR_JSON_DESERIALIZATION, e);
        }
    }

    /**
     * Gets the stack trace as a string.
     * @param throwable the throwable to get stack trace for
     * @return the stack trace as a string
     * @throws IOException 
     */
    public static String getStackTraceString(Throwable throwable) {
        String output = "";

        try {
            ByteArrayOutputStream bs = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(bs);

            throwable.printStackTrace(ps);

            ps.close();
            bs.close();

            output = bs.toString();
        }catch(Exception e) {
            //Ignore exception since this occurred during handling of another exception
        }

        return output;
    }

    private static void setJsonPropertyFilter(JsonConfig jsonConfig) {
        PropertyFilter filter = new PropertyFilter(){   
            public boolean apply( Object source, String name, Object value ) {   
                if( value == null ){ // skip serializing any nulls
                    return true;
                }
                if( source instanceof OwlSchema ) {
                    //skip generated columns for OwlSchema
                    if( "columnCount".equals(name) ||
                            "schemaString".equals(name) ) {
                        return true;
                    }
                }

                return false;   
            }   
        };

        jsonConfig.setJsonPropertyFilter(filter);
        jsonConfig.setJavaPropertyFilter(filter);
    }

    /**
     * To lower case.
     * 
     * @param input the input
     * @return the string
     */
    public static String toLowerCase(String input) {
        return input == null ? null : input.toLowerCase();
    }

    /**
     * Validate length.
     * 
     * @param variable the variable
     * @param length the length
     * @return true, if successful
     */
    public static boolean validateLength(String variable, int length){
        if (variable.length() <= length){
            return true;
        }else{
            return false;
        }

    }
}
