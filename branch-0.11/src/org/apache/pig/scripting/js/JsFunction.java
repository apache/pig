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
package org.apache.pig.scripting.js;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import org.mozilla.javascript.Context;
import org.mozilla.javascript.NativeJavaObject;
import org.mozilla.javascript.NativeObject;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.Undefined;

public class JsFunction extends EvalFunc<Object> {
    private static final Log LOG = LogFactory.getLog(JsFunction.class);

    /**
     * helper function for debugging
     * @param schema a PIG schema
     * @return a readable string representation of the schema
     */
    private static String stringify(Schema schema) {
        StringBuffer buffer = new StringBuffer();
        stringify(schema, buffer);
        return buffer.toString();
    }

    /**
     * 
     *  helper function for debugging
     *  @param schema a PIG schema
     *  @param buffer where to write the readable representation of the schema
     */
    private static void stringify(Schema schema, StringBuffer buffer) {
        if (schema != null) {
            buffer.append("( ");
            List<FieldSchema> fields = schema.getFields();
            for (int i = 0; i < fields.size(); i++) {
                FieldSchema array_element = fields.get(i);
                if (i!=0) { 
                    buffer.append(", ");
                }
                buffer
                .append(DataType.findTypeName(array_element.type))
                .append(": ")
                .append(array_element.alias)
                .append(" ");
                stringify(array_element.schema, buffer);
            }
            buffer.append(" )");
        }
    }

    private String functionName;
    private JsScriptEngine jsScriptEngine;
    private Schema outputSchema;

    ///////////////////////
    // Debugging functions
    ///////////////////////

    private void debugConvertPigToJS(int depth, String pigType, Object value, Schema schema) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(indent(depth)+"converting from Pig " + pigType + " " + value + " using " + stringify(schema));
        }
    }

    private void debugConvertJSToPig(int depth, String pigType, Object value, Schema schema) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(indent(depth)+"converting to Pig " + pigType + " " + toString(value) + " using " + stringify(schema));
        }
    }

    private void debugReturn(int depth, Object value) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(indent(depth) + "returning " + toString(value));
        }
    }

    /**
     * generates a String of white spaces for debugging indentation
     * @param depth the call depth
     * @return a string of spaces of length 2*depth
     */
    private String indent(int depth) {
        StringBuffer b = new StringBuffer(depth*2);
        for (int i = 0; i < depth * 2; i++) {
            b.append(' ');
        }
        return b.toString();
    }

    /**
     * debug utility to display a JS object
     * @param buffer whil append the representation here
     * @param object special treatment if a javascript object
     */
    private void append(StringBuffer buffer, Object object) {
        if (object instanceof Scriptable) {
            Scriptable scriptable = (Scriptable) object;
            buffer.append("{");
            boolean first = true;
            for (Object id : scriptable.getIds()) {
                if (first) {
                    first = false;
                } else {
                    buffer.append(", ");    
                }
                if (id instanceof String) {
                    buffer.append(id).append(": ");
                    append(buffer, scriptable.get((String)id, jsScriptEngine.getScope()));
                } else {
                    buffer.append("[").append(id).append("]: ");
                    append(buffer, scriptable.get((Integer)id, jsScriptEngine.getScope()));
                }
            }
            buffer.append("}");

        } else {
            buffer.append(object);
        }
    }

    /**
     * debug utility to display a JS object
     * @param object an object
     * @return a string representation of the object
     */
    private String toString(Object object) {
        StringBuffer buffer = new StringBuffer();
        append(buffer, object);
        return buffer.toString();

    }

    //////////////////////

    public JsFunction(String functionName) {
        this.jsScriptEngine = JsScriptEngine.getInstance();
        this.functionName = functionName;

        String outputSchemaDef;
        outputSchemaDef = jsScriptEngine.jsEval(this.getClass().getName()+"(String)", functionName+".outputSchema").toString();
        try {
            this.outputSchema = Utils.getSchemaFromString(outputSchemaDef);
        } catch (ParserException e) {
            throw new IllegalArgumentException(functionName+".outputSchema is not a valid schema: "+e.getMessage(), e); 
        }

    }

    ///////////////////////////
    // EvalFunc implementation
    ///////////////////////////

    @Override
    public Object exec(Tuple tuple) throws IOException {
    	Schema inputSchema = this.getInputSchema();
        if (LOG.isDebugEnabled()) {
            LOG.debug( "CALL " + stringify(outputSchema) + " " + functionName + " " + stringify(inputSchema));
        }
        // UDF always take a tuple: unwrapping when not necessary to simplify UDFs
        if (inputSchema.size() == 1 && inputSchema.getField(0).type == DataType.TUPLE) {
            inputSchema = inputSchema.getField(0).schema;
        }

        Scriptable params = pigTupleToJS(tuple, inputSchema, 0);

        Object[] passedParams = new Object[inputSchema.size()];
        for (int j = 0; j < passedParams.length; j++) {
            passedParams[j] = params.get(inputSchema.getField(j).alias, params);
        }

        Object result = jsScriptEngine.jsCall(functionName, passedParams);
        if (LOG.isDebugEnabled()) {
            LOG.debug( "call "+functionName+"("+Arrays.toString(passedParams)+") => "+toString(result));
        }

        // result is always a tuple: automatically wrapping when necessary to simplify UDFs 
        if (outputSchema.size() == 1 && !(result instanceof NativeObject)) {
            Scriptable wrapper = jsScriptEngine.jsNewObject();
            wrapper.put(outputSchema.getField(0).alias, wrapper, result);
            result = wrapper;
        }
        Tuple evalTuple = jsToPigTuple((Scriptable)result, outputSchema, 0);
        Object eval = outputSchema.size() == 1 ? evalTuple.get(0) : evalTuple;
        LOG.debug(eval);
        return eval;
    }

    @Override
    public Schema outputSchema(Schema input) {
        this.setInputSchema(input);
        return outputSchema;
    }

    /////////////////////////
    // Conversion Pig to JS 
    /////////////////////////

    /**
     * converts a tuple to javascript object based on a schema
     * @param tuple the tuple to convert
     * @param schema the schema to use for conversion
     * @param depth call depth used for debugging messages
     * @return the resulting javascript object 
     */
    @SuppressWarnings("unchecked")
    private Scriptable pigTupleToJS(Tuple tuple, Schema schema, int depth) throws FrontendException, ExecException {
        debugConvertPigToJS(depth, "Tuple", tuple, schema);
        Scriptable object = null;
        if (tuple != null) {
            object = jsScriptEngine.jsNewObject(); 

            for (int i = 0; i < schema.size(); i++) {
                FieldSchema field = schema.getField(i);
                Object value;
                if (field.type == DataType.BAG) {
                    value = pigBagToJS((DataBag)tuple.get(i), field.schema, depth + 1);
                } else if (field.type == DataType.TUPLE) {
                    value = pigTupleToJS((Tuple)tuple.get(i), field.schema, depth + 1);
                } else if (field.type == DataType.MAP) {
                    value = pigMapToJS((Map<String, Object>)tuple.get(i), field.schema, depth + 1);
                } else {
                    debugConvertPigToJS(depth+1, "value", tuple.get(i), field.schema);
                    value = Context.javaToJS(tuple.get(i), jsScriptEngine.getScope());
                    debugReturn(depth + 1, value);
                }
                object.put(field.alias, object, value);
            }
        }
        debugReturn(depth, object);
        return object;
    }

    /**
     * converts a map to javascript object based on a schema
     * @param map the map to convert
     * @param schema the schema to use for conversion
     * @param depth call depth used for debugging messages
     * @return the resulting javascript object
     */
    private Scriptable pigMapToJS(Map<String, Object> map, Schema schema, int depth) {
        debugConvertPigToJS(depth, "Map", map, schema);
        Scriptable object = jsScriptEngine.jsNewObject();

        for (Entry<String, Object> entry : map.entrySet()) {
            object.put(entry.getKey(), object, entry.getValue());
        }
        debugReturn(depth, object);
        return object;
    }

    /**
     * converts a bag to javascript object based on a schema
     * @param bag the bag to convert
     * @param schema the schema to use for conversion
     * @param depth call depth used for debugging messages
     * @return the resulting javascript object
     * @throws FrontendException
     * @throws ExecException
     */
    private Scriptable pigBagToJS(DataBag bag, Schema schema, int depth) throws FrontendException, ExecException {
        debugConvertPigToJS(depth, "Bag", bag, schema);
        if (schema.size() == 1 && schema.getField(0).type == DataType.TUPLE) {
            // unwrapping as bags always contain a tuple
            schema = schema.getField(0).schema;
        }
        Scriptable array = jsScriptEngine.jsNewArray(bag.size());
        array.setParentScope(jsScriptEngine.getScope());
        int i= 0;
        for (Tuple t : bag) {
            array.put(i++, array, pigTupleToJS(t, schema, depth + 1));
        }
        debugReturn(depth, array);
        return array;
    }

    /////////////////////////
    // Conversion JS to Pig 
    /////////////////////////

    private Tuple jsToPigTuple(Scriptable object, Schema schema, int depth) throws FrontendException, ExecException {
        debugConvertJSToPig(depth, "Tuple", object, schema);
        Tuple t = TupleFactory.getInstance().newTuple(schema.size());
        for (int i = 0; i < schema.size(); i++) {
            FieldSchema field = schema.getField(i);
            if (object.has(field.alias, jsScriptEngine.getScope())) {
                Object attr = object.get(field.alias, object);
                Object value;
                if (field.type == DataType.BAG) {
                    value = jsToPigBag((Scriptable)attr, field.schema, depth + 1);
                } else if (field.type == DataType.TUPLE) {
                    value = jsToPigTuple((Scriptable)attr, field.schema, depth + 1);
                } else if (field.type == DataType.MAP) {
                    value = jsToPigMap((Scriptable)attr, field.schema, depth + 1);
                } else if (attr instanceof NativeJavaObject) {
                    value = ((NativeJavaObject)attr).unwrap();
                } else if (attr instanceof Undefined) {
                    value = null;
                } else {
                    value = attr;
                }
                t.set(i, value);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("X( "+field.alias+" NOT FOUND");
                }
            }
        }
        debugReturn(depth, t);
        return t;
    }

    private Object jsToPigMap(Scriptable object, Schema schema, int depth) {
        debugConvertJSToPig(depth, "Map", object, schema);
        Map<String, Object> map = new HashMap<String, Object>();
        Object[] ids = object.getIds();
        for (Object id : ids) {
            if (id instanceof String) {
                String name = (String) id;
                Object value = object.get(name, object);
                if (value instanceof NativeJavaObject) {
                    value = ((NativeJavaObject)value).unwrap();
                } else if (value instanceof Undefined) {
                    value = null;
                }
                map.put(name, value);
            }
        }
        debugReturn(depth, map);
        return map;
    }

    private DataBag jsToPigBag(Scriptable array, Schema schema, int depth) throws FrontendException, ExecException {
        debugConvertJSToPig(depth, "Bag", array, schema);
        if (schema.size() == 1 && schema.getField(0).type == DataType.TUPLE) {
            schema = schema.getField(0).schema;
        }
        List<Tuple> bag = new ArrayList<Tuple>();
        for (Object id : array.getIds()) {
            Scriptable arrayValue = (Scriptable)array.get(((Integer)id).intValue(), null);
            bag.add(jsToPigTuple(arrayValue, schema, depth + 1));
        }
        DataBag result = BagFactory.getInstance().newDefaultBag(bag);
        debugReturn(depth, result);
        return result;
    }

}
