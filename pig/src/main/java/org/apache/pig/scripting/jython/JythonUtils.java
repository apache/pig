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

package org.apache.pig.scripting.jython;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.python.core.Py;
import org.python.core.PyBoolean;
import org.python.core.PyDictionary;
import org.python.core.PyFloat;
import org.python.core.PyInteger;
import org.python.core.PyList;
import org.python.core.PyLong;
import org.python.core.PyNone;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.core.PyTuple;

public class JythonUtils {

    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = DefaultBagFactory.getInstance();

    @SuppressWarnings("unchecked")
    public static Object pythonToPig(PyObject pyObject) throws ExecException {
        try {
            Object javaObj = null;
            // Add code for all supported pig types here
            // Tuple, bag, map, int, long, float, double, chararray, bytearray 
            if (pyObject instanceof PyTuple) {
                PyTuple pyTuple = (PyTuple) pyObject;
                Object[] tuple = new Object[pyTuple.size()];
                int i = 0;
                for (PyObject tupleObject : pyTuple.getArray()) {
                    tuple[i++] = pythonToPig(tupleObject);
                }
                javaObj = tupleFactory.newTuple(Arrays.asList(tuple));
            } else if (pyObject instanceof PyList) {
                DataBag list = bagFactory.newDefaultBag();
                for (PyObject bagTuple : ((PyList) pyObject).asIterable()) {
                    // If the item of the array is not a tuple, 
                    // wrap it into tuple before adding to bag
                    Object pigBagItem = pythonToPig(bagTuple);
                    Tuple pigBagTuple;
                    if (!(pigBagItem instanceof Tuple)) {
                        pigBagTuple = TupleFactory.getInstance().newTuple(1);
                        pigBagTuple.set(0, pigBagItem);
                    } else {
                        pigBagTuple = (Tuple)pigBagItem;
                    }
                    list.add(pigBagTuple);
                }
                javaObj = list;
            } else if (pyObject instanceof PyDictionary) {
                Map<?, Object> map = Py.tojava(pyObject, Map.class);
                Map<Object, Object> newMap = new HashMap<Object, Object>();
                for (Map.Entry<?, Object> entry : map.entrySet()) {
                    if (entry.getValue() instanceof PyObject) {
                        newMap.put(entry.getKey(), pythonToPig((PyObject) entry.getValue()));
                    } else {
                        // Jython sometimes uses directly the java class: for example for integers
                        newMap.put(entry.getKey(), entry.getValue());
                    }
                }
                javaObj = newMap;
            } else if (pyObject instanceof PyLong) {
                javaObj = pyObject.__tojava__(Long.class);
            } else if (pyObject instanceof PyBoolean) {
            	javaObj = pyObject.__tojava__(Boolean.class);
            } else if (pyObject instanceof PyInteger) {
                javaObj = pyObject.__tojava__(Integer.class);
            } else if (pyObject instanceof PyFloat) {
                // J(P)ython is loosely typed, supports only float type, 
                // hence we convert everything to double to save precision
                javaObj = pyObject.__tojava__(Double.class);
            } else if (pyObject instanceof PyString) {
                javaObj = pyObject.__tojava__(String.class);
            } else if (pyObject instanceof PyNone) {
                return null;
            } else {
                javaObj = pyObject.__tojava__(byte[].class);
                // if we successfully converted to byte[]
                if(javaObj instanceof byte[]) {
                    javaObj = new DataByteArray((byte[])javaObj);
                }
                else {
                    throw new ExecException("Non supported pig datatype found, cast failed: "+(pyObject==null?null:pyObject.getClass().getName()));
                }
            }
            if(javaObj.equals(Py.NoConversion)) {
                throw new ExecException("Cannot cast into any pig supported type: "+(pyObject==null?null:pyObject.getClass().getName()));
            }
            return javaObj;
        } catch (Exception e) {
            throw new ExecException("Cannot convert jython type ("+(pyObject==null?null:pyObject.getClass().getName())+") to pig datatype "+ e, e);
        }
    }

    public static PyObject pigToPython(Object object) {
        if (object instanceof Tuple) {
            return pigTupleToPyTuple((Tuple) object);
        } else if (object instanceof DataBag) {
            PyList list = new PyList();
            for (Tuple bagTuple : (DataBag) object) {
                list.add(pigTupleToPyTuple(bagTuple));
            }
            return list;
        } else if (object instanceof Map<?, ?>) {
            PyDictionary newMap = new PyDictionary();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) object).entrySet()) {
                newMap.put(entry.getKey(), pigToPython(entry.getValue()));
            }
            return newMap;
        } else if (object instanceof DataByteArray) {
            return Py.java2py(((DataByteArray) object).get());
        } else {
            return Py.java2py(object);
        }
    }

    public static PyTuple pigTupleToPyTuple(Tuple tuple) {
        PyObject[] pyTuple = new PyObject[tuple.size()];
        int i = 0;
        for (Object object : tuple.getAll()) {
            pyTuple[i++] = pigToPython(object);
        }
        return new PyTuple(pyTuple);
    }

}

