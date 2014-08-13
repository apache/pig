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

package org.apache.pig.scripting.jruby;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.lang.reflect.Method;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.runtime.load.Library;
import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyBignum;
import org.jruby.RubyBoolean;
import org.jruby.RubyFixnum;
import org.jruby.RubyFloat;
import org.jruby.RubyHash;
import org.jruby.RubyInteger;
import org.jruby.RubyNil;
import org.jruby.RubyString;
import org.jruby.RubySymbol;
import org.jruby.RubyEnumerator;
import org.jruby.runtime.builtin.IRubyObject;

import com.google.common.collect.Maps;

/**
 * This class provides the ability to present to Ruby a library that was written in Java.
 * In JRuby, there are two ways to present a library to ruby: one is to implement it in
 * ruby as a module or class, and the other is to implement it in Java and then register
 * it with the runtime. For the Pig datatypes we provide Ruby implementations for,
 * it was easier to implement them in Java and provide a Ruby interface via the annotations
 * that Jruby provides.
 *
 * Additionally, this class provides static object conversion functionality to and from Pig
 * and JRuby.
 */

public class PigJrubyLibrary implements Library {
    /**
     * This method is called from JRuby to register any classes in
     * the library.
     *
     * @param  runtime the current Ruby runtime
     * @param  wrap    ignored
     * @throws IOException
     */
    @Override
    public void load(Ruby runtime, boolean wrap) throws IOException {
        RubyDataBag.define(runtime);
        RubyDataByteArray.define(runtime);
        RubySchema.define(runtime);
    }

    private static final TupleFactory tupleFactory = TupleFactory.getInstance();

    /**
     * This method facilitates conversion from Ruby objects to Pig objects. This is
     * a general class which detects the subclass and invokes the appropriate conversion
     * routine. It will fail on an unsupported datatype.
     *
     * @param  rbObject      a Ruby object to convert
     * @return               the Pig analogue of the Ruby object
     * @throws ExecException if rbObject is not of a known type that can be converted
     */
    @SuppressWarnings("unchecked")
    public static Object rubyToPig(IRubyObject rbObject) throws ExecException {
        if (rbObject == null || rbObject instanceof RubyNil) {
            return null;
        } else if (rbObject instanceof RubyArray) {
            return rubyToPig((RubyArray)rbObject);
        } else if (rbObject instanceof RubyHash) {
            return rubyToPig((RubyHash)rbObject);
        } else if (rbObject instanceof RubyString) {
            return rubyToPig((RubyString)rbObject);
        } else if (rbObject instanceof RubyBignum) {
            return rubyToPig((RubyBignum)rbObject);
        } else if (rbObject instanceof RubyFixnum) {
            return rubyToPig((RubyFixnum)rbObject);
        } else if (rbObject instanceof RubyFloat) {
            return rubyToPig((RubyFloat)rbObject);
        } else if (rbObject instanceof RubyInteger) {
            return rubyToPig((RubyInteger)rbObject);
        } else if (rbObject instanceof RubyDataBag) {
            return rubyToPig((RubyDataBag)rbObject);
        } else if (rbObject instanceof RubyDataByteArray) {
            return rubyToPig((RubyDataByteArray)rbObject);
        } else if (rbObject instanceof RubySchema) {
            return rubyToPig((RubySchema)rbObject);
        } else if (rbObject instanceof RubyBoolean) {
            return rubyToPig((RubyBoolean)rbObject);
        } else {
            throw new ExecException("Cannot cast into any pig supported type: " + rbObject.getClass().getName());
        }
    }

    /**
     * A type specific conversion routine.
     *
     * @param  rbObject object to convert
     * @return          analogous Pig type
     */
    public static Tuple rubyToPig(RubyArray rbObject) throws ExecException {
        Tuple out = tupleFactory.newTuple(rbObject.size());
        int i = 0;
        for (IRubyObject arrayObj : rbObject.toJavaArray())
            out.set(i++, rubyToPig(arrayObj));
        return out;
    }

    //TODO need to convert to output a String
    /**
     * A type specific conversion routine.
     *
     * @param  rbObject object to convert
     * @return          analogous Pig type
     */
    @SuppressWarnings("unchecked")
    public static Map<String, ?> rubyToPig(RubyHash rbObject) throws ExecException {
        Map<String, Object> newMap = Maps.newHashMap();
        for (Map.Entry<Object, Object> entry : (Set<Map.Entry<Object, Object>>)rbObject.entrySet()) {
            Object key = entry.getKey();

            if (!(key instanceof String || key instanceof RubyString || key instanceof RubySymbol))
                throw new ExecException("Hash must have String or Symbol key. Was given: " + key.getClass().getName());

            String keyStr = key.toString();
            if (entry.getValue() instanceof IRubyObject) {
                newMap.put(keyStr, rubyToPig((IRubyObject)entry.getValue()));
            } else {
                newMap.put(keyStr, entry.getValue());
            }
        }
        return newMap;
    }

    /**
     * A type specific conversion routine.
     *
     * @param  rbObject object to convert
     * @return          analogous Pig type
     */
    public static Boolean rubyToPig(RubyBoolean rbObject) {
        return rbObject.isTrue();
    }

    /**
     * A type specific conversion routine.
     *
     * @param  rbObject object to convert
     * @return          analogous Pig type
     */
    public static Schema rubyToPig(RubySchema rbObject) {
        return rbObject.getInternalSchema();
    }

    /**
     * A type specific conversion routine.
     *
     * @param  rbObject object to convert
     * @return          analogous Pig type
     */
    public static String rubyToPig(RubyString rbObject) {
        return rbObject.toString();
    }

    /**
     * A type specific conversion routine.
     *
     * @param  rbObject object to convert
     * @return          analogous Pig type
     */
    public static Long rubyToPig(RubyBignum rbObject) {
        return rbObject.getLongValue();
    }

    /**
     * A type specific conversion routine.
     *
     * @param  rbObject object to convert
     * @return          analogous Pig type
     */
    public static Long rubyToPig(RubyFixnum rbObject) {
        return rbObject.getLongValue();
    }

    /**
     * A type specific conversion routine.
     *
     * @param  rbObject object to convert
     * @return          analogous Pig type
     */
    public static Double rubyToPig(RubyFloat rbObject) {
        return rbObject.getDoubleValue();
    }

    /**
     * A type specific conversion routine.
     *
     * @param  rbObject object to convert
     * @return          analogous Pig type
     */
    public static Integer rubyToPig(RubyInteger rbObject) {
        return (Integer)rbObject.toJava(Integer.class);
    }

    /**
     * A type specific conversion routine.
     *
     * @param  rbObject object to convert
     * @return          analogous Pig type
     */
    public static DataBag rubyToPig(RubyDataBag rbObject) {
        return rbObject.getBag();
    }

    /**
     * A type specific conversion routine.
     *
     * @param  rbObject object to convert
     * @return          analogous Pig type
     */
    public static DataByteArray rubyToPig(RubyDataByteArray rbObject) {
        return rbObject.getDBA();
    }

    /**
     * A type specific conversion routine.
     *
     * @param  rbObject object to convert
     * @return          analogous Pig type
     */
    public static Object rubyToPig(RubyNil rbObject) {
        return null;
    }

    /**
     * This is the method which provides conversion from Pig to Ruby. In this case, an
     * instance of the Ruby runtime is necessary. This method provides the general detection
     * of the type and then calls the more specific conversion methods.
     *
     * @param  ruby          the Ruby runtime to create objects in
     * @param  object        the Pig object to convert to Ruby
     * @return               Ruby analogue of object
     * @throws ExecException object is not a convertible Pig type
     */
    @SuppressWarnings("unchecked")
    public static IRubyObject pigToRuby(Ruby ruby, Object object) throws ExecException {
        if (object == null) {
            return ruby.getNil();
        } else if (object instanceof Tuple) {
            return pigToRuby(ruby, (Tuple)object);
        } else if (object instanceof DataBag) {
           return pigToRuby(ruby, (DataBag)object);
        } else if (object instanceof Map<?, ?>) {
            return pigToRuby(ruby, (Map<String, ?>)object);
        } else if (object instanceof DataByteArray) {
            return pigToRuby(ruby, (DataByteArray)object);
        } else if (object instanceof Schema) {
            return pigToRuby(ruby, ((Schema)object));
        } else if (object instanceof String) {
            return pigToRuby(ruby, (String)object);
        } else if (object instanceof Integer) {
            return pigToRuby(ruby, (Integer)object);
        } else if (object instanceof Long) {
            return pigToRuby(ruby, (Long)object);
        } else if (object instanceof Float) {
            return pigToRuby(ruby, (Float)object);
        } else if (object instanceof Double) {
            return pigToRuby(ruby, (Double)object);
        } else if (object instanceof Boolean) {
            return pigToRuby(ruby, (Boolean)object);
        } else {
            throw new ExecException("Object of unknown type " + object.getClass().getName() + " passed to pigToRuby");
        }
    }

    /**
     * A type specific conversion routine.
     *
     * @param  ruby          the Ruby runtime to create objects in
     * @param  object        object to convert
     * @return               analogous Ruby type
     * @throws ExecException object contained an object that could not convert
     */
    public static RubyArray pigToRuby(Ruby ruby, Tuple object) throws ExecException{
        RubyArray rubyArray = ruby.newArray();

        for (Object o : object.getAll())
            rubyArray.add(pigToRuby(ruby, o));

        return rubyArray;
    }

    /**
     * A type specific conversion routine.
     *
     * @param ruby   the Ruby runtime to create objects in
     * @param object object to convert
     * @return       analogous Ruby type
     */
    public static RubyBoolean pigToRuby(Ruby ruby, Boolean object) {
        return RubyBoolean.newBoolean(ruby, object.booleanValue());
    }

    /**
     * A type specific conversion routine.
     *
     * @param ruby   the Ruby runtime to create objects in
     * @param object object to convert
     * @return       analogous Ruby type
     */
    public static RubyDataBag pigToRuby(Ruby ruby, DataBag object) {
        return new RubyDataBag(ruby, ruby.getClass("DataBag"), object);
    }

    /**
     * A type specific conversion routine.
     *
     * @param ruby   the Ruby runtime to create objects in
     * @param object object to convert
     * @return       analogous Ruby type
     */
    public static RubySchema pigToRuby(Ruby ruby, Schema object) {
        return new RubySchema(ruby, ruby.getClass("Schema"), object);
    }

    /**
     * A type specific conversion routine for Pig Maps. This only deals with maps
     * with String keys, which is all that Pig supports.
     *
     * @param  ruby          the Ruby runtime to create objects in
     * @param  object        map to convert. In Pig, only maps with String keys are
     *                       supported
     * @return               analogous Ruby type
     * @throws ExecException object contains a key that can't be convert to a Ruby type
     */
    public static <T> RubyHash pigToRuby(Ruby ruby, Map<T, ?> object) throws ExecException {
        RubyHash newMap = RubyHash.newHash(ruby);

        boolean checkType = false;

        for (Map.Entry<T, ?> entry : object.entrySet()) {
            T key = entry.getKey();
            if (!checkType) {
                if (!(key instanceof String))
                    throw new ExecException("pigToRuby only supports converting Maps with String keys");
                checkType = true;
            }
            newMap.put(key, pigToRuby(ruby, entry.getValue()));
        }

        return newMap;
    }

    /**
     * A type specific conversion routine.
     *
     * @param ruby   the Ruby runtime to create objects in
     * @param object object to convert
     * @return       analogous Ruby type
     */
    public static RubyDataByteArray pigToRuby(Ruby ruby, DataByteArray object) {
        return new RubyDataByteArray(ruby, ruby.getClass("DataByteArray"), object);
    }

    /**
     * A type specific conversion routine.
     *
     * @param ruby   the Ruby runtime to create objects in
     * @param object object to convert
     * @return       analogous Ruby type
     */
    public static RubyString pigToRuby(Ruby ruby, String object) {
        return ruby.newString(object.toString());
    }

    /**
     * A type specific conversion routine.
     *
     * @param ruby   the Ruby runtime to create objects in
     * @param object object to convert
     * @return       analogous Ruby type
     */
    public static RubyFixnum pigToRuby(Ruby ruby, Integer object) {
        return RubyFixnum.newFixnum(ruby, object.longValue());
    }

    /**
     * A type specific conversion routine.
     *
     * @param ruby   the Ruby runtime to create objects in
     * @param object object to convert
     * @return       analogous Ruby type
     */
    public static RubyFixnum pigToRuby(Ruby ruby, Long object) {
        return RubyFixnum.newFixnum(ruby, object);
    }

    /**
     * A type specific conversion routine.
     *
     * @param ruby   the Ruby runtime to create objects in
     * @param object object to convert
     * @return       analogous Ruby type
     */
    public static RubyFloat pigToRuby(Ruby ruby, Float object) {
        return RubyFloat.newFloat(ruby, object.doubleValue());
    }

    /**
     * A type specific conversion routine.
     *
     * @param ruby   the Ruby runtime to create objects in
     * @param object object to convert
     * @return       analogous Ruby type
     */
    public static RubyFloat pigToRuby(Ruby ruby, Double object) {
        return RubyFloat.newFloat(ruby, object);
    }

    /**
     * A type specific conversion routine.
     *
     * @param ruby   the Ruby runtime to create objects in
     * @param object object to convert
     * @return       analogous Ruby type
     */
    private static final Method enumeratorizeMethod;
    static {
        try {
            enumeratorizeMethod = RubyEnumerator.class.getDeclaredMethod("enumeratorize", Ruby.class, IRubyObject.class, String.class);
            enumeratorizeMethod.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Enumeratorize method not found", e);
        }
    }

   /**
    * This is a hack to get around the fact that in JRuby 1.6.7, the enumeratorize method
    * isn't public. In 1.7.0, it will be made public and this can be gotten rid of, but
    * until then the Jruby API doesn't provide an easy way (or even a difficult way, really)
    * to provide this functionality; thus, it felt much cleaner to use reflection to make
    * public a method that will soon be public anyway instead of doing something much hairier.
    *
    * @param runtime the Ruby runtime to create objects in
    * @param obj     the Enumerable object to wrap
    * @param name    the name of the method that still needs a block (ie each or flatten)
    * @return        enumerator Ruby object wrapping the given Enumerable
    */
    public static IRubyObject enumeratorize(Ruby runtime, IRubyObject obj, String name) {
        try {
            return (IRubyObject)enumeratorizeMethod.invoke(null, runtime, obj, name);
        } catch (Exception e) {
            throw new RuntimeException("Unable to properly enumeratorize", e);
        }
    }
}
