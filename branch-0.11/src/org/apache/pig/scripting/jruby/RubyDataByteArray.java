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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataByteArray;

import org.jruby.Ruby;
import org.jruby.RubyBoolean;
import org.jruby.RubyClass;
import org.jruby.RubyEnumerator;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.RubySymbol;
import org.jruby.RubyString;
import org.jruby.RubyFixnum;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.javasupport.JavaUtil;
import org.jruby.javasupport.JavaObject;

/**
 * This class presents a native Ruby object for interacting with and manipulating
 * DataByteArray objects. For more information on what the annotations mean, see
 * {@link RubyDataBag}.
 */
@JRubyClass(name="DataByteArray")
public class RubyDataByteArray extends RubyObject {
    private static final long serialVersionUID = 1L;
    private static final ObjectAllocator ALLOCATOR = new ObjectAllocator() {
        public IRubyObject allocate(Ruby runtime, RubyClass klass) {
            return new RubyDataByteArray(runtime, klass);
        }
    };


    /**
     * Registers the DataByteArray class with the Ruby runtime.
     *
     * @param runtime an instance of the Ruby runtime
     * @return a RubyClass object with metadata about the registered class
     */
    public static RubyClass define(Ruby runtime) {
        RubyClass result = runtime.defineClass("DataByteArray", runtime.getObject(), ALLOCATOR);

        result.kindOf = new RubyModule.KindOf() {
            public boolean isKindOf(IRubyObject obj, RubyModule type) {
                return obj instanceof RubyDataByteArray;
            }
        };

        result.defineAnnotatedMethods(RubyDataByteArray.class);

        return result;
    }

    private DataByteArray internalDBA;

    /**
     * This constructor encapsulated a null DataByteArray.
     *
     * @param ruby an instance of the ruby runtime
     * @param rc   an instance of the class object with meatadata
     */
    protected RubyDataByteArray(final Ruby ruby, RubyClass rc) {
        super(ruby,rc);
        internalDBA = new DataByteArray();
    }

    /**
     * Given a DataByteArray, this constructor creates a new one which copies the underlying bytes.
     *
     * @param ruby an instance of the ruby runtime
     * @param rc   an instance of the class object with meatadata
     * @param dba  a DataByteArray to copy then encapsulate
     */
    protected RubyDataByteArray(final Ruby ruby, RubyClass rc, DataByteArray dba) {
        this(ruby, rc);
        byte[] buf1 = dba.get();
        byte[] buf2 = new byte[buf1.length];
        System.arraycopy(buf1, 0, buf2, 0, buf1.length);
        internalDBA = new DataByteArray(buf2);
    }

    /**
     * This constructor makes a RubyDataByteArray whose underlying bytes are a concatenation
     * of the given bytes. The new DataByteArray will not point to the original bytes.
     *
     * @param ruby an instance of the ruby runtime
     * @param rc   an instance of the class object with meatadata
     * @param dba1 first DataByteArray in the concatentation
     * @param dba2 second DAtaByteArray whose bytes will be concatenated to the first's
     */
    protected RubyDataByteArray(final Ruby ruby, RubyClass rc, DataByteArray dba1, DataByteArray dba2) {
        super(ruby, rc);
        internalDBA = new DataByteArray(dba1, dba2);
    }

    /**
     * This constructor creates a new DataByteArray with a reference to the provided bytes.
     *
     * @param ruby an instance of the ruby runtime
     * @param rc   an instance of the class object with meatadata
     * @param buf  a byte array to encapsulate directly
     */
    protected RubyDataByteArray(final Ruby ruby, RubyClass rc, byte[] buf) {
        super(ruby,rc);
        internalDBA = new DataByteArray(buf);
    }

    public DataByteArray getDBA() {
        return internalDBA;
    }

    /**
     * This is the default initializer, which returns an empty DataByteArray.
     *
     * @return the initialized RubyDataByteArray
     */
    @JRubyMethod
    @SuppressWarnings("deprecation")
    public RubyDataByteArray initialize() {
        internalDBA = new DataByteArray();
        return this;
    }

    /**
     * Given a String or a set of bytes[], initializes the encapsulated DataByteArray
     * using {@link DataByteArray#set}. In the case of a DataByteArray, will copy
     * the underlying bytes.
     *
     * @param arg a value to set the encapsulated DataByteArray to. A DataByteArray
                  will be copied, whereas a byte array will be encapsulated directly,
                  and a string's bits will be used per {@link DataByteArray#set}.
     * @return    the initialized RubyDataByteArray
     */
    @JRubyMethod
    public RubyDataByteArray initialize(IRubyObject arg) {
        if (arg instanceof RubyString) {
            internalDBA.set(arg.toString());
        } else if (arg instanceof RubyDataByteArray) {
            byte[] buf1 = ((RubyDataByteArray)arg).getDBA().get();
            byte[] buf2 = new byte[buf1.length];
            System.arraycopy(buf1, 0, buf2, 0, buf1.length);
            internalDBA = new DataByteArray(buf2);
        } else {
            internalDBA.set((byte[])arg.toJava(byte[].class));
        }
        return this;
    }

    /**
     * Given two RubyDataByteArrays, initializes the encapsulated DataByteArray
     * to be a concatentation of the copied bits of the first and second.
     *
     * @param arg1 the first RubyDataByteArray whose bits will be used
     * @param arg2 the second RubyDataByteArray whose bits will be concatenated
                   after the first's
     * @return     the initialized RubyDataBytearray
     */
    @JRubyMethod
    public RubyDataByteArray initialize(IRubyObject arg1, IRubyObject arg2) {
        if (arg1 instanceof RubyDataByteArray && arg2 instanceof RubyDataByteArray) {
           internalDBA = new DataByteArray(((RubyDataByteArray)arg1).getDBA(), ((RubyDataByteArray)arg2).getDBA());
        } else {
            throw new IllegalArgumentException("Invalid arguments passed to DataByteArray");
        }
        return this;
    }

    /**
     * This calls to the append function of the underlying DataByteArray. This accepts
     * the same types that set accepts.
     *
     * @param context the context the method is being executed in
     * @param arg a value to append to the encpasulated DataByteArray. In the case of a
     *            RubyDataByteArray, the bytes will be copied and appended; in the case
     *            of a String, the bits will be added; in the case of a byte array,
     *            the bytes will be appended directly.
     */
    @JRubyMethod(name = {"add!", "append"})
    public void append(ThreadContext context, IRubyObject arg) {
        Ruby runtime = context.getRuntime();
        RubyDataByteArray toAppend = new RubyDataByteArray(runtime, runtime.getClass("DataByteArray")).initialize(arg);
        internalDBA.append(toAppend.getDBA());
    }

    /**
     * This calls to the static method compare of DataByteArray. Given two DataByteArrays, it will call it
     * on the underlying bytes.
     *
     * @param context the context the method is being executed in
     * @param self    an class which contains metadata on the calling class (required for static Ruby methods)
     * @param arg1    a RubyDataByteArray or byte array to compare
     * @param arg2    a RubyDataByteArray or byte array to compare
     * @return        the Fixnum result of comparing arg1 and arg2's bytes
     */
    @JRubyMethod
    public static RubyFixnum compare(ThreadContext context, IRubyObject self, IRubyObject arg1, IRubyObject arg2) {
        byte[] buf1, buf2;
        if (arg1 instanceof RubyDataByteArray) {
            buf1 = ((RubyDataByteArray)arg1).getDBA().get();
        } else {
            buf1 = (byte[])arg1.toJava(byte[].class);
        }
        if (arg2 instanceof RubyDataByteArray) {
            buf2 = ((RubyDataByteArray)arg2).getDBA().get();
        } else {
            buf2 = (byte[])arg2.toJava(byte[].class);
        }
        return RubyFixnum.newFixnum(context.getRuntime(), DataByteArray.compare(buf1, buf2));
    }

    /**
     * This calls the compareTo method of the encapsulated DataByteArray.
     *
     * @param context the context the method is being executed in
     * @param arg     a RubyDataByteArray or byte array to compare to the
     *                encapsulated DataByteArray
     * @return        the result of compareTo on the encapsulated DataByteArray
                      and arg
     */
    @JRubyMethod
    public RubyFixnum compareTo(ThreadContext context, IRubyObject arg) {
        int compResult;
        if (arg instanceof RubyDataByteArray) {
            compResult = internalDBA.compareTo(((RubyDataByteArray)arg).getDBA());
        } else {
            compResult = internalDBA.compareTo(new DataByteArray((byte[])arg.toJava(byte[].class)));
        }
        return RubyFixnum.newFixnum(context.getRuntime(), compResult);
    }

    /**
     * Overrides equality leveraging DataByteArray's equality.
     *
     * @param context the context the method is being executed in
     * @param arg     a RubyDataByteArray against which to test equality
     * @return        true if they are equal, false otherwise
     */
    @JRubyMethod(name = {"eql?", "=="})
    public RubyBoolean equals(ThreadContext context, IRubyObject arg) {
        Ruby runtime = context.getRuntime();
        if (arg instanceof RubyDataByteArray) {
            return RubyBoolean.newBoolean(runtime, internalDBA.equals(((RubyDataByteArray)arg).getDBA()));
        } else {
            return runtime.getFalse();
        }
    }

    /**
     * Overrides ruby's hash leveraging DataByteArray's hash.
     *
     * @param context the context the method is being executed in
     * @return        the encapsulated DataByteArray's hashCode()
     */
    @JRubyMethod
    public IRubyObject hash(ThreadContext context) {
        return RubyFixnum.newFixnum(context.getRuntime(), internalDBA.hashCode());
    }

    /**
     * This method calls the set method of the underlying DataByteArray with one exception:
     * if given a RubyDataByteArray, it will set the encapsulated DataByteArray to be equal.
     *
     * @param arg an object to set the encapsulated DataByteArray's bits to. In the case of
     *            a RubyString or byte array, the underlying bytes will be sit directly. In
     *            the case of a RubyDataByteArray, the encapsulated DataByteArray will be set
     *            equal to arg.
     */
    @JRubyMethod
    public void set(IRubyObject arg) {
        if (arg instanceof RubyDataByteArray) {
            internalDBA = ((RubyDataByteArray)arg).getDBA();
        } else if (arg instanceof RubyString) {
            internalDBA.set(arg.toString());
        } else {
            internalDBA.set((byte[])arg.toJava(byte[].class));
        }
    }

    /**
     * @param context the context the method is being executed in
     * @return the size of the encapsulated DataByteArray
     */
    @JRubyMethod(name = {"size", "length"})
    public RubyFixnum size(ThreadContext context) {
        return RubyFixnum.newFixnum(context.getRuntime(), internalDBA.size());
    }

    /**
     * This method accepts as an argument anything that could be passed to set (ie a
     * RubyDataByteArray, RubyString, or byte array). It then adds those values together.
     *
     * @param context the context the method is being executed in
     * @param arg     any argument that can validly initialize a RubyDataByteArray
     * @return        a <u>new</u> RubyDataByteArray that is the concatentation of
     *                the encapsulated DataByteArray and arg
     */
    @JRubyMethod(name = "+")
    public IRubyObject plus(ThreadContext context, IRubyObject arg) {
        Ruby runtime = context.getRuntime();
        RubyDataByteArray toAdd = new RubyDataByteArray(runtime, runtime.getClass("DataByteArray")).initialize(arg);
        return new RubyDataByteArray(runtime, runtime.getClass("DataByteArray"), internalDBA, toAdd.getDBA());
    }

    /**
     * @param context the context the method is being executed in
     * @return        the string representation of the encapsulated DataByteArray
     */
    @JRubyMethod(name = {"to_s", "inspect"})
    public IRubyObject toString(ThreadContext context) {
        return RubyString.newString(context.getRuntime(), internalDBA.toString());
    }
}
