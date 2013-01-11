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
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Arrays;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.data.DataType;
import org.apache.pig.parser.ParserException;
import org.apache.pig.impl.logicalLayer.FrontendException;

import org.jruby.Ruby;
import org.jruby.RubyHash;
import org.jruby.RubyArray;
import org.jruby.RubyClass;
import org.jruby.RubyFixnum;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.RubyRange;
import org.jruby.RubyString;
import org.jruby.RubySymbol;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.Block;
import org.jruby.runtime.builtin.IRubyObject;

//TODO implement all of the merge functions

/**
 * This class encapsulated a native Schema object, and provides a more convenient
 * interface for manipulating Schemas. It hides the Schema/FieldSchema distinction
 * from the user, and tries to present a cleaner, more Ruby-esque API to the user.
 * For general information on JRuby's API definition annotations,
 * see {@link RubyDataBag}.
 */
@JRubyClass(name = "Schema")
public class RubySchema extends RubyObject {

    private static final long serialVersionUID = 1L;

    /**
     * This is a pattern used in the conversion from ruby arguments to a valid Schema. It detects
     * cases where there is a bag, map, or tuple without being followed by {}, [], or () respectively.
     * It is used for convenience.
     */
    private static final Pattern bmtPattern = Pattern.compile("(?:\\S+:)?(bag|map|tuple)\\s*(?:,|$)", Pattern.CASE_INSENSITIVE);

    /**
     * This is the encapsulated Schema object.
     */
    private Schema internalSchema;

    private static final ObjectAllocator ALLOCATOR = new ObjectAllocator() {
        public IRubyObject allocate(Ruby runtime, RubyClass klass) {
            return new RubySchema(runtime, klass);
        }
    };

    /**
     * This method registers the class with the given runtime.
     *
     * @param runtime an instance of the Ruby runtime
     * @return        a RubyClass object with metadata about the registered class
     */
    public static RubyClass define(Ruby runtime) {
        RubyClass result = runtime.defineClass("Schema",runtime.getObject(), ALLOCATOR);

        result.kindOf = new RubyModule.KindOf() {
            public boolean isKindOf(IRubyObject obj, RubyModule type) {
                return obj instanceof RubySchema;
            }
        };

        result.includeModule(runtime.getEnumerable());

        result.defineAnnotatedMethods(RubySchema.class);

        return result;
    }

    protected RubySchema(final Ruby ruby, RubyClass rc) {
        super(ruby,rc);
        internalSchema = new Schema();
    }

    /**
     * This constructor sets the encapsulated Schema to be equal to
     * the given Schema. If copy is true, it is set equal to a copy.
     * If it is false, it is set directly equal.
     *
     * @param ruby an instance of the ruby runtime
     * @param rc   an instance of the class object with meatadata
     * @param s    a Schema to encapsulate
     * @param copy a boolean value. If true, s will be copied and the copy
     *             will be encapsulated. If false, it will be encapsulated
     *             directly.
     */
    protected RubySchema(final Ruby ruby, RubyClass rc, Schema s, boolean copy) {
        super(ruby,rc);
        if (copy) {
            internalSchema = new Schema(s);
        } else {
            internalSchema = s;
        }
    }

    /**
     * This constructor sets the encapsulated Schema to be equal to the
     * given Schema.
     *
     * @param ruby an instance of the ruby runtime
     * @param rc   an instance of the class object with meatadata
     * @param s    a Schema to encapsulate
     */
    protected RubySchema(final Ruby ruby, RubyClass rc, Schema s) {
        this(ruby, rc, s, true);
    }

    /**
     * This constructor is provided for convenience and sets the
     * internal Schema equal to the result of a call to
     * {@link Utils#getSchemaFromString}.
     *
     * @param ruby an instance of the ruby runtime
     * @param rc   an instance of the class object with meatadata
     * @param s    a String which will be passed to
     *             {@link Utils#getSchemaFromString}
     */
    protected RubySchema(final Ruby ruby, RubyClass rc, String s) {
        super(ruby, rc);
        try {
            internalSchema = Utils.getSchemaFromString(s);
        } catch (ParserException e) {
            throw new RuntimeException("Error converting String to Schema: " + s, e);
        }
    }

    /**
     * The ruby initializer accepts any number of arguments. With no arguments,
     * it will return an empty Schema object. It can accept any number of arguments.
     * To understand the valid arguments, see the documentation for {@link #rubyArgToSchema}.
     *
     * @param args a varargs which can take any number of valid arguments to
     *             {@link #rubyArgToSchema}
     * @return     the initialized RubySchema
     */
    @JRubyMethod(rest = true)
    public RubySchema initialize(IRubyObject[] args) {
        internalSchema = new Schema();
        for (IRubyObject arg : args) {
            Schema rs = rubyArgToSchema(arg);
            for (Schema.FieldSchema i : rs.getFields())
                internalSchema.add(i);
        }
        RubySchema.fixSchemaNames(internalSchema);
        return this;
    }

    /**
     * This is a static helper method to create a null aliased bytearray Schema.
     * This is useful in cases where you do not want the output to have an explicit
     * name, which {@link Utils#getSchemaFromString} will assign.
     *
     * @param context the context the method is being executed in
     * @param self    an instance of the RubyClass with metadata on
     *                the Ruby class object this method is being
     *                statically invoked against
     * @return        a null-aliased bytearray schema
     */
    @JRubyMethod(meta = true, name = {"by", "bytearray"})
    public static RubySchema nullBytearray(ThreadContext context, IRubyObject self) {
       return makeNullAliasRubySchema(context, DataType.BYTEARRAY);
    }

    /**
     * This is a static helper method to create a null aliased Boolean Schema.
     * This is useful in cases where you do not want the output to have an explicit
     * name, which {@link Utils#getSchemaFromString} will assign.
     *
     * @param context the context the method is being executed in
     * @param self    an instance of the RubyClass with metadata on
     *                the Ruby class object this method is being
     *                statically invoked against
     * @return        a null-aliased bytearray schema
     */
    @JRubyMethod(meta = true, name = {"bool", "boolean"})
    public static RubySchema nullBoolean(ThreadContext context, IRubyObject self) {
       return makeNullAliasRubySchema(context, DataType.BOOLEAN);
    }


    /**
     * This is a static helper method to create a null aliased chararray Schema.
     * This is useful in cases where you do not want the output to have an explicit
     * name, which {@link Utils#getSchemaFromString} will assign.
     *
     * @param context the context the method is being executed in
     * @param self    an instance of the RubyClass with metadata on
     *                the Ruby class object this method is being
     *                statically invoked against
     * @return        a null-aliased bytearray schema
     */
    @JRubyMethod(meta = true, name = {"c", "chararray"})
    public static RubySchema nullChararray(ThreadContext context, IRubyObject self) {
       return makeNullAliasRubySchema(context, DataType.CHARARRAY);
    }

    /**
     * This is a static helper method to create a null aliased long Schema.
     * This is useful in cases where you do not want the output to have an explicit
     * name, which {@link Utils#getSchemaFromString} will assign.
     *
     * @param context the context the method is being executed in
     * @param self    an instance of the RubyClass with metadata on
     *                the Ruby class object this method is being
     *                statically invoked against
     * @return        a null-aliased bytearray schema
     */
   @JRubyMethod(meta = true, name = {"l", "long"})
    public static RubySchema nullLong(ThreadContext context, IRubyObject self) {
       return makeNullAliasRubySchema(context, DataType.LONG);
    }

    /**
     * This is a static helper method to create a null aliased int Schema.
     * This is useful in cases where you do not want the output to have an explicit
     * name, which {@link Utils#getSchemaFromString} will assign.
     *
     * @param context the context the method is being executed in
     * @param self    an instance of the RubyClass with metadata on
     *                the Ruby class object this method is being
     *                statically invoked against
     * @return        a null-aliased bytearray schema
     */
    @JRubyMethod(meta = true, name = {"i", "int"})
    public static RubySchema nullInt(ThreadContext context, IRubyObject self) {
       return makeNullAliasRubySchema(context, DataType.INTEGER);
    }

    /**
     * This is a static helper method to create a null aliased double Schema.
     * This is useful in cases where you do not want the output to have an explicit
     * name, which {@link Utils#getSchemaFromString} will assign.
     *
     * @param context the context the method is being executed in
     * @param self    an instance of the RubyClass with metadata on
     *                the Ruby class object this method is being
     *                statically invoked against
     * @return        a null-aliased bytearray schema
     */
    @JRubyMethod(meta = true, name = {"d", "double"})
    public static RubySchema nullDouble(ThreadContext context, IRubyObject self) {
       return makeNullAliasRubySchema(context, DataType.DOUBLE);
    }

    /**
     * This is a static helper method to create a null aliased float Schema.
     * This is useful in cases where you do not want the output to have an explicit
     * name, which {@link Utils#getSchemaFromString} will assign.
     *
     * @param context the context the method is being executed in
     * @param self    an instance of the RubyClass with metadata on
     *                the Ruby class object this method is being
     *                statically invoked against
     * @return        a null-aliased bytearray schema
     */
    @JRubyMethod(meta = true, name = {"f", "float"})
    public static RubySchema nullFloate(ThreadContext context, IRubyObject self) {
       return makeNullAliasRubySchema(context, DataType.FLOAT);
    }

    /**
     * This is a static helper method to create a null aliased datetime Schema.
     * This is useful in cases where you do not want the output to have an explicit
     * name, which {@link Utils#getSchemaFromString} will assign.
     *
     * @param context the context the method is being executed in
     * @param self    an instance of the RubyClass with metadata on
     *                the Ruby class object this method is being
     *                statically invoked against
     * @return        a null-aliased bytearray schema
     */
    @JRubyMethod(meta = true, name = {"dt", "datetime"})
    public static RubySchema nullDateTime(ThreadContext context, IRubyObject self) {
       return makeNullAliasRubySchema(context, DataType.DATETIME);
    }

    /**
     * This is a static helper method to create a null aliased tuple Schema.
     * This is useful in cases where you do not want the output to have an explicit
     * name, which {@link Utils#getSchemaFromString} will assign.
     *
     * @param context the context the method is being executed in
     * @param self    an instance of the RubyClass with metadata on
     *                the Ruby class object this method is being
     *                statically invoked against
     * @return        a null-aliased bytearray schema
     */
    @JRubyMethod(meta = true, name = {"t", "tuple"})
    public static RubySchema nullTuple(ThreadContext context, IRubyObject self) {
       return makeNullAliasRubySchema(context, DataType.TUPLE);
    }

    /**
     * This is a static helper method to create a null aliased bag Schema.
     * This is useful in cases where you do not want the output to have an explicit
     * name, which {@link Utils#getSchemaFromString} will assign.
     *
     * @param context the context the method is being executed in
     * @param self    an instance of the RubyClass with metadata on
     *                the Ruby class object this method is being
     *                statically invoked against
     * @return        a null-aliased bytearray schema
     */
    @JRubyMethod(meta = true, name = {"b", "bag"})
    public static RubySchema nullBag(ThreadContext context, IRubyObject self) {
       return makeNullAliasRubySchema(context, DataType.BAG);
    }

    /**
     * This is a static helper method to create a null aliased map Schema.
     * This is useful in cases where you do not want the output to have an explicit
     * name, which {@link Utils#getSchemaFromString} will assign.
     *
     * @param context the context the method is being executed in
     * @param self    an instance of the RubyClass with metadata on
     *                the Ruby class object this method is being
     *                statically invoked against
     * @return        a null-aliased bytearray schema
     */
    @JRubyMethod(meta = true, name = {"m", "map"})
    public static RubySchema nullMap(ThreadContext context, IRubyObject self) {
       return makeNullAliasRubySchema(context, DataType.MAP);
    }

    /**
     * This is a helper method to generate a RubySchema of the given type without an alias.
     *
     * @param context the context the method is being executed in
     * @param type    the DataType.PIGTYPE value to make the Schema from
     * @return        a RubySchema object encapsulated a Schema of the specified type
     */
    private static RubySchema makeNullAliasRubySchema(ThreadContext context, byte type) {
       Ruby runtime = context.getRuntime();
       return new RubySchema(runtime, runtime.getClass("Schema"), new Schema(new Schema.FieldSchema(null, type)));
    }

    /**
     * This is a helper function which converts objects into Schema objects. The valid
     * options are as follows:
     * <p>
     * A RubyString, which will have {@link Utils#getSchemaFromString} called on it, and
     * it will be added.
     * <p>
     * A RubySchema, which will be added directly. IMPORTANT NOTE: since this API abstracts
     * away from the distinction between Schema/FieldSchema, its important to understand
     * how a Schema is added to another. In this case, the FieldSchema is pulled directly
     * out of the given Schema. Thus, where in Pig a Schema.FieldSchema might be passed around,
     * internally to this class, generally a Schema will be passed around encapsulating it.
     * <p>
     * A list will create the Schema for a Tuple whose elements will be the elements of the
     * list. Each element will be subjected to the same rules applied here.
     * <p>
     * A hash in the form of:<br>
     * <code>{"name:tuple"=>["x:int","y:int,z:int"], "name2:bag"=>["a:chararray"]}</code><br>
     * The keys must be a tuple, bag, or map, and the value must be an array.
     *
     * @param arg an object (generally an IRubyObject or String) to convert. See above for
                  the rules on valid arguments
     * @return    the Schema constructed for the given argument
     */
    public static Schema rubyArgToSchema(Object arg) {
        try {
            /**
             * Given a String or a RubyString, calls {@link Utils#getSchemaFromString}.
             * Additionally, as a convenience to the user, this method uses a regex to
             * detect any case where a schema declaration of "bag", "tuple", or "map"
             * does not have the trailing "{}", "()", or "[]" that
             * {@link Utils#getSchemaFromString} requires.
             */
            if (arg instanceof String || arg instanceof RubyString) {
                String s = arg.toString();
                Matcher m = bmtPattern.matcher(s);
                while (m.find()) {
                    String type = m.group(1);
                    String inter = s.substring(0, m.start(1));

                    if (type.equalsIgnoreCase("bag")) {
                         inter += "{}";
                    } else if (type.equalsIgnoreCase("map")) {
                         inter += "[]";
                    } else if (type.equalsIgnoreCase("tuple")) {
                         inter += "()";
                    } else {
                        throw new RuntimeException("Arriving here should be impossible");
                    }

                    s = inter + s.substring(m.end(1));
                    m = bmtPattern.matcher(s);
                }
                return Utils.getSchemaFromString(s);
            // In the case of a RubySchema, can just return the encapsulated Schema
            } else if (arg instanceof RubySchema) {
                return ((RubySchema)arg).getInternalSchema();
            // In the case of a RubyArray, the elements of the array are passed to this
            // method, and they will be treated as elements of a Tuple Schema.
            } else if (arg instanceof RubyArray) {
                RubyArray ary = (RubyArray)arg;
                Schema s = new Schema();
                for (Object o : ary) {
                    Schema ts = rubyArgToSchema(o);
                    for (Schema.FieldSchema fs : ts.getFields()) {
                      s.add(fs);
                    }
                }
                return new Schema(new Schema.FieldSchema("tuple_0", s, DataType.TUPLE));
            /**
             * In the case of a RubyHash, the key serves defines a Schema that will encapsulate
             * other elements. This mainly is for the convenience of being able to name
             * bags, maps, and tuples while easily being able to have interchangeable elements.
             * The key will be given to this method, but must return a singular map, tuple, or
             * bag, or an error will be thrown. The value to that key must be an array, and
             * each element will be passed to this method and then added to the Schema for
             * the key.
             */
            } else if (arg instanceof RubyHash) {
                RubyHash hash = (RubyHash)arg;
                Schema hashSchema = new Schema();
                for (Object o : hash.keySet()) {
                    Schema s = rubyArgToSchema(o);
                    if (s.size() != 1) {
                        throw new RuntimeException("Hash key must be singular");
                    }
                    Schema.FieldSchema fs = s.getField(0);
                    Object v = hash.get(o);
                    if (v instanceof RubyArray) {
                        byte type = fs.type;
                        if (type == DataType.BAG) {
                            fs.schema = rubyArgToSchema(v);
                        } else if (type == DataType.TUPLE || type == DataType.MAP) {
                            fs.schema = rubyArgToSchema(v).getField(0).schema;
                        } else {
                            throw new RuntimeException("Hash key must be tuple map or bag");
                        }
                    } else {
                        throw new RuntimeException("Hash value must be an Array");
                    }
                    hashSchema.add(fs);
                }
                return hashSchema;
            } else {
                throw new RuntimeException("Bad argument given to rubyToSchema: " + arg + (arg != null ? " class type " + arg.getClass().toString() : ""));
            }
        } catch (IOException e) {
            throw new RuntimeException("Error converting ruby to Schema: " + arg, e);
        }
    }

    /**
     * This is a ruby method which takes a name and an array of arguments and constructs a Tuple schema
     * from them.
     *
     * @param context the context the method is being executed in
     * @param self    the RubyClass for the Class object this was invoked on
     * @param arg1    the name for the RubySchema
     * @param arg2    a list of arguments to instantiate the new RubySchema
     * @return        the new Tuple RubySchema
     */
    @JRubyMethod(meta = true, name = {"t", "tuple"})
    public static RubySchema tuple(ThreadContext context, IRubyObject self, IRubyObject arg1, IRubyObject arg2) {
         RubySchema rs = tuple(context, self, arg2);
         rs.setNameIf(arg1);
         return rs;
    }

    /**
     * This is a ruby method which takes an array of arguments and constructs a Tuple schema from them. The name
     * will be set automatically.
     *
     * @param context the context the method is being executed in
     * @param self    the RubyClass for the Class object this was invoked on
     * @param arg     a list of arguments to instantiate the new RubySchema
     * @return        the new RubySchema
     */
    @JRubyMethod(meta = true, name = {"t", "tuple"})
    public static RubySchema tuple(ThreadContext context, IRubyObject self, IRubyObject arg) {
        if (arg instanceof RubyArray) {
            Schema s = rubyArgToSchema(arg);
            Ruby runtime = context.getRuntime();
            return new RubySchema(runtime, runtime.getClass("Schema"), s);
        } else {
            throw new RuntimeException("Bad argument given to Schema.tuple");
        }
    }

    /**
     * This is a ruby method which takes a name and an array of arguments and constructs a Map schema
     * from them.
     *
     * @param context the context the method is being executed in
     * @param self    the RubyClass for the Class object this was invoked on
     * @param arg1    the name for the RubySchema
     * @param arg2    a list of arguments to instantiate the new RubySchema
     * @return        the new RubySchema
     */
    @JRubyMethod(meta = true, name = {"m", "map"})
    public static RubySchema map(ThreadContext context, IRubyObject self, IRubyObject arg1, IRubyObject arg2) {
         RubySchema rs = map(context, self, arg2);
         rs.setNameIf(arg1);
         return rs;
    }

    /**
     * This is a ruby method which takes an array of arguments and constructs a Map schema from them. The name
     * will be set automatically.
     *
     * @param context the context the method is being executed in
     * @param self    the RubyClass for the Class object this was invoked on
     * @param arg     a list of arguments to instantiate the new RubySchema
     * @return        the new RubySchema
     */
    @JRubyMethod(meta = true, name = {"m", "map"})
    public static RubySchema map(ThreadContext context, IRubyObject self, IRubyObject arg) {
        Schema s = tuple(context, self, arg).getInternalSchema();
        Ruby runtime = context.getRuntime();
        try {
            return new RubySchema(runtime, runtime.getClass("Schema"), new Schema(new Schema.FieldSchema("map_0", s.getField(0).schema, DataType.MAP)));
        } catch (FrontendException e) {
            throw new RuntimeException("Error making map", e);
        }
    }

    /**
     * This is a ruby method which takes a name and an array of arguments and constructs a Bag schema
     * from them.
     *
     * @param context the context the method is being executed in
     * @param self    the RubyClass for the Class object this was invoked on
     * @param arg1    the name for the RubySchema
     * @param arg2    a list of arguments to instantiate the new RubySchema
     * @return        the new RubySchema
     */
    @JRubyMethod(meta = true, name={"b", "bag"})
    public static RubySchema bag(ThreadContext context, IRubyObject self, IRubyObject arg1, IRubyObject arg2) {
         RubySchema rs = bag(context, self, arg2);
         rs.setNameIf(arg1);
         return rs;
    }

    /**
     * This is a ruby method which takes an array of arguments and constructs a Bag schema from them. The name
     * will be set automatically.
     *
     * @param context the context the method is being executed in
     * @param self    the RubyClass for the Class object this was invoked on
     * @param arg     a list of arguments to instantiate the new RubySchema
     * @return        the new RubySchema
     */
    @JRubyMethod(meta = true, name = {"b", "bag"})
    public static RubySchema bag(ThreadContext context, IRubyObject self, IRubyObject arg) {
        Schema s = tuple(context, self, arg).getInternalSchema();
        Ruby runtime = context.getRuntime();
        try {
            return new RubySchema(runtime, runtime.getClass("Schema"), new Schema(new Schema.FieldSchema("bag_0", s, DataType.BAG)));
        } catch (FrontendException e) {
            throw new RuntimeException("Error making map", e);
        }
    }

    /**
     * This method will fix any name conflicts in a schema. It's important to note that
     * this will change the Schema object itself. It will deal with any collisions in things
     * named tuple_#, bag_#, map_#, or val_#, as these are generally names generated by
     * Util.getSchemaFromString. In the case of another name conflict, it will not be
     * changed, as that name conflict was created by the user.
     *
     * @param s a Schema object to fix in place
     */
    private static void fixSchemaNames(Schema s) {
        if (s == null)
             return;
        // This regex detects names that could possibly collide that we should change
        Pattern p = Pattern.compile("(bag_|tuple_|map_|val_)(\\d+)", Pattern.CASE_INSENSITIVE);
        Set<String> names = new HashSet<String>(s.size(), 1.0f);
        for (Schema.FieldSchema fs : s.getFields()) {
            if (fs.alias == null)
                 continue;
            Matcher m = p.matcher(fs.alias);
            if (m.matches() && names.contains(fs.alias)) {
                String prefix = m.group(1);
                int suffix = Integer.parseInt(m.group(2));
                while (names.contains(prefix + suffix))
                    suffix++;
                fs.alias = prefix + suffix;
            }
            names.add(fs.alias);
            if (fs.schema != null) {
                if (fs.type == DataType.BAG) {
                    try {
                        fixSchemaNames(fs.schema.getField(0).schema);
                    } catch (FrontendException e) {
                        throw new RuntimeException("Error recursively fixing schema: " + s, e);
                    }
                } else {
                    fixSchemaNames(fs.schema);
                }
            }
        }
    }

    /**
     * This is just a convenience method which sets the name of the internalSchema to the argument that was given.
     *
     * @param arg a RubyString to set the name of the encapsulated Schema object
     */
    private void setNameIf(IRubyObject arg) {
        if (arg instanceof RubyString) {
            setName(arg.toString());
        } else {
            throw new RuntimeException("Bad name given");
        }
    }

    /**
     * This method sets the name of a RubySchema to the name given. It's important to note that
     * if the RubySchema represents anything other than a tuple, databag, or map then an error
     * will be thrown.
     *
     * @param name a String to set the name of the encapsulated Schema object
     */
    private void setName(String name) {
        Schema.FieldSchema fs;

        try {
            fs = internalSchema.getField(0);
        } catch (FrontendException e) {
            throw new RuntimeException("Error getting field from schema: " + internalSchema, e);
        }

        byte type = fs.type;

        if (type == DataType.TUPLE || type == DataType.BAG || type == DataType.MAP) {
            fs.alias = name;
        } else {
            throw new RuntimeException("setName cannot be set on Schema: " + internalSchema);
        }
    }

    /**
     * The toString method just leverages Schema's printing.
     *
     * @param context the context the method is being executed in
     * @return        a String representation of the encapsulated Schema object
     */
    @JRubyMethod(name = {"to_s", "inspect"})
    public RubyString toString(ThreadContext context) {
        return RubyString.newString(context.getRuntime(), internalSchema.toString());
    }

    /**
     * This is the ruby method which allows people to access elements of the RubySchema object.
     * It can be given either a single numeric index, or a Range object to specify a range of indices.
     * It's important to note that the Schema object returned from this references the Schema stored
     * internally, so if the user wants to make changes without affecting this object, it must be cloned.
     *
     * @param context the context the method is being executed in
     * @param arg     a Fixnum index, Range object to specify a range of values to return, or
     *                a String to look up by alias name
     * @return        the RubySchema object encapsulated the found Schema
     */
    @JRubyMethod(name = {"[]", "slice"})
    public RubySchema get(ThreadContext context, IRubyObject arg) {
        Ruby runtime = context.getRuntime();
        if (arg instanceof RubyFixnum) {
            int index = (int)((RubyFixnum)arg).getLongValue();
            Schema s;
            try {
                s = new Schema(internalSchema.getField(index));
            } catch (FrontendException e) {
                throw new RuntimeException("Invalid index given to get function: " + index, e);
            }
            return new RubySchema(runtime, runtime.getClass("Schema"), s, false); //returns the actual object itself
        } else if (arg instanceof RubyRange) {
            int min = (int)((RubyFixnum)((RubyRange)arg).min(context, Block.NULL_BLOCK)).getLongValue();
            int max = (int)((RubyFixnum)((RubyRange)arg).max(context, Block.NULL_BLOCK)).getLongValue();
            return new RubySchema(runtime, runtime.getClass("Schema"), new Schema(internalSchema.getFields().subList(min, max + 1)), false);
        } else if (arg instanceof RubyString) {
             try {
                 return new RubySchema(runtime, runtime.getClass("Schema"), new Schema(internalSchema.getField(arg.toString())), false);
             } catch (FrontendException e) {
                 throw new RuntimeException("Unable to find field " + arg.toString() + " in schema " + internalSchema, e);
             }
        } else {
            throw new RuntimeException("Invalid argument given to get function: " + arg.toString());
        }
    }

    /**
     * This is a version of [] which allows the range to be specified as such: [1,2].
     *
     * @param context the context the method is being executed in
     * @param arg1    a Fixnum start index
     * @param arg2    a Fixnum end index
     * @return        the RubySchema object encapsulated the found Schema
     */
    @JRubyMethod(name = {"[]", "slice"})
    public RubySchema get(ThreadContext context, IRubyObject arg1, IRubyObject arg2) {
        if (arg1 instanceof RubyFixnum && arg2 instanceof RubyFixnum) {
            Ruby runtime = context.getRuntime();
            int min = (int)((RubyFixnum)arg1).getLongValue();
            int max = (int)((RubyFixnum)arg2).getLongValue() - 1;
            return new RubySchema(runtime, runtime.getClass("Schema"), new Schema(internalSchema.getFields().subList(min, max + 1)), false);
        } else {
            throw new RuntimeException("Bad arguments given to get function: ( " + arg1.toString() + " , " + arg2.toString()+ " )");
        }
    }

    /**
     * This allows the users to set an index or a range of values to
     * a specified RubySchema. The first argument must be a Fixnum or Range,
     * and the second argument may optionally be a Fixnum. The given index
     * (or range of indices) will be replaced by a RubySchema instantiated
     * based on the remaining arguments.
     *
     * @param context the contextthe method is being executed in
     * @param args    a varargs which has to be at least length two.
     * @return        the RubySchema that was added
     */
    @JRubyMethod(name = {"[]=", "set"}, required = 2, rest = true)
    public RubySchema set(ThreadContext context, IRubyObject[] args) {
        IRubyObject arg1 = args[0];
        IRubyObject arg2 = args[1];
        IRubyObject[] arg3 = Arrays.copyOfRange(args, 1, args.length);
        Schema s = internalSchema;
        Ruby runtime = context.getRuntime();
        List<Schema.FieldSchema> lfs = s.getFields();
        int min, max;
        if (arg1 instanceof RubyFixnum && arg2 instanceof RubyFixnum) {
            min = (int)((RubyFixnum)arg1).getLongValue();
            max = (int)((RubyFixnum)arg2).getLongValue();
            arg3 = Arrays.copyOfRange(args, 2, args.length);
        } else if (arg1 instanceof RubyFixnum) {
            min = (int)((RubyFixnum)arg1).getLongValue();
            max = min + 1;
        } else if (arg1 instanceof RubyRange) {
            min = (int)((RubyFixnum)((RubyRange)arg1).min(context, Block.NULL_BLOCK)).getLongValue();
            max = (int)((RubyFixnum)((RubyRange)arg1).max(context, Block.NULL_BLOCK)).getLongValue() + 1;
        } else {
            throw new RuntimeException("Bad arguments given to get function: ( " + arg1.toString() + " , " + arg2.toString()+ " )");
        }
        for (int i = min; i < max; i++)
            lfs.remove(min);
        if (arg3 == null || arg3.length == 0)
            throw new RuntimeException("Must have schema argument for []=");
        RubySchema rs = new RubySchema(runtime, runtime.getClass("Schema")).initialize(arg3);
        for (Schema.FieldSchema fs : rs.getInternalSchema().getFields())
            lfs.add(min++, fs);
        RubySchema.fixSchemaNames(internalSchema);
        return rs;
    }

    /**
     * This method provides addition semantics, without modifying the original Schema.
     * This method can be given any number of arguments, much as with the constructor.
     *
     * @param context the context the method is being executed in
     * @param args    a varargs which can be any valid set of arguments that
     *                can initialize a RubySchema
     * @return        the Rresult of the addition
     */
    @JRubyMethod(name = {"add", "+"}, rest = true)
    public RubySchema add(ThreadContext context, IRubyObject[] args) {
        RubySchema rsClone = clone(context);
        rsClone.addInPlace(context, args);
        return rsClone;
    }

    /**
     * This method provides addition semantics, modifying the original Schema in place.
     * This method can be given any number of arguments, much as with the constructor.
     *
     * @param context the context the method is being executed in
     * @param args    a varargs which can be any valid set of arguments that
     *                can initialize a RubySchema
     */
    @JRubyMethod(name = "add!", rest = true)
    public void addInPlace(ThreadContext context, IRubyObject[] args) {
        Ruby runtime = context.getRuntime();
        List<Schema.FieldSchema> lfs = internalSchema.getFields();
        RubySchema rs = new RubySchema(runtime, runtime.getClass("Schema")).initialize(args);
        for (Schema.FieldSchema fs : rs.getInternalSchema().getFields())
            lfs.add(fs);
        RubySchema.fixSchemaNames(internalSchema);
    }

    /**
     * @param context the context the method is being executed in
     * @return        a RubySchema copy of the Schema
     */
    @JRubyMethod
    public RubySchema clone(ThreadContext context) {
        Ruby runtime = context.getRuntime();
        return new RubySchema(runtime, runtime.getClass("Schema"), internalSchema);
    }

    /**
     * Given a field name this string will search the RubySchema for a FieldSchema
     * with that name and return it encapsulated in a Schema.
     *
     * @param context the context the method is being executed in
     * @param arg     a RubyString serving as an alias to look
     *                for in the Schema
     * @return        the found RubySchema
     */
    @JRubyMethod
    public RubySchema find(ThreadContext context, IRubyObject arg) {
        if (arg instanceof RubyString) {
            Ruby runtime = context.getRuntime();
            return new RubySchema(runtime, runtime.getClass("Schema"), RubySchema.find(internalSchema, arg.toString()), false);
        } else {
            throw new RuntimeException("Invalid arguement passed to find: " + arg);
        }
    }

    /**
     * This is a helper method which recursively searches for an alias in the Schema
     * encapsulated by RubySchema. This is necessary because findFieldSchema uses
     * canonicalName, not name.
     *
     * @param s     the Schema to search through
     * @param alias
     * @return      the found RubySchema
     */
    private static Schema find(Schema s, String alias) {
        for (Schema.FieldSchema fs : s.getFields())
            if (alias.equals(fs.alias))
                 return new Schema(fs);
        for (Schema.FieldSchema fs : s.getFields())
            if (fs.schema != null) {
                 Schema r = RubySchema.find(fs.schema, alias);
                 if (r != null)
                     return r;
            }
        return new Schema();
    }

    /**
     * Given a field name, this will return the index of it in the schema.
     *
     * @param context the context the method is being executed in
     * @param arg     a field name to look for
     * @return        the index for that field name
     */
    @JRubyMethod
    public RubyFixnum index(ThreadContext context, IRubyObject arg) {
        if (arg instanceof RubyString) {
            try {
                return new RubyFixnum(context.getRuntime(), internalSchema.getPosition(arg.toString()));
            } catch (FrontendException e) {
                throw new RuntimeException("Unable to find position for argument: " + arg);
            }
        } else {
            throw new RuntimeException("Invalid arguement passed to index: " + arg);
        }
    }

    /**
     * @param context the context the method is being executed in
     * @return        the size of the encapsulated Schema
     */
    @JRubyMethod(name = {"size", "length"})
    public RubyFixnum size(ThreadContext context) {
        return new RubyFixnum(context.getRuntime(), internalSchema.size());
    }

    /**
     * This is a helper method to pull out the native Java type from the ruby object.
     *
     * @return the encapsulated Schema
     */
    public Schema getInternalSchema() {
        return internalSchema;
    }

    /**
     * This method allows access into the Schema nested in the encapsulated Schema. For example,
     * if the encapsulated Schema is a bag Schema, this allows the user to access the schema of
     * the interior Tuple.
     *
     * @param context the context the method is being executed in
     * @return        a RubySchema encapsulating the nested Schema
     */
    @JRubyMethod(name = {"get", "inner", "in"})
    public RubySchema get(ThreadContext context) {
        if (internalSchema.size() != 1)
            throw new RuntimeException("Can only return nested schema if there is one schema to get");
        Ruby runtime = context.getRuntime();
        try {
            return new RubySchema(runtime, runtime.getClass("Schema"), internalSchema.getField(0).schema, false);
        } catch (FrontendException e) {
            throw new RuntimeException("Schema does not have a nested FieldScema", e);
        }
    }

    /**
     * This method allows the user to see the name of the alias of the FieldSchema of the encapsulated
     * Schema. This method only works if the Schema has one FieldSchema.
     *
     * @param context the context the method is being executed in
     * @return        the name of the Schema
     */
    @JRubyMethod(name = "name")
    public RubyString getName(ThreadContext context) {
        try {
            if (internalSchema.size() != 1)
                 throw new RuntimeException("Can only get name if there is one schema present");

            return RubyString.newString(context.getRuntime(), internalSchema.getField(0).alias);
        } catch (FrontendException e) {
            throw new RuntimeException("Unable to get field from Schema", e);
        }
    }

    /**
     * This method allows the user to set the name of the alias of the FieldSchema of the encapsulated
     * Schema. This method only works if the Schema has one FieldSchema.
     *
     * @param arg a RubyString to set the name to
     * @return    the new name
     */
    @JRubyMethod(name = "name=")
    public RubyString setName(IRubyObject arg) {
        if (arg instanceof RubyString) {
             if (internalSchema.size() != 1)
                 throw new RuntimeException("Can only set name if there is one schema present");
             try {
                 internalSchema.getField(0).alias = arg.toString();
                 return (RubyString)arg;
             } catch (FrontendException e) {
                 throw new RuntimeException("Unable to get field from Schema", e);
             }
        } else {
             throw new RuntimeException("Improper argument passed to 'name=':" + arg);
        }
    }
}
