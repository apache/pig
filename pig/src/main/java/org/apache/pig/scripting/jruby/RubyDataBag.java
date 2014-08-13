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

import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyBoolean;
import org.jruby.RubyClass;
import org.jruby.RubyEnumerator;
import org.jruby.RubyFixnum;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.RubySymbol;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.Block;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;

//TODO: need to fix the enumerator piece!
//TODO: need to fix the flatten semantics

/**
 * This provides a Ruby-esque way to interact with DataBag objects. It encapsulates
 * a bag object, and provides an easy to use interface. One difference between the
 * Ruby and the the Java API on DataBag is that in Ruby you iterate on the bag directly.
 * <p>
 * The RubyDataBag class  uses JRuby's API for the defintion Ruby class using Java code.
 * The comments in this class will more extensively explain the annotations for those not
 * familiar with JRuby.
 * <p>
 * In JRuby, the annotations are provided for convenience, and are detected and used
 * by the "defineAnnotatedMethods" method. The JRubyClass annotation sets the class name
 * as it will be seen in the Ruby runtime, and alows you to include any modules. In the
 * case of the RubyDataBag, within Ruby we just want it to be called DataBag, and we
 * want it to be enumerable.
 */
@JRubyClass(name = "DataBag", include = "Enumerable")
public class RubyDataBag extends RubyObject implements Iterable<Tuple> {
    private static final long serialVersionUID = 1L;
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static BagFactory mBagFactory = BagFactory.getInstance();

    private DataBag internalDB; // The encapsulated bag object

    public DataBag getBag() {
        return internalDB;
    }

    /**
     * This is an object allocator which is necessary for the define method.
     * Given a runtime and a klass object, it instantiates the default object.
     */
    private static final ObjectAllocator ALLOCATOR = new ObjectAllocator() {
        public IRubyObject allocate(Ruby runtime, RubyClass klass) {
            return new RubyDataBag(runtime, klass);
        }
    };

    /**
     * This method registers the class with the given runtime. It is not necessary to do this here,
     * but it is simpler to associate the methods necessary to register the class with the class
     * itself, so on the Library side it is possible to just specify "RubyDataBag.define(runtime)".
     *
     * @param runtime an instance of the Ruby runtime
     * @return        a RubyClass object with metadata about the registered class
     */
    public static RubyClass define(Ruby runtime) {
        // This generates the class object associated with DataBag, and registers it with the
        // runtime. The RubyClass object has all the metadata associated with a Class itself.
        RubyClass result = runtime.defineClass("DataBag", runtime.getObject(), ALLOCATOR);

        // This registers a method which can be used to know whether a module is an
        // instance of the class.
        result.kindOf = new RubyModule.KindOf() {
            public boolean isKindOf(IRubyObject obj, RubyModule type) {
                return obj instanceof RubyDataBag;
            }
        };

        // This includes the Enumerable module that we specified.
        result.includeModule(runtime.getEnumerable());

        // This method actually reads the annotations we placed and registers
        // all of the methods.
        result.defineAnnotatedMethods(RubyDataBag.class);

        // This returns the RubyClass object with all the new metadata.
        return result;
    }

    /**
     * This constructor encapsulated an empty bag.
     *
     * @param ruby an instance of the ruby runtime
     * @param rc   an instance of the class object with meatadata
     */
    protected RubyDataBag(final Ruby ruby, RubyClass rc) {
        super(ruby,rc);
        internalDB = mBagFactory.newDefaultBag();
    }

    /**
     * This constructor encapsulates the bag that is passed to it. Note:
     * the resultant RubyDataBag will encapsulated that bag directly, not
     * a copy.
     *
     * @param ruby an instance of the ruby runtime
     * @param rc   an instance of the class object with meatadata
     * @param db   a DataBag to encapsulate
     */
    protected RubyDataBag(final Ruby ruby, RubyClass rc, DataBag db) {
        super(ruby,rc);
        internalDB = db;
    }

    /**
     * The initialize method is the method used on the Ruby side to construct
     * the RubyDataBag object. The default is just an empty bag.
     *
     * @return the initialized RubyDataBag
     */
    @JRubyMethod
    @SuppressWarnings("deprecation")
    public RubyDataBag initialize() {
        internalDB = mBagFactory.newDefaultBag();
        return this;
    }

    /**
     * The initialize method can optionally receive a DataBag. In the case of
     * a RubyDataBag, a RubyDataBag will be returned that directly encapsulates it.
     *
     * @param arg an IRubyObject that is a RubyDataBag to encapsulate
     * @return    the initialized RubyDataBag
     */
    @JRubyMethod
    public RubyDataBag initialize(IRubyObject arg) {
        if (arg instanceof RubyDataBag) {
            internalDB = ((RubyDataBag)arg).getBag();
        } else {
            throw new IllegalArgumentException("Bag argument passed to DataBag initializer");
        }
        return this;
    }

    /**
     * This method deletes all of the entries in the underlying DataBag.
     */
    @JRubyMethod
    public void clear() {
        internalDB.clear();
    }

    /**
     * This returns whether the encapsulated DatBag is distinct, per the distinct setting.
     *
     * @param context the context the method is being executed in
     * @return        true if it the encapsulated is distinct, false otherwise
     */
    @JRubyMethod(name = {"distinct?", "is_distinct?"})
    public RubyBoolean isDistinct(ThreadContext context) {
        return RubyBoolean.newBoolean(context.getRuntime(), internalDB.isDistinct());
    }

    /**
     * This returns whether the encapsulated DatBag is distinct, per the sorted setting.
     *
     * @param context the context the method is being executed in
     * @return        true if it the encapsulated is sorted, false otherwise
     */
    @JRubyMethod(name = {"sorted?", "is_sorted?"})
    public RubyBoolean isSorted(ThreadContext context) {
        return RubyBoolean.newBoolean(context.getRuntime(), internalDB.isSorted());
    }

    /**
     * This returns the size of the encapsulated DataBag.
     *
     * @param context the context the method is being executed in
     * @return        the size of the encapsulated DataBag
     */
    @JRubyMethod(name={"size","length"})
    public RubyFixnum size(ThreadContext context) {
        return RubyFixnum.newFixnum(context.getRuntime(), internalDB.size());
    }

    /**
     * The add method accepts a varargs argument; each argument can be either a random
     * object, a DataBag, or a RubyArray. In the case of a random object, that object
     * will be converted to a Pig object and put into a Tuple. In the case of a
     * RubyArray, it will be treated as a Tuple and added. In the case of a DataBag,
     * it will iterate over the DataBag and add all of the elements to the element
     * encapsulated by RubyDataBag.
     *
     * @param context the context the method is being executed in
     * @param args    varargs passed to add. Each argument can be a RubyDataBag, whose
                      contents will be copied; a RubyArray, which will be treated as a
                      Tuple, or another object which will be converted over per
                      {@link PigJrubyLibrary#rubyToPig}.
     */
    @JRubyMethod(required = 1, rest = true)
    public void add(ThreadContext context, IRubyObject[] args) throws ExecException {
        for (IRubyObject arg : args) {
            if (arg instanceof RubyDataBag) {
               for (Tuple t : (RubyDataBag)arg)
                 internalDB.add(t);
            } else if (arg instanceof RubyArray) {
               internalDB.add(PigJrubyLibrary.rubyToPig((RubyArray)arg));
            } else {
               internalDB.add(mTupleFactory.newTuple(PigJrubyLibrary.rubyToPig(arg)));
            }
        }
    }

    /**
     * This method returns a copy of the encapsulated DataBag.
     *
     * @param context the context the method is being executed in
     * @return        the copied RubyDataBag
     */
    //TODO see if a deepcopy is necessary as well (and consider adding to DataBag and Tuple)
    @JRubyMethod
    public RubyDataBag clone(ThreadContext context) {
        DataBag b = mBagFactory.newDefaultBag();
        for (Tuple t : this)
            b.add(t);
        Ruby runtime = context.getRuntime();
        return new RubyDataBag(runtime, runtime.getClass("DataBag"), b);
    }

    /**
     * This method returns whether or not the encapsulated DataBag is empty.
     *
     * @param context the context the method is being executed in
     i @return        true if the encapsulated DAtaBag is empty, false otherwise
     */
    @JRubyMethod(name = "empty?")
    public RubyBoolean isEmpty(ThreadContext context) {
        return RubyBoolean.newBoolean(context.getRuntime(), internalDB.size() == 0);
    }

    /**
     * This method returns a string representation of the RubyDataBag. If given an optional
     * argument, then if that argument is true, the contents of the bag will also be printed.
     *
     * @param context the context the method is being executed in
     * @param args    optional true/false argument passed to inspect
     * @return        string representation of the RubyDataBag
     */
    @JRubyMethod(name = {"inspect", "to_s", "to_string"}, optional = 1)
    public RubyString inspect(ThreadContext context, IRubyObject[] args) {
        Ruby runtime = context.getRuntime();
        StringBuilder sb = new StringBuilder();
        sb.append("[DataBag: size: ").append(internalDB.size());
        if (args.length > 0 && args[0].isTrue())
            sb.append(" = ").append(internalDB.toString());
        sb.append("]");
        return RubyString.newString(runtime, sb);
    }

    public Iterator<Tuple> iterator() {
        return internalDB.iterator();
    }

    /**
     * This is an implementation of the each method which opens up the Enumerable interface,
     * and makes it very convenient to iterate over the elements of a DataBag. Note that currently,
     * due to a deficiency in JRuby, it is not possible to call each without a block given.
     *
     * @param context the context the method is being executed in
     * @param block   a block to call on the elements of the bag
     * @return        enumerator object if null block given, nil otherwise
     */
    @JRubyMethod
    public IRubyObject each(ThreadContext context, Block block) throws ExecException{
        Ruby runtime = context.getRuntime();

        if (!block.isGiven())
            return PigJrubyLibrary.enumeratorize(runtime, this, "each");
        /*  In a future release of JRuby when enumeratorize is made public (which is planned), should replace the above with the below
        if (!block.isGiven())
            return RubyEnumerator.enumeratorize(context.getRuntime(), this, "each");
        */

        for (Tuple t : this)
            block.yield(context, PigJrubyLibrary.pigToRuby(runtime, t));

        return context.nil;
    }

    //TODO let them specify which element will be returned, or if it will just iterate over each ie a true flatten
    /**
     * This is a convenience method which will run the given block on the first element
     * of each tuple contained.
     *
     * @param context the context the method is being executed in
     * @param block   a block to call on the elements of the bag
     * @return        enumerator object if null block given, nil otherwise
     */
    @JRubyMethod(name = {"flat_each", "flatten"})
    public IRubyObject flatten(ThreadContext context, Block block) throws ExecException {
        Ruby runtime = context.getRuntime();

        if (!block.isGiven())
            return PigJrubyLibrary.enumeratorize(runtime, this, "flatten");
        /*  In a future release of JRuby when enumeratorize is made public (which is planned), should replace the above with the below
        if (!block.isGiven())
            return RubyEnumerator.enumeratorize(context.getRuntime(), this, "flatten");
        */

        for (Tuple t : this)
            block.yield(context, PigJrubyLibrary.pigToRuby(runtime, t.get(0)));

        return context.nil;
    }
}
