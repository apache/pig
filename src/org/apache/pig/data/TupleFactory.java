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
package org.apache.pig.data;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTupleDefaultRawComparator;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * A factory to construct tuples.  This class is abstract so that users can
 * override the tuple factory if they desire to provide their own that
 * returns their implementation of a tuple.  If the property
 * pig.data.tuple.factory.name is set to a class name and
 * pig.data.tuple.factory.jar is set to a URL pointing to a jar that
 * contains the above named class, then {@link #getInstance()} will create a
 * an instance of the named class using the indicated jar.  Otherwise, it
 * will create an instance of {@link DefaultTupleFactory}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class TupleFactory implements TupleMaker<Tuple> {
    private static TupleFactory gSelf = null;

    /**
     * Get a reference to the singleton factory.
     * @return The TupleFactory to use to construct tuples.
     */
    public static TupleFactory getInstance() {
        if (gSelf == null) {
            String factoryName =
                System.getProperty("pig.data.tuple.factory.name");
            String factoryJar =
                System.getProperty("pig.data.tuple.factory.jar");
            if (factoryName != null && factoryJar != null) {
                try {
                    URL[] urls = new URL[1];
                    urls[0] = new URL(factoryJar);
                    ClassLoader loader = new URLClassLoader(urls,
                        TupleFactory.class.getClassLoader());
                    Class c = Class.forName(factoryName, true, loader);
                    Object o = c.newInstance();
                    if (!(o instanceof TupleFactory)) {
                        throw new RuntimeException("Provided factory " +
                            factoryName + " does not extend TupleFactory!");
                    }
                    gSelf = (TupleFactory)o;
                } catch (Exception e) {
                    if (e instanceof RuntimeException) {
                        // We just threw this
                        RuntimeException re = (RuntimeException)e;
                        throw re;
                    }
                    throw new RuntimeException("Unable to instantiate "
                        + "tuple factory " + factoryName, e);
                }
            } else if (factoryName != null) {
                try {
                    Class c = Class.forName(factoryName);
                    Object o = c.newInstance();
                    if (!(o instanceof TupleFactory)) {
                        throw new RuntimeException("Provided factory " +
                            factoryName + " does not extend TupleFactory!");
                    }
                    gSelf = (TupleFactory)o;
                } catch (Exception e) {
                    if (e instanceof RuntimeException) {
                      // We just threw this
                      RuntimeException re = (RuntimeException)e;
                      throw re;
                    }
                    throw new RuntimeException("Unable to instantiate "
                        + "tuple factory " + factoryName, e);
                }
            } else {
                gSelf = new BinSedesTupleFactory();
            }
        }
        return gSelf;
    }
    
    /**
     * Create an empty tuple.  This should be used as infrequently as
     * possible, use newTuple(int) instead.
     * @return Empty new tuple.
     */
    public abstract Tuple newTuple();

    /**
     * Create a tuple with size fields.  Whenever possible this is preferred
     * over the null constructor, as the constructor can preallocate the
     * size of the container holding the fields.  Once this is called, it
     * is legal to call Tuple.set(x, object), where x &lt; size.
     * @param size Number of fields in the tuple.
     * @return Tuple with size fields
     */
    public abstract Tuple newTuple(int size);
    
    /**
     * Create a tuple from the provided list of objects.  The underlying list
     * will be copied.
     * @param c List of objects to use as the fields of the tuple.
     * @return A tuple with the list objects as its fields
     */
    public abstract Tuple newTuple(List c);

    /**
     * Create a tuple from a provided list of objects, keeping the provided
     * list.  The new tuple will take over ownership of the provided list.
     * @param list List of objects that will become the fields of the tuple.
     * @return A tuple with the list objects as its fields
     */
    public abstract Tuple newTupleNoCopy(List list);

    /**
     * Create a tuple with a single element.  This is useful because of
     * the fact that bags (currently) only take tuples, we often end up
     * sticking a single element in a tuple in order to put it in a bag.
     * @param datum Datum to put in the tuple.
     * @return A tuple with one field
     */
    public abstract Tuple newTuple(Object datum);

    /**
     * Return the actual class representing a tuple that the implementing
     * factory will be returning.  This is needed because Hadoop needs
     * to know the exact class we will be using for input and output.
     * @return Class that implements tuple.
     */
    public abstract Class<? extends Tuple> tupleClass();
    
    protected TupleFactory() {
    }

    /**
     * Provided for testing purposes only.  This function should never be
     * called by anybody but the unit tests.
     */
    public static void resetSelf() {
        gSelf = null;
    }
    
    /**
     * Return the actual class implementing the raw comparator for tuples
     * that the factory will be returning. Ovverride this to allow Hadoop to
     * speed up tuple sorting. The actual returned class should know the
     * serialization details for the tuple. The default implementation 
     * (PigTupleDefaultRawComparator) will serialize the data before comparison
     * @return Class that implements tuple raw comparator.
     */
    public Class<? extends TupleRawComparator> tupleRawComparatorClass() {
        return PigTupleDefaultRawComparator.class;
    }

    /**
     * This method is used to inspect whether the Tuples created by this factory
     * will be of a fixed size when they are created. In practical terms, this means
     * whether they support append or not.
     * @return where the Tuple is fixed or not
     */
    public abstract boolean isFixedSize();

}

