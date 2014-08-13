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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.SchemaTuple.SchemaTupleQuickGenerator;
import org.apache.pig.data.SchemaTupleClassGenerator.GenContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This is an implementation of TupleFactory that will instantiate
 * SchemaTuple's. This class has nothing to do with the actual generation
 * of code, and instead simply encapsulates the classes which allow
 * for efficiently creating SchemaTuples.
 */
public class SchemaTupleFactory implements TupleMaker<SchemaTuple<?>> {
    static final Log LOG = LogFactory.getLog(SchemaTupleFactory.class);

    private SchemaTupleQuickGenerator<? extends SchemaTuple<?>> generator;
    private Class<SchemaTuple<?>> clazz;
    private int tupleSize;

    protected SchemaTupleFactory(Class<SchemaTuple<?>> clazz,
            SchemaTupleQuickGenerator<? extends SchemaTuple<?>> generator) {
        this.clazz = clazz;
        this.generator = generator;
        tupleSize = generator.make().size();
    }

    /**
     * This method inspects a Schema to see whether or
     * not a SchemaTuple implementation can be generated
     * for the types present. Currently, bags and maps
     * are not supported.
     * @param   s as Schema
     * @return  boolean type value, true if it is generatable
     */
    public static boolean isGeneratable(Schema s) {
        if (s == null || s.size() == 0) {
            return false;
        }

        for (Schema.FieldSchema fs : s.getFields()) {
            if (fs.type == DataType.TUPLE && !isGeneratable(fs.schema)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public SchemaTuple<?> newTuple() {
        return generator.make();
    }

    @Override
    public SchemaTuple<?> newTuple(int size) {
        if (size != tupleSize) {
            throw new RuntimeException("Request a SchemaTuple of the wrong size! Requested ["
                    + size + "], can only be [" + tupleSize + "]" );
        }
        return generator.make();
    }

    public Class<SchemaTuple<?>> tupleClass() {
        return clazz;
    }

    // We could make this faster by caching the result, but I doubt it will be called
    // in any great volume.
    public boolean isFixedSize() {
        return clazz.isAssignableFrom(AppendableSchemaTuple.class);
    }

    /**
     * This method is the publicly facing method which returns a SchemaTupleFactory
     * which will generate the SchemaTuple associated with the given identifier. This method
     * is primarily for internal use in cases where the problem SchemaTuple is known
     * based on the identifier associated with it (such as when deserializing).
     * @param   id as int, means identifier
     * @return  SchemaTupleFactory which will return SchemaTuple's of the given identifier
     */
    protected static SchemaTupleFactory getInstance(int id) {
        return SchemaTupleBackend.newSchemaTupleFactory(id);
    }

    /**
     * This method is the publicly facing method which returns a SchemaTupleFactory
     * which will generate SchemaTuples of the given Schema. Note that this method
     * returns null if such a given SchemaTupleFactory does not exist, instead of
     * throwing an error. The GenContext is used to specify the context in which we
     * are requesting a SchemaTupleFactory. This is necessary so that the use
     * of SchemaTuple can be controlled -- it is possible that someone wants a
     * factory that generates code in the context of joins, but wants to disable such
     * use for udfs.
     * @param   s          the Schema generated
     * @param   isAppendable    whether or not the SchemaTuple should be appendable
     * @param   context         the context in which we want a SchemaTupleFactory
     * @return  SchemaTupleFactory which will return SchemaTuple's of the desired Schema
     */
    public static SchemaTupleFactory getInstance(Schema s, boolean isAppendable, GenContext context) {
        return SchemaTupleBackend.newSchemaTupleFactory(s, isAppendable, context);
    }

    public static SchemaTupleFactory getInstance(Schema s, boolean isAppendable) {
        return getInstance(s, isAppendable, GenContext.FORCE_LOAD);
    }

    public static SchemaTupleFactory getInstance(Schema s) {
        return getInstance(s, false);
    }
}
