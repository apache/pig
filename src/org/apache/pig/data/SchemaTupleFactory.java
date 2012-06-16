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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.SchemaTuple.SchemaTupleQuickGenerator;
import org.apache.pig.data.utils.MethodHelper;
import org.apache.pig.data.utils.MethodHelper.NotImplemented;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * This is an implementation of TupleFactory that will instantiate
 * SchemaTuple's. This class has nothing to do with the actual generation
 * of code, and instead simply encapsulates the classes which allow
 * for efficiently creating SchemaTuples.
 */
public class SchemaTupleFactory extends TupleFactory {
    static final Log LOG = LogFactory.getLog(SchemaTupleFactory.class);

    private SchemaTupleQuickGenerator<? extends SchemaTuple<?>> generator;
    private Class<SchemaTuple<?>> clazz;

    protected SchemaTupleFactory(Class<SchemaTuple<?>> clazz,
            SchemaTupleQuickGenerator<? extends SchemaTuple<?>> generator) {
        this.clazz = clazz;
        this.generator = generator;
    }

    /**
     * This method inspects a Schema to see whether or
     * not a SchemaTuple implementation can be generated
     * for the types present. Currently, bags and maps
     * are not supported.
     * @param   schema
     * @return  true if it is generatable
     */
    public static boolean isGeneratable(Schema s) {
        if (s == null) {
            return false;
        }

        for (Schema.FieldSchema fs : s.getFields()) {
            if (fs.type == DataType.BAG || fs.type == DataType.MAP) {
                return false;
            }

            if (fs.type == DataType.TUPLE && !isGeneratable(fs.schema)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Tuple newTuple() {
        return generator.make();
    }

    /**
     * The notion of instantiating a SchemaTuple with a given
     * size doesn't really make sense, as the size is set
     * by the Schema.
     */
    @Override
    @NotImplemented
    public Tuple newTuple(int size) {
        throw MethodHelper.methodNotImplemented();
    }

    /**
     * As with newTuple(int), it doesn't make much sense
     * to instantiate a Tuple with a notion of Schema from
     * an untyped list of objects. Note: in the future
     * we may inspect the type of the Objects in the list
     * and see if they match with the underlying Schema, and if so,
     * generate the Tuple. For now the gain seems minimal.
     */
    @Override
    @NotImplemented
    public Tuple newTuple(List c) {
        throw MethodHelper.methodNotImplemented();
    }

    @Override
    @NotImplemented
    public Tuple newTupleNoCopy(List c) {
        throw MethodHelper.methodNotImplemented();
    }

    /**
     * It does not make any sense to instantiate with
     * one object a Tuple whose size and type is already known.
     */
    @Override
    @NotImplemented
    public Tuple newTuple(Object datum) {
        throw MethodHelper.methodNotImplemented();
    }

    @Override
    public Class<SchemaTuple<?>> tupleClass() {
        return clazz;
    }
}
