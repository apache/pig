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

package org.apache.pig.test;

import static org.junit.Assert.*;
import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestEvalFuncOutputAnnotation {

    @OutputSchema("foo:chararray")
    public static class AnnotatedFunc extends EvalFunc<String> {
        @Override
        public String exec(Tuple input) throws IOException {
            return null;
        }
    }

    @OutputSchema("foo:chararray")
    public static class OverriddenFunc extends EvalFunc<String> {
        @Override
        public String exec(Tuple input) throws IOException {
            return null;
        }
        @Override
        public Schema outputSchema(Schema input) {
            return new Schema(new FieldSchema("bar", DataType.CHARARRAY));
        }
    }

    // This would give the same result: "y:bag{tuple(len:int,word:chararray)}"
    @OutputSchema("y:bag{(len:int,word:chararray)}")
    public static class ComplexFunc extends EvalFunc<DataBag> {
        @Override
        public DataBag exec(Tuple input) throws IOException {
            return null;
        }
    }

    public static class UnannotatedFunc extends EvalFunc<DataBag> {
        @Override
        public DataBag exec(Tuple input) throws IOException {
            return null;
        }
    }

    @Test
    public void testSimpleAnnotation() {
        EvalFunc<String> myFunc =new AnnotatedFunc();
        Schema s = new Schema(new FieldSchema("foo", DataType.CHARARRAY));
        assertEquals(s, myFunc.outputSchema(null));
    }

    @Test
    public void testOverriddenAnnotation() {
        EvalFunc<String> myFunc =new OverriddenFunc();
        Schema s = new Schema(new FieldSchema("bar", DataType.CHARARRAY));
        assertEquals(s, myFunc.outputSchema(null));
    }

    @Test
    public void testUnannotated() {
        EvalFunc<DataBag> myFunc = new UnannotatedFunc();
        assertNull(myFunc.outputSchema(null));
    }

    @Test
    public void testComplex() throws FrontendException {
        EvalFunc<DataBag> myFunc = new ComplexFunc();
      //  y:bag{t:tuple(len:int,word:chararray)}
        Schema ts = new Schema(Lists.asList(new FieldSchema("len", DataType.INTEGER),
                new FieldSchema[] {new FieldSchema("word", DataType.CHARARRAY)}));
        // Pig silently drops the name of a tuple the bag hold, since it's more or less invisible.
       FieldSchema bfs = new FieldSchema(null, ts, DataType.TUPLE);
       Schema bs = new Schema();
       bs.add(bfs);
       Schema s = new Schema();
       s.add(new FieldSchema("y", bs, DataType.BAG));
       assertEquals(s, myFunc.outputSchema(null));
    }


}
