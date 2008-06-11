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
package org.apache.pig.piggybank.test.evaluation;

import java.io.File;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Properties;

import junit.framework.TestCase;

import org.junit.Test;

import org.apache.pig.FilterFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigServer;
import org.apache.pig.EvalFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.builtin.*;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataAtom;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataMap;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.AtomSchema;
import org.apache.pig.impl.logicalLayer.schema.TupleSchema;
import org.apache.pig.impl.builtin.ShellBagEvalFunc;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import static org.apache.pig.PigServer.ExecType.LOCAL;

import org.apache.pig.piggybank.evaluation.string.UPPER;

// This class tests all string eval functions.

public class TestEvalString extends TestCase {
	
    @Test
    public void testUPPER() throws Exception {
        UPPER func = new UPPER();

        // test excution
        String data = "Hello World!";
        String expected = "HELLO WORLD!";

        DataAtom field = new DataAtom(data);
        Tuple input = new Tuple(field);
        DataAtom output = new DataAtom();

        func.exec(input, output);
        assertTrue(output.strval().equals(expected));

        // test schema creation
        String fieldName = "field1";
        AtomSchema fieldSchema = new AtomSchema(fieldName);
        TupleSchema tupleSchema = new TupleSchema();
        tupleSchema.add(fieldSchema, false);
        Schema outSchema = func.outputSchema(tupleSchema);
        assertTrue(outSchema.toString().equals("upper_" + fieldName));

    }
}
