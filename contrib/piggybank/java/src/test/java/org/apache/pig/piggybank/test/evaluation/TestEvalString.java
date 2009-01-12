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

import org.apache.pig.data.Tuple;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import org.apache.pig.piggybank.evaluation.string.UPPER;

// This class tests all string eval functions.

public class TestEvalString extends TestCase {
	
    @Test
    public void testUPPER() throws Exception {
        UPPER func = new UPPER();

        // test excution
        String in = "Hello World!";
        String expected = "HELLO WORLD!";

        Tuple input = DefaultTupleFactory.getInstance().newTuple(in);

        String output = func.exec(input);
        assertTrue(output.equals(expected));

        // test schema creation

        // FIXME
        //Schema outSchema = func.outputSchema(tupleSchema);
        //assertTrue(outSchema.toString().equals("upper_" + fieldName));

    }
}
