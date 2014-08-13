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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSplit {
    private static final String[] data = new String[] { "1", "2", "3", "4", "5", "6" };
    private static File file;
    private PigServer pig;

    @BeforeClass
    public static void oneTimeSetUp() throws IOException {
        file = Util.createLocalInputFile("split_input", data);
    }

    @Before
    public void setUp() throws ExecException {
        pig = new PigServer(ExecType.LOCAL);
    }

    @Test
    public void testSplit1() throws IOException {
        String query = 
            "a = load '" + Util.encodeEscape(file.getAbsolutePath()) + "' as (id:int);" + 
            "split a into b if id > 3, c if id < 3, d otherwise;"
            ;

        Util.registerMultiLineQuery(pig, query);
        Iterator<Tuple> it = pig.openIterator("d");

        List<Tuple> expectedRes = Util.getTuplesFromConstantTupleStrings(new String[] { "(3)" });
        Util.checkQueryOutputs(it, expectedRes);
    }
    
    @Test
    public void testSplit2() throws IOException {
        String query = 
            "a = load '" + Util.encodeEscape(file.getAbsolutePath()) + "' as (id:int);" + 
            "split a into b if id % 2 == 0, d otherwise;"
            ;

        Util.registerMultiLineQuery(pig, query);
        Iterator<Tuple> it = pig.openIterator("d");

        List<Tuple> expectedRes = Util.getTuplesFromConstantTupleStrings(new String[] { "(1)", "(3)", "(5)" });
        Util.checkQueryOutputsAfterSort(it, expectedRes);
    }
    
    @Test
    public void testSplitMacro() throws IOException {
        String query =
            "define split_into_two (A,key) returns B, C {" +
            "    SPLIT $A INTO $B IF $key<4, $C OTHERWISE;" +
            "};"  +
            "a = load '" + Util.encodeEscape(file.getAbsolutePath()) + "' as (id:int);" +
            "B, C = split_into_two(a, id);"
            ;

        Util.registerMultiLineQuery(pig, query);
        Iterator<Tuple> it = pig.openIterator("B");

        List<Tuple> expectedRes = Util.getTuplesFromConstantTupleStrings(new String[] { "(1)", "(2)", "(3)" });
        Util.checkQueryOutputsAfterSort(it, expectedRes);

        it = pig.openIterator("C");
        expectedRes = Util.getTuplesFromConstantTupleStrings(new String[] { "(4)", "(5)", "(6)" });
        Util.checkQueryOutputsAfterSort(it, expectedRes);
    }

    @Test(expected=FrontendException.class)
    public void testSplitNondeterministic() throws IOException {
        String query = 
            "a = load '" + Util.encodeEscape(file.getAbsolutePath()) + "' as (id:int);" + 
            "split a into b if RANDOM() < 0.5, d otherwise;"
            ;

        try {
            Util.registerMultiLineQuery(pig, query);
        } catch (FrontendException fe) {
            Util.checkMessageInException(fe,
                    "Can not use Otherwise in Split with an expression containing a @Nondeterministic UDF");
            throw fe;
        }

    }
}
    
