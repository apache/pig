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

import static org.junit.Assert.assertEquals;
import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;

import java.io.IOException;
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSplit {
    static private PigServer pig;
    static private Data data;

    @BeforeClass
    public static void setup() throws ExecException, IOException {
        pig = new PigServer(ExecType.LOCAL);
    }

    @Before
    public void setUp() throws Exception {
        data = resetData(pig);
        data.set("input",
                tuple(1),
                tuple(2),
                tuple(3),
                tuple(4),
                tuple((Integer)null),
                tuple(5),
                tuple(6)
        );
    }

    @Test
    public void testSplit1() throws IOException {
        String query = 
            "a = load 'input' using mock.Storage() as (id:int);" +
            "split a into b if id > 3, c if id < 3, d otherwise;" +
            "d_sorted = order d by id;" +
            "store d_sorted into 'output' using mock.Storage();"
            ;

        Util.registerMultiLineQuery(pig, query);
        List<Tuple> out = data.get("output");
        assertEquals(1, out.size());
        assertEquals(tuple(3), out.get(0));
    }
    
    @Test
    public void testSplit2() throws IOException {
        String query = 
            "a = load 'input' using mock.Storage() as (id:int);" +
            "split a into b if id % 2 == 0, d otherwise;" +
            "d_sorted = order d by id;" +
            "store d_sorted into 'output' using mock.Storage();"
            ;

         Util.registerMultiLineQuery(pig, query);
         List<Tuple> out = data.get("output");
         assertEquals(3, out.size());
         assertEquals(tuple(1), out.get(0));
         assertEquals(tuple(3), out.get(1));
         assertEquals(tuple(5), out.get(2));
    }

    @Test
    public void testOtherwiseAll() throws IOException {
    String query =
        "a = load 'input' using mock.Storage() as (id:int);" +
        "split a into b if id % 2 == 0, d otherwise all;" +
        "d_sorted = order d by id;" +
        "store d_sorted into 'output' using mock.Storage();"
        ;
        Util.registerMultiLineQuery(pig, query);
        List<Tuple> out = data.get("output");
        // Null should be passed through since the ALL keyword was specified in
        // the otherwise branch
        assertEquals(4, out.size());
        assertEquals(tuple((Integer)null), out.get(0));
        assertEquals(tuple(1), out.get(1));
        assertEquals(tuple(3), out.get(2));
        assertEquals(tuple(5), out.get(3));
    }
    
    @Test
    public void testSplitMacro() throws IOException {
        String query =
            "define split_into_two (A,key) returns B, C {" +
            "    SPLIT $A INTO $B IF $key<4, $C OTHERWISE;" +
            "};" +
            "a = load 'input' using mock.Storage() as (id:int);" +
            "b, c = split_into_two(a, id);" +
            "b_sorted = order b by id;" +
            "c_sorted = order c by id;" +
            "store b_sorted into 'output1' using mock.Storage();" +
            "store c_sorted into 'output2' using mock.Storage();"
            ;

        Util.registerMultiLineQuery(pig, query);
        List<Tuple> out = data.get("output1");
        assertEquals(3, out.size());
        assertEquals(tuple(1), out.get(0));
        assertEquals(tuple(2), out.get(1));
        assertEquals(tuple(3), out.get(2));

        out = data.get("output2");
        assertEquals(3, out.size());
        assertEquals(tuple(4), out.get(0));
        assertEquals(tuple(5), out.get(1));
        assertEquals(tuple(6), out.get(2));
    }

    @Test(expected=FrontendException.class)
    public void testSplitNondeterministic() throws IOException {
        String query = 
            "a = load 'input' using mock.Storage() as (id:int);" +
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
    
