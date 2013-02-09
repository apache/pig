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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.parser.ParserException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test "foreach alias generate *"
 */
public class TestForEachStar {
    private static final String INPUT_FILE = "TestForEachStarInput";

    @BeforeClass
    public static void oneTimeSetup() throws Exception{
        String[] input = { "one\ttwo" };
        Util.createLocalInputFile(INPUT_FILE, input);
    }
    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        new File(INPUT_FILE).delete();
    }
 
    
    @Before
    public void setUp() throws Exception {

    }
  

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testForeachStarSchemaUnkown() throws IOException, ParserException{
        PigServer pig = new PigServer(ExecType.LOCAL);
        String query =
            "  l1 = load '" + INPUT_FILE + "' ;"
            + "f1 = foreach l1 generate * ;"
        ; 
        Util.registerMultiLineQuery(pig, query);
        pig.explain("f1",System.out);
        Iterator<Tuple> it = pig.openIterator("f1");
        
        
        Tuple expectedResCharArray = (Tuple)Util.getPigConstant("('one','two')");
        Tuple expectedRes = TupleFactory.getInstance().newTuple();
        for(Object field :  expectedResCharArray.getAll() ){
            expectedRes.append(new DataByteArray(field.toString()));
        }
        assertTrue("has output", it.hasNext());
        assertEquals(expectedRes, it.next());
    }
    
    
    
}
