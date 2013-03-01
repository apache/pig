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

package org.apache.pig.piggybank.test.storage;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;

import org.apache.pig.ExecType;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.tools.parameters.ParseException;
import org.apache.pig.test.Util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFixedWidthStorer  {

    private static final String dataDir = "build/test/tmpdata/";
    private static final String TEST_SCRIPT = "test/pigscripts/test_script.pig";
    private static final String STORE_SCRIPT = "test/pigscripts/test_store_function.pig";
    private static final String PIG_STORAGE_DELIMITER = "|";

    static PigServer pig;

    @Before
    public void setup() throws IOException {
        pig = new PigServer(ExecType.LOCAL);

        Util.deleteDirectory(new File(dataDir));
        try {
            pig.mkdirs(dataDir);
        } catch (IOException e) {};
    }

    @After
    public void cleanup() throws IOException {
        Util.deleteDirectory(new File(dataDir));
        pig.shutdown();
    }

    @Test
    public void storeScalarTypes() throws IOException, ParseException {
        String input = "pig_storage_scalar_data";
        String schema = "i: int, l: long, f: float, d: double, " +
                        "b: boolean, dt: datetime, c_arr: chararray, b_arr: bytearray";
        String output = "fixed_width_storage_scalar_data";

        Util.createLocalInputFile(dataDir + input,
            new String[] {
                "1|10|2.718|3.14159|true|2007-04-05T14:30:10Z|aardvark|wooooolololo",
                "1|100|1.234||false||cerulean skies|"
        });

        // Load input using PigStorage and store it using FixedWidthStorer
        pig.registerQuery(
            "data = load '" + dataDir + input + "' " +
            "using PigStorage('|') as (" + schema + ");"
        );
        pig.store("data", dataDir + output, 
                  "org.apache.pig.piggybank.storage.FixedWidthStorer('-5, 8-12, 15-19, 22-27, 29-33, 35-58, 62-69, 70-81', 'WRITE_HEADER')");

        // Load the output and see if it is what it ought to be
        pig.registerQuery(
            "data = load '" + dataDir + output + "' " +
            "using TextLoader() as (line: chararray);"
        );

        Iterator<Tuple> data = pig.openIterator("data");
        String[] expected = {
            // All columns right-aligned
            "(    i      l      f       d     b                       dt      c_arr       b_arr)", // Header written
            "(    1     10  2.718  3.1416  true 2007-04-05T14:30:10.000Z   aardvarkwooooolololo)", // 3.14159 rounded to fit in column
            "(    1    100  1.234         false                                                )"  // "cerulean skies" does not fit, so a null is written (spaces)
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(data, "\n"));
    }
}