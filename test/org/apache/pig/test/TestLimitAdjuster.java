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

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestLimitAdjuster {

    private static final MiniGenericCluster cluster = MiniGenericCluster.buildCluster();

    private PigServer pig;

    @Before
    public void setUp() throws Exception {
        pig = new PigServer(cluster.getExecType(), cluster.getProperties());
    }

    @After
    public void tearDown() throws Exception {
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void simpleTest() throws Exception {
        String INPUT_FILE = "input";

        PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
        w.println("1\torange");
        w.println("2\tapple");
        w.println("3\tcoconut");
        w.println("4\tmango");
        w.println("5\tgrape");
        w.println("6\tpear");
        w.close();

        Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);

        pig.registerQuery("a = load '" + INPUT_FILE + "' as (x:int, y:chararray);");
        pig.registerQuery("b = order a by x parallel 2;");
        pig.registerQuery("c = limit b 1;");
        pig.registerQuery("d = foreach c generate y;");

        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStrings(
                new String[] { "('orange')" });

        Iterator<Tuple> iter = pig.openIterator("d");
        int counter = 0;
        while (iter.hasNext()) {
            assertEquals(expectedResults.get(counter++).toString(), iter.next().toString());
        }
        assertEquals(expectedResults.size(), counter);
    }

}
