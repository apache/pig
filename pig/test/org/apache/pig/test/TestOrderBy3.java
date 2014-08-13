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

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.test.utils.GenRandomData;
import org.junit.Before;
import org.junit.Test;

public class TestOrderBy3 {
    private final Log log = LogFactory.getLog(getClass());

    private static PigServer pigServer;
    private Data data;

    private static final int MAX = 10;

    @Before
    public void setUp() throws Exception {
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();

        log.info("Setting up");

        pigServer = new PigServer(ExecType.LOCAL);
        data = resetData(pigServer);

        Random r = new Random();
        for (int i = 0; i < MAX; i++) {
            tuples.add(tuple(i,GenRandomData.genRandString(r)));
        }

        Schema s = new Schema();
        s.add(new Schema.FieldSchema("index", DataType.INTEGER));
        s.add(new Schema.FieldSchema("name", DataType.CHARARRAY));
        data.set("test", s, tuples);
    }

    public void testNames(boolean ascOrdering) throws Exception {
        String order = (ascOrdering) ? "ASC" : "DESC";

        String query = "A = load 'test' USING mock.Storage();" +
        "B = order A by name " + order + ";" +
        "store B into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Iterator<Tuple> it = data.get("result").iterator();

        Tuple t1 = (Tuple) it.next();
        Tuple t2 = (Tuple) it.next();

        int comparision;
        boolean resultComparision;
        String value1, value2;

        while (t2 != null) {

            value1 = (String) t1.get(1);
            value2 = (String) t2.get(1);

            comparision = DataType.compare(value1, value2);
            resultComparision = (ascOrdering) ? (comparision <= 0)
                    : (comparision >= 0);

            System.out.println("RESULT: " + value1 + "," + value2 + " = "
                    + comparision);
            assertTrue(resultComparision);

            if(!it.hasNext()) break;

            t1 = t2;
            t2 = (Tuple) it.next();
        }
    }

    public void testIndexes(boolean ascOrdering) throws Exception {

        String order = (ascOrdering) ? "ASC" : "DESC";

        String query = "A = load 'test' USING mock.Storage();" +
        "B = order A by index " + order + ";" +
        "store B into 'result' using mock.Storage();";

        Util.registerMultiLineQuery(pigServer, query);

        Iterator<Tuple> it = data.get("result").iterator();

        int toCompare, value;

        for (int i = 0; i < MAX; i++) {

            Tuple t = (Tuple) it.next();
            value = DataType.toInteger(t.get(0));
            toCompare = (ascOrdering) ? i : MAX - i - 1;

            System.out.println("RESULT: " + toCompare + "," + value);

            assertEquals(toCompare, value);
        }

        assertFalse(it.hasNext());
    }

    @Test
    public void testIndexesAsc() throws Exception {
        testIndexes(true);
    }

    @Test
    public void testIndexesDesc() throws Exception {
        testIndexes(false);
    }

    @Test
    public void testValuesASC() throws Exception {
        testNames(true);
    }

    @Test
    public void testValuesDESC() throws Exception {
        testNames(false);
    }
}