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

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.TupleFormat;

public class TestTuple extends TestCase {

    public void testTupleFormat() {

        try {
            Tuple tuple = TupleFactory.getInstance().newTuple(7);
            tuple.set(0, 12);
            Map<String, String> map = new HashMap<String, String>();
            map.put("pig", "scalability");
            tuple.set(1, map);
            tuple.set(2, null);
            tuple.set(3, 12L);
            tuple.set(4, 1.2F);

            Tuple innerTuple = TupleFactory.getInstance().newTuple(1);
            innerTuple.set(0, "innerTuple");
            tuple.set(5, innerTuple);

            DataBag bag = BagFactory.getInstance().newDefaultBag();
            bag.add(innerTuple);
            tuple.set(6, bag);

            assertEquals(
                    "(12,[pig#scalability],,12L,1.2F,(innerTuple),{(innerTuple)})",
                    TupleFormat.format(tuple));
        } catch (ExecException e) {
            e.printStackTrace();
            fail();
        }

    }
    
    // See PIG-1443
    public void testTupleSizeWithString() {
        Tuple t = Util.createTuple(new String[] {"1234567", "bar"});
        long size = t.getMemorySize();
        assertTrue(size==156);
    }
}
