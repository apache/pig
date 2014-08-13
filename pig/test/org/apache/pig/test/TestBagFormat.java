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

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.BagFormat;
import org.junit.Test;

public class TestBagFormat {

    @Test
    public void testBagFormat() throws Exception {
        DataBag bag = BagFactory.getInstance().newDefaultBag();

        Tuple tuple_1 = TupleFactory.getInstance().newTuple(1);
        tuple_1.set(0, 12);
        bag.add(tuple_1);

        Tuple tuple_2 = TupleFactory.getInstance().newTuple(1);
        DataBag innerBag = BagFactory.getInstance().newDefaultBag();
        innerBag.add(tuple_1);
        tuple_2.set(0, (innerBag));
        bag.add(tuple_2);

        System.out.println(BagFormat.format(bag));
        assertEquals("{(12),({(12)})}", BagFormat.format(bag));
    }
}
