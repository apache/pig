package org.apache.pig.piggybank.test.evaluation;

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

import com.google.common.collect.Lists;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.MaxTupleBy1stField;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestMaxTupleBy1stField {

    private static List<Tuple> inputTuples = new ArrayList<>();
    private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
    private static final BagFactory BAG_FACTORY = BagFactory.getInstance();

    @BeforeClass
    public static void setup() throws Exception {
        inputTuples.add(TUPLE_FACTORY.newTuple(Lists.newArrayList(0L, "Fruit", "orange", 21F)));
        inputTuples.add(TUPLE_FACTORY.newTuple(Lists.newArrayList(1L, "Fruit", "apple", 9.9F)));
        inputTuples.add(TUPLE_FACTORY.newTuple(Lists.newArrayList(2L, "Vegetable", "paprika", 30F)));
        inputTuples.add(TUPLE_FACTORY.newTuple(Lists.newArrayList(3L, "Fruit", "blueberry", 40F)));
        inputTuples.add(TUPLE_FACTORY.newTuple(Lists.newArrayList(4L, "Vegetable", "carrot", 50F)));
        inputTuples.add(TUPLE_FACTORY.newTuple(Lists.newArrayList(5L, "Fruit", "blueberry", 41F)));
        inputTuples.add(TUPLE_FACTORY.newTuple(Lists.newArrayList(6L, "Vegetable", "paprika", 31F)));
        inputTuples.add(TUPLE_FACTORY.newTuple(Lists.newArrayList(7L, "Fruit", "orange", 20.5F)));
        inputTuples.add(TUPLE_FACTORY.newTuple(Lists.newArrayList(8L, "Fruit", "apple", 10.1F)));
        inputTuples.add(TUPLE_FACTORY.newTuple(Lists.newArrayList(9L, "Fruit", "apple", 10.2F)));
    }


    @Test
    public void testExecFunc() throws Exception {
        MaxTupleBy1stField udf = new MaxTupleBy1stField();
        Tuple inputTuple = createTupleFromInputList(0,inputTuples.size());

        Tuple result = udf.exec(inputTuple);
        Assert.assertEquals("apple", result.get(2));
        Assert.assertEquals(10.2F, (Float) result.get(3), 1E-8);
    }

    @Test
    public void testAccumulator() throws Exception {
        MaxTupleBy1stField udf = new MaxTupleBy1stField();

        Tuple inputTuple = createTupleFromInputList(0, 3);
        udf.accumulate(inputTuple);
        Tuple result = udf.getValue();
        Assert.assertEquals("paprika", result.get(2));
        Assert.assertEquals(30F, (Float) result.get(3), 1E-6);

        inputTuple = createTupleFromInputList(3, 6);
        udf.accumulate(inputTuple);
        result = udf.getValue();
        Assert.assertEquals("apple", result.get(2));
        Assert.assertEquals(10.1F, (Float) result.get(3), 1E-6);

        udf.cleanup();
        Assert.assertEquals(null,udf.getValue());
    }

    private static Tuple createTupleFromInputList(int offset, int length) {
        DataBag inputBag = BAG_FACTORY.newDefaultBag();
        for (int i = offset; i < offset+length; ++i) {
            inputBag.add(inputTuples.get(i));
        }
        Tuple inputTuple = TUPLE_FACTORY.newTuple();
        inputTuple.append(inputBag);
        return inputTuple;
    }

}
