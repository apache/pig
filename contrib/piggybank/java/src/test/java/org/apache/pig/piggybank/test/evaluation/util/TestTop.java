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
package org.apache.pig.piggybank.test.evaluation.util;

import java.util.Iterator;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.piggybank.evaluation.util.Top;
import org.junit.Test;

import junit.framework.TestCase;

public class TestTop extends TestCase {

  @Test
  public void testTop() throws Exception {
    Top top = new Top();
    Tuple inputTuple = DefaultTupleFactory.getInstance().newTuple(3);
    // set N = 10 i.e retain top 10 tuples
    inputTuple.set(0, 10);
    // compare tuples by field number 1
    inputTuple.set(1, 1);
    // set the data bag containing the tuples
    DataBag dBag = DefaultBagFactory.getInstance().newDefaultBag();
    inputTuple.set(2, dBag);
    // generate tuples of the form (group-1, 1), (group-2, 2) ...
    for (long i = 0; i < 100; i++) {
      Tuple nestedTuple = DefaultTupleFactory.getInstance().newTuple(2);
      nestedTuple.set(0, "group-" + i);
      nestedTuple.set(1, i);
      dBag.add(nestedTuple);
    }

    DataBag outBag = top.exec(inputTuple);
    super.assertEquals(outBag.size(), 10L);
    Iterator<Tuple> itr = outBag.iterator();
    while (itr.hasNext()) {
      Tuple next = itr.next();
      Long value = (Long) next.get(1);
      super.assertTrue("Value " + value + " exceeded the expected limit",
          value > 89);
    }
  }
}
