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

package org.apache.pig.piggybank.test.evaluation.string;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.string.Stuff;
import org.junit.Test;

public class TestStuff {

	Stuff udf = new Stuff();

	@Test
	public void test() throws IOException {
		Tuple tupleWithNullElement = TupleFactory.getInstance().newTuple(1);
		String result = udf.exec(tupleWithNullElement);
		assertNull(result);

		Tuple t = TupleFactory.getInstance().newTuple(4);
		t.set(0, "Chocolate Cake");
		t.set(1, 10);
		t.set(2, 4);
		t.set(3, "Pie");
		result = udf.exec(t);
		assertEquals("Chocolate Pie", result);

		t.set(0, "Old Zealand");
		t.set(1, 0);
		t.set(2, 3);
		t.set(3, "New");
		result = udf.exec(t);
		assertEquals("New Zealand", result);

		t.set(0, "Take a look");
		t.set(1, 5);
		t.set(2, 1);
		t.set(3, "b");
		result = udf.exec(t);
		assertEquals("Take b look", result);

		t.set(0, "Take a look");
		t.set(1, 5);
		t.set(2, 6);// replace until the end
		t.set(3, "b");
		result = udf.exec(t);
		assertEquals("Take b", result);

		t.set(0, "Take a look");
		t.set(1, 4);
		t.set(2, 7);// replace until the end
		t.set(3, null);// delete the specified portion
		result = udf.exec(t);
		assertEquals("Take", result);

	}

	@Test(expected = IOException.class)
	public void testArgumentCountMismatch() throws IOException {
		Tuple t = TupleFactory.getInstance().newTuple(1);
		t.set(0, "some_val");
		udf.exec(t);
	}

	@Test(expected = IOException.class)
	public void testIndexOutOfBounds() throws IOException {
		Tuple t = TupleFactory.getInstance().newTuple(4);
		t.set(0, "Replace me now!");
		t.set(1, -1);
		t.set(2, 2);
		t.set(3, "test");
		udf.exec(t);
	}

	@Test(expected = IOException.class)
	public void testNegativeNumCharsToDelete() throws IOException {
		Tuple t = TupleFactory.getInstance().newTuple(4);
		t.set(0, "Replace me now!");
		t.set(1, 0);
		t.set(2, -2);
		t.set(3, "test");
		udf.exec(t);
	}

	@Test
	public void testWithOtherTypes() throws IOException {
		Tuple t = TupleFactory.getInstance().newTuple(4);
		t.set(0, "Replace me now!");
		t.set(1, new Double(8));
		t.set(2, 2);
		t.set(3, "ZZ");
		String result = udf.exec(t);
		assertEquals("Replace ZZ now!", result);

		t = TupleFactory.getInstance().newTuple(4);
		t.set(0, "Replace me now!");
		t.set(1, new Float(14));
		t.set(2, 1);
		t.set(3, ";");
		result = udf.exec(t);
		assertEquals("Replace me now;", result);
	}
}
