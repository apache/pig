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

package org.apache.pig.piggybank.test.evaluation;

import java.util.Arrays;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.piggybank.evaluation.ExtremalTupleByNthField;
import org.junit.Assert;
import org.junit.Test;

public class TestExtremalTupleByNthField {

	@Test
	public void testMin() throws Exception {
		ExtremalTupleByNthField o = new ExtremalTupleByNthField("3", "min");

		DataBag input = BagFactory.getInstance().newDefaultBag();

		for (int i = 100; i > 0; --i) {
			Tuple t = TupleFactory.getInstance().newTuple();
			t.append(i);
			t.append(" " + i);
			t.append(i);
			input.add(t);
		}

		Tuple tupleInput = TupleFactory.getInstance().newTuple();
		tupleInput.append(input);

		Tuple out = o.exec(tupleInput);

		Assert.assertEquals(" 1", (String) out.get(1));
	}

	@Test
	public void testMax() throws Exception {
		ExtremalTupleByNthField o = new ExtremalTupleByNthField("4", "max");

		DataBag input = BagFactory.getInstance().newDefaultBag();

		for (int i = 0; i < 100; i++) {
			Tuple t = TupleFactory.getInstance().newTuple();
			t.append(i);
			t.append(" " + i);
			t.append(i);
			t.append(i);
			input.add(t);
		}

		Tuple tupleInput = TupleFactory.getInstance().newTuple();
		tupleInput.append(input);

		Tuple out = o.exec(tupleInput);

		Assert.assertEquals(" 99", (String) out.get(1));
	}

	@Test
	public void testMaxComplexKey() throws Exception {
		ExtremalTupleByNthField o = new ExtremalTupleByNthField("3", "max");

		DataBag input = BagFactory.getInstance().newDefaultBag();

		for (int j = 0; j < 3; ++j) {
			for (int i = 0; i < 100; i++) {
				Tuple t = TupleFactory.getInstance().newTuple();
				t.append(-i);
				t.append(" " + j + ", " + i);
				Tuple key = TupleFactory.getInstance().newTuple();
				key.append(j);
				key.append(i);
				t.append(key);
				t.append(-i);
				input.add(t);
			}
		}

		Tuple tupleInput = TupleFactory.getInstance().newTuple();
		tupleInput.append(input);

		Tuple out = o.exec(tupleInput);

		Assert.assertEquals(" 2, 99", (String) out.get(1));
	}

	@Test
	public void testMinComplexKey() throws Exception {
		ExtremalTupleByNthField o = new ExtremalTupleByNthField("3", "min");

		DataBag input = BagFactory.getInstance().newDefaultBag();

		for (int j = 0; j < 3; ++j) {
			for (int i = 0; i < 100; i++) {
				Tuple t = TupleFactory.getInstance().newTuple();
				t.append(-i);
				t.append(" " + j + ", " + i);
				Tuple key = TupleFactory.getInstance().newTuple();
				key.append(j);
				key.append(i);
				t.append(key);
				t.append(-i);
				input.add(t);
			}
		}

		Tuple tupleInput = TupleFactory.getInstance().newTuple();
		tupleInput.append(input);

		Tuple out = o.exec(tupleInput);

		Assert.assertEquals(" 0, 0", (String) out.get(1));
	}

	@Test
	public void testMinStringey() throws Exception {
		ExtremalTupleByNthField o = new ExtremalTupleByNthField("4", "min");

		DataBag input = BagFactory.getInstance().newDefaultBag();
		Tuple t = TupleFactory.getInstance().newTuple();
		t.append("a");
		t.append("a");
		t.append("a");
		t.append("min");
		input.add(t);

		t = TupleFactory.getInstance().newTuple();
		t.append("b");
		t.append("b");
		t.append("b");
		t.append("max");
		input.add(t);

		Tuple tupleInput = TupleFactory.getInstance().newTuple();
		tupleInput.append(input);

		Tuple out = o.exec(tupleInput);

		// ironically "max" is smaller than "min"
		Assert.assertEquals("b", (String) out.get(1));
	}

	@Test
	public void testBiggerBag() throws Exception {
		ExtremalTupleByNthField o = new ExtremalTupleByNthField("1", "max");

		DataBag input = BagFactory.getInstance().newDefaultBag();
		DataBag dbSmaller = BagFactory.getInstance().newDefaultBag();
		dbSmaller.add(TupleFactory.getInstance().newTuple(
				Arrays.asList("This bag has three items")));
		dbSmaller.add(TupleFactory.getInstance().newTuple(
				Arrays.asList("This bag has three items")));
		dbSmaller.add(TupleFactory.getInstance().newTuple(
				Arrays.asList("This bag has three items")));
		input.add(TupleFactory.getInstance().newTuple(
				Arrays.asList(dbSmaller, "smaller")));

		DataBag dbBigger = BagFactory.getInstance().newDefaultBag();
		dbBigger.add(TupleFactory.getInstance().newTuple(
				Arrays.asList("This bag has four items")));
		dbBigger.add(TupleFactory.getInstance().newTuple(
				Arrays.asList("This bag has four items")));
		dbBigger.add(TupleFactory.getInstance().newTuple(
				Arrays.asList("This bag has four items")));
		dbBigger.add(TupleFactory.getInstance().newTuple(
				Arrays.asList("This bag has four items")));
		dbBigger.add(TupleFactory.getInstance().newTuple(
				Arrays.asList("This bag has four items")));
		input.add(TupleFactory.getInstance().newTuple(
				Arrays.asList(dbBigger, "bigger")));

		Tuple tupleInput = TupleFactory.getInstance().newTuple();
		tupleInput.append(input);

		Tuple out = o.exec(tupleInput);

		// DataBags are ordered by size, so the bigger one will be the one
		// containing 4 items
		Assert.assertEquals("bigger", out.get(1));
	}

	@Test
	public void testBiggerTuple() throws Exception {
		ExtremalTupleByNthField o = new ExtremalTupleByNthField("1", "min");

		DataBag input = BagFactory.getInstance().newDefaultBag();
		Tuple tpSmaller = TupleFactory.getInstance().newTuple();
		tpSmaller.append("This is a smaller tuple.");
		tpSmaller.append("This is a smaller tuple.");
		tpSmaller.append("This is a smaller tuple.");
		input.add(TupleFactory.getInstance().newTuple(
				Arrays.asList(tpSmaller, "smaller")));

		Tuple tpBigger = TupleFactory.getInstance().newTuple();
		tpBigger.append("This is a bigger tuple.");
		tpBigger.append("This is a bigger tuple.");
		tpBigger.append("This is a bigger tuple.");
		tpBigger.append("This is a bigger tuple.");
		input.add(TupleFactory.getInstance().newTuple(
				Arrays.asList(tpBigger, "bigger")));

		Tuple tupleInput = TupleFactory.getInstance().newTuple();
		tupleInput.append(input);

		Tuple out = o.exec(tupleInput);

		// DataBags are ordered by size, so the bigger one will be the one
		// containing 4 items
		Assert.assertEquals("smaller", out.get(1));
	}

	@Test
	public void testMaxAccumulated() throws Exception {

		ExtremalTupleByNthField o = new ExtremalTupleByNthField("5", "max");

		for (int j = 0; j < 5; ++j) {
			for (int i = 0; i < 100; i++) {
				Tuple t = TupleFactory.getInstance().newTuple();
				t.append(i + j);
				t.append(" " + (i + j));
				t.append(i + j);
				t.append(i + j);
				t.append(i + j);
				o.accumulate(t);
			}

			Tuple out = o.getValue();
			Assert.assertEquals(" " + (99 + j), (String) out.get(1));
			o.cleanup();
		}
	}
}
