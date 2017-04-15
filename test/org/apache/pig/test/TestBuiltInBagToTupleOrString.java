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

import java.util.Iterator;
import java.util.List;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.BagToString;
import org.apache.pig.builtin.BagToTuple;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import static org.apache.pig.builtin.mock.Storage.*;

import org.junit.Test;

/**
 *
 * Test cases for BagToTuple and BagToString UDFs
 *
 * @author hluu
 *
 */
public class TestBuiltInBagToTupleOrString {
	private BagFactory bf = BagFactory.getInstance();
	private TupleFactory tf = TupleFactory.getInstance();

	@Test
	public void testNullInputBagToTupleUDF() throws Exception {
		BagToTuple udf = new BagToTuple();
		Tuple udfInput = tf.newTuple(1);
		udfInput.set(0, null);
		Tuple output = udf.exec(udfInput);
		assertNull(output);
	}

	@Test
	public void testBasicBagToTupleUDF() throws Exception {

		Tuple t1 = tf.newTuple(2);
		t1.set(0, "a");
		t1.set(1, 5);

		Tuple t2 = tf.newTuple(2);
		t2.set(0, "c");
		t2.set(1, 6);

		DataBag bag = bf.newDefaultBag();
		bag.add(t1);
		bag.add(t2);

		Tuple udfInput = tf.newTuple(1);
		udfInput.set(0, bag);

		// invoking UDF
		BagToTuple udf = new BagToTuple();
		Tuple result = udf.exec(udfInput);

		int totalExpectedSize = t1.size() + t2.size();
		assertEquals(totalExpectedSize, result.size());

		for (int i = 0; i < t1.size(); i++) {
			assertEquals(t1.get(i), result.get(i));
		}

		for (int i = 0; i < t2.size(); i++) {
			assertEquals(t2.get(i), result.get(t1.size() + i));
		}
	}

	@Test
	public void testNonuniformTuplesInBagForBagToTupleUDF() throws Exception {

		Tuple t1 = tf.newTuple(2);
		t1.set(0, "a");
		t1.set(1, 5);

		Tuple t2 = tf.newTuple(3);
		t2.set(0, "b");
		t2.set(1, 6);
		t2.set(2, 7);

		Tuple t3 = tf.newTuple(4);
		t3.set(0, "c");
		t3.set(1, 8);
		t3.set(2, 9.7);
		t3.set(3, 10);

		DataBag bag = bf.newDefaultBag();
		bag.add(t1);
		bag.add(t2);
		bag.add(t3);

		Tuple udfInput = tf.newTuple(1);
		udfInput.set(0, bag);

		// invoking UDF
		BagToTuple udf = new BagToTuple();
		Tuple outputTuple = udf.exec(udfInput);

		int totalExpectedSize = t1.size() + t2.size() + t3.size();
		assertEquals(totalExpectedSize, outputTuple.size());

		for (int i = 0; i < t1.size(); i++) {
			assertEquals(t1.get(i), outputTuple.get(i));
		}

		for (int i = 0; i < t2.size(); i++) {
			assertEquals(t2.get(i), outputTuple.get(t1.size() + i));
		}

		int startIndex = t1.size() + t2.size();
		for (int i = 0; i < t3.size(); i++) {
			assertEquals(t3.get(i), outputTuple.get(startIndex + i));
		}
	}

	@Test
	public void testNestedDataElementsForBagToTupleUDF() throws Exception {

		DataBag inputBag = buildBagWithNestedTupleAndBag();


		BagToTuple udf = new BagToTuple();
		Tuple udfInput = tf.newTuple(1);
		udfInput.set(0, inputBag);
		Tuple outputTuple = udf.exec(udfInput);


		Iterator<Tuple> inputBagIterator = inputBag.iterator();
		Tuple firstTuple = inputBagIterator.next();
		for (int i = 0; i < firstTuple.size(); i++) {
			assertEquals(firstTuple.get(i), outputTuple.get(i));
		}

		Tuple secondTuple = inputBagIterator.next();
		for (int i = 0; i < secondTuple.size(); i++) {
			assertEquals(secondTuple.get(i), outputTuple.get(firstTuple.size() + i));
		}

		int startIndex = firstTuple.size() + secondTuple.size();
		Tuple thirdTuple = inputBagIterator.next();
		for (int i = 0; i < thirdTuple.size(); i++) {
			assertEquals(thirdTuple.get(i), outputTuple.get(startIndex + i));
		}
	}

	@Test
	public void testOutputSchemaForBagToTupleUDF() throws Exception {
		Schema expectedSch = Schema.generateNestedSchema(DataType.TUPLE,
				DataType.INTEGER, DataType.CHARARRAY);

		FieldSchema tupSch = new FieldSchema(null, DataType.TUPLE);
		tupSch.schema = new Schema();
		tupSch.schema.add(new FieldSchema(null, DataType.INTEGER));
		tupSch.schema.add(new FieldSchema(null, DataType.CHARARRAY));

		FieldSchema bagSch = new FieldSchema(null, DataType.BAG);
		bagSch.schema = new Schema(tupSch);

		Schema inputSch = new Schema();
		inputSch.add(bagSch);

		BagToTuple udf = new BagToTuple();
		Schema outputSchema = udf.outputSchema(inputSch);

		assertEquals("schema of BagToTuple input", expectedSch.size(),
				outputSchema.size());
		assertTrue("schema of BagToTuple input",
				Schema.equals(expectedSch, outputSchema, false, true));
	}

	@Test(expected=org.apache.pig.backend.executionengine.ExecException.class)
	public void testInvalidInputToBagToTupleUDF() throws Exception {
		TupleFactory tf = TupleFactory.getInstance();
		Tuple udfInput = tf.newTuple(1);
		// input contains tuple instead of bag
		udfInput.set(0, tf.newTuple());
		BagToTuple udf = new BagToTuple();

		// expecting an exception because the input if of type Tuple, not DataBag
		udf.exec(udfInput);
	}


	@Test
	public void testNullInputBagToStringUDF() throws Exception {
		BagToString udf = new BagToString();
		Tuple udfInput = tf.newTuple(1);
		udfInput.set(0, null);
		String output = udf.exec(udfInput);
		assertNull(output);
	}

	@Test(expected=org.apache.pig.backend.executionengine.ExecException.class)
	public void testInvalidInputForBagToStringUDF() throws Exception {
		TupleFactory tf = TupleFactory.getInstance();
		Tuple udfInput = tf.newTuple(1);
		// input contains tuple instead of bag
		udfInput.set(0, tf.newTuple());
		BagToString udf = new BagToString();

		// expecting an exception because the input if of type Tuple, not DataBag
		udf.exec(udfInput);
	}

	@Test
	public void testUseDefaultDelimiterBagToStringUDF() throws Exception {
		BagFactory bf = BagFactory.getInstance();
		TupleFactory tf = TupleFactory.getInstance();

		Tuple t1 = tf.newTuple(2);
		t1.set(0, "a");
		t1.set(1, 5);

		Tuple t2 = tf.newTuple(2);
		t2.set(0, "c");
		t2.set(1, 6);

		DataBag bag = bf.newDefaultBag();
		bag.add(t1);
		bag.add(t2);

		BagToString udf = new BagToString();
		Tuple udfInput = tf.newTuple(1);
		udfInput.set(0, bag);
		String result = udf.exec(udfInput);

		assertEquals("a_5_c_6", result);
	}

	@Test
	public void testBasicBagToStringUDF() throws Exception {
		BagFactory bf = BagFactory.getInstance();
		TupleFactory tf = TupleFactory.getInstance();

		Tuple t1 = tf.newTuple(2);
		t1.set(0, "a");
		t1.set(1, 5);

		Tuple t2 = tf.newTuple(2);
		t2.set(0, "c");
		t2.set(1, 6);

		DataBag bag = bf.newDefaultBag();
		bag.add(t1);
		bag.add(t2);

		BagToString udf = new BagToString();
		Tuple udfInput = tf.newTuple(2);
		udfInput.set(0, bag);
		udfInput.set(1, "-");
		String result = udf.exec(udfInput);

		assertEquals("a-5-c-6", result);
	}

	@Test
	public void testNestedTupleForBagToStringUDF() throws Exception {
		BagFactory bf = BagFactory.getInstance();
		TupleFactory tf = TupleFactory.getInstance();

		Tuple t1 = tf.newTuple(2);
		t1.set(0, "a");
		t1.set(1, 5);

		Tuple nestedTuple = tf.newTuple(2);
		nestedTuple.set(0, "d");
		nestedTuple.set(1, 7);

		Tuple t2 = tf.newTuple(3);
		t2.set(0, "c");
		t2.set(1, 6);
		t2.set(2, nestedTuple);

		DataBag inputBag = bf.newDefaultBag();
		inputBag.add(t1);
		inputBag.add(t2);

		BagToString udf = new BagToString();
		Tuple udfInput = tf.newTuple(2);
		udfInput.set(0, inputBag);
		udfInput.set(1, "_");
		String result = udf.exec(udfInput);

		assertEquals("a_5_c_6_(d,7)", result);
	}

	@Test
	public void testNestedDataElementsForBagToStringUDF() throws Exception {

		DataBag inputBag = buildBagWithNestedTupleAndBag();

		BagToString udf = new BagToString();
		Tuple udfInput = tf.newTuple(2);
		udfInput.set(0, inputBag);
		udfInput.set(1, "*");

		String result = udf.exec(udfInput);
		assertEquals("a*5*c*6*(d,7)*{(in bag,10)}", result);
	}


	@Test(expected=java.lang.RuntimeException.class)
	public void testInvalidZeroInputToOutputSchemaForBagToTupleStringUDF() throws Exception {


		Schema inputSch = new Schema();

		BagToString udf = new BagToString();
		Schema outputSchema = udf.outputSchema(inputSch);

		assertEquals("schema of BagToTuple input", outputSchema.getField(0).type,
				DataType.CHARARRAY);

	}

	@Test
	public void testOutputSchemaForBagToTupleStringUDF() throws Exception {

		FieldSchema tupSch = new FieldSchema(null, DataType.TUPLE);
		tupSch.schema = new Schema();
		tupSch.schema.add(new FieldSchema(null, DataType.INTEGER));
		tupSch.schema.add(new FieldSchema(null, DataType.CHARARRAY));

		FieldSchema bagSch = new FieldSchema(null, DataType.BAG);
		bagSch.schema = new Schema(tupSch);

		Schema inputSch = new Schema();
		inputSch.add(bagSch);
		inputSch.add(new FieldSchema(null, DataType.CHARARRAY));

		BagToString udf = new BagToString();
		Schema outputSchema = udf.outputSchema(inputSch);

		assertEquals("schema of BagToTuple input", outputSchema.getField(0).type,
				DataType.CHARARRAY);

	}

	@Test
	public void testOutputSchemaWithDefaultDelimiterForBagToTupleStringUDF() throws Exception {

		FieldSchema tupSch = new FieldSchema(null, DataType.TUPLE);
		tupSch.schema = new Schema();
		tupSch.schema.add(new FieldSchema(null, DataType.INTEGER));
		tupSch.schema.add(new FieldSchema(null, DataType.CHARARRAY));

		FieldSchema bagSch = new FieldSchema(null, DataType.BAG);
		bagSch.schema = new Schema(tupSch);

		Schema inputSch = new Schema();
		inputSch.add(bagSch);

		BagToString udf = new BagToString();
		Schema outputSchema = udf.outputSchema(inputSch);

		assertEquals("schema of BagToTuple input", outputSchema.getField(0).type,
				DataType.CHARARRAY);

	}

	@Test(expected=java.lang.RuntimeException.class)
	public void testInvalidOutputSchemaForBagToTupleStringUDF() throws Exception {

		FieldSchema tupSch = new FieldSchema(null, DataType.TUPLE);
		tupSch.schema = new Schema();
		tupSch.schema.add(new FieldSchema(null, DataType.INTEGER));
		tupSch.schema.add(new FieldSchema(null, DataType.CHARARRAY));

		FieldSchema bagSch = new FieldSchema(null, DataType.BAG);
		bagSch.schema = new Schema(tupSch);

		Schema inputSch = new Schema();
		inputSch.add(bagSch);
		inputSch.add(new FieldSchema(null, DataType.DOUBLE));

		BagToString udf = new BagToString();
		// expecting an exception because the delimiter is not of type Data.CHARARRAY
		udf.outputSchema(inputSch);
	}

	@Test
	public void testPigScriptForBagToTupleUDF() throws Exception {
		PigServer pigServer = new PigServer(Util.getLocalTestMode());
		Data data = resetData(pigServer);

		// bag of chararray
		data.set("foo", "myBag:bag{t:(l:chararray)}",
				tuple(bag(tuple("a"), tuple("b"), tuple("c"))));
		pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
		pigServer.registerQuery("B = FOREACH A GENERATE BagToTuple(myBag) as myBag;");
	    pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

	    assertEquals(schema("myBag:(l:chararray)"), data.getSchema("bar"));

	    List<Tuple> out = data.get("bar");
	    assertEquals(tuple("a", "b","c"), out.get(0).get(0));

	    // bag of longs
	    data = resetData(pigServer);
		data.set("foo", "myBag:bag{t:(l:long)}",
				tuple(bag(tuple(1), tuple(2), tuple(3))));
		pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
		pigServer.registerQuery("B = FOREACH A GENERATE BagToTuple(myBag) as myBag;");
	    pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

	    out = data.get("bar");
	    assertEquals(tuple(1, 2, 3), out.get(0).get(0));
	}

	@Test
	public void testPigScriptMultipleElmementsPerTupleForBagTupleUDF() throws Exception {
		PigServer pigServer = new PigServer(Util.getLocalTestMode());
		Data data = resetData(pigServer);

		data.set("foo", "myBag:bag{t:(l:chararray)}",
				tuple(bag(tuple("a", "b"), tuple("c", "d"), tuple("e", "f"))));
		pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
		pigServer.registerQuery("B = FOREACH A GENERATE BagToTuple(myBag) as myBag;");
		pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

	    List<Tuple> out = data.get("bar");
	    assertEquals(tuple("a", "b","c", "d", "e", "f"), out.get(0).get(0));
	}

	@Test
	public void testPigScriptNestedTupleForBagToTupleDF() throws Exception {
		PigServer pigServer = new PigServer(Util.getLocalTestMode());
		Data data = resetData(pigServer);

	    Tuple nestedTuple = tuple(bag(tuple("c"), tuple("d")));
	    data.set("foo", "myBag:bag{t:(l:chararray)}",
				tuple(bag(tuple("a"), tuple("b"), nestedTuple, tuple("e"))));

		pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
		pigServer.registerQuery("B = FOREACH A GENERATE BagToTuple(myBag) as myBag;");
	    pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

	    List<Tuple> out = data.get("bar");
	    assertEquals(tuple("a", "b",bag(tuple("c"), tuple("d")), "e"), out.get(0).get(0));

	}

	@Test
	public void testPigScriptEmptyBagForBagToTupleUDF() throws Exception {
		PigServer pigServer = new PigServer(Util.getLocalTestMode());
		Data data = resetData(pigServer);

	    data.set("foo", "myBag:bag{t:(l:chararray)}",
				tuple(bag()));

		pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
		pigServer.registerQuery("B = FOREACH A GENERATE BagToTuple(myBag) as myBag;");
	    pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

	    List<Tuple> out = data.get("bar");
	    // empty bag will generate empty tuple
	    assertEquals(tuple(), out.get(0).get(0));

	}

	@Test
	public void testPigScriptrForBagToStringUDF() throws Exception {
		PigServer pigServer = new PigServer(Util.getLocalTestMode());
		Data data = resetData(pigServer);

		data.set("foo", "myBag:bag{t:(l:chararray)}",
				tuple(bag(tuple("a"), tuple("b"), tuple("c"))));
		pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
		pigServer.registerQuery("B = FOREACH A GENERATE BagToString(myBag) as myBag;");
	    pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

	    pigServer.registerQuery("C = FOREACH A GENERATE BagToString(myBag, '==') as myBag;");
	    pigServer.registerQuery("STORE C INTO 'baz' USING mock.Storage();");

	    List<Tuple> out = data.get("bar");
	    assertEquals(schema("myBag:chararray"), data.getSchema("bar"));
	    assertEquals(tuple("a_b_c"), out.get(0));

	    out = data.get("baz");
	    assertEquals(tuple("a==b==c"), out.get(0));
	}

	@Test
	public void testPigScriptMultipleElmementsPerTupleForBagToStringUDF() throws Exception {
		PigServer pigServer = new PigServer(Util.getLocalTestMode());
		Data data = resetData(pigServer);

		data.set("foo", "myBag:bag{t:(l:chararray)}",
				tuple(bag(tuple("a", "b"), tuple("c", "d"), tuple("e", "f"))));
		pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
		pigServer.registerQuery("B = FOREACH A GENERATE BagToString(myBag) as myBag;");
		pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

		pigServer.registerQuery("C = FOREACH A GENERATE BagToString(myBag, '^') as myBag;");
		pigServer.registerQuery("STORE C INTO 'baz' USING mock.Storage();");

	    List<Tuple> out = data.get("bar");
	    assertEquals(tuple("a_b_c_d_e_f"), out.get(0));

	    out = data.get("baz");
	    assertEquals(tuple("a^b^c^d^e^f"), out.get(0));
	}

	@Test
	public void testPigScriptNestedTupleForBagToStringUDF() throws Exception {
		PigServer pigServer = new PigServer(Util.getLocalTestMode());
		Data data = resetData(pigServer);

	    Tuple nestedTuple = tuple(bag(tuple("c"), tuple("d")));
	    data.set("foo", "myBag:bag{t:(l:chararray)}",
				tuple(bag(tuple("a"), tuple("b"), nestedTuple, tuple("e"))));

		pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
		pigServer.registerQuery("B = FOREACH A GENERATE BagToString(myBag) as myBag;");
	    pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

	    List<Tuple> out = data.get("bar");
	    assertEquals(tuple("a_b_{(c),(d)}_e"), out.get(0));

	}

	@Test
	public void testPigScriptEmptyBagForBagToStringUDF() throws Exception {
		PigServer pigServer = new PigServer(Util.getLocalTestMode());
		Data data = resetData(pigServer);

	    data.set("foo", "myBag:bag{t:(l:chararray)}",
				tuple(bag()));

		pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
		pigServer.registerQuery("B = FOREACH A GENERATE BagToString(myBag) as myBag;");
	    pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");

	    List<Tuple> out = data.get("bar");
	    // empty bag will generate empty string
	    assertEquals(tuple(""), out.get(0));

	}

	private DataBag buildBagWithNestedTupleAndBag() throws ExecException {
		Tuple t1 = tf.newTuple(2);
		t1.set(0, "a");
		t1.set(1, 5);

		Tuple nestedTuple = tf.newTuple(2);
		nestedTuple.set(0, "d");
		nestedTuple.set(1, 7);

		Tuple t2 = tf.newTuple(3);
		t2.set(0, "c");
		t2.set(1, 6);
		t2.set(2, nestedTuple);

		DataBag nestedBag = bf.newDefaultBag();
		Tuple tupleInNestedBag = tf.newTuple(2);
		tupleInNestedBag.set(0, "in bag");
		tupleInNestedBag.set(1, 10);
		nestedBag.add(tupleInNestedBag);

		Tuple t3 = tf.newTuple(1);
		t3.set(0, nestedBag);

		DataBag bag = bf.newDefaultBag();
		bag.add(t1);
		bag.add(t2);
		bag.add(t3);
		return bag;
	}
}
