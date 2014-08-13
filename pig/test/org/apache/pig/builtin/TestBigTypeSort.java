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
package org.apache.pig.builtin;

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestBigTypeSort {
	private static PigServer pigServer;

	@Before
	public void setUp() throws Exception {
		pigServer = new PigServer(ExecType.LOCAL);
	}

	@Test
	public void testBigTypeSort() throws Exception {
		Data data = resetData(pigServer);

		List<Tuple> bigtypes = Lists.newArrayList();
		bigtypes.add(tuple("123456789012314", "123000456000789000123000456000789000123000456000789.123"));
		bigtypes.add(tuple("123456789012334", "1.0E40"));
		bigtypes.add(tuple("103456789012034", "123000456000780000123000456000789000123000456000789.113"));

		data.set("bigtypes", bigtypes);

		pigServer.registerQuery("A = LOAD 'bigtypes' USING mock.Storage() as (x:biginteger,y:bigdecimal);");
		pigServer.registerQuery("B = ORDER A BY x ASC;");
		pigServer.registerQuery("C = ORDER A BY y ASC;");

		Iterator<Tuple> it = pigServer.openIterator("B");
		assertTrue(it.hasNext());
		assertEquals(new BigInteger("103456789012034"), it.next().get(0));
		assertEquals(new BigInteger("123456789012314"), it.next().get(0));
		assertEquals(new BigInteger("123456789012334"), it.next().get(0));
		assertFalse(it.hasNext());

		it = pigServer.openIterator("C");
		assertTrue(it.hasNext());
		assertEquals(new BigDecimal("1.0E40"), it.next().get(1));
		assertEquals(new BigDecimal("123000456000780000123000456000789000123000456000789.113"), it.next().get(1));
		assertEquals(new BigDecimal("123000456000789000123000456000789000123000456000789.123"), it.next().get(1));
		assertFalse(it.hasNext());

	}
}
