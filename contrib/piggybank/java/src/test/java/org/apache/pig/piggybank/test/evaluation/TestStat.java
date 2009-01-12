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

import java.util.Iterator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.piggybank.evaluation.stats.COR;
import org.apache.pig.piggybank.evaluation.stats.COV;

import junit.framework.TestCase;

public class TestStat extends TestCase{
	
	public void testCOV() throws Exception{
		COV cov = new COV("a","b");
		DataBag dBag = DefaultBagFactory.getInstance().newDefaultBag();
		Tuple tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 1.0);
		dBag.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 4.0);
		dBag.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 8.0);
		dBag.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 4.0);
		dBag.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 7.0);
		dBag.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 8.0);
		dBag.add(tup1);
		DataBag dBag1 = DefaultBagFactory.getInstance().newDefaultBag();
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 2.0);
		dBag1.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 2.0);
		dBag1.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 3.0);
		dBag1.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 3.0);
		dBag1.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 2.0);
		dBag1.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 4.0);
		dBag1.add(tup1);
		Tuple input = DefaultTupleFactory.getInstance().newTuple(2);
		input.set(0, dBag);
		input.set(1, dBag1);
		DataBag output = cov.exec(input);
		Iterator<Tuple> it = output.iterator();
		Tuple ans = (Tuple)it.next();
		assertEquals((String)ans.get(0),"a");
		assertEquals((String)ans.get(1),"b");
		assertEquals(1.11111, (Double)ans.get(2),0.0005);
	}
	
	public void testCOR() throws Exception{
		COR cor = new COR("a","b");
		DataBag dBag = DefaultBagFactory.getInstance().newDefaultBag();
		Tuple tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 1.0);
		dBag.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 4.0);
		dBag.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 8.0);
		dBag.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 4.0);
		dBag.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 7.0);
		dBag.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 8.0);
		dBag.add(tup1);
		DataBag dBag1 = DefaultBagFactory.getInstance().newDefaultBag();
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 2.0);
		dBag1.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 2.0);
		dBag1.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 3.0);
		dBag1.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 3.0);
		dBag1.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 2.0);
		dBag1.add(tup1);
		tup1 = DefaultTupleFactory.getInstance().newTuple(1);
		tup1.set(0, 4.0);
		dBag1.add(tup1);
		Tuple input = DefaultTupleFactory.getInstance().newTuple(2);
		input.set(0, dBag);
		input.set(1, dBag1);
		DataBag output = cor.exec(input);
		Iterator<Tuple> it = output.iterator();
		Tuple ans = (Tuple) it.next();
		assertEquals((String)ans.get(0),"a");
		assertEquals((String)ans.get(1),"b");
		assertEquals(0.582222509739582, (Double)ans.get(2) ,0.0005);
	}
}
