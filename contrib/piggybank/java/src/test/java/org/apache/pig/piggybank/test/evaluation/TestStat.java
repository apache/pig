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
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.piggybank.evaluation.stats.COR;
import org.apache.pig.piggybank.evaluation.stats.COV;

import junit.framework.TestCase;

public class TestStat extends TestCase{
	
	public void testCOV() throws Exception{
		EvalFunc<DataBag> COV = new COV("a","b");
		DataBag dBag = new DefaultDataBag();
		Tuple tup1 = new Tuple(1);
		tup1.setField(0, 1);
		dBag.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 4);
		dBag.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 8);
		dBag.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 4);
		dBag.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 7);
		dBag.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 8);
		dBag.add(tup1);
		DataBag dBag1 = new DefaultDataBag();
		tup1 = new Tuple(1);
		tup1.setField(0, 2);
		dBag1.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 2);
		dBag1.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 3);
		dBag1.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 3);
		dBag1.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 2);
		dBag1.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 4);
		dBag1.add(tup1);
		Tuple input = new Tuple(2);
		input.setField(0, dBag);
		input.setField(1, dBag1);
		DataBag output = new DefaultDataBag();
		COV.exec(input, output);
		Iterator<Tuple> it = output.iterator();
		Tuple ans = (Tuple) it.next();
		assertEquals(ans.getAtomField(0).toString(),"a");
		assertEquals(ans.getAtomField(1).toString(),"b");
		assertEquals(1.11111, ans.getAtomField(2).numval(),0.0005);
	}
	
	public void testCOR() throws Exception{
		EvalFunc<DataBag> COR = new COR("a","b");
		DataBag dBag = new DefaultDataBag();
		Tuple tup1 = new Tuple(1);
		tup1.setField(0, 1);
		dBag.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 4);
		dBag.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 8);
		dBag.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 4);
		dBag.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 7);
		dBag.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 8);
		dBag.add(tup1);
		DataBag dBag1 = new DefaultDataBag();
		tup1 = new Tuple(1);
		tup1.setField(0, 2);
		dBag1.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 2);
		dBag1.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 3);
		dBag1.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 3);
		dBag1.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 2);
		dBag1.add(tup1);
		tup1 = new Tuple(1);
		tup1.setField(0, 4);
		dBag1.add(tup1);
		Tuple input = new Tuple(2);
		input.setField(0, dBag);
		input.setField(1, dBag1);
		DataBag output = new DefaultDataBag();
		COR.exec(input, output);
		Iterator<Tuple> it = output.iterator();
		Tuple ans = (Tuple) it.next();
		assertEquals(ans.getAtomField(0).toString(),"a");
		assertEquals(ans.getAtomField(1).toString(),"b");
		assertEquals(0.582222509739582, ans.getAtomField(2).numval(),0.0005);
	}
	
	
}
