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

import java.util.Map;
import java.util.Random;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POMapLookUp;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.test.utils.GenRandomData;
import org.junit.Test;

import junit.framework.TestCase;

public class TestPOMapLookUp extends TestCase {
	
	Random r = new Random();
	Map<String, Object> map;// = GenRandomData.genRandMap(r, 10);
	
	@Test
	public void testMapLookUp() throws PlanException, ExecException {
		
		POProject prj = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		POMapLookUp op = new POMapLookUp(new OperatorKey("", r.nextLong()), -1);
		PhysicalPlan plan = new PhysicalPlan();
		plan.add(op);
		plan.add(prj);
		plan.connect(prj, op);
		
		for(int i = 0 ; i < 10; i++) {
			map = GenRandomData.genRandMap(r, 10);
			Tuple t = TupleFactory.getInstance().newTuple();
			t.append(map);
			for(Map.Entry<String, Object> e : map.entrySet()) {
				op.setLookUpKey(e.getKey());
				plan.attachInput(t);
				Result res = op.getNext(map);
				//System.out.println(e.getValue() + " : " + res.result);
				assertEquals(e.getValue(), res.result);
			}
			
			
		}
		
	}
}
