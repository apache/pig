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

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.pig.ComparisonFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.physicalLayer.relationalOperators.PORead;
import org.apache.pig.impl.physicalLayer.relationalOperators.POSort;
import org.apache.pig.impl.physicalLayer.expressionOperators.POProject;
import org.apache.pig.impl.physicalLayer.expressionOperators.POUserComparisonFunc;
import org.apache.pig.impl.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.test.utils.GenRandomData;
import org.junit.Test;

public class TestPOSort extends TestCase {
	Random r = new Random();
	int MAX_TUPLES = 10;

	@Test
	public void testPOSortAscString() throws ExecException {
		DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBag(r,
				MAX_TUPLES, 100);
		List<PhysicalPlan> sortPlans = new LinkedList<PhysicalPlan>();
		POProject pr1 = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		pr1.setResultType(DataType.CHARARRAY);
		PhysicalPlan expPlan = new PhysicalPlan();
		expPlan.add(pr1);
		sortPlans.add(expPlan);
		List<Boolean> mAscCols = new LinkedList<Boolean>();
		mAscCols.add(true);
		PORead read = new PORead(new OperatorKey("", r.nextLong()), input);
		List<PhysicalOperator> inputs = new LinkedList<PhysicalOperator>();
		inputs.add(read);
		POSort sort = new POSort(new OperatorKey("", r.nextLong()), -1, inputs,
				sortPlans, mAscCols, null);
		Tuple t = null;
		Result res1 = sort.getNext(t);
		// System.out.println(res1.result);
		Result res2 = sort.getNext(t);
		while (res2.returnStatus != POStatus.STATUS_EOP) {
			Object i1 = ((Tuple) res1.result).get(0);
			Object i2 = ((Tuple) res2.result).get(0);
			int i = DataType.compare(i1, i2);
			// System.out.println(res2.result + " i = " + i);
			assertEquals(true, (i <= 0));
			res1 = res2;
			res2 = sort.getNext(t);
		}
	}

	@Test
	public void testPOSortDescString() throws ExecException {
		DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBag(r,
				MAX_TUPLES, 100);
		List<PhysicalPlan> sortPlans = new LinkedList<PhysicalPlan>();
		POProject pr1 = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
		pr1.setResultType(DataType.CHARARRAY);
		PhysicalPlan expPlan = new PhysicalPlan();
		expPlan.add(pr1);
		sortPlans.add(expPlan);
		List<Boolean> mAscCols = new LinkedList<Boolean>();
		mAscCols.add(false);
		PORead read = new PORead(new OperatorKey("", r.nextLong()), input);
		List<PhysicalOperator> inputs = new LinkedList<PhysicalOperator>();
		inputs.add(read);
		POSort sort = new POSort(new OperatorKey("", r.nextLong()), -1, inputs,
				sortPlans, mAscCols, null);
		Tuple t = null;
		Result res1 = sort.getNext(t);
		// System.out.println(res1.result);
		Result res2 = sort.getNext(t);
		while (res2.returnStatus != POStatus.STATUS_EOP) {
			Object i1 = ((Tuple) res1.result).get(0);
			Object i2 = ((Tuple) res2.result).get(0);
			int i = DataType.compare(i1, i2);
			// System.out.println(res2.result + " i = " + i);
			assertEquals(true, (i >= 0));
			res1 = res2;
			res2 = sort.getNext(t);
		}
	}

	@Test
	public void testPOSortAsc() throws ExecException {
		DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBag(r,
				MAX_TUPLES, 100);
		List<PhysicalPlan> sortPlans = new LinkedList<PhysicalPlan>();
		POProject pr1 = new POProject(new OperatorKey("", r.nextLong()), -1, 1);
		pr1.setResultType(DataType.INTEGER);
		PhysicalPlan expPlan = new PhysicalPlan();
		expPlan.add(pr1);
		sortPlans.add(expPlan);
		List<Boolean> mAscCols = new LinkedList<Boolean>();
		mAscCols.add(true);
		PORead read = new PORead(new OperatorKey("", r.nextLong()), input);
		List<PhysicalOperator> inputs = new LinkedList<PhysicalOperator>();
		inputs.add(read);
		POSort sort = new POSort(new OperatorKey("", r.nextLong()), -1, inputs,
				sortPlans, mAscCols, null);
		Tuple t = null;
		Result res1 = sort.getNext(t);
		// System.out.println(res1.result);
		Result res2 = sort.getNext(t);
		while (res2.returnStatus != POStatus.STATUS_EOP) {
			Object i1 = ((Tuple) res1.result).get(1);
			Object i2 = ((Tuple) res2.result).get(1);
			int i = DataType.compare(i1, i2);
			assertEquals(true, (i <= 0));
			// System.out.println(res2.result);
			res1 = res2;
			res2 = sort.getNext(t);
		}
	}

	@Test
	public void testPOSortDesc() throws ExecException {
		DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBag(r,
				MAX_TUPLES, 100);
		List<PhysicalPlan> sortPlans = new LinkedList<PhysicalPlan>();
		POProject pr1 = new POProject(new OperatorKey("", r.nextLong()), -1, 1);
		pr1.setResultType(DataType.INTEGER);
		PhysicalPlan expPlan = new PhysicalPlan();
		expPlan.add(pr1);
		sortPlans.add(expPlan);
		List<Boolean> mAscCols = new LinkedList<Boolean>();
		mAscCols.add(false);
		PORead read = new PORead(new OperatorKey("", r.nextLong()), input);
		List<PhysicalOperator> inputs = new LinkedList<PhysicalOperator>();
		inputs.add(read);
		POSort sort = new POSort(new OperatorKey("", r.nextLong()), -1, inputs,
				sortPlans, mAscCols, null);
		Tuple t = null;
		Result res1 = sort.getNext(t);
		// System.out.println(res1.result);
		Result res2 = sort.getNext(t);
		while (res2.returnStatus != POStatus.STATUS_EOP) {
			Object i1 = ((Tuple) res1.result).get(1);
			Object i2 = ((Tuple) res2.result).get(1);
			int i = DataType.compare(i1, i2);
			assertEquals(true, (i >= 0));
			// System.out.println(res2.result);
			res1 = res2;
			res2 = sort.getNext(t);
		}
	}

	@Test
	public void testPOSortUDF() throws ExecException {
		DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBag(r,
				MAX_TUPLES, 100);
		PORead read = new PORead(new OperatorKey("", r.nextLong()), input);
		List<PhysicalOperator> inputs = new LinkedList<PhysicalOperator>();
		inputs.add(read);
		String funcName = WeirdComparator.class.getName() + "()";
		/*POUserFunc comparator = new POUserFunc(
				new OperatorKey("", r.nextLong()), -1, inputs, funcName);*/
		POUserComparisonFunc comparator = new POUserComparisonFunc(
				new OperatorKey("", r.nextLong()), -1, null, funcName);
		POSort sort = new POSort(new OperatorKey("", r.nextLong()), -1, inputs,
				null, null, comparator);
		Tuple t = null;
		Result res1 = sort.getNext(t);
		// System.out.println(res1.result);
		Result res2 = sort.getNext(t);
		while (res2.returnStatus != POStatus.STATUS_EOP) {
			int i1 = (Integer) ((Tuple) res1.result).get(1);
			int i2 = (Integer) ((Tuple) res2.result).get(1);
			int i = (i1 - 50) * (i1 - 50) - (i2 - 50) * (i2 - 50);
			assertEquals(true, (i <= 0));
			System.out.println(i + " : " + res2.result);
			res1 = res2;
			res2 = sort.getNext(t);
		}
	}

	// sorts values in ascending order of their distance from 50
	public static class WeirdComparator extends ComparisonFunc {

		@Override
		public int compare(Tuple t1, Tuple t2) {
			// TODO Auto-generated method stub
			int result = 0;
			try {
				int i1 = (Integer) t1.get(1);
				int i2 = (Integer) t2.get(1);
				result = (i1 - 50) * (i1 - 50) - (i2 - 50) * (i2 - 50);
			} catch (ExecException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return result;
		}

	}
}
