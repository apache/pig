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
import junit.framework.Assert;

import org.apache.pig.ComparisonFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;
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

    /***
     * Sorting
     *  (1, 10)
     *  (3, 8)
     *  (2, 8)
     *
     *  BY $1 DESC, $0 ASC 
     *
     *  should return
     *  (1, 10)
     *  (2 ,8)
     *  (3, 8)
     *
     * @throws ExecException
     */
    @Test
    public void testPOSortMixAscDesc1() throws ExecException {
        DataBag input = DefaultBagFactory.getInstance().newDefaultBag() ;

        Tuple t1 = DefaultTupleFactory.getInstance().newTuple() ;
        t1.append(1);
        t1.append(10);
        input.add(t1);

        Tuple t2 = DefaultTupleFactory.getInstance().newTuple() ;
        t2.append(3);
        t2.append(8);
        input.add(t2);


        Tuple t3 = DefaultTupleFactory.getInstance().newTuple() ;
        t3.append(2);
        t3.append(8);
        input.add(t3);

        List<PhysicalPlan> sortPlans = new LinkedList<PhysicalPlan>();

        POProject pr1 = new POProject(new OperatorKey("", r.nextLong()), -1, 1);
        pr1.setResultType(DataType.INTEGER);
        PhysicalPlan expPlan1 = new PhysicalPlan();
        expPlan1.add(pr1);
        sortPlans.add(expPlan1);

        POProject pr2 = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        pr2.setResultType(DataType.INTEGER);
        PhysicalPlan expPlan2 = new PhysicalPlan();
        expPlan2.add(pr2);
        sortPlans.add(expPlan2);

        List<Boolean> mAscCols = new LinkedList<Boolean>();
        mAscCols.add(false);
        mAscCols.add(true);

        PORead read = new PORead(new OperatorKey("", r.nextLong()), input);
        List<PhysicalOperator> inputs = new LinkedList<PhysicalOperator>();
        inputs.add(read);

        POSort sort = new POSort(new OperatorKey("", r.nextLong()), -1, inputs,
                                 sortPlans, mAscCols, null);

        Tuple t = null;
        Result res ;
        // output line 1
        res = sort.getNext(t);
        Assert.assertEquals(((Tuple) res.result).get(0), 1) ;
        Assert.assertEquals(((Tuple) res.result).get(1), 10) ;
        // output line 2
        res = sort.getNext(t);
        Assert.assertEquals(((Tuple) res.result).get(0), 2) ;
        Assert.assertEquals(((Tuple) res.result).get(1), 8) ;
        // output line 3
        res = sort.getNext(t);
        Assert.assertEquals(((Tuple) res.result).get(0), 3) ;
        Assert.assertEquals(((Tuple) res.result).get(1), 8) ;
        
    }


    /***
     * Sorting
     *  (1, 2)
     *  (3, 5)
     *  (3, 8)
     *
     *  BY $0 DESC, $1 ASC
     *
     *  should return
     *  (3, 5)
     *  (3 ,8)
     *  (1, 2)
     *
     * @throws ExecException
     */
    
    @Test
    public void testPOSortMixAscDesc2() throws ExecException {
        DataBag input = DefaultBagFactory.getInstance().newDefaultBag() ;

        Tuple t1 = DefaultTupleFactory.getInstance().newTuple() ;
        t1.append(1);
        t1.append(2);
        input.add(t1);

        Tuple t2 = DefaultTupleFactory.getInstance().newTuple() ;
        t2.append(3);
        t2.append(5);
        input.add(t2);


        Tuple t3 = DefaultTupleFactory.getInstance().newTuple() ;
        t3.append(3);
        t3.append(8);
        input.add(t3);

        List<PhysicalPlan> sortPlans = new LinkedList<PhysicalPlan>();

        POProject pr1 = new POProject(new OperatorKey("", r.nextLong()), -1, 0);
        pr1.setResultType(DataType.INTEGER);
        PhysicalPlan expPlan1 = new PhysicalPlan();
        expPlan1.add(pr1);
        sortPlans.add(expPlan1);

        POProject pr2 = new POProject(new OperatorKey("", r.nextLong()), -1, 1);
        pr2.setResultType(DataType.INTEGER);
        PhysicalPlan expPlan2 = new PhysicalPlan();
        expPlan2.add(pr2);
        sortPlans.add(expPlan2);

        List<Boolean> mAscCols = new LinkedList<Boolean>();
        mAscCols.add(false);
        mAscCols.add(true);

        PORead read = new PORead(new OperatorKey("", r.nextLong()), input);
        List<PhysicalOperator> inputs = new LinkedList<PhysicalOperator>();
        inputs.add(read);

        POSort sort = new POSort(new OperatorKey("", r.nextLong()), -1, inputs,
                                 sortPlans, mAscCols, null);

        Tuple t = null;
        Result res ;
        // output line 1
        res = sort.getNext(t);
        Assert.assertEquals(((Tuple) res.result).get(0), 3) ;
        Assert.assertEquals(((Tuple) res.result).get(1), 5) ;
        // output line 2
        res = sort.getNext(t);
        Assert.assertEquals(((Tuple) res.result).get(0), 3) ;
        Assert.assertEquals(((Tuple) res.result).get(1), 8) ;
        // output line 3
        res = sort.getNext(t);
        Assert.assertEquals(((Tuple) res.result).get(0), 1) ;
        Assert.assertEquals(((Tuple) res.result).get(1), 2) ;

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
				new OperatorKey("", r.nextLong()), -1, null, new FuncSpec(funcName));
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
