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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.pig.ComparisonFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserComparisonFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.test.utils.GenRandomData;
import org.junit.Test;

public class TestPOSort {
    Random r = new Random(42L);
    int MAX_TUPLES = 10;

    @Test
    public void testPOSortAscString() throws ExecException {
        DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBag(r,
                MAX_TUPLES, 100);
        poSortAscString( input );
    }

    @Test
    public void testPOSortAscStringWithNull() throws ExecException {
        DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBagWithNulls(r,
                MAX_TUPLES, 100);
        poSortAscString( input );
    }



    public void poSortAscString(DataBag input) throws ExecException {

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

        //verify
        Tuple t = null;
        Result res1 = sort.getNextTuple();
        Result res2 = sort.getNextTuple();

        while (res2.returnStatus != POStatus.STATUS_EOP) {
            Object i1 = ((Tuple) res1.result).get(0);
            Object i2 = ((Tuple) res2.result).get(0);

            //System.out.println("i1: " + i1.toString() + " i2: " + i2.toString());
            int i = DataType.compare(i1, i2);
            System.out.println("RESULT2=i : " + res2.result + " i = " + i);
            assertEquals(true, (i <= 0));
            res1 = res2;
            res2 = sort.getNextTuple();
        }
    }


    @Test
    public void testPOSortDescString() throws ExecException {
        DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBag(r,
                MAX_TUPLES, 100);
        poSortDescString(input);
    }

    @Test
    public void testPOSortDescStringWithNulls() throws ExecException {
        DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBagWithNulls(r,
                MAX_TUPLES, 100);
        poSortDescString(input);
    }


    public void poSortDescString(DataBag input) throws ExecException {

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
        Result res1 = sort.getNextTuple();
        // System.out.println(res1.result);
        Result res2 = sort.getNextTuple();
        while (res2.returnStatus != POStatus.STATUS_EOP) {
            Object i1 = ((Tuple) res1.result).get(0);
            Object i2 = ((Tuple) res2.result).get(0);
            int i = DataType.compare(i1, i2);
            // System.out.println(res2.result + " i = " + i);
            assertEquals(true, (i >= 0));
            res1 = res2;
            res2 = sort.getNextTuple();
        }
    }

    @Test
    public void testPOSortAsc() throws ExecException {
        DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBag(r,
                MAX_TUPLES, 100);
        poSortAscInt( input );

    }

    @Test
    public void testPOSortAscWithNulls() throws ExecException {
        DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBagWithNulls(r,
                MAX_TUPLES, 100);
        poSortAscInt( input );

    }


    public void poSortAscInt( DataBag input) throws ExecException {

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
        Result res1 = sort.getNextTuple();
        // System.out.println(res1.result);
        Result res2 = sort.getNextTuple();
        while (res2.returnStatus != POStatus.STATUS_EOP) {
            Object i1 = ((Tuple) res1.result).get(1);
            Object i2 = ((Tuple) res2.result).get(1);
            int i = DataType.compare(i1, i2);
            assertEquals(true, (i <= 0));
            // System.out.println(res2.result);
            res1 = res2;
            res2 = sort.getNextTuple();
        }
    }

    @Test
    public void testPOSortDescInt() throws ExecException {
        DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBag(r,
                MAX_TUPLES, 100);
        poSortDescInt(input );
    }

    @Test
    public void testPOSortDescIntWithNulls() throws ExecException {
        DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBagWithNulls(r,
                MAX_TUPLES, 100);
        poSortDescInt(input );
    }

    public void poSortDescInt(DataBag input) throws ExecException {
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
        Result res1 = sort.getNextTuple();
        // System.out.println(res1.result);
        Result res2 = sort.getNextTuple();
        while (res2.returnStatus != POStatus.STATUS_EOP) {
            Object i1 = ((Tuple) res1.result).get(1);
            Object i2 = ((Tuple) res2.result).get(1);
            int i = DataType.compare(i1, i2);
            assertEquals(true, (i >= 0));
            // System.out.println(res2.result);
            res1 = res2;
            res2 = sort.getNextTuple();
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

        Tuple t1 = TupleFactory.getInstance().newTuple() ;
        t1.append(1);
        t1.append(10);
        input.add(t1);

        Tuple t2 = TupleFactory.getInstance().newTuple() ;
        t2.append(3);
        t2.append(8);
        input.add(t2);


        Tuple t3 = TupleFactory.getInstance().newTuple() ;
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
        res = sort.getNextTuple();
        assertEquals(((Tuple) res.result).get(0), 1) ;
        assertEquals(((Tuple) res.result).get(1), 10) ;
        // output line 2
        res = sort.getNextTuple();
        assertEquals(((Tuple) res.result).get(0), 2) ;
        assertEquals(((Tuple) res.result).get(1), 8) ;
        // output line 3
        res = sort.getNextTuple();
        assertEquals(((Tuple) res.result).get(0), 3) ;
        assertEquals(((Tuple) res.result).get(1), 8) ;

    }

    /***
     * Sorting
     *  (null, 10)
     *  (1, 8)
     *  (1, null)
     *  (null,null)
     *  (3, 8)
     *
     *  BY $1 DESC, $0 ASC
     *
     *  should return
     *  (null, 10)
     *  (1, 8 )
     *  (3, 8 )
     *  (null,null)
     *  (1, null)


     * @throws ExecException
     */
    @Test
    public void testPOSortMixAscDesc1WithNull() throws ExecException {
        DataBag input = DefaultBagFactory.getInstance().newDefaultBag() ;

        Tuple t1 = TupleFactory.getInstance().newTuple() ;
        t1.append(null);
        t1.append(10);
        input.add(t1);

        Tuple t2 = TupleFactory.getInstance().newTuple() ;
        t2.append(1);
        t2.append(8);
        input.add(t2);


        Tuple t3 = TupleFactory.getInstance().newTuple() ;
        t3.append(1);
        t3.append(null);
        input.add(t3);

        Tuple t4 = TupleFactory.getInstance().newTuple() ;
        t4.append(null);
        t4.append(null);
        input.add(t4);

        Tuple t5 = TupleFactory.getInstance().newTuple() ;
        t5.append(3);
        t5.append(8);
        input.add(t5);

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
        res = sort.getNextTuple();
        assertNull(((Tuple) res.result).get(0)) ;
        assertEquals(((Tuple) res.result).get(1), 10) ;
         // output line 2
        res = sort.getNextTuple();
        assertEquals(((Tuple) res.result).get(0), 1) ;
        assertEquals(((Tuple) res.result).get(1), 8) ;
        // output line 3
        res = sort.getNextTuple();
        assertEquals(((Tuple) res.result).get(0), 3) ;
        assertEquals(((Tuple) res.result).get(1), 8) ;
        // output line 4
        res = sort.getNextTuple();
        assertNull(((Tuple) res.result).get(0)) ;
        assertNull(((Tuple) res.result).get(1)) ;
        // output line 5
        res = sort.getNextTuple();
        assertEquals(((Tuple) res.result).get(0), 1 );
        assertNull(((Tuple) res.result).get(1)) ;


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

        Tuple t1 = TupleFactory.getInstance().newTuple() ;
        t1.append(1);
        t1.append(2);
        input.add(t1);

        Tuple t2 = TupleFactory.getInstance().newTuple() ;
        t2.append(3);
        t2.append(5);
        input.add(t2);


        Tuple t3 = TupleFactory.getInstance().newTuple() ;
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
        res = sort.getNextTuple();
        assertEquals(((Tuple) res.result).get(0), 3) ;
        assertEquals(((Tuple) res.result).get(1), 5) ;
        // output line 2
        res = sort.getNextTuple();
        assertEquals(((Tuple) res.result).get(0), 3) ;
        assertEquals(((Tuple) res.result).get(1), 8) ;
        // output line 3
        res = sort.getNextTuple();
        assertEquals(((Tuple) res.result).get(0), 1) ;
        assertEquals(((Tuple) res.result).get(1), 2) ;

    }

    /***
     * Sorting
     *  (null, 10)
     *  (1, 8)
     *  (1, null)
     *  (null,null)
     *  (3, 8)
     *
     *  BY $0 DESC, $1 ASC
     *
     *  should return
     *  (3, 8 )
     *  (1, null)
     *  (1, 8 )
     *  (null,null)
     *  (null, 10)
     * @throws ExecException
     */

    @Test
    public void testPOSortMixAscDesc2Null() throws ExecException {
        DataBag input = DefaultBagFactory.getInstance().newDefaultBag() ;

        Tuple t1 = TupleFactory.getInstance().newTuple() ;
        t1.append(null);
        t1.append(10);
        input.add(t1);

        Tuple t2 = TupleFactory.getInstance().newTuple() ;
        t2.append(1);
        t2.append(8);
        input.add(t2);


        Tuple t3 = TupleFactory.getInstance().newTuple() ;
        t3.append(1);
        t3.append(null);
        input.add(t3);

        Tuple t4 = TupleFactory.getInstance().newTuple() ;
        t4.append(null);
        t4.append(null);
        input.add(t4);

        Tuple t5 = TupleFactory.getInstance().newTuple() ;
        t5.append(3);
        t5.append(8);
        input.add(t5);

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
        res = sort.getNextTuple();
        assertEquals(((Tuple) res.result).get(0), 3) ;
        assertEquals(((Tuple) res.result).get(1), 8) ;
        // output line 2
        res = sort.getNextTuple();
        assertEquals(((Tuple) res.result).get(0), 1) ;
        assertNull(((Tuple) res.result).get(1)) ;
        // output line 3
        res = sort.getNextTuple();
        assertEquals(((Tuple) res.result).get(0), 1) ;
        assertEquals(((Tuple) res.result).get(1), 8) ;
        // output line 4
        res = sort.getNextTuple();
        assertNull(((Tuple) res.result).get(0)) ;
        assertNull(((Tuple) res.result).get(1)) ;
        // output line 5
        res = sort.getNextTuple();
        assertNull(((Tuple) res.result).get(0)) ;
        assertEquals(((Tuple) res.result).get(1), 10) ;


    }

    @Test
    public void testPOSortUDF() throws ExecException {
        DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBag(r,
                MAX_TUPLES, 100);
        poSortUDFWithNull(input);

    }

    @Test
    public void testPOSortUDFWithNull() throws ExecException {
        DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBagWithNulls(r,
                MAX_TUPLES, 100);
        poSortUDFWithNull(input);

    }


    public void poSortUDFWithNull(DataBag input) throws ExecException {
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
        Result res1 = sort.getNextTuple();
        // System.out.println(res1.result);
        Result res2 = sort.getNextTuple();
        while (res2.returnStatus != POStatus.STATUS_EOP) {
            int i1 = ((Integer) ((Tuple) res1.result).get(1) == null ? 0 : (Integer) ((Tuple) res1.result).get(1));
            int i2 = ((Integer) ((Tuple) res2.result).get(1) == null ? 0 : (Integer) ((Tuple) res2.result).get(1));
            int i = (i1 - 50) * (i1 - 50) - (i2 - 50) * (i2 - 50);
            assertEquals(true, (i <= 0));
            System.out.println(i + " : " + res2.result);
            res1 = res2;
            res2 = sort.getNextTuple();
        }

    }
    // sorts values in ascending order of their distance from 50
    public static class WeirdComparator extends ComparisonFunc {

        @Override
        public int compare(Tuple t1, Tuple t2) {
            // TODO Auto-generated method stub
            int result = 0;
            try {
                int i1 = ((Integer) t1.get(1) == null ? 0 : (Integer)t1.get(1));
                int i2 = ((Integer) t2.get(1) == null ? 0 : (Integer)t2.get(1));
                result = (i1 - 50) * (i1 - 50) - (i2 - 50) * (i2 - 50);
            } catch (ExecException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return result;
        }
    }
}
