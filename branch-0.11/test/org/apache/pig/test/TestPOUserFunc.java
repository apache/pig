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

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.pig.Algebraic;
import org.apache.pig.ComparisonFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.test.PORead;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserComparisonFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.test.utils.GenRandomData;
import org.junit.Test;

public class TestPOUserFunc extends TestCase {
	Random r = new Random();
	int MAX_TUPLES = 10;

	public static class ARITY extends EvalFunc<Integer> {

		@Override
		public Integer exec(Tuple input) throws IOException {
			try {
                return new Integer(((Tuple)input.get(0)).size());
            } catch (ExecException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return 0;
		}

		@Override
		public Schema outputSchema(Schema input) {
            return new Schema(new Schema.FieldSchema(null, DataType.INTEGER)); 
		}
	}

	public static class WeirdComparator extends ComparisonFunc {

		@Override
		public int compare(Tuple t1, Tuple t2) {
			// TODO Auto-generated method stub
			Object o1 = null;
			Object o2 = null;
			try {
				o1 = t1.get(2);
				o2 = t2.get(2);
			} catch (ExecException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
                        if ( o1==null || o2==null ){
                           return -1;
                        }
			int i1 = (Integer) o1 - 2;
			int i2 = (Integer) o2 - 2;

			return (int) (i1 * i1 - i2 * i2);
		}

	}

	/**
	 * Generates the average of the values of the first field of a tuple. This
	 * class is Algebraic in implemenation, so if possible the execution will be
	 * split into a local and global application
	 */
	public static class AVG extends EvalFunc<Double> implements Algebraic {

		private static TupleFactory mTupleFactory = TupleFactory.getInstance();

		@Override
		public Double exec(Tuple input) throws IOException {
			double sum = 0;
			double count = 0;
			
			try {
				sum = sum(input);
				count = count(input);
			} catch (ExecException e) {
				e.printStackTrace();
			}

			double avg = 0;
			if (count > 0)
				avg = sum / count;

			return new Double(avg);
		}

		public String getInitial() {
			return Initial.class.getName();
		}

		public String getIntermed() {
			return Intermed.class.getName();
		}

		public String getFinal() {
			return Final.class.getName();
		}

		static public class Initial extends EvalFunc<Tuple> {
			@Override
			public Tuple exec(Tuple input) throws IOException {
				try {
					Tuple t = mTupleFactory.newTuple(2);
					t.set(0, new Double(sum(input)));
					t.set(1, new Long(count(input)));
					return t;
				} catch (ExecException t) {
					throw new RuntimeException(t.getMessage() + ": " + input);
				}
			}
		}

		static public class Intermed extends EvalFunc<Tuple> {
			@Override
			public Tuple exec(Tuple input) throws IOException {
				DataBag b = null;
				Tuple t = null;
				try {
					b = (DataBag) input.get(0);
					t = combine(b);
				} catch (ExecException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return t;
			}
		}

		static public class Final extends EvalFunc<Double> {
			@Override
			public Double exec(Tuple input) throws IOException {
				double sum = 0;
				double count = 0;
				try {
					DataBag b = (DataBag) input.get(0);
					Tuple combined = combine(b);

					sum = (Double) combined.get(0);
					count = (Long) combined.get(1);
				} catch (ExecException e) {
					e.printStackTrace();
				}

				double avg = 0;
				if (count > 0) {
					avg = sum / count;
				}
				return new Double(avg);
			}
		}

		static protected Tuple combine(DataBag values) throws ExecException {
			double sum = 0;
			long count = 0;

			Tuple output = mTupleFactory.newTuple(2);

			for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
				Tuple t = it.next();
				sum += (Double) t.get(0);
				count += (Long) t.get(1);
			}

			output.set(0, new Double(sum));
			output.set(1, new Long(count));
			return output;
		}

		static protected long count(Tuple input) throws ExecException {
			DataBag values = (DataBag) input.get(0);
			return values.size();
		}

		static protected double sum(Tuple input) throws ExecException {
			DataBag values = (DataBag) input.get(0);

			double sum = 0;
			for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
				Tuple t = it.next();
				Double d = DataType.toDouble(t.get(0));
				if (d == null)
					continue;
				sum += d;
			}

			return sum;
		}

		@Override
		public Schema outputSchema(Schema input) {
            return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE)); 
		}

	}

	@Test
	public void testUserFuncArity() throws ExecException {
		DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBag(r,
				MAX_TUPLES, 100);
		userFuncArity( input );
        }

	@Test
	public void testUserFuncArityWithNulls() throws ExecException {
		DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBagWithNulls(r,
				MAX_TUPLES, 100);
		userFuncArity( input );
        }

	public void userFuncArity(DataBag input ) throws ExecException {
		String funcSpec = ARITY.class.getName() + "()";
		PORead read = new PORead(new OperatorKey("", r.nextLong()), input);
		List<PhysicalOperator> inputs = new LinkedList<PhysicalOperator>();
		inputs.add(read);
		POUserFunc userFunc = new POUserFunc(new OperatorKey("", r.nextLong()),
				-1, inputs, new FuncSpec(funcSpec));
		Result res = new Result();
		Integer i = null;
		res = userFunc.getNext(i);
		while (res.returnStatus != POStatus.STATUS_EOP) {
			// System.out.println(res.result);
			int result = (Integer) res.result;
			assertEquals(2, result);
			res = userFunc.getNext(i);
		}
	}


	@Test
	public void testUDFCompare() throws ExecException {

		DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBag(r, 2, 100);
	        udfCompare(input);

        }

	@Test
	public void testUDFCompareWithNulls() throws ExecException {

		DataBag input = (DataBag) GenRandomData.genRandSmallTupDataBagWithNulls(r, 2, 100);
	        udfCompare(input);

        }

	public void udfCompare(DataBag input) throws ExecException {

		String funcSpec = WeirdComparator.class.getName() + "()";
		POUserComparisonFunc userFunc = new POUserComparisonFunc(new OperatorKey("", r.nextLong()),
				-1, null, new FuncSpec(funcSpec));
		Iterator<Tuple> it = input.iterator();
		Tuple t1 = it.next();
		Tuple t2 = it.next();
		t1.append(2);
		t2.append(3);
		userFunc.attachInput(t1, t2);
		Integer i = null;
		// System.out.println(t1 + " " + t2);
		int result = (Integer) (userFunc.getNext(i).result);
		assertEquals(-1, result);
	}

	@Test
	public void testAlgebraicAVG() throws IOException, ExecException {

	     Integer input[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
             algebraicAVG( input, 55, 10, 110, 20, 5.5 );

        }

        /* NOTE: for calculating the average
         *
         * A pig "count" will include data that had "null",and the sum will
         * A pig "count" will include data that had "null",and the sum will
         * treat the null as a 0, impacting the average
         * A SQL "count" will exclude data that had "null"
         */
	@Test
	public void testAlgebraicAVGWithNulls() throws IOException, ExecException {

	     Integer input[] = { 1, 2, 3, 4, null, 6, 7, 8, 9, 10 };
             algebraicAVG( input, 50, 10, 100, 20, 5 );

        }

	@Test
	public void algebraicAVG( 
                 Integer[] input 
               , double initialExpectedSum, long initialExpectedCount
               , double intermedExpectedSum, long intermedExpectedCount
               , double expectedAvg
         ) throws IOException, ExecException {

                // generate data
		byte INIT = 0;
		byte INTERMED = 1;
		byte FINAL = 2;
		Tuple tup1 = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1),
				input);
		Tuple tup2 = Util.loadNestTuple(TupleFactory.getInstance().newTuple(1),
				input);
		// System.out.println("Input = " + tup1);
		String funcSpec = AVG.class.getName() + "()";

		POUserFunc po = new POUserFunc(new OperatorKey("", r.nextLong()), -1,
				null, new FuncSpec(funcSpec));

                //************ Initial Calculations ******************
		TupleFactory tf = TupleFactory.getInstance();
		po.setAlgebraicFunction(INIT);
		po.attachInput(tup1);
		Tuple t = null;
		Result res = po.getNext(t);
		Tuple outputInitial1 = (res.returnStatus == POStatus.STATUS_OK) ? (Tuple) res.result
				: null;
		Tuple outputInitial2 = (res.returnStatus == POStatus.STATUS_OK) ? (Tuple) res.result
				: null;
		System.out.println(outputInitial1 + " " + outputInitial2);
		assertEquals(outputInitial1, outputInitial2);
		double sum = (Double) outputInitial1.get(0);
		long count = (Long) outputInitial1.get(1);
		assertEquals(initialExpectedSum, sum);
		assertEquals(initialExpectedCount, count);

                //************ Intermediate Data and Calculations ******************
		DataBag bag = BagFactory.getInstance().newDefaultBag();
		bag.add(outputInitial1);
		bag.add(outputInitial2);
		Tuple outputInitial = tf.newTuple();
		outputInitial.append(bag);
		// Tuple outputIntermed = intermed.exec(outputInitial);
		po = new POUserFunc(new OperatorKey("", r.nextLong()), -1, null,
				new FuncSpec(funcSpec));
		po.setAlgebraicFunction(INTERMED);
		po.attachInput(outputInitial);
		res = po.getNext(t);
		Tuple outputIntermed = (res.returnStatus == POStatus.STATUS_OK) ? (Tuple) res.result
				: null;

		sum = (Double) outputIntermed.get(0);
		count = (Long) outputIntermed.get(1);
		assertEquals(intermedExpectedSum, sum);
		assertEquals(intermedExpectedCount, count);
		System.out.println(outputIntermed);

                //************ Final Calculations ******************
		po = new POUserFunc(new OperatorKey("", r.nextLong()), -1, null,
				new FuncSpec(funcSpec));
		po.setAlgebraicFunction(FINAL);
		po.attachInput(outputInitial);
		res = po.getNext(t);
		Double output = (res.returnStatus == POStatus.STATUS_OK) ? (Double) res.result
				: null;
		// Double output = fin.exec(outputInitial);
		assertEquals( expectedAvg, output);
		// System.out.println("output = " + output);

	}
}
