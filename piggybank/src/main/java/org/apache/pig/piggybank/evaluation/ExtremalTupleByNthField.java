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
package org.apache.pig.piggybank.evaluation;

import java.io.IOException;
import java.lang.reflect.Type;

import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigProgressable;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * 
 * This class is similar to MaxTupleBy1stField except that it allows you to
 * specify with field to use (instead of just using 1st field) and to specify
 * ascending or descending. The first parameter in the constructor specifies
 * which field to use and the second parameter to the constructor specifies
 * which extremal to retrieve. Strings prefixed by "min", "least", "desc",
 * "small" and "-", irrespective of capitalization and leading white spacing,
 * specifies the computation of the minimum and all other strings means maximum;
 * 
 * <h2>Example1: Invoking the UDF</h2> e.g. Using this udf: <br />
 * <code>
 * 		define myMin ExtremalTupleByNthField( '4', 'min' ); 
 * 		T = group G ALL; 
 * 		R = foreach T generate myMin(G);
 * </code> is equivalent to:<br />
 * 
 * <code>
 * 		T = order G by $3 asc; 
 * 		R = limit G 1;
 * </code>
 * 
 * Note above 4 indicates the field with index 3 in the tuple. The 4th field can
 * be any comparable type, so you can use float, int, string, or even tuples.
 * 
 * By default constructor, this UDF behaves as MaxTupleBy1stField in that it
 * chooses the max tuple by the comparable in the first field.
 * 
 * <h2>Example 2: Default behavior</h2> This class also has a one parameter
 * constructor that specifies the index and takes the max tuple from the bag.
 * <code>
 * 		define myMax ExtremalTupleByNthField( '3' ); 
 * 		T = group G ALL; 
 * 		R = foreach T generate myMax(G);
 * </code> is equivalent to:<br />
 * 
 * <code>
 * 		T = order G by $2 desc; 
 * 		R = limit G 1;
 * </code>
 * 
 * <h2>Example 3: Choosing a Large Bag or Tuple</h2> Another possible use case
 * is the choosing of larger or smaller bags/tuples. In pig, bags and tuples are
 * comparable and the comparison is based on size.
 * 
 * <code>
 *      define biggestBag ExtremalTupleByNthField('1', max);
 * 		R = group TABLE by (key1, key2);
 *      G = cogroup L by key1, R by group.key1; 
 * 		V = foreach G generate L, biggestBag(R);
 * </code>
 * 
 * This results in each L(eft) bag associated with only the largest bag from the
 * R(ight) table. If all bags in R are of equal size, the comparator continues
 * on to perform element-wise comparison. In case of a complete tie in the
 * comparison, which result is returned is nondeterministic. But because this
 * class is able to compare any comparable we are able to specify a secondary
 * key.
 * 
 * <h2>Example 4: Secondary Sort Key</h2>
 * 
 * <code>
 *      define biggestBag ExtremalTupleByNthField('1', max);
 *      G = cogroup L by key1, M by key1, R by key1; 
 * 		V = foreach G generate FLATTEN(L),
 *                             biggestBag(R.($0, $1, $2, $5)) as best_result_by_0, 
 *                             biggestBag(R.($3, $1, $2, $5)) as best_result_by_3,
 *                             biggestBag(M.($0, $2)) as best_misc_data;
 * </code>
 * 
 * this will generate two sets of results and misc data based on two separate
 * criterion. Since all tuples in the bags have the same size (4, 4, 2
 * respectively), the tuple comparator continues on and compares the members of
 * tuples until it finds one. best_result_by_0 and best_result_by3 are ordered
 * by 1st and 4th member of the tuples. Within each group, ties are broken by
 * second and third field.
 * 
 * 
 * Finally, note that the udf implements both Algebraic and Accumulator, so it
 * is relatively efficient because it's a one-pass algorithm.
 * 
 * 
 */
public class ExtremalTupleByNthField extends EvalFunc<Tuple> implements
		Algebraic, Accumulator<Tuple> {
	/**
	 * Indicates once for how many items progress heart beat should be sent.
	 * This number has been increased from 10 to reduce verbosity.
	 */
	private static final int PROGRESS_FREQUENCY = 10000;

	int fieldIndex;
	int sign;

	/**
	 * 
	 * 
	 * 
	 * 
	 * 
	 * Constructors
	 * 
	 * @throws ExecException
	 * 
	 */

	// defaults to max by first field
	public ExtremalTupleByNthField() throws ExecException {
		this("1", "max");
	}

	// defaults to max
	public ExtremalTupleByNthField(String fieldIndexString)
			throws ExecException {
		this(fieldIndexString, "max");
	}

	public ExtremalTupleByNthField(String fieldIndexString, String order)
			throws ExecException {
		super();
		this.fieldIndex = parseFieldIndex(fieldIndexString);
		this.sign = parseOrdering(order);
	}

	/**
	 * 
	 * 
	 * 
	 * 
	 * 
	 * The EvalFunc interface
	 */
	@Override
	public Tuple exec(Tuple input) throws IOException {
		return extreme(fieldIndex, sign, input, reporter);
	}

	@Override
	public Type getReturnType() {
		return Tuple.class;
	}

	public Schema outputSchema(Schema input) {
		return input;
	}

	/**
	 * 
	 * 
	 * 
	 * 
	 * 
	 * Algebraic interface
	 */
	@Override
	public String getInitial() {
		return HelperClass.class.getName();
	}

	@Override
	public String getIntermed() {
		return HelperClass.class.getName();
	}

	@Override
	public String getFinal() {
		return HelperClass.class.getName();
	}

	/**
	 * 
	 * 
	 * 
	 * 
	 * 
	 * The Accumulator interface
	 */
	Tuple intermediate = null;
	DataBag tempDb = BagFactory.getInstance().newDefaultBag();
	Tuple parameterToExtreme = TupleFactory.getInstance().newTuple(tempDb);

	@Override
	public void accumulate(Tuple b) throws IOException {
		try {
			if (b != null) {
				if (intermediate == null) {
					// intermediate = b;
					// make a shallow copy in case the Tuple was reused.
					intermediate = TupleFactory.getInstance()
							.newTuple(b.size());
					for (int i = 0; i < b.size(); ++i) {
						intermediate.set(i, b.get(i));
					}
				} else {
					tempDb.clear();
					tempDb.add(b);
					tempDb.add(intermediate);
					intermediate = extreme(fieldIndex, sign,
							parameterToExtreme, reporter);
				}
			}// new result is null, don't consider it

		} catch (ExecException ee) {
			throw ee;
		} catch (Exception e) {
			int errCode = -1;
			String msg = "Error while computing ExtremalTupleByNthField in "
					+ this.getClass().getSimpleName();
			throw new ExecException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void cleanup() {
		intermediate = null;
	}

	@Override
	public Tuple getValue() {
		return intermediate; // could be null correctly
	}

	/**
	 * 
	 * 
	 * 
	 * 
	 * Utility classes and methods
	 * 
	 */
	public static final class HelperClass extends EvalFunc<Tuple> {
		int fieldIndex, sign;

		public HelperClass() throws ExecException {
			this("1", "max");
		}

		public HelperClass(String fieldIndexString) throws ExecException {
			this(fieldIndexString, "max");
		}

		public HelperClass(String fieldIndexString, String order)
				throws ExecException {

			this.fieldIndex = parseFieldIndex(fieldIndexString);
			this.sign = parseOrdering(order);
		}

		public Tuple exec(Tuple input) throws IOException {
			return extreme(fieldIndex, sign, input, reporter);
		}

	}

	@SuppressWarnings("unchecked")
	protected final static Tuple extreme(int pind, int psign, Tuple input,
			PigProgressable reporter) throws ExecException {
		DataBag values = (DataBag) input.get(0);

		// if we were handed an empty bag, return NULL
		// this is in compliance with SQL standard
		if (values.size() == 0)
			return null;

		java.lang.Comparable curMax = null;
		Tuple curMaxTuple = null;
		int n = 0;
		for (Tuple t : values) {
			if (reporter != null && ++n % PROGRESS_FREQUENCY == 0)
				reporter.progress();
			if (t == null) {
				// just in case.
				continue;
			}
			try {
				Object o = t.get(pind);
				if (o == null) {
					// if the comparison field is null it will never be
					// returned, we won't even compare.
					continue;
				}

				java.lang.Comparable d = (java.lang.Comparable) o;

				if (curMax == null) {
					curMax = d;
					curMaxTuple = t;
				} else {
					/**
					 * <pre>
					 * c > 0 iff ((sign==1 && d>curMax) || (sign==-1 && d<curMax))
					 * </pre>
					 * 
					 * In both case we want to replace curMax/curMaxTuple by the
					 * new values
					 * 
					 **/
					int c = psign * d.compareTo(curMax);
					if (c > 0) {
						curMax = d;
						curMaxTuple = t;
					}
				}
			} catch (ExecException ee) {
				throw ee;
			} catch (Exception e) {
				int errCode = -1;
				String msg = "Error while computing ExtremalTupleByNthField in ExtremalTupleByNthField,";
				throw new ExecException(msg, errCode, PigException.ERROR, e);
			}
		}

		return curMaxTuple;
	}

	protected static int parseFieldIndex(String inputFieldIndex)
			throws ExecException {
		// using a decrement to make sure that the subtraction happens correctly
		int fieldIndex = Integer.valueOf(inputFieldIndex);

		// to make fieldIndex 1-based instead of 0-based
		--fieldIndex;
		if (fieldIndex < 0) {
			throw new ExecException("field index cannot be less than 1:"
					+ inputFieldIndex, -1, PigException.ERROR, null);
		}
		return fieldIndex;
	}

	protected static int parseOrdering(String order) {
		int sign = 1;
		order = order.toLowerCase().trim();
		if (order != null
				&& (order.startsWith("min") || order.startsWith("desc")
						|| order.startsWith("-") || order.startsWith("small") || order
						.startsWith("least"))) {
			sign = -1;
		} else {
			// either default to 1 by not specifying order(null) or it indicated
			// "min" which is the string "min" the string "desc" or any string
			// starting with a minus sign.
			sign = 1;
		}
		return sign;
	}
}