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
package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;

@SuppressWarnings({ "serial"})
public class CollectedGroupConverter implements RDDConverter<Tuple, Tuple, POCollectedGroup> {

	@Override
  public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
      POCollectedGroup physicalOperator) throws IOException {
    SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
    RDD<Tuple> rdd = predecessors.get(0);
    CollectedGroupFunction collectedGroupFunction
        = new CollectedGroupFunction(physicalOperator);
    return rdd.toJavaRDD().mapPartitions(collectedGroupFunction, true)
			.rdd();
  }

	private static class CollectedGroupFunction
			implements FlatMapFunction<Iterator<Tuple>, Tuple> {

		private POCollectedGroup poCollectedGroup;

		public long current_val;
		public boolean proceed;

		private CollectedGroupFunction(POCollectedGroup poCollectedGroup) {
			this.poCollectedGroup = poCollectedGroup;
			this.current_val = 0;
		}

		public Iterable<Tuple> call(final Iterator<Tuple> input) {

			  return new Iterable<Tuple>() {

				@Override
				public Iterator<Tuple> iterator() {

				  return new OutputConsumerIterator(input) {

					  @Override
					  protected void attach(Tuple tuple) {
						  poCollectedGroup.setInputs(null);
						  poCollectedGroup.attachInput(tuple);
					  }

					  @Override
					  protected Result getNextResult() throws ExecException {
					      return poCollectedGroup.getNextTuple();
					  }

					  @Override
					  protected void endOfInput() {
						  poCollectedGroup.getParentPlan().endOfAllInput = true;
					  }
				  };
				}
		  };
		}
	}
}
