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
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PORank;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

public class RankConverter implements RDDConverter<Tuple, Tuple, PORank> {

	private static final Log LOG = LogFactory.getLog(RankConverter.class);
	
	@Override
	public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, PORank poRank)
			throws IOException {
		SparkUtil.assertPredecessorSize(predecessors, poRank, 1);
        RDD<Tuple> rdd = predecessors.get(0);
		JavaPairRDD<Integer, Long> javaPairRdd = rdd.toJavaRDD()
				.mapToPair(new ToPairRdd());
		JavaPairRDD<Integer, Iterable<Long>> groupedByIndex = javaPairRdd
				.groupByKey();
		JavaPairRDD<Integer, Long> countsByIndex = groupedByIndex
				.mapToPair(new IndexCounters());
		JavaPairRDD<Integer, Long> sortedCountsByIndex = countsByIndex
				.sortByKey(true);
		Map<Integer, Long> counts = sortedCountsByIndex.collectAsMap();
		JavaRDD<Tuple> finalRdd = rdd.toJavaRDD()
				.map(new RankFunction(new HashMap<Integer, Long>(counts)));
		return finalRdd.rdd();
	}

	@SuppressWarnings("serial")
	private static class ToPairRdd implements 
		PairFunction<Tuple, Integer, Long>, Serializable {

        @Override
        public Tuple2<Integer, Long> call(Tuple t) {
            try {
                Integer key = (Integer) t.get(0);
                Long value = (Long) t.get(1);
                return new Tuple2<Integer, Long>(key, value);
            } catch (ExecException e) {
                throw new RuntimeException(e);
            }
        }
    }
	
	@SuppressWarnings("serial")
	private static class IndexCounters implements 
		PairFunction<Tuple2<Integer, Iterable<Long>>, Integer, Long>, 
		Serializable {
		@Override
		public Tuple2<Integer, Long> call(Tuple2<Integer, 
				Iterable<Long>> input) {
			long lastVaue = 0L;
			
			for (Long t : input._2()) {
				lastVaue = (t > lastVaue) ? t : lastVaue;
			}

			return new Tuple2<Integer, Long>(input._1(), lastVaue);
		}
    }
	
	@SuppressWarnings("serial")
	private static class RankFunction implements Function<Tuple, Tuple>, 
			Serializable {
		private final HashMap<Integer, Long> counts;
		
		private RankFunction(HashMap<Integer, Long> counts) {
			this.counts = counts;
		}
		
		@Override
		public Tuple call(Tuple input) throws Exception {
			Tuple output = TupleFactory.getInstance()
					.newTuple(input.getAll().size() - 2);
			
			for (int i = 1; i < input.getAll().size() - 2; i ++) {
				output.set(i, input.get(i+2));
			}
			
			long offset = calculateOffset((Integer) input.get(0));
			output.set(0, offset + (Long)input.get(2));
			return output;
		}
		
		private long calculateOffset(Integer index) {
			long offset = 0;
			
			if (index > 0) {
				for (int i = 0; i < index; i++) {
					if (counts.containsKey(i)) {
						offset += counts.get(i);
					}
				}
			}
			return offset;
		}
	}
}
