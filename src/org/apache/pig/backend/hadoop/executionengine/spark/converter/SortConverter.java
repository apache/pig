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
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.spark.FlatMapFunctionAdapter;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkPigContext;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.data.Tuple;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;

@SuppressWarnings("serial")
public class SortConverter implements RDDConverter<Tuple, Tuple, POSort> {
    private static final Log LOG = LogFactory.getLog(SortConverter.class);

    private static final FlatMapFunctionAdapter<Iterator<Tuple2<Tuple, Object>>, Tuple> TO_VALUE_FUNCTION = new ToValueFunction();

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POSort sortOperator)
            throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, sortOperator, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        int parallelism = SparkPigContext.get().getParallelism(predecessors, sortOperator);
        RDD<Tuple2<Tuple, Object>> rddPair = rdd.map(new ToKeyValueFunction(),
                SparkUtil.<Tuple, Object> getTuple2Manifest());

        JavaPairRDD<Tuple, Object> r = new JavaPairRDD<Tuple, Object>(rddPair,
                SparkUtil.getManifest(Tuple.class),
                SparkUtil.getManifest(Object.class));

        JavaPairRDD<Tuple, Object> sorted = r.sortByKey(
                sortOperator.getMComparator(), true, parallelism);
        JavaRDD<Tuple> mapped = sorted.mapPartitions(SparkUtil.flatMapFunction(TO_VALUE_FUNCTION));

        return mapped.rdd();
    }

    private static class ToValueFunction implements
            FlatMapFunctionAdapter<Iterator<Tuple2<Tuple, Object>>, Tuple>, Serializable {

        private class Tuple2TransformIterable implements Iterable<Tuple> {

            Iterator<Tuple2<Tuple, Object>> in;

            Tuple2TransformIterable(Iterator<Tuple2<Tuple, Object>> input) {
                in = input;
            }

            public Iterator<Tuple> iterator() {
                return new IteratorTransform<Tuple2<Tuple, Object>, Tuple>(in) {
                    @Override
                    protected Tuple transform(Tuple2<Tuple, Object> next) {
                        return next._1();
                    }
                };
            }
        }

        @Override
        public Iterator<Tuple> call(Iterator<Tuple2<Tuple, Object>> input) {
            return new Tuple2TransformIterable(input).iterator();
        }
    }

    private static class ToKeyValueFunction extends
            AbstractFunction1<Tuple, Tuple2<Tuple, Object>> implements
            Serializable {

        @Override
        public Tuple2<Tuple, Object> apply(Tuple t) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sort ToKeyValueFunction in " + t);
            }
            Tuple key = t;
            Object value = null;
            // (key, value)
            Tuple2<Tuple, Object> out = new Tuple2<Tuple, Object>(key, value);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sort ToKeyValueFunction out " + out);
            }
            return out;
        }
    }

}
