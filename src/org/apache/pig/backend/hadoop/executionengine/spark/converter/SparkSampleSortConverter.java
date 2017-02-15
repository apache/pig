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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import scala.Tuple2;
import scala.runtime.AbstractFunction1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POSampleSortSpark;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.rdd.RDD;
  /*
   sort the sample data and convert the sample data to the format (all,{(sampleEle1),(sampleEle2),...})

   */
@SuppressWarnings("serial")
public class SparkSampleSortConverter implements RDDConverter<Tuple, Tuple, POSampleSortSpark> {
    private static final Log LOG = LogFactory.getLog(SparkSampleSortConverter.class);
    private static TupleFactory tf = TupleFactory.getInstance();
    private static BagFactory bf = DefaultBagFactory.getInstance();

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POSampleSortSpark sortOperator)
            throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, sortOperator, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        RDD<Tuple2<Tuple, Object>> rddPair = rdd.map(new ToKeyValueFunction(),
                SparkUtil.<Tuple, Object> getTuple2Manifest());

        JavaPairRDD<Tuple, Object> r = new JavaPairRDD<Tuple, Object>(rddPair,
                SparkUtil.getManifest(Tuple.class),
                SparkUtil.getManifest(Object.class));
         //sort sample data
        JavaPairRDD<Tuple, Object> sorted = r.sortByKey(true);
         //convert every element in sample data from element to (all, element) format
        JavaPairRDD<String, Tuple> mapped = sorted.mapPartitionsToPair(new AggregateFunction());
        //use groupByKey to aggregate all values( the format will be ((all),{(sampleEle1),(sampleEle2),...} )
        JavaRDD<Tuple> groupByKey= mapped.groupByKey().map(new ToValueFunction());
        return  groupByKey.rdd();
    }


    private static class MergeFunction implements org.apache.spark.api.java.function.Function2<Tuple, Tuple, Tuple>
            , Serializable {

        @Override
        public Tuple call(Tuple v1, Tuple v2) {
                Tuple res = tf.newTuple();
                res.append(v1);
                res.append(v2);
                LOG.info("MergeFunction out:"+res);
                return res;
        }
    }

    // input: Tuple2<Tuple,Object>
    // output: Tuple2("all", Tuple)
    private static class AggregateFunction implements
            PairFlatMapFunction<Iterator<Tuple2<Tuple, Object>>, String,Tuple>, Serializable {

        private class Tuple2TransformIterable implements Iterable<Tuple2<String,Tuple>> {

            Iterator<Tuple2<Tuple, Object>> in;

            Tuple2TransformIterable(Iterator<Tuple2<Tuple, Object>> input) {
                in = input;
            }

            public Iterator<Tuple2<String,Tuple>> iterator() {
                return new IteratorTransform<Tuple2<Tuple, Object>, Tuple2<String,Tuple>>(in) {
                    @Override
                    protected Tuple2<String,Tuple> transform(Tuple2<Tuple, Object> next) {
                        LOG.info("AggregateFunction in:"+ next._1()) ;
                        return new Tuple2<String,Tuple>("all",next._1());
                    }
                };
            }
        }

        @Override
        public Iterable<Tuple2<String, Tuple>> call(Iterator<Tuple2<Tuple, Object>> input) throws Exception {
            return new Tuple2TransformIterable(input);
        }

    }

    private static class ToValueFunction implements Function<Tuple2<String, Iterable<Tuple>>, Tuple> {
        @Override
        public Tuple call(Tuple2<String, Iterable<Tuple>> next) throws Exception {
            Tuple res = tf.newTuple();
            res.append(next._1());
            Iterator<Tuple> iter = next._2().iterator();
            DataBag bag = bf.newDefaultBag();
            while(iter.hasNext()) {
                bag.add(iter.next());
            }
            res.append(bag);
            LOG.info("ToValueFunction1 out:" + res);
            return res;
        }
    }

    private static class ToKeyValueFunction extends
            AbstractFunction1<Tuple, Tuple2<Tuple, Object>> implements
            Serializable {

        @Override
        public Tuple2<Tuple, Object> apply(Tuple t) {
            if (LOG.isDebugEnabled()) {
                LOG.info("Sort ToKeyValueFunction in " + t);
            }
            Tuple key = t;
            Object value = null;
            // (key, value)
            Tuple2<Tuple, Object> out = new Tuple2<Tuple, Object>(key, value);
            if (LOG.isDebugEnabled()) {
                LOG.info("Sort ToKeyValueFunction out " + out);
            }
            return out;
        }
    }
}
