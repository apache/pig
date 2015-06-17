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
import java.util.List;

import scala.Tuple2;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.spark.rdd.PairRDDFunctions;
import org.apache.spark.rdd.RDD;

@SuppressWarnings({ "serial" })
public class DistinctConverter implements RDDConverter<Tuple, Tuple, PODistinct> {
    private static final Log LOG = LogFactory.getLog(DistinctConverter.class);

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
            PODistinct op) throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, op, 1);
        RDD<Tuple> rdd = predecessors.get(0);

        // In DISTINCT operation, the key is the entire tuple.
        // RDD<Tuple> -> RDD<Tuple2<Tuple, null>>
        RDD<Tuple2<Tuple, Object>> keyValRDD = rdd.map(new ToKeyValueFunction(),
                SparkUtil.<Tuple, Object> getTuple2Manifest());
        PairRDDFunctions<Tuple, Object> pairRDDFunctions
          = new PairRDDFunctions<Tuple, Object>(keyValRDD,
                SparkUtil.getManifest(Tuple.class),
                SparkUtil.getManifest(Object.class), null);
        int parallelism = SparkUtil.getParallelism(predecessors, op);
        return pairRDDFunctions.reduceByKey(
                SparkUtil.getPartitioner(op.getCustomPartitioner(), parallelism),
                new MergeValuesFunction())
                .map(new ToValueFunction(), SparkUtil.getManifest(Tuple.class));
    }

    /**
     * Tuple -> Tuple2<Tuple, null>
     */
    private static final class ToKeyValueFunction extends
            AbstractFunction1<Tuple, Tuple2<Tuple, Object>> implements
            Serializable {
        @Override
        public Tuple2<Tuple, Object> apply(Tuple t) {
            if (LOG.isDebugEnabled())
                LOG.debug("DistinctConverter.ToKeyValueFunction in " + t);

            Tuple key = t;
            Object value = null;
            Tuple2<Tuple, Object> out = new Tuple2<Tuple, Object>(key, value);

            if (LOG.isDebugEnabled())
                LOG.debug("DistinctConverter.ToKeyValueFunction out " + out);
            return out;
        }
    }

    /**
     * No-op
     */
    private static final class MergeValuesFunction extends
            AbstractFunction2<Object, Object, Object> implements Serializable {
        @Override
        public Object apply(Object arg0, Object arg1) {
            return null;
        }
    }

    /**
     * Tuple2<Tuple, null> -> Tuple
     */
    private static final class ToValueFunction extends
            AbstractFunction1<Tuple2<Tuple, Object>, Tuple> implements
            Serializable {
        @Override
        public Tuple apply(Tuple2<Tuple, Object> input) {
            return input._1;
        }
    }
}