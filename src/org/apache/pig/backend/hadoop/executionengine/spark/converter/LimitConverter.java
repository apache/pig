package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;

import scala.collection.Iterator;
import scala.collection.JavaConversions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.JavaSparkContext;

@SuppressWarnings({ "serial" })
public class LimitConverter implements POConverter<Tuple, Tuple, POLimit> {

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POLimit poLimit)
            throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, poLimit, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        LimitFunction limitFunction = new LimitFunction(poLimit);
        RDD<Tuple> rdd2 = rdd.coalesce(1, false);
        return rdd2.mapPartitions(limitFunction, false,
                SparkUtil.getManifest(Tuple.class));
    }

    private static class LimitFunction extends
            Function<Iterator<Tuple>, Iterator<Tuple>> implements Serializable {

        private final POLimit poLimit;

        public LimitFunction(POLimit poLimit) {
            this.poLimit = poLimit;
        }

        @Override
        public Iterator<Tuple> call(Iterator<Tuple> i) {
            final java.util.Iterator<Tuple> tuples = JavaConversions
                    .asJavaIterator(i);

            return JavaConversions
                    .asScalaIterator(new POOutputConsumerIterator(tuples) {

                        protected void attach(Tuple tuple) {
                            poLimit.setInputs(null);
                            poLimit.attachInput(tuple);
                        }

                        protected Result getNextResult() throws ExecException {
                            return poLimit.getNextTuple();
                        }
                    });
        }

    }

}
