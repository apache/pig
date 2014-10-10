package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;

/**
 * Convert that is able to convert an RRD to another RRD using a POForEach
 */
@SuppressWarnings({ "serial" })
public class ForEachConverter implements POConverter<Tuple, Tuple, POForEach> {

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
            POForEach physicalOperator) {
        SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        ForEachFunction forEachFunction = new ForEachFunction(physicalOperator);
        return rdd.toJavaRDD().mapPartitions(forEachFunction, true).rdd();
    }

    private static class ForEachFunction implements
            FlatMapFunction<Iterator<Tuple>, Tuple>, Serializable {

        private POForEach poForEach;

        private ForEachFunction(POForEach poForEach) {
            this.poForEach = poForEach;
        }

        public Iterable<Tuple> call(final Iterator<Tuple> input) {

            return new Iterable<Tuple>() {

                @Override
                public Iterator<Tuple> iterator() {
                    return new POOutputConsumerIterator(input) {

                        protected void attach(Tuple tuple) {
                            poForEach.setInputs(null);
                            poForEach.attachInput(tuple);
                        }

                        protected Result getNextResult() throws ExecException {
                            return poForEach.getNextTuple();
                        }
                    };
                }
            };
        }
    }
}
