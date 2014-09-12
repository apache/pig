package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;

import scala.runtime.AbstractFunction1;
import org.apache.spark.rdd.RDD;

@SuppressWarnings({ "serial" })
public class LocalRearrangeConverter implements
        POConverter<Tuple, Tuple, POLocalRearrange> {
    private static final Log LOG = LogFactory
            .getLog(GlobalRearrangeConverter.class);

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
            POLocalRearrange physicalOperator) throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        // call local rearrange to get key and value
        return rdd.map(new LocalRearrangeFunction(physicalOperator),
                SparkUtil.getManifest(Tuple.class));

    }

    private static class LocalRearrangeFunction extends
            AbstractFunction1<Tuple, Tuple> implements Serializable {

        private final POLocalRearrange physicalOperator;

        public LocalRearrangeFunction(POLocalRearrange physicalOperator) {
            this.physicalOperator = physicalOperator;
        }

        @Override
        public Tuple apply(Tuple t) {
            Result result;
            try {
                physicalOperator.setInputs(null);
                physicalOperator.attachInput(t);
                result = physicalOperator.getNextTuple();

                if (result == null) {
                    throw new RuntimeException(
                            "Null response found for LocalRearange on tuple: "
                                    + t);
                }

                switch (result.returnStatus) {
                case POStatus.STATUS_OK:
                    // (index, key, value without keys)
                    Tuple resultTuple = (Tuple) result.result;
                    if (LOG.isDebugEnabled())
                        LOG.debug("LocalRearrangeFunction out " + resultTuple);
                    return resultTuple;
                default:
                    throw new RuntimeException(
                            "Unexpected response code from operator "
                                    + physicalOperator + " : " + result);
                }
            } catch (ExecException e) {
                throw new RuntimeException(
                        "Couldn't do LocalRearange on tuple: " + t, e);
            }
        }

    }

}
