package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;
import scala.runtime.AbstractFunction1;

public class SkewedJoinConverter implements
        POConverter<Tuple, Tuple, POSkewedJoin>, Serializable {

    private POLocalRearrange[] LRs;
    private POSkewedJoin poSkewedJoin;

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
                              POSkewedJoin poSkewedJoin) throws IOException {

        SparkUtil.assertPredecessorSize(predecessors, poSkewedJoin, 2);
        LRs = new POLocalRearrange[2];
        this.poSkewedJoin = poSkewedJoin;

        createJoinPlans(poSkewedJoin.getJoinPlans());

        // extract the two RDDs
        RDD<Tuple> rdd1 = predecessors.get(0);
        RDD<Tuple> rdd2 = predecessors.get(1);

        // make (key, value) pairs, key has type Object, value has type Tuple
        RDD<Tuple2<Object, Tuple>> rdd1Pair = rdd1.map(new ExtractKeyFunction(
                this, 0), SparkUtil.<Object, Tuple>getTuple2Manifest());
        RDD<Tuple2<Object, Tuple>> rdd2Pair = rdd2.map(new ExtractKeyFunction(
                this, 1), SparkUtil.<Object, Tuple>getTuple2Manifest());

        // join fn is present in JavaPairRDD class ..
        JavaPairRDD<Object, Tuple> rdd1Pair_javaRDD = new JavaPairRDD<Object, Tuple>(
                rdd1Pair, SparkUtil.getManifest(Object.class),
                SparkUtil.getManifest(Tuple.class));
        JavaPairRDD<Object, Tuple> rdd2Pair_javaRDD = new JavaPairRDD<Object, Tuple>(
                rdd2Pair, SparkUtil.getManifest(Object.class),
                SparkUtil.getManifest(Tuple.class));

        // do the join
        JavaPairRDD<Object, Tuple2<Tuple, Tuple>> result_KeyValue = rdd1Pair_javaRDD
                .join(rdd2Pair_javaRDD);

        // map to get RDD<Tuple> from RDD<Object, Tuple2<Tuple, Tuple>> by
        // ignoring the key (of type Object) and appending the values (the
        // Tuples)
        JavaRDD<Tuple> result = result_KeyValue
                .mapPartitions(new ToValueFunction());

        // return type is RDD<Tuple>, so take it from JavaRDD<Tuple>
        return result.rdd();
    }

    private void createJoinPlans(MultiMap<PhysicalOperator, PhysicalPlan> inpPlans) throws PlanException {

        int i = -1;
        for (PhysicalOperator inpPhyOp : inpPlans.keySet()) {
            ++i;
            POLocalRearrange lr = new POLocalRearrange(genKey());
            try {
                lr.setIndex(i);
            } catch (ExecException e) {
                throw new PlanException(e.getMessage(), e.getErrorCode(), e.getErrorSource(), e);
            }
            lr.setResultType(DataType.TUPLE);
            lr.setKeyType(DataType.TUPLE);//keyTypes.get(i).size() > 1 ? DataType.TUPLE : keyTypes.get(i).get(0));
            lr.setPlans(inpPlans.get(inpPhyOp));
            LRs[i] = lr;
        }
    }

    private OperatorKey genKey() {
        return new OperatorKey(poSkewedJoin.getOperatorKey().scope, NodeIdGenerator.getGenerator().getNextNodeId(poSkewedJoin.getOperatorKey().scope));
    }

    private static class ExtractKeyFunction extends
            AbstractFunction1<Tuple, Tuple2<Object, Tuple>> implements
            Serializable {

        private final SkewedJoinConverter poSkewedJoin;
        private final int LR_index; // 0 for left table, 1 for right table

        public ExtractKeyFunction(SkewedJoinConverter poSkewedJoin, int LR_index) {
            this.poSkewedJoin = poSkewedJoin;
            this.LR_index = LR_index;
        }

        @Override
        public Tuple2<Object, Tuple> apply(Tuple tuple) {

            // attach tuple to LocalRearrange
            poSkewedJoin.LRs[LR_index].attachInput(tuple);

            try {
                // getNextTuple() returns the rearranged tuple
                Result lrOut = poSkewedJoin.LRs[LR_index].getNextTuple();

                // If tuple is (AA, 5) and key index is $1, then it lrOut is 0 5
                // (AA), so get(1) returns key
                Object key = ((Tuple) lrOut.result).get(1);

                Tuple value = tuple;

                // make a (key, value) pair
                Tuple2<Object, Tuple> tuple_KeyValue = new Tuple2<Object, Tuple>(
                        key, value);

                return tuple_KeyValue;

            } catch (Exception e) {
                System.out.print(e);
                return null;
            }
        }

    }

    private static class ToValueFunction
            extends
            FlatMapFunction<Iterator<Tuple2<Object, Tuple2<Tuple, Tuple>>>, Tuple>
            implements Serializable {

        private class Tuple2TransformIterable implements Iterable<Tuple> {

            Iterator<Tuple2<Object, Tuple2<Tuple, Tuple>>> in;

            Tuple2TransformIterable(
                    Iterator<Tuple2<Object, Tuple2<Tuple, Tuple>>> input) {
                in = input;
            }

            public Iterator<Tuple> iterator() {
                return new IteratorTransform<Tuple2<Object, Tuple2<Tuple, Tuple>>, Tuple>(
                        in) {
                    @Override
                    protected Tuple transform(
                            Tuple2<Object, Tuple2<Tuple, Tuple>> next) {
                        try {

                            Tuple leftTuple = next._2._1;
                            Tuple rightTuple = next._2._2;

                            TupleFactory tf = TupleFactory.getInstance();
                            Tuple result = tf.newTuple(leftTuple.size()
                                    + rightTuple.size());

                            // append the two tuples together to make a
                            // resulting tuple
                            for (int i = 0; i < leftTuple.size(); i++)
                                result.set(i, leftTuple.get(i));
                            for (int i = 0; i < rightTuple.size(); i++)
                                result.set(i + leftTuple.size(),
                                        rightTuple.get(i));

                            System.out.println("MJC: Result = "
                                    + result.toDelimitedString(" "));

                            return result;

                        } catch (Exception e) {
                            System.out.println(e);
                        }
                        return null;
                    }
                };
            }
        }

        @Override
        public Iterable<Tuple> call(
                Iterator<Tuple2<Object, Tuple2<Tuple, Tuple>>> input) {
            return new Tuple2TransformIterable(input);
        }
    }
}