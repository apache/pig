package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;

import scala.collection.JavaConversions;
import org.apache.spark.rdd.UnionRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkContext;

public class UnionConverter implements POConverter<Tuple, Tuple, POUnion> {

    private final SparkContext sc;

    public UnionConverter(SparkContext sc) {
        this.sc = sc;
    }

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
            POUnion physicalOperator) throws IOException {
        SparkUtil.assertPredecessorSizeGreaterThan(predecessors,
                physicalOperator, 0);
        UnionRDD<Tuple> unionRDD = new UnionRDD<Tuple>(sc,
                JavaConversions.asScalaBuffer(predecessors),
                SparkUtil.getManifest(Tuple.class));
        return unionRDD;
    }

}
