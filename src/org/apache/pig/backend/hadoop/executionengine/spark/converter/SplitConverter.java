package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;

import org.apache.spark.rdd.RDD;

public class SplitConverter implements POConverter<Tuple, Tuple, POSplit> {

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POSplit poSplit)
            throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, poSplit, 1);
        return predecessors.get(0);
    }

}
