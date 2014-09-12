package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;

import org.apache.spark.rdd.RDD;

/**
 * Given an RDD and a PhysicalOperater, and implementation of this class can
 * convert the RDD to another RDD.
 */
public interface POConverter<IN, OUT, T extends PhysicalOperator> {
    RDD<OUT> convert(List<RDD<IN>> rdd, T physicalOperator) throws IOException;
}
