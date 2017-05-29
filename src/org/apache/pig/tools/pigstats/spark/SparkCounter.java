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

package org.apache.pig.tools.pigstats.spark;


import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.Accumulator;
import org.apache.spark.AccumulatorParam;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class SparkCounter<T> implements Serializable {

    private String name;
    private String displayName;
    private Accumulator<T> accumulator;

    public SparkCounter() {
        // For serialization.
    }

    public SparkCounter(
            String name,
            String displayName,
            String groupName,
            T initValue,
            JavaSparkContext sparkContext) {

        this.name = name;
        this.displayName = displayName;

        String accumulatorName = groupName + "_" + name;

        if (sparkContext == null){
            //Spark executors can register new Accumulators but they won't make it back to the driver hence the limitation
            throw new  RuntimeException("Not allowed to create SparkCounter on backend executor.");

        }
        this.accumulator = sparkContext.accumulator(initValue, accumulatorName,  createAccumulatorParam());

    }

    protected abstract AccumulatorParam<T> createAccumulatorParam();

    public T getValue() {
        if (accumulator != null) {
            return accumulator.value();
        } else {
            return null;
        }
    }

    public void increment(T incr) {
        accumulator.add(incr);
    }

    public String getName() {
        return name;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public static class LongSparkCounter extends SparkCounter<Long> {

        public LongSparkCounter(){}

        public LongSparkCounter(
                String name,
                String displayName,
                String groupName,
                Long initValue,
                JavaSparkContext sparkContext){
            super(name, displayName, groupName, initValue, sparkContext);
        }

        @Override
        protected AccumulatorParam<Long> createAccumulatorParam() {
            return new LongAccumulatorParam();
        }

        private class LongAccumulatorParam implements AccumulatorParam<Long> {

            @Override
            public Long addAccumulator(Long t1, Long t2) {
                return t1 + t2;
            }

            @Override
            public Long addInPlace(Long r1, Long r2) {
                return r1 + r2;
            }

            @Override
            public Long zero(Long initialValue) {
                return 0L;
            }
        }
    }

    public static class MapSparkCounter extends SparkCounter<Map<String,Long>> {

        public MapSparkCounter(){}

        public MapSparkCounter(
                String name,
                String displayName,
                String groupName,
                Map<String,Long> initValue,
                JavaSparkContext sparkContext){
            super(name, displayName, groupName, initValue, sparkContext);
        }

        @Override
        protected AccumulatorParam<Map<String, Long>> createAccumulatorParam() {
            return new MapAccumulatorParam();
        }

        private class MapAccumulatorParam implements AccumulatorParam<Map<String,Long>> {

            @Override
            public Map<String, Long> addAccumulator(Map<String, Long> t1, Map<String, Long> t2) {
                return addInPlace(t1, t2);
            }

            @Override
            public Map<String, Long> addInPlace(Map<String, Long> r1, Map<String, Long> r2) {
                for (String key : r2.keySet()){
                    Long r1val = r1.get(key);
                    Long r2val = r2.get(key);
                    r1.put(key,r1val == null ? r2val : r1val+r2val);
                }
                return r1;
            }

            @Override
            public Map<String, Long> zero(Map<String, Long> initialValue) {
                return new HashMap<>();
            }
        }
    }

}
