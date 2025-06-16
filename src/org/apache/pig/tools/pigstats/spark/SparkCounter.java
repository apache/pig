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

import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class SparkCounter<T> implements Serializable {

    private String name;
    private String displayName;
    private AccumulatorV2<T,T> accumulator;

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
        accumulator = getAccumulator(); 
        sparkContext.sc().register(accumulator, accumulatorName);
    }

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

    abstract public AccumulatorV2<T,T> getAccumulator();

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
        public AccumulatorV2<Long,Long> getAccumulator() {
          return new LongAccumulator();
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
        public AccumulatorV2<Map<String,Long>, Map<String,Long>> getAccumulator() {
            return new MapAccumulator();
        }


        private class MapAccumulator extends AccumulatorV2<Map<String,Long>, Map<String,Long>> {

            private Map<String, Long> map;

            public MapAccumulator() {
                this(new HashMap<String, Long>());
            }

            public MapAccumulator(Map <String,Long> in) {
                map = in;
            }

            @Override
            public AccumulatorV2<Map<String, Long>, Map<String, Long>> copy() {
                return new MapAccumulator(value());
            }

            @Override
            public void add(Map<String,Long> r2) {
                for (String key : r2.keySet()){
                    Long r1val = map.get(key);
                    Long r2val = r2.get(key);
                    map.put(key,r1val == null ? r2val : r1val+r2val);
                }
                return;
            }

            @Override
            public boolean isZero() {
                return map == null || map.isEmpty();
            }

            @Override
            public void merge(AccumulatorV2<Map<String,Long>,Map<String,Long>> other) {
                add(other.value());
            }

            @Override
            public void reset() {
                if( map != null ) {
                    map.clear();
                } else {
                    map = new HashMap<String, Long> ();
                }
            }


            @Override
            public Map<String, Long> value() {
                return map;
            }
        }
    }

}
