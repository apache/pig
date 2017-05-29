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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigWarning;
import org.apache.pig.tools.pigstats.PigWarnCounter;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SparkCounters implements Serializable, PigWarnCounter {
    private static final long serialVersionUID = 1L;

    private static final Log LOG = LogFactory.getLog(SparkCounters.class);

    private Map<String, SparkCounterGroup> sparkCounterGroups;

    private final transient JavaSparkContext javaSparkContext;

    private SparkCounters() {
        this(null);
    }

    public SparkCounters(JavaSparkContext javaSparkContext) {
        this.javaSparkContext = javaSparkContext;
        this.sparkCounterGroups = new HashMap<String, SparkCounterGroup>();
    }

    public void createCounter(Enum<?> key) {
        createCounter(key.getDeclaringClass().getName(), key.name());
    }

    public void createCounter(String groupName, Enum<?> key) {
        createCounter(groupName, key.name(), 0L);
    }

    public void createCounter(String groupName, String counterName) {
        createCounter(groupName, counterName, 0L);
    }

    public void createCounter(String groupName, String counterName, Object initValue) {
        getGroup(groupName).createCounter(counterName, initValue);
    }

    public void increment(Enum<?> key, long incrValue) {
        increment(key.getDeclaringClass().getName(), key.name(), incrValue);
    }

    public void increment(String groupName, String counterName, long value) {
        SparkCounter counter = getGroup(groupName).getCounter(counterName);
        if (counter == null) {
            LOG.error(String.format("counter[%s, %s] has not initialized before.", groupName, counterName));
        } else {
            counter.increment(value);
        }
    }

    public Object getValue(String groupName, String counterName) {
        SparkCounter counter = getGroup(groupName).getCounter(counterName);
        if (counter == null) {
            LOG.error(String.format("counter[%s, %s] has not initialized before.", groupName, counterName));
            return null;
        } else {
            return counter.getValue();
        }
    }

    public SparkCounter getCounter(String groupName, String counterName) {
        return getGroup(groupName).getCounter(counterName);
    }

    public SparkCounter getCounter(Enum<?> key) {
        return getCounter(key.getDeclaringClass().getName(), key.name());
    }

    private SparkCounterGroup getGroup(String groupName) {
        SparkCounterGroup group = sparkCounterGroups.get(groupName);
        if (group == null) {
            group = new SparkCounterGroup.LongSparkCounterGroup(groupName, groupName, javaSparkContext);
            sparkCounterGroups.put(groupName, group);
        }
        return group;
    }

    public Map<String, SparkCounterGroup> getSparkCounterGroups() {
        return sparkCounterGroups;
    }


    @Override
    public boolean incrWarnCounter(Enum<?> name, Object incr) {
        SparkCounter counter = getCounter(PigWarning.SPARK_WARN);
        return _incrWarnCounter(counter, name.name(), (Long) incr);
    }

    @Override
    public boolean incrWarnCounter(String group, String name, Object incr) {
        SparkCounter counter = getCounter(PigWarning.SPARK_CUSTOM_WARN);
        return _incrWarnCounter(counter, group+"::"+name, (Long) incr);
    }

    private static boolean _incrWarnCounter(SparkCounter counter, String name, Long incr) {
        if (counter == null){
            return false;
        }
        Map<String,Long> map = new HashMap<String,Long>();
        map.put(name, incr);
        counter.increment(map);
        return true;
    }
}
