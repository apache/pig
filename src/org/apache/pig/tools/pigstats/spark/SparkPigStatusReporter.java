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

import org.apache.pig.JVMReuseManager;
import org.apache.pig.StaticDataCleanup;

/**
 * Just like PigStatusReporter which will create/reset Hadoop counters, SparkPigStatusReporter will
 * create/reset Spark counters.
 * Note that, it is not suitable to make SparkCounters as a Singleton, it will be created/reset for
 * a given pig script or a Dump/Store action in Grunt mode.
 */
public class SparkPigStatusReporter {
    private static SparkPigStatusReporter reporter;
    private SparkCounters counters;

    static {
        JVMReuseManager.getInstance().registerForStaticDataCleanup(SparkPigStatusReporter.class);
    }

    @StaticDataCleanup
    public static void staticDataCleanup() {
        reporter = null;
    }

    private SparkPigStatusReporter() {
    }

    public static SparkPigStatusReporter getInstance() {
        if (reporter == null) {
            reporter = new SparkPigStatusReporter();
        }
        return reporter;
    }

    public void createCounter(String groupName, String counterName) {
        if (counters != null) {
            counters.createCounter(groupName, counterName);
        }
    }

    public SparkCounters getCounters() {
        return counters;
    }

    public void setCounters(SparkCounters counters) {
        this.counters = counters;
    }
}
