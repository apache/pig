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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.util.Map;
import java.util.WeakHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigLogger;
import org.apache.pig.tools.pigstats.PigStatusReporter;

/**
 *
 * A singleton class that implements the PigLogger interface
 * for use in map reduce context. Provides ability to aggregate
 * warning messages
 */
public final class PigHadoopLogger implements PigLogger {
    private static class PigHadoopLoggerHelper {
        private static PigHadoopLogger instance = new PigHadoopLogger();
    }

    public static PigHadoopLogger getInstance() {
        return PigHadoopLoggerHelper.instance;
    }

    private static Log log = LogFactory.getLog(PigHadoopLogger.class);

    private PigStatusReporter reporter = null;

    private boolean aggregate = false;

    private Map<Object, String> msgMap = new WeakHashMap<Object, String>();

    private PigHadoopLogger() {
    }

    @SuppressWarnings("rawtypes")
    public void warn(Object o, String msg, Enum warningEnum) {
        String className = o.getClass().getName();
        String displayMessage = className + "(" + warningEnum + "): " + msg;

        if (getAggregate()) {
            if (reporter != null) {
                // log atleast once
                if (msgMap.get(o) == null || !msgMap.get(o).equals(displayMessage)) {
                    log.warn(displayMessage);
                    msgMap.put(o, displayMessage);
                }
                if (o instanceof EvalFunc || o instanceof LoadFunc || o instanceof StoreFunc) {
                    reporter.getCounter(className, warningEnum.name()).increment(1);
                } else {
                    reporter.getCounter(warningEnum).increment(1);
                }
            } else {
                //TODO:
                //in local mode of execution if the PigHadoopLogger is used initially,
                //then aggregation cannot be performed as the reporter will be null.
                //The reference to a reporter is given by Hadoop at run time.
                //In local mode, due to the absence of Hadoop there will be no reporter
                //Just print the warning message as is.
                //If a warning message is printed in map reduce mode when aggregation
                //is turned on then we have a problem, its a bug.
                log.warn(displayMessage);
            }
        } else {
            log.warn(displayMessage);
        }
    }

    public synchronized void setReporter(PigStatusReporter rep) {
        this.reporter = rep;
    }

    public synchronized boolean getAggregate() {
        return aggregate;
    }

    public synchronized void setAggregate(boolean aggregate) {
        this.aggregate = aggregate;
    }

}
