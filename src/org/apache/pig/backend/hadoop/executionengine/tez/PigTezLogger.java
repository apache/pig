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
package org.apache.pig.backend.hadoop.executionengine.tez;

import java.util.Map;
import java.util.WeakHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigLogger;

public class PigTezLogger implements PigLogger {

    private static Log log = LogFactory.getLog(PigTezLogger.class);

    private TezStatusReporter reporter = null;

    private boolean aggregate = false;

    private Map<Object, String> msgMap = new WeakHashMap<Object, String>();

    public PigTezLogger(TezStatusReporter tezStatusReporter, boolean aggregate) {
        this.reporter = tezStatusReporter;
        this.aggregate = aggregate;
    }

    @Override
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
                //There is one issue in PigHadoopLogger in local mode since reporter is null.
                //It is probably also true to PigTezLogger once tez local mode is done, need to
                //check back. Please refer to PigHadoopLogger for detail
                log.warn(displayMessage);
            }
        } else {
            log.warn(displayMessage);
        }
    }

    public synchronized boolean getAggregate() {
        return aggregate;
    }

    public void destroy() {
        this.reporter = null;
    }

}