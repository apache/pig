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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValueReader;

public class ReadScalarsTez extends EvalFunc<Object> implements TezInput {
    private static final Log LOG = LogFactory.getLog(ReadScalarsTez.class);
    private String inputKey;
    private transient Tuple t;
    private transient LogicalInput input;

    public ReadScalarsTez(String inputKey) {
        this.inputKey = inputKey;
    }

    @Override
    public String[] getTezInputs() {
        return new String[] { inputKey };
    }

    @Override
    public void replaceInput(String oldInputKey, String newInputKey) {
        if (oldInputKey.equals(inputKey)) {
            inputKey = newInputKey;
        }
    }

    @Override
    public void addInputsToSkip(Set<String> inputsToSkip) {
        String cacheKey = "scalar-" + inputKey;
        Object cacheValue = ObjectCache.getInstance().retrieve(cacheKey);
        if (cacheValue != null) {
            inputsToSkip.add(inputKey);
        }
    }

    @Override
    public void attachInputs(Map<String, LogicalInput> inputs,
            Configuration conf) throws ExecException {
        String cacheKey = "scalar-" + inputKey;
        Object cacheValue = ObjectCache.getInstance().retrieve(cacheKey);
        if (cacheValue != null) {
            t = (Tuple) cacheValue;
            return;
        }
        input = inputs.get(inputKey);
        if (input == null) {
            throw new ExecException("Input from vertex " + inputKey + " is missing");
        }
        try {
            KeyValueReader reader = (KeyValueReader) input.getReader();
            if (reader.next()) {
                t = (Tuple) reader.getCurrentValue();
                String first = t == null ? null : t.toString();
                if (reader.next()) {
                    String msg = "Scalar has more than one row in the output. "
                            + "1st : " + first + ", 2nd :"
                            + reader.getCurrentValue();
                    throw new ExecException(msg);
                }
            } else {
                LOG.info("Scalar input from vertex " + inputKey + " is null");
            }
            LOG.info("Attached input from vertex " + inputKey + " : input=" + input + ", reader=" + reader);
        } catch (Exception e) {
            throw new ExecException(e);
        }
        ObjectCache.getInstance().cache(cacheKey, t);
        log.info("Cached scalar in Tez ObjectRegistry with vertex scope. cachekey=" + cacheKey);
    }

    @Override
    public Object exec(Tuple input) throws IOException {
        int pos = (Integer) input.get(0);
        Object obj = t.get(pos);
        return obj;
    }
}
