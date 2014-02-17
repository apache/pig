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
package org.apache.pig.impl.builtin;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.tez.TezLoad;
import org.apache.pig.data.Tuple;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.broadcast.input.BroadcastKVReader;

public class ReadScalarsTez extends EvalFunc<Object> implements TezLoad {
    private static final Log LOG = LogFactory.getLog(ReadScalarsTez.class);
    String inputKey;
    Tuple t;
    public ReadScalarsTez(String inputKey) {
        this.inputKey = inputKey;
    }
    public void attachInputs(Map<String, LogicalInput> inputs, Configuration conf)
            throws ExecException {
        LogicalInput input = inputs.get(inputKey);
        if (input == null) {
            throw new ExecException("Input from vertex " + inputKey + " is missing");
        }
        try {
            BroadcastKVReader reader = (BroadcastKVReader)input.getReader();
            reader.next();
            t = (Tuple)reader.getCurrentValue();
            LOG.info("Attached input from vertex " + inputKey + " : input=" + input + ", reader=" + reader);
        } catch (Exception e) {
            throw new ExecException(e);
        }
    }

    @Override
    public Object exec(Tuple input) throws IOException {
        int pos = (Integer)input.get(0);
        Object obj = t.get(pos);
        return obj;
    }
}
