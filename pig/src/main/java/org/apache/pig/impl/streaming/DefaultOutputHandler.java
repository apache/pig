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
package org.apache.pig.impl.streaming;

import org.apache.pig.StreamToPig;
import org.apache.pig.impl.PigContext;
import org.apache.pig.builtin.PigStreaming;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;

/**
 * FileOutputHandler handles the output from the Pig-Streaming
 * executable in an synchronous manner by reading its output
 * via its <code>stdout</code>.
 */
public class DefaultOutputHandler extends OutputHandler {
    
    public DefaultOutputHandler() {
        deserializer = new PigStreaming();
    }
    
    public DefaultOutputHandler(HandleSpec spec) {
        deserializer = (StreamToPig)PigContext.instantiateFuncFromSpec(spec.spec);
    }

    @Override
    public OutputType getOutputType() {
        return OutputType.SYNCHRONOUS;
    }
}
