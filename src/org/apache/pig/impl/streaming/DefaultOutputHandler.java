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

import java.io.IOException;

import org.apache.pig.LoadFunc;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;

/**
 * {@link FileOutputHandler} handles the output from the Pig-Streaming
 * executable in an {@link OutputType#SYNCHRONOUS} manner by reading its output
 * via its <code>stdout</code>.
 */
public class DefaultOutputHandler extends OutputHandler {
    BufferedPositionedInputStream stdout;
    
    public DefaultOutputHandler() {
        deserializer = new PigStorage();
    }
    
    public DefaultOutputHandler(HandleSpec spec) {
        deserializer = (LoadFunc)PigContext.instantiateFuncFromSpec(spec.spec);
    }

    public OutputType getOutputType() {
        return OutputType.SYNCHRONOUS;
    }

    public void bindTo(String fileName, BufferedPositionedInputStream is,
            long offset, long end) throws IOException {
        stdout = is;
        super.bindTo(fileName, stdout, offset, end);
    }

    public void close() throws IOException {
        super.close();
        stdout.close();
        stdout = null;
    }
}
