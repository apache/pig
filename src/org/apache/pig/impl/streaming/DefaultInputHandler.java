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

import org.apache.pig.PigToStream;
import org.apache.pig.impl.PigContext;
import org.apache.pig.builtin.PigStreaming;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;

/**
 * DefaultInputHandler handles the input for the Pig-Streaming
 * executable in a synchronous manner by feeding it input
 * via its <code>stdin</code>.  
 */
public class DefaultInputHandler extends InputHandler {
       
    public DefaultInputHandler() {
        serializer = new PigStreaming();
    }
    
    public DefaultInputHandler(HandleSpec spec) {
        serializer = (PigToStream)PigContext.instantiateFuncFromSpec(spec.spec);
    }
    
    @Override
    public InputType getInputType() {
        return InputType.SYNCHRONOUS;
    }
    
    @Override
    public synchronized void close(Process process) throws IOException {            
        try {
            super.close(process);
        } catch(IOException e) {
            // check if we got an exception because
            // the process actually completed and we were
            // trying to flush and close it's stdin
            if (process == null || process.exitValue() != 0) {
                // the process had not terminated normally 
                // throw the exception we got                    
                throw e;
            }
        }
    }
}
