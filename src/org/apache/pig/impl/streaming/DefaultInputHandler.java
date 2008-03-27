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
import java.io.OutputStream;

import org.apache.pig.StoreFunc;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;

/**
 * {@link DefaultInputHandler} handles the input for the Pig-Streaming
 * executable in a {@link InputType#SYNCHRONOUS} manner by feeding it input
 * via its <code>stdin</code>.  
 */
public class DefaultInputHandler extends InputHandler {
    
    OutputStream stdin;
    
    public DefaultInputHandler() {
        serializer = new PigStorage();
    }
    
    public DefaultInputHandler(HandleSpec spec) {
        serializer = (StoreFunc)PigContext.instantiateFuncFromSpec(spec.spec);
    }
    
    public InputType getInputType() {
        return InputType.SYNCHRONOUS;
    }
    
    public void bindTo(OutputStream os) throws IOException {
        stdin = os;
        super.bindTo(stdin);
    }
    
    public void close() throws IOException {
        super.close();
        stdin.flush();
        stdin.close();
        stdin = null;
    }
}
