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

import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.streaming.StreamingCommand.Handle;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;

/**
 * Factory to create an {@link InputHandler} or {@link OutputHandler}
 * depending on the specification of the {@link StreamingCommand}.
 */
public class HandlerFactory {

    /**
     * Create an <code>InputHandler</code> for the given input specification
     * of the <code>StreamingCommand</code>.
     * 
     * @param command <code>StreamingCommand</code>
     * @return <code>InputHandler</code> for the given input specification
     * @throws ExecException
     */
    public static InputHandler createInputHandler(StreamingCommand command) 
    throws ExecException {
        List<HandleSpec> inputSpecs = command.getHandleSpecs(Handle.INPUT);
        
        HandleSpec in = null;
        if (inputSpecs == null || (in = inputSpecs.get(0)) == null) {
            return new DefaultInputHandler();
        }
        
        return (in.name.equals("stdin")) ? new DefaultInputHandler(in) :
                                           new FileInputHandler(in);
    }
    
    /**
     * Create an <code>OutputHandler</code> for the given output specification
     * of the <code>StreamingCommand</code>.
     * 
     * @param command <code>StreamingCommand</code>
     * @return <code>OutputHandler</code> for the given output specification
     * @throws ExecException
     */
    public static OutputHandler createOutputHandler(StreamingCommand command) 
    throws ExecException {
        List<HandleSpec> outputSpecs = command.getHandleSpecs(Handle.OUTPUT);
        
        HandleSpec out = null;
        if (outputSpecs == null || (out = outputSpecs.get(0)) == null) {
            return new DefaultOutputHandler();
        }
        
        return (out.name.equals("stdout")) ? new DefaultOutputHandler(out) :
                                             new FileOutputHandler(out);
    }
}
