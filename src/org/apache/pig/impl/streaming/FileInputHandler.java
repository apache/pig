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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.pig.PigToStream;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;

/**
 * FileInputHandler handles the input for the Pig-Streaming
 * executable in an asynchronous manner by feeding it input
 * via an external file specified by the user.  
 */
public class FileInputHandler extends InputHandler {
   
    OutputStream fileOutStream;
    
    public FileInputHandler(HandleSpec handleSpec) throws ExecException {
        String fileName = handleSpec.name;
        serializer = 
            (PigToStream) PigContext.instantiateFuncFromSpec(handleSpec.spec);
        
        try {
            fileOutStream = new FileOutputStream(new File(fileName));
            super.bindTo(fileOutStream);
        } catch (IOException fnfe) {
            int errCode = 2046;
            String msg = "Unable to create FileInputHandler.";
            throw new ExecException(msg, errCode, PigException.BUG, fnfe);
        }
    }

    @Override
    public InputType getInputType() {
        return InputType.ASYNCHRONOUS;
    }
    
    @Override
    public void bindTo(OutputStream os) throws IOException {
        throw new UnsupportedOperationException("Cannot call bindTo on " +
        		                                "FileInputHandler");
    }
    
    public synchronized void close(Process process) throws IOException {
        super.close(process);
        if (fileOutStream != null) {
            fileOutStream.flush();
            fileOutStream.close();
            fileOutStream = null;
        }
    }
       
}
