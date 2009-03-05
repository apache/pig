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
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;


public class RandomSampleLoader extends BinStorage {
    
    public static int defaultNumSamples = 100;
    int numSamples = defaultNumSamples;
    private long skipInterval;
    
    @Override
    public void bindTo(String fileName, BufferedPositionedInputStream is, long offset, long end) throws IOException {
        skipInterval = (end - offset)/numSamples;
        super.bindTo(fileName, is, offset, end);
    }
    
    @Override
    public Tuple getNext() throws IOException {
        long initialPos = in.getPosition();
        Tuple t = super.getNext();
        long finalPos = in.getPosition();
        
        long toSkip = skipInterval - (finalPos - initialPos);
        if (toSkip > 0) {
            long rc = in.skip(toSkip);
            
            // if we did not skip enough
            // in the first attempt, call
            // in.skip() repeatedly till we
            // skip enough
            long remainingSkip = toSkip - rc;
            while(remainingSkip > 0) {
                rc = in.skip(remainingSkip);
                if(rc == 0) {
                    // underlying stream saw EOF
                    break;
                }
                remainingSkip -= rc;
            }
        }
        return t;
    }
    
    @Override
    public void bindTo(OutputStream os) throws IOException {
        int errCode = 2101;
        String msg = this.getClass().getName() + " should not be used for storing.";
        throw new ExecException(msg, errCode, PigException.BUG);
    }
    
    
    
}
