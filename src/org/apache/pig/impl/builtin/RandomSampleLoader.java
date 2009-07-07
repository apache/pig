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

import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.SamplableLoader;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema;


/**
 * A loader that samples the data.  This loader can subsume loader that
 * can handle starting in the middle of a record.  Loaders that can
 * handle this should implement the SamplableLoader interface.
 */
public class RandomSampleLoader implements LoadFunc {
    
    private int numSamples;
    private long skipInterval;
    private SamplableLoader loader;
    
    /**
     * Construct with a class of loader to use.
     * @param funcSpec func spec of the loader to use.
     * @param ns Number of samples per map to collect. 
     * Arguments are passed as strings instead of actual types (FuncSpec, int) 
     * because FuncSpec only supports string arguments to
     * UDF constructors.
     */
    public RandomSampleLoader(String funcSpec, String ns) {
        loader = (SamplableLoader)PigContext.instantiateFuncFromSpec(funcSpec);
        numSamples = Integer.valueOf(ns);
    }
    
    @Override
    public void bindTo(String fileName, BufferedPositionedInputStream is, long offset, long end) throws IOException {
        skipInterval = (end - offset)/numSamples;
        loader.bindTo(fileName, is, offset, end);
    }
    
    @Override
    public Tuple getNext() throws IOException {
        long initialPos = loader.getPosition();
        Tuple t = loader.getSampledTuple();
        long finalPos = loader.getPosition();
        
        long toSkip = skipInterval - (finalPos - initialPos);
        if (toSkip > 0) {
            long rc = loader.skip(toSkip);
            
            // if we did not skip enough
            // in the first attempt, call
            // in.skip() repeatedly till we
            // skip enough
            long remainingSkip = toSkip - rc;
            while(remainingSkip > 0) {
                rc = loader.skip(remainingSkip);
                if(rc == 0) {
                    // underlying stream saw EOF
                    break;
                }
                remainingSkip -= rc;
            }
        }
        return t;
    }
    
    public Integer bytesToInteger(byte[] b) throws IOException {
        return loader.bytesToInteger(b);
    }

    public Long bytesToLong(byte[] b) throws IOException {
        return loader.bytesToLong(b);
    }

    public Float bytesToFloat(byte[] b) throws IOException {
        return loader.bytesToFloat(b);
    }

    public Double bytesToDouble(byte[] b) throws IOException {
        return loader.bytesToDouble(b);
    }

    public String bytesToCharArray(byte[] b) throws IOException {
        return loader.bytesToCharArray(b);
    }

    public Map<String, Object> bytesToMap(byte[] b) throws IOException {
        return loader.bytesToMap(b);
    }

    public Tuple bytesToTuple(byte[] b) throws IOException {
        return loader.bytesToTuple(b);
    }

    public DataBag bytesToBag(byte[] b) throws IOException {
        return loader.bytesToBag(b);
    }

    public void fieldsToRead(Schema schema) {
        loader.fieldsToRead(schema);
    }

    public Schema determineSchema(
            String fileName,
            ExecType execType,
            DataStorage storage) throws IOException {
        return loader.determineSchema(fileName, execType, storage);
    }
}