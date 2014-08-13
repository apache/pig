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
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.util.Pair;

/**
 * Abstract class that specifies the interface for sample loaders
 *
 */
public abstract class SampleLoader extends LoadFunc {

    // number of samples to be sampled
    protected int numSamples;

    protected LoadFunc loader;

    // RecordReader used by the underlying loader
    private RecordReader<?, ?> recordReader= null;

    public SampleLoader(String funcSpec) {
        funcSpec = funcSpec.replaceAll("\\\\'", "'");
        loader = (LoadFunc)PigContext.instantiateFuncFromSpec(funcSpec);
    }

    public void setNumSamples(int n) {
        numSamples = n;
    }

    public int getNumSamples() {
        return numSamples;
    }

    @Override
    public InputFormat<?,?> getInputFormat() throws IOException {
        return loader.getInputFormat();
    } 

    public boolean skipNext() throws IOException {
        try {
            return recordReader.nextKeyValue();
        } catch (InterruptedException e) {
            throw new IOException("Error getting input",e);
        }
    }

    public void computeSamples(ArrayList<Pair<FileSpec, Boolean>> inputs, 
            PigContext pc) throws ExecException {
    }

    @Override
    public LoadCaster getLoadCaster() throws IOException {
        return loader.getLoadCaster();
    }

    @Override
    public String relativeToAbsolutePath(String location, Path curDir)
    throws IOException {
        return loader.relativeToAbsolutePath(location, curDir);
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        loader.prepareToRead(reader, split);
        this.recordReader = reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        loader.setLocation(location, job);
    }

    @Override
    public void setUDFContextSignature(String signature) {
        loader.setUDFContextSignature(signature);
    }
}
