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
package org.apache.pig;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;

/**
 * Convenience class to extend when decorating a LoadFunc. Subclasses must call the setLoadFunc
 * with an instance of LoadFunc before other methods can be called. Not doing so will
 * result in an IllegalArgumentException when the method is called.
 */
public class LoadFuncWrapper extends LoadFunc {

    private LoadFunc loadFunc;

    protected LoadFuncWrapper() {}

    /**
     * The wrapped LoadFunc object must be set before method calls are made on this object.
     * Typically, this is done with via constructor, but often times the wrapped object can
     * not be properly initialized until later in the lifecycle of the wrapper object.
     * @param loadFunc
     */
    protected void setLoadFunc(LoadFunc loadFunc) {
        this.loadFunc = loadFunc;
    }

    @Override
    public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
        return loadFunc().relativeToAbsolutePath(location, curDir);
    } 
    
    @Override
    public void setLocation(String location, Job job) throws IOException {
        loadFunc().setLocation(location, job);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return loadFunc().getInputFormat();
    }
    
    @Override
    public LoadCaster getLoadCaster() throws IOException {
        return loadFunc().getLoadCaster();
    }
        
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        loadFunc().prepareToRead(reader, split);
    }

    @Override
    public Tuple getNext() throws IOException {
       return loadFunc().getNext();
    }

    @Override
    public void setUDFContextSignature(String signature) {
        loadFunc().setUDFContextSignature(signature);
    }
    
    protected LoadFunc loadFunc() {
        if (this.loadFunc == null) {
            // Pig does not re-throw the exception with a stack trace in the parse phase.
            throw new IllegalArgumentException("Method calls can not be made on the " +
                "LoadFuncWrapper object before the wrapped LoadFunc object has been "
                + "set. Failed on method call " + getMethodName(1));
        }
        return loadFunc;
    }
    
    /**
     * Returns a method in the call stack at the given depth. Depth 0 will return the method that
     * called this getMethodName, depth 1 the method that called it, etc...
     * @param depth
     * @return method name as String
     */
    protected String getMethodName(final int depth) {
        final StackTraceElement[] ste = Thread.currentThread().getStackTrace();
        return ste[2 + depth].getMethodName();
    }

}
