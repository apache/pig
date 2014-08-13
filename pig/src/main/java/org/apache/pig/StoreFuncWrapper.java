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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.Utils;

import java.io.IOException;

/**
 * Convenience class to extend when decorating a StoreFunc. It's not abstract so that it will fail
 * to compile if new methods get added to StoreFuncInterface. Subclasses must call the setStoreFunc
 * with an instance of StoreFuncInterface before other methods can be called. Not doing so will
 * result in an IllegalArgumentException when the method is called.
 */
public class StoreFuncWrapper implements StoreFuncInterface {

    private StoreFuncInterface storeFunc;

    protected StoreFuncWrapper() {}

    /**
     * The wrapped StoreFuncInterface object must be set before method calls are made on this object.
     * Typically, this is done with via constructor, but often times the wrapped object can
     * not be properly initialized until later in the lifecycle of the wrapper object.
     * @param storeFunc
     */
    protected void setStoreFunc(StoreFuncInterface storeFunc) {
        this.storeFunc = storeFunc;
    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path path) throws IOException {
        return storeFunc().relToAbsPathForStoreLocation(location, path);
    }

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        return storeFunc().getOutputFormat();
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        storeFunc().setStoreLocation(location, job);
    }

    @Override
    public void checkSchema(ResourceSchema resourceSchema) throws IOException {
        storeFunc().checkSchema(resourceSchema);
    }

    @Override
    public void prepareToWrite(RecordWriter recordWriter) throws IOException {
        storeFunc().prepareToWrite(recordWriter);
    }

    @Override
    public void putNext(Tuple tuple) throws IOException {
        storeFunc().putNext(tuple);
    }

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        storeFunc().setStoreFuncUDFContextSignature(signature);
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
        storeFunc().cleanupOnFailure(location, job);
    }

    @Override
    public void cleanupOnSuccess(String location, Job job) throws IOException {
        storeFunc().cleanupOnSuccess(location, job);
    }

    private StoreFuncInterface storeFunc() {
        if (this.storeFunc == null) {
            // Pig does not re-throw the exception with a stack trace in the parse phase.
            throw new IllegalArgumentException("Method calls can not be made on the " +
                "StoreFuncWrapper object before the wrapped StoreFuncInterface object has been "
                + "set. Failed on method call " + getMethodName(1));
        }
        return storeFunc;
    }

    /**
     * Returns a method in the call stack at the given depth. Depth 0 will return the method that
     * called this getMethodName, depth 1 the method that called it, etc...
     * @param depth
     * @return method name as String
     */
    protected String getMethodName(final int depth) {
        final StackTraceElement[] ste = Thread.currentThread().getStackTrace();
        int index;
        if (Utils.isVendorIBM()) {
          index = 3 + depth;
        } else {
          index = 2 + depth;
        }
        return ste[index].getMethodName();
    }
}