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

import org.apache.hadoop.mapreduce.Job;
import java.io.IOException;

/**
 * Convenience class to extend when decorating a class that extends LoadFunc and
 * implements LoadMetadata.
 */
public class LoadFuncMetadataWrapper extends LoadFuncWrapper implements LoadMetadata {

    private LoadMetadata loadMetadata;

    protected LoadFuncMetadataWrapper() {
        super();
    }

    /**
     * The wrapped LoadMetadata object must be set before method calls are made on this object.
     * Typically, this is done with via constructor, but often times the wrapped object can
     * not be properly initialized until later in the lifecycle of the wrapper object.
     * @param loadFunc
     */
    protected void setLoadFunc(LoadMetadata loadFunc) {
        super.setLoadFunc((LoadFunc)loadFunc);
        this.loadMetadata = loadFunc;
    }

    @Override
    public ResourceSchema getSchema(String location, Job job) throws IOException {
        return loadMetadata().getSchema(location, job);
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job) throws IOException {
        return loadMetadata().getStatistics(location, job);
    }

    @Override
    public String[] getPartitionKeys(String location, Job job) throws IOException {
        return loadMetadata().getPartitionKeys(location, job);
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter) throws IOException {
       loadMetadata().setPartitionFilter(partitionFilter);
    }
    
    private LoadMetadata loadMetadata() {
        if (this.loadMetadata == null) {
            throw new IllegalArgumentException("Method calls can not be made on the " +
                "LoadFuncMetadataWrapper object before the wrapped LoadMetadata object has been "
                + "set. Failed on method call " + getMethodName(1));
        }
        return loadMetadata;
    }

}
