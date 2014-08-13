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
 * Convenience class to extend when decorating a class that implements both StoreFunc and
 * StoreMetadata. It's not abstract so that it will fail to compile if new methods get added to
 * either interface.
 */
public class StoreFuncMetadataWrapper extends StoreFuncWrapper implements StoreMetadata {

    private StoreMetadata storeMetadata;

    protected StoreFuncMetadataWrapper() {
        super();
    }

    /**
     * The wrapped StoreMetadata object must be set before method calls are made on this object.
     * Typically, this is done with via constructor, but often times the wrapped object can
     * not be properly initialized until later in the lifecycle of the wrapper object.
     * @param storeFunc
     */
    protected void setStoreFunc(StoreMetadata storeFunc) {
        super.setStoreFunc((StoreFuncInterface)storeFunc);
        this.storeMetadata = storeFunc;
    }

    @Override
    public void storeStatistics(ResourceStatistics resourceStatistics, String location, Job job)
    throws IOException {
        storeMetadata().storeStatistics(resourceStatistics, location, job);
    }

    @Override
    public void storeSchema(ResourceSchema resourceSchema, String location, Job job)
    throws IOException {
        storeMetadata().storeSchema(resourceSchema, location, job);
    }

    private StoreMetadata storeMetadata() {
        if (this.storeMetadata == null) {
            throw new IllegalArgumentException("Method calls can not be made on the " +
                "StoreFuncMetadataWrapper object before the wrapped StoreMetadata object has been "
                + "set. Failed on method call " + getMethodName(1));
        }
        return storeMetadata;
    }
}
