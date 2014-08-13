/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.pig.builtin;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.StoreFuncMetadataWrapper;
import org.apache.pig.StoreMetadata;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.JarManager;

/**
 * Wrapper class which will delegate calls to parquet.pig.ParquetStorer
 */
public class ParquetStorer extends StoreFuncMetadataWrapper {

    public ParquetStorer() throws FrontendException {
        try {
            init(new parquet.pig.ParquetStorer());
        }
        // if compile time dependency not found at runtime
        catch (NoClassDefFoundError e) {
            throw new FrontendException(String.format("Cannot instantiate class %s (%s)",
                    getClass().getName(), "parquet.pig.ParquetStorer"), 2259, e);
        }
    }
    
    private void init(StoreMetadata storeMetadata) {
        setStoreFunc(storeMetadata);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        JarManager.addDependencyJars(job, parquet.Version.class);
        super.setStoreLocation(location, job);
    }
    
}
