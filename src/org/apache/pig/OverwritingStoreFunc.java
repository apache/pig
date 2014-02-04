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

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;

/**
 * This interface defines whether storefunc will cleanup the output before writing data
 */
public interface OverwritingStoreFunc {
    /**
     * @return whether the implementation supports overwrite
     */
    public boolean isOverwrite();

    /**
     * cleanup the old output if you want to overwrite
     * 
     * @param store
     *            the store information you would like to delete
     * @param job
     *            used for deletion operation
     * @throws IOException
     */
    public void cleanupOutput(POStore store, Job job) throws IOException;
}
