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
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.newplan.logical.rules.InputOutputFileValidator;

/**
 * A {@link StoreFunc} should implement this interface to enable overwriting its
 * store/output location if it already exists.
 */
public interface OverwritableStoreFunc {
    /**
     * This method is called by the Pig runtime to determine whether to ignore output validation problems
     * (see {@link PigOutputFormat#checkOutputSpecs} and {@link InputOutputFileValidator#validate}) and to delete the existing output.
     * @return whether to overwrite the store/output location of this {@link StoreFunc}.
     */
    public boolean shouldOverwrite();

    /**
     * This method is called to cleanup the store/output location of this {@link StoreFunc}.
     * 
     * @param store The {@link POStore} object to get info about the store
     * operator for this store function.
     * @param job The {@link Job} object to get job related information.
     * @throws IOException if an exception occurs during the cleanup.
     */    
    public void cleanupOutput(POStore store, Job job) throws IOException;
}
