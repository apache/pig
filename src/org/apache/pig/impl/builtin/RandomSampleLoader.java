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

import org.apache.pig.LoadFunc;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 * A loader that samples the data.  This loader can subsume loader that
 * can handle starting in the middle of a record.  Loaders that can
 * handle this should implement the SamplableLoader interface.
 */
public class RandomSampleLoader extends SampleLoader {
 
    /**
     * Construct with a class of loader to use.
     * @param funcSpec func spec of the loader to use.
     * @param ns Number of samples per map to collect. 
     * Arguments are passed as strings instead of actual types (FuncSpec, int) 
     * because FuncSpec only supports string arguments to
     * UDF constructors.
     */
    public RandomSampleLoader(String funcSpec, String ns) {
    	// instantiate the loader
        super(funcSpec);
        // set the number of samples
        super.setNumSamples(Integer.valueOf(ns));
    }
    
    
    @Override
    public void setNumSamples(int n) {
    	// Setting it to 100 as default for order by
    	super.setNumSamples(100);
    }
    
    @Override
    public LoadFunc.RequiredFieldResponse fieldsToRead(RequiredFieldList requiredFieldList) throws FrontendException {
        return loader.fieldsToRead(requiredFieldList);
    }
 
}
