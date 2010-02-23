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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import java.io.IOException;

import org.apache.pig.StoreFuncInterface;

/**
 * This class is used to specify the actual behavior of the store
 * operator just when ready to start execution.
 */
public abstract class POStoreImpl {
    /**
     * Set up the storer 
     * @param store - the POStore object
     * @throws IOException
     */
    public abstract StoreFuncInterface createStoreFunc(POStore store) 
        throws IOException;
    
    /**
     * At the end of processing, the outputstream is closed
     * using this method
     * @throws IOException
     */
    public void tearDown() throws IOException{
    }
    
    /**
     * To perform cleanup when there is an error.
     * Uses the FileLocalizer method which only 
     * deletes the file but not the dirs created
     * with it.
     * @throws IOException
     */
    public void cleanUp() throws IOException{
    }
}
