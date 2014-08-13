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

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * This interface implemented by a {@link LoadFunc} implementations indicates to 
 * Pig that it has the capability to load data such that all instances of a key 
 * will occur in same split.
 * @since Pig 0.7
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface CollectableLoadFunc {

    /**
     * When this method is called, Pig is communicating to the Loader that it must
     * load data such that all instances of a key are in same split. Pig will
     * make no further checks at runtime to ensure whether the contract is honored
     * or not.
     * @throws IOException
     */
    public void ensureAllKeyInstancesInSameSplit() throws IOException;
}
