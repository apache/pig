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

/**
 * This interface is used to implement classes that can perform both
 * Load and Store functionalities in a symmetric fashion (thus reversible). 
 * 
 * The symmetry property of implementations is used in the optimization
 * engine therefore violation of this property while implementing this 
 * interface is likely to result in unexpected output from executions.
 * 
 */
public interface ReversibleLoadStoreFunc extends LoadFunc, StoreFunc {

}
