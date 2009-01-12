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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer;

import java.io.Serializable;

public class Result implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /*
     * When returnStatus is set to POStatus.STATUS_ERR
     * Operators can choose to use the result field
     * to put a meaning error message which will be 
     * printed out in the final message shown to the user
     */
    
    public byte returnStatus;

    public Object result;

    public Result() {
        returnStatus = POStatus.STATUS_ERR;
        result = null;
    }
    
    public Result(byte returnStatus, Object result) {
        this.returnStatus = returnStatus ;
        this.result = result;
    }

    @Override
    public String toString() {
        return (result!=null)?result.toString():"NULL";
    }
    
    
}
