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
package org.apache.pig.penny;

public class LogicalLocation implements Location {

    private final String id;
    
    public LogicalLocation(String id) {
        this.id = id;
    }
    
    @Override
    public LogicalLocation asLogical() {
        return this;
    }

    @Override
    public boolean isLogicalOnly() {
        return true;
    }

    @Override
    public String logId() {
        return id;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LogicalLocation)) return false;
        LogicalLocation llOther = (LogicalLocation) other;        
        return (llOther.id.equals(this.id));
    }
    
    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "Loc[" + id + "]";
    }

}
