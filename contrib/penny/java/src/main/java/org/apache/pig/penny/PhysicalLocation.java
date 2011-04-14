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


public class PhysicalLocation implements Location {
    
    private final LogicalLocation logLoc;
    private final int physId;
    
    private static final PhysicalLocation coordinatorLocation = new PhysicalLocation("", -1);
    public static PhysicalLocation coordinatorLocation() {
        return coordinatorLocation;
    }
    
    public PhysicalLocation(LogicalLocation logLoc, int physId) {
        this.logLoc = logLoc;
        this.physId = physId;
    }
    
    public PhysicalLocation(String logId, int physId) {
        this(new LogicalLocation(logId), physId);
    }

    @Override
    public LogicalLocation asLogical() {
        return logLoc;
    }

    @Override
    public boolean isLogicalOnly() {
        return false;
    }
    
    @Override
    public String logId() {
        return logLoc.logId();
    }

    public int physId() {
        return physId;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PhysicalLocation)) return false;
        PhysicalLocation plOther = (PhysicalLocation) other;
        return (plOther.logLoc.equals(this.logLoc) && (plOther.physId == this.physId));
    }
    
    @Override
    public int hashCode() {
        return logLoc.hashCode() + physId;
    }
    
    @Override
    public String toString() {
        if (this.equals(coordinatorLocation())) return "Loc[COORD]";
        else return "Loc[" + logId() + ";" + physId + "]";
    }

}
