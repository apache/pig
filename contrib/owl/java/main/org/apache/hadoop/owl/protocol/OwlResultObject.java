/*
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
package org.apache.hadoop.owl.protocol;

import java.util.List;

/** The object returned from Owl Client API */
public class OwlResultObject {

    /** Enum specifying the Owl operation status */
    public enum Status {
        SUCCESS,
        FAILURE
    }

    /** The round-trip command execution time in milliseconds */
    long executionTime;

    /** The status of the command execution */
    private Status status;

    /** The error message in case status is FAILURE */
    private String errorMessage;

    /** The output of the Owl command execution */
    private List<? extends OwlObject> output;

    /**
     * Gets the value of executionTime
     * @return the executionTime
     */
    public long getExecutionTime() {
        return executionTime;
    }

    /**
     * Gets the value of status
     * @return the status
     */
    public Status getStatus() {
        return status;
    }

    /**
     * Gets the value of errorMessage
     * @return the errorMessage
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Gets the value of output
     * @return the output
     */
    public List<? extends OwlObject> getOutput() {
        return output;
    }

    /**
     * Sets the value of executionTime
     * @param executionTime the executionTime to set
     */
    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }

    /**
     * Sets the value of status
     * @param status the status to set
     */
    public void setStatus(Status status) {
        this.status = status;
    }

    /**
     * Sets the value of errorMessage
     * @param errorMessage the errorMessage to set
     */
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    /**
     * Sets the value of output
     * @param output the output to set
     */
    public void setOutput(List<? extends OwlObject> output) {
        this.output = output;
    }
}
