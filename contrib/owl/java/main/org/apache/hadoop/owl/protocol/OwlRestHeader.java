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
package org.apache.hadoop.owl.protocol;

import org.apache.hadoop.owl.common.ErrorType;

/**
 * This class represents a header of a REST message (see OwlRestMessage)
 */
public class OwlRestHeader {

    /** The version of client sending the request */
    private String    version;

    /** An optional unique identifier for the request */
    private String    id;

    /** An optional (currently) authentication string */
    private String    auth;

    /** The execution status */
    private OwlResultObject.Status    status; 

    /** The error message in case status is FAILURE */
    private String    error;

    /** The errorType enum */
    private ErrorType errorType;

    /** The type of objects being returned, populated by server to indicate what type of results are returned */
    private String objectType;


    /**
     * Instantiates a new owl rest header.
     */
    public OwlRestHeader() {
    }

    /**
     * Gets the value of version
     * @return the version
     */
    public String getVersion() {
        return version;
    }

    /**
     * Gets the value of id
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the value of auth
     * @return the auth
     */
    public String getAuth() {
        return auth;
    }

    /**
     * Gets the value of status
     * @return the status
     */
    public OwlResultObject.Status getStatus() {
        return status;
    }

    /**
     * Gets the value of error
     * @return the error
     */
    public String getError() {
        return error;
    }

    /**
     * Gets the value of errorType
     * @return the errorType
     */
    public ErrorType getErrorType() {
        return errorType;
    }

    /**
     * Sets the value of version
     * @param version the version to set
     */
    public void setVersion(String version) {
        this.version = version;
    }

    /**
     * Sets the value of id
     * @param id the id to set
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Sets the value of auth
     * @param auth the auth to set
     */
    public void setAuth(String auth) {
        this.auth = auth;
    }

    /**
     * Sets the value of status
     * @param status the status to set
     */
    public void setStatus(OwlResultObject.Status status) {
        this.status = status;
    }

    /**
     * Sets the value of error
     * @param error the error to set
     */
    public void setError(String error) {
        this.error = error;
    }

    /**
     * Sets the value of errorType
     * @param errorType the errorType to set
     */
    public void setErrorType(ErrorType errorType) {
        this.errorType = errorType;
    }

    /**
     * Sets the object type.
     * 
     * @param objectType
     *            the new object type
     */
    public void setObjectType(String objectType) {
        this.objectType = objectType;
    }

    /**
     * Gets the object type.
     * 
     * @return the object type
     */
    public String getObjectType() {
        return objectType;
    }

}

// eof
