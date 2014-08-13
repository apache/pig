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
package org.apache.pig.impl.streaming;

import org.apache.pig.backend.executionengine.ExecException;

public class StreamingUDFException extends ExecException {
    static final long serialVersionUID = 1;
    private String message;
    private String language;
    private Integer lineNumber;

    public StreamingUDFException() {
    }

    public StreamingUDFException(String message) {
        this.message = message;
    }

    public StreamingUDFException(String message, Integer lineNumber) {
        this.message = message;
        this.lineNumber = lineNumber;
    }

    public StreamingUDFException(String language, String message, Throwable cause) {
        super(cause);
        this.language = language;
        this.message = message + "\n" + cause.getMessage() + "\n";
    }

    public StreamingUDFException(String language, String message) {
        this(language, message, (Integer) null);
    }

    public StreamingUDFException(String language, String message, Integer lineNumber) {
        this.language = language;
        this.message = message;
        this.lineNumber = lineNumber;
    }

    public String getLanguage() {
        return language;
    }

    public Integer getLineNumber() {
        return lineNumber;
    }

    @Override
    public String getMessage() {
        return this.message;
    }
    
    @Override
    public String toString() {
        //Don't modify this without also modifying Launcher.getExceptionFromStrings!
        String s = getClass().getName();
        String message = getMessage();
        String lineNumber = this.getLineNumber() == null ? "" : "" + this.getLineNumber();
        return (message != null) ? (s + ": " + "LINE " + lineNumber + ": " + message) : s;

    }
}
