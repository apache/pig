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
package org.apache.pig.impl.logicalLayer;


import org.apache.pig.PigException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.parser.SourceLocation;

public class FrontendException extends PigException {

    // Change this if you modify the class.
    static final long serialVersionUID = 1L;

    /**
     * Create a new FrontendException with null as the error message.
     */
    public FrontendException() {
        super();
    }
    
    /**
     * Create a new FrontendException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     */
    public FrontendException(String message) {
        super(message);
    }
    
    /**
     * Create a new FrontendException with the specified cause.
     *
     * @param cause - The cause (which is saved for later retrieval by the <link>Throwable.getCause()</link> method) indicating the source of this exception. A null value is permitted, and indicates that the cause is nonexistent or unknown.
     */
    public FrontendException(Throwable cause) {
        super(cause);
    }

    /**
     * Create a new FrontendException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param cause - The cause (which is saved for later retrieval by the <link>Throwable.getCause()</link> method) indicating the source of this exception. A null value is permitted, and indicates that the cause is nonexistent or unknown.
     */
    public FrontendException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Create a new FrontendException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param location - The location of the exception.
     */
    public FrontendException(String message, int errCode, SourceLocation location) {
        super(message, errCode, location);
    }
    
    /**
     * Create a new FrontendException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     */
    public FrontendException(String message, int errCode) {
        super(message, errCode);
    }

    /**
     * Create a new FrontendException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param cause - The cause (which is saved for later retrieval by the <link>Throwable.getCause()</link> method) indicating the source of this exception. A null value is permitted, and indicates that the cause is nonexistent or unknown. 
     */
    public FrontendException(String message, int errCode, Throwable cause) {
        super(message, errCode, cause);
    }

    /**
     * Create a new FrontendException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param errSrc - The error source 
     */
    public FrontendException(String message, int errCode, byte errSrc) {
        super(message, errCode, errSrc);
    }   

    /**
     * Create a new FrontendException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param errSrc - The error source
     * @param cause - The cause (which is saved for later retrieval by the <link>Throwable.getCause()</link> method) indicating the source of this exception. A null value is permitted, and indicates that the cause is nonexistent or unknown. 
     */
    public FrontendException(String message, int errCode, byte errSrc, Throwable cause) {
        super(message, errCode, errSrc, cause);
    }
    
    /**
     * Create a new FrontendException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param errSrc - The error source
     * @param cause - The cause (which is saved for later retrieval by the <link>Throwable.getCause()</link> method) indicating the source of this exception. A null value is permitted, and indicates that the cause is nonexistent or unknown. 
     * @param location - The location of the exception.
     */
    public FrontendException(String message, int errCode, byte errSrc, Throwable cause, SourceLocation location) {
        super(message, errCode, errSrc, cause, location);
    }
    
    /**
     * Create a new FrontendException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param retry - If the exception is retriable or not
     */ 
    public FrontendException(String message, int errCode, boolean retry) {
        super(message, errCode, retry);
    }

    /**
     * Create a new FrontendException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param errSrc - The error source 
     * @param retry - If the exception is retriable or not
     */
    public FrontendException(String message, int errCode, byte errSrc,
            boolean retry) {
        super(message, errCode, errSrc, retry);
    }    

    /**
     * Create a new FrontendException with the specified message, error code, error source, retriable or not, detalied message for the developer and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param errSrc - The error source 
     * @param retry - If the exception is retriable or not
     * @param detailedMsg - The detailed message shown to the developer 
     */
    public FrontendException(String message, int errCode, byte errSrc,
            boolean retry, String detailedMsg) {
        super(message, errCode, errSrc, retry, detailedMsg);
    }
    
    /**
     * Create a new FrontendException with the specified message, error code, error source, retriable or not, detalied message for the developer and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param errSrc - The error source 
     * @param retry - If the exception is retriable or not
     * @param detailedMsg - The detailed message shown to the developer 
     * @param cause - The cause (which is saved for later retrieval by the <link>Throwable.getCause()</link> method) indicating the source of this exception. A null value is permitted, and indicates that the cause is nonexistent or unknown.
     */
    public FrontendException(String message, int errCode, byte errSrc,
            boolean retry, String detailedMsg, Throwable cause) {
        super(message, errCode, errSrc, retry, detailedMsg, cause);
    }

    /**
     * Create a new FrontendException with the specified message and cause.
     *
     * @param op - logical operator where the exception occurs
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     */
    public FrontendException(Operator op, String message) {
        super( op.getLocation() + message, op.getLocation() );
    }

    /**
     * Create a new FrontendException with the specified message and cause.
     *
     * @param op - logical operator where the exception occurs
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     */
    public FrontendException(Operator op, String message, int errCode) {
        this( op.getLocation() + message, errCode, op.getLocation() );
    }

    /**
     * Create a new FrontendException with the specified message and cause.
     *
     * @param op - logical operator where the exception occurs
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param errSrc - The error source 
     */
    public FrontendException(Operator op, String message, int errCode,
            byte errSrc) {
        this( op.getLocation() + message, errCode, op.getLocation() );
    }

    /**
     * Create a new FrontendException with the specified message and cause.
     *
     * @param op - logical operator where the exception occurs
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param errSrc - The error source
     * @param cause - The cause (which is saved for later retrieval by the <link>Throwable.getCause()</link> method) indicating the source of this exception. A null value is permitted, and indicates that the cause is nonexistent or unknown. 
     */
    public FrontendException(Operator op, String message, int errCode,
            byte errSrc, Throwable cause) {
        this( op.getLocation() + message, errCode, errSrc, cause, op.getLocation() );
    }

    /**
     * Create a new FrontendException with the specified message and cause.
     *
     * @param op - logical operator where the exception occurs
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param cause - The cause (which is saved for later retrieval by the <link>Throwable.getCause()</link> method) indicating the source of this exception. A null value is permitted, and indicates that the cause is nonexistent or unknown. 
     */
    public FrontendException(Operator op, String message, int errCode, Throwable cause) {
        super( op.getLocation() + message, errCode, cause, op.getLocation() );
    }

    /**
     * Create a new FrontendException with the specified message, error code, error source, retriable or not, detalied message for the developer and cause.
     *
     * @param op - logical operator where the exception occurs
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param errSrc - The error source 
     * @param retry - If the exception is retriable or not
     * @param detailedMsg - The detailed message shown to the developer 
     * @param cause - The cause (which is saved for later retrieval by the <link>Throwable.getCause()</link> method) indicating the source of this exception. A null value is permitted, and indicates that the cause is nonexistent or unknown.
     */
    public FrontendException(Operator op, String message, int errCode, byte errSrc,
            boolean retry, String detailedMsg, Throwable cause) {
        super(op.getLocation() + message, errCode, errSrc, retry, detailedMsg, cause, op.getLocation() );
    }

}
