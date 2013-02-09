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

/**
 * All exceptions in Pig are encapsulated in the <code>PigException</code>
 * class. Details such as the source of the error, error message, error
 * code, etc. are contained in this class. The default values for the
 * attributes are:
 * errorSource = BUG
 * errorCode = 0
 * retriable = false
 * detailedMessage = null
 */
public class PigException extends IOException {

	// Change this if you modify the class.
	static final long serialVersionUID = 1L;

    /*
     * Instead of using an enum for the source of the error,
     * the classic style of using static final is adopted
     */
    public static final byte INPUT = 2;
    public static final byte BUG = 4;
    public static final byte USER_ENVIRONMENT = 8;
    public static final byte REMOTE_ENVIRONMENT = 16;
    public static final byte ERROR = -1;

    /**
     * A static method to query if an error source is due to
     * an input or not.
     *
     * @param errSource - byte that indicates the error source
     * @return true if the error source is an input; false otherwise
     */
    public static boolean isInput(byte errSource) {
        return ((errSource & INPUT) == 0 ? false : true);
    }

    /**
     * A static method to query if an error source is due to
     * a bug or not.
     *
     * @param errSource - byte that indicates the error source
     * @return true if the error source is a bug; false otherwise
     */
    public static boolean isBug(byte errSource) {
        return ((errSource & BUG) == 0 ? false : true);
    }

    /**
     * A static method to query if an error source is due to
     * the user environment or not.
     *
     * @param errSource - byte that indicates the error source
     * @return true if the error source is due to the user environment; false otherwise
     */
    public static boolean isUserEnvironment(byte errSource) {
        return ((errSource & USER_ENVIRONMENT) == 0 ? false : true);
    }

    /**
     * A static method to query if an error source is due to
     * the remote environment or not.
     *
     * @param errSource - byte that indicates the error source
     * @return true if the error source is due to the remote environment; false otherwise
     */
    public static boolean isRemoteEnvironment(byte errSource) {
        return ((errSource & REMOTE_ENVIRONMENT) == 0 ? false : true);
    }
    
    /**
     * A static method to determine the error source given the error code
     * 
     *  @param errCode - integer error code
     *  @return byte that indicates the error source
     */
    public static byte determineErrorSource(int errCode) {
    	if(errCode >= 100 && errCode <= 1999) {
    		return PigException.INPUT;
    	} else if (errCode >= 2000 && errCode <= 2999) {
    		return PigException.BUG;
    	} else if (errCode >= 3000 && errCode <= 4999) {
    		return PigException.USER_ENVIRONMENT;
    	} else if (errCode >= 5000 && errCode <= 6999) {
    		return PigException.REMOTE_ENVIRONMENT;
    	}
    	return PigException.ERROR;
    }
    
    protected int errorCode = 0;
    protected byte errorSource = BUG;
    protected boolean retriable = false;
    protected String detailedMessage = null;
    protected boolean markedAsShowToUser = false;

    /**
     * Create a new PigException with null as the error message.
     */
    public PigException() {
        super();
    }
    
    /**
     * Create a new PigException with the specified message.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     */
    public PigException(String message) {
        super(message);
    }
    
    /**
     * Create a new PigException with the specified cause.
     *
     * @param cause - The cause (which is saved for later retrieval by the <link>Throwable.getCause()</link> method) indicating the source of this exception. A null value is permitted, and indicates that the cause is nonexistent or unknown.
     */
    public PigException(Throwable cause) {
        super(cause);
    }

    /**
     * Create a new PigException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param cause - The cause (which is saved for later retrieval by the <link>Throwable.getCause()</link> method) indicating the source of this exception. A null value is permitted, and indicates that the cause is nonexistent or unknown.
     */
    public PigException (String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Create a new PigException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     */
    public PigException (String message, int errCode) {
        this(message);
        errorCode = errCode;
    }

    /**
     * Create a new PigException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param cause - The cause (which is saved for later retrieval by the <link>Throwable.getCause()</link> method) indicating the source of this exception. A null value is permitted, and indicates that the cause is nonexistent or unknown. 
     */
    public PigException (String message, int errCode, Throwable cause) {
        this(message, cause);
        errorCode = errCode;
    }

    /**
     * Create a new PigException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param errSrc - The error source 
     */
    public PigException (String message, int errCode, byte errSrc) {
        this(message, errCode);
        errorSource = errSrc;
    }

    /**
     * Create a new PigException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param errSrc - The error source
     * @param cause - The cause (which is saved for later retrieval by the <link>Throwable.getCause()</link> method) indicating the source of this exception. A null value is permitted, and indicates that the cause is nonexistent or unknown. 
     */
    public PigException (String message, int errCode, byte errSrc, Throwable cause) {
        this(message, errCode, errSrc, false, null, cause);
    }

    /**
     * Create a new PigException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param retry - If the exception is retriable or not
     */
    public PigException (String message, int errCode, boolean retry) {
        this(message, errCode);
        retriable = retry;
    }

    /**
     * Create a new PigException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param errSrc - The error source 
     * @param retry - If the exception is retriable or not
     */
    public PigException (String message, int errCode, byte errSrc, boolean retry) {
        this(message, errCode, errSrc);
        retriable = retry;
    }

    /**
     * Create a new PigException with the specified message and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param errSrc - The error source 
     * @param retry - If the exception is retriable or not
     * @param detailedMsg - The detailed message shown to the developer 
     */
    public PigException (String message, int errCode, byte errSrc, boolean retry, String detailedMsg) {
        this(message, errCode, errSrc, retry);
        detailedMessage = detailedMsg;
    }

    /**
     * Create a new PigException with the specified message, error code, error source, retriable or not, detalied message for the developer and cause.
     *
     * @param message - The error message (which is saved for later retrieval by the <link>Throwable.getMessage()</link> method) shown to the user 
     * @param errCode - The error code shown to the user 
     * @param errSrc - The error source 
     * @param retry - If the exception is retriable or not
     * @param detailedMsg - The detailed message shown to the developer 
     * @param cause - The cause (which is saved for later retrieval by the <link>Throwable.getCause()</link> method) indicating the source of this exception. A null value is permitted, and indicates that the cause is nonexistent or unknown.
     */
    public PigException (String message, int errCode, byte errSrc, boolean retry, String detailedMsg, Throwable cause) {
        super(message, cause);
        errorCode = errCode;
        errorSource = errSrc;
        retriable = retry;
        detailedMessage = detailedMsg;
    }    
    
    /**
     * Checks if the exception is retriable.
     * @return if the exception is retriable or not
     */
    public boolean retriable() {
        return retriable;
    }

    /**
     * Set the retriable attribute of the exception
     * @param retry - true if retriable; false otherwise
     */
    public void setRetriable(boolean retry) {
        retriable = retry;
    }

    /**
     * Returns the error source of the exception. Can be more than one source.
     * @return error sources represented as a byte
     */
    public byte getErrorSource() {
        return errorSource;
    }

    /**
     * Set the error source of the exception
     * @param src - byte representing the error sources
     */
    public void setErrorSource(byte src) {
        errorSource = src;
    }

    /**
     * Returns the error code of the exception
     * @return error code of the exception
     */
    public int getErrorCode() {
        return errorCode;
    }

    /**
     * Set the error code of the exception
     * @param code - error code
     */
    public void setErrorCode(int code) {
        errorCode = code;
    }

    /**
     * Returns the detailed message used by developers for debugging
     * @return detailed message
     */
    public String getDetailedMessage() {
        return detailedMessage;
    }

    /**
     * Set the detailed message of the exception
     * @param detailMsg - detailed message to be used by developers
     */
    public void setDetailedMessage(String detailMsg) {
        detailedMessage = detailMsg;
    }
    
    /**
     * Check if this PigException is marked as the ones whose message is to be 
     * displayed to the user. This can be used to indicate if the corresponding 
     * error message is a good candidate for displaying to the end user, instead
     * of drilling down the stack trace further.
     * @return true if this pig exception is marked as appropriate to be 
     * displayed to the user
     */
    public boolean getMarkedAsShowToUser() {
        return markedAsShowToUser;
    }
    
    /**
     * Mark this exception as a good candidate for showing its message to the 
     * pig user 
     */
    public void setMarkedAsShowToUser(boolean showToUser) {
        markedAsShowToUser = showToUser;
    }
    
    /**
     * Returns a short description of this throwable.
     * The result is the concatenation of:
     * <ul>
     * <li> the {@linkplain Class#getName() name} of the class of this object
     * <li> ": " (a colon and a space)
     * <li> "ERROR " (the string ERROR followed by a a space)
     * <li> the result of invoking this object's {@link #getErrorCode} method
     * <li> ": " (a colon and a space)
     * <li> the result of invoking {@link Throwable#getLocalizedMessage() getLocalizedMessage}
     *      method
     * </ul>
     * If <tt>getLocalizedMessage</tt> returns <tt>null</tt>, then just
     * the class name is returned.
     *
     * @return a string representation of this throwable.
     */
    @Override
    public String toString() {
        String s = getClass().getName();
        String message = getLocalizedMessage();
        return (message != null) ? (s + ": " + "ERROR " + getErrorCode() + ": " + message) : s;
    }
}
