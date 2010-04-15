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

package org.apache.hadoop.owl.backend;

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.LogHandler;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.entity.OwlEntity;
import org.apache.hadoop.owl.orm.OwlEntityManager;
import org.apache.hadoop.owl.orm.PersistenceWrapper;

/**
 * This class has static functions called from the backend code for doing error handling. 
 */
public class BackendError {


    /**
     * Handle backend exception, tries to generate a more meaningful error
     * message and returns a OwlException. Any active transaction is first
     * rolled back.
     * 
     * @param em
     *            the Resource Entity Manager
     * @param cause
     *            the causal exception
     * @param errorType
     *            the error type
     * 
     * @return the meta data exception to be thrown
     */
    public static OwlException handleBackendException(
            OwlEntityManager<? extends OwlEntity> em,
            Exception cause,
            ErrorType errorType
    ) {
        if( LogHandler.getLogger("server").isInfoEnabled() ) {
            LogHandler.getLogger("server").info(OwlUtil.getStackTraceString(cause)); 
        }

        //This function returns exception instead of throwing one so that the callers can do a explicit throw,
        //this helps in not having to add dummy returns in the callers to satisfy the compiler
        try {
            if(em != null) {
                if( em.isActive() ) {
                    em.rollbackTransaction();
                }

                em.close();
            }
        } catch(Exception transactionException) {
            //TODO log transaction rollback failure
            //transactionException.printStackTrace();
            OwlException mex = new OwlException(
                    errorType,
                    "Error recovering from failure, transaction rollback failed",
                    cause
            );
            LogHandler.getLogger("server").debug(mex.getMessage());
            return mex;
        }

        Throwable persistenceException = getExceptionCause(cause, java.sql.BatchUpdateException.class);

        if(persistenceException == null) {
            persistenceException = getExceptionCause(cause, javax.persistence.PersistenceException.class );
        }

        //For PersistenceException, try to map to a specific owl error type
        if( persistenceException != null && persistenceException.getMessage() != null) {
            LogHandler.getLogger("server").debug(persistenceException.getMessage());

            //Error messages are different based on database, there is no generic way which works across databases
            //to map transaction commit failure to the cause, so relying on the error message to
            //give a more meaningful error for the common causes.

            StringBuffer messageBuffer = new StringBuffer(persistenceException.getMessage());

            //Map the PersistenceException message to an owl exception
            ErrorType owlError = getOwlErrorType(messageBuffer, cause);
            // ErrorType owlError = getOwlErrorType(messageBuffer);
            if( owlError != null ) {

                cleanupErrorMessage(messageBuffer);

                OwlException mex = new OwlException(
                        owlError,
                        messageBuffer.toString(),
                        persistenceException
                );
                LogHandler.getLogger("server").debug(mex.getMessage());
                return mex;
            }            


            //If we can't map it properly, default to the passed in error type,
            //but set the cause to the persistenceException. That way the error will be
            //more meaningful than the original cause
            OwlException mex = new OwlException(
                    errorType,
                    persistenceException
            );
            LogHandler.getLogger("server").debug(mex.getMessage());
            return mex;
        }

        //Could not create a specific error message, pass on the exception or create one from the input errorType
        if( cause instanceof OwlException ) {
            LogHandler.getLogger("server").debug(cause.getMessage());
            return (OwlException) cause;
        } else {
            OwlException mex = new OwlException(errorType, cause);
            LogHandler.getLogger("server").debug(mex.getMessage());
            return mex;
        }
    }

    /**
     * Gets the owl error type, mapping the persistence error message to an owl error type if possible.
     * 
     * @param errorBuffer
     *            the error buffer, this is modified to have only the actual error message.
     * 
     * @return the owl error type
     */
    private static ErrorType getOwlErrorType(StringBuffer errorBuffer, Exception cause) {
        // private static ErrorType getOwlErrorType(StringBuffer errorBuffer) {
        int index;
        PersistenceWrapper.DatabaseType dbType = PersistenceWrapper.getDataBaseType();
        System.out.println("printing dbtype");

        if( dbType == PersistenceWrapper.DatabaseType.MYSQL ) {
            if((index = errorBuffer.indexOf("Duplicate entry")) != -1) {
                errorBuffer.delete(0, index);
                return ErrorType.ERROR_UNIQUE_KEY_CONSTRAINT;
            } else if((index = errorBuffer.indexOf("Cannot add or update a child row: a foreign key constraint fails")) != -1) {
                errorBuffer.delete(0, index);
                return ErrorType.ERROR_FOREIGN_KEY_INSERT;
            } else if((index = errorBuffer.indexOf("Cannot delete or update a parent row: a foreign key constraint fails")) != -1) {
                errorBuffer.delete(0, index);
                return ErrorType.ERROR_FOREIGN_KEY_DELETE;
            }
        }
        else  if(dbType == PersistenceWrapper.DatabaseType.ORACLE)
        {
            Throwable oraException = getExceptionCause(cause, java.sql.SQLException.class);

            if(oraException == null) {
                oraException = cause;
            } else {
                errorBuffer.setLength(0);
                errorBuffer.append(oraException.getMessage());
            }

            if((index = errorBuffer.indexOf("ORA-00001")) != -1) {
                errorBuffer.delete(0, index);
                return ErrorType.ERROR_UNIQUE_KEY_CONSTRAINT;
            }     else if((index = errorBuffer.indexOf("ORA-02291")) != -1) {
                errorBuffer.delete(0, index);
                return ErrorType.ERROR_FOREIGN_KEY_INSERT;
            }  else if((index = errorBuffer.indexOf("ORA-02292")) != -1) {
                errorBuffer.delete(0, index);
                return ErrorType.ERROR_FOREIGN_KEY_DELETE;
            } 
        }

        return null;
    }



    /**
     * Cleanup error message, remove characters after a newline character. This is
     * required since datanucleus seems to insert stack traces in the message sometimes.
     * 
     * @param errorBuffer the error buffer, this gets modified after the cleanup
     */
    private static void cleanupErrorMessage(StringBuffer errorBuffer) {
        int newLineIndex = errorBuffer.indexOf("\r");
        if( newLineIndex == -1) {
            newLineIndex = errorBuffer.indexOf("\n");
        }

        if( newLineIndex != -1 ) {
            errorBuffer.setLength(newLineIndex);
        }
    }


    /**
     * Check if passed topLevelException is or has causeException as one of its
     * causes. If it has the causeClass as cause, gets the cause exception.
     * 
     * @param topLevelException
     *            the top level exception
     * @param causeClass
     *            the cause exception class
     * 
     * @return the cause exception if found, null if causeClass is not one of the causes
     */
    public static Throwable getExceptionCause (
            Exception topLevelException,
            Class<? extends Throwable> causeClass) {
        Throwable e = topLevelException;

        while ( e != null ) {
            if( e.getMessage() != null ) {
                LogHandler.getLogger("server").debug(e.getMessage());
            }

            if( causeClass.getName().equals(e.getClass().getName()) ) {
                //Not using instanceof here since it causes mismatch in some cases 
                //(like javax.persistence.RollbackException is derived from javax.persistence.PersistenceException)

                //Passed top level exception has causeException as one of its causes
                return e;

            }

            e = e.getCause();
        }

        return null;
    }
}

