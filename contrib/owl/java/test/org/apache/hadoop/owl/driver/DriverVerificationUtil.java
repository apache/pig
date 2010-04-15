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

package org.apache.hadoop.owl.driver;

import org.apache.hadoop.owl.client.OwlClient;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.OwlDatabase;
import org.apache.hadoop.owl.protocol.OwlTable;
import org.apache.hadoop.owl.protocol.OwlTableName;


/**
 * Provides utility functions for driver testing
 */

public class DriverVerificationUtil {

    public static void dropOwlTableIfExists(OwlDriver driver, OwlTableName name) throws OwlException {
        try {
            driver.dropOwlTable(new OwlTable(name));
        } catch (OwlException oe){
            if (! ((oe.getErrorType().equals(ErrorType.ERROR_UNKNOWN_DATABASE)) || (oe.getErrorType().equals(ErrorType.ERROR_UNKNOWN_OWLTABLE)))){
                throw oe;
            }
        }
    }

    public static void dropOwlDatabaseIfExists(OwlDriver driver, String dbname) throws OwlException {
        try {
            driver.dropOwlDatabase(new OwlDatabase(dbname, null));
        } catch (OwlException oe){
            if (! oe.getErrorType().equals(ErrorType.ERROR_UNKNOWN_DATABASE)){
                throw oe;
            }
        }
    }

    public static void runClientExecuteIgnoreExceptionsSpecified(OwlClient client, String command, ErrorType[] allowedErrorTypes) throws OwlException {
        try {
            client.execute(command);
        } catch(OwlException oe) {
            boolean exceptionAllowed = false;
            for (int i = 0; ((i < allowedErrorTypes.length)&&(!exceptionAllowed)) ; i++){
                if (oe.getErrorType().equals(allowedErrorTypes[i])){
                    exceptionAllowed = true;
                }
            }
            if (! exceptionAllowed){
                throw oe;
            }
        }
    }


    public static void dropOwlDatabaseIfExists(OwlClient client, String dbName) throws OwlException {
        DriverVerificationUtil.runClientExecuteIgnoreExceptionsSpecified(
                client,
                "drop owldatabase " + dbName, 
                new ErrorType[] {
                        ErrorType.ERROR_UNKNOWN_DATABASE,
                }
        );
    }

    public static void dropOwlTableIfExists(OwlClient client, String tableName, String dbName) throws OwlException {
        DriverVerificationUtil.runClientExecuteIgnoreExceptionsSpecified(
                client,
                "drop owltable " + tableName + " within owldatabase " + dbName, 
                new ErrorType[] {
                        ErrorType.ERROR_UNKNOWN_DATABASE,
                        ErrorType.ERROR_UNKNOWN_OWLTABLE
                }
        );
    }


}
