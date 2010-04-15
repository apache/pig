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
package org.apache.hadoop.owl.common;

/**
 * Enum type representing the various errors throws by Owl.  
 */
public enum ErrorType {
    
    /* OR Mapping errors, range 1000 - 1999 */
    ERROR_DB_INIT                       (1000, "Error initializing database session"),
    ERROR_DB_CLOSE                      (1001, "Error closing database session"),
    ERROR_DB_BEGIN                      (1002, "Error starting database transaction"),
    ERROR_DB_COMMIT                     (1003, "Error comitting database transaction"),
    ERROR_DB_ROLLBACK                   (1004, "Error rolling back database transaction"),
    ERROR_DB_CHECKTRANS                 (1005, "Error checking database transaction state"),
    ERROR_DB_INSERT                     (1006, "Error inserting into database"),
    ERROR_DB_DELETE                     (1007, "Error deleting from database"),
    ERROR_DB_UPDATE                     (1008, "Error updating database"),
    ERROR_DB_REFRESH                    (1009, "Error refreshing from database"),
    ERROR_DB_FLUSH                      (1010, "Error flushing changes to database"),
    ERROR_DB_FETCH_BY_ID                (1011, "Error fetching row by id from database"),
    ERROR_DB_QUERY_EXEC                 (1012, "Error running database query"),
    ERROR_DB_CLEAR                      (1013, "Error clearing list of objects to update in database"),
    ERROR_DB_JDBC_DRIVER                (1014, "JDBC driver not found in CLASSPATH. Check your JDBC driver class " +
                                               "name and check if you have the driver JAR file in the webapp " +
                                               "owl/WEB-INF/lib directory", false),

    
    /* Backend errors, range 2000 - 2999 */
    ERROR_RESOURCE_TYPE                 (2000, "Invalid resource type passed for update"),
    ERROR_ENTITY_MANAGER_INIT           (2001, "Error initializing the entity manager"),
    ERROR_CREATING_RESOURCE_OBJECT      (2002, "Error creating resource object for entity class"),
    ERROR_CREATING_RESOURCE_ARRAY       (2003, "Error creating resource object array for entity class"),
    ERROR_FOREIGN_KEY_DELETE            (2004, "Operation failed because there might be objects referring to the resource being deleted", false),
    ERROR_RESOURCE_CREATE               (2005, "Error occurred during creation of the resource"),
    ERROR_RESOURCE_UPDATE               (2006, "Error occurred during updation of the resource"),
    ERROR_RESOURCE_DELETE               (2007, "Error occurred during deletion of the resource"),
    ERROR_UNIQUE_KEY_CONSTRAINT         (2008, "Operation failed because a uniqueness check was not satisfied", false),
    ERROR_FOREIGN_KEY_INSERT            (2009, "Operation failed because object refers to invalid resources", false),
    ERROR_MISMATCH_VERSON_NUMBER        (2010, "The object version number does not matched expected value"),
    ERROR_INVALID_OWLTABLE              (2011, "Invalid OwlTable Id specified"),
    ERROR_INVALID_KEY_NAME              (2012, "Invalid key name specified"),
    ERROR_INVALID_PARTITION_LEVEL       (2013, "Invalid value specified for partition level"),
    ERROR_PARTITION_KEY_MISSING         (2014, "Partition key specification has a missing level"),
    ERROR_DATABASE_NONEMPTY             (2015, "OwlTables within database have to be dropped before dropping database"),
    ERROR_INVALID_KEY_DATATYPE          (2016, "Invalid key DataType specified"),
    ERROR_INVALID_LIST_VALUE            (2017, "Invalid value specified for key"),
    ERROR_DATE_FORMAT                   (2018, "Invalid value specified for date"),
    ERROR_DATESTAMP_VALUE               (2019, "Time specified is lesser than the interval start time"),
    ERROR_NO_COLUMNNUMBER_OF_COLUMNSCHEMAENTITY            (2020, "OwlColumnSchemaEntity does NOT contain a column number"),
    

    /* Logical layer errors, range 3000 - 3999 */
    ERROR_BASIC_DATASET_OPERATION_ON_COMPOSITE_DATASET 
                                        (3000, "Attempt to perform a basic-dataset-only operation on a composite dataset"),
    ERROR_COMPOSITE_DATASET_OPERATION_ON_BASIC_DATASET
                                        (3001, "Attempt to perform a composite-dataset-only operation on a basic dataset"),
    ERROR_UNKNOWN_COMMAND               (3002, "Unable to map command passed in to appropriate Command action"),
    ERROR_COMMAND_INSTANTIATION_FAILURE (3003, "Unable to instantiate appropriate Command action"),

    ERROR_UNKNOWN_DATABASE              (3020, "Unable to map given database name to a single existing database"),
    ERROR_UNKNOWN_OWLTABLE              (3021, "Unable to map given owltable name in given database to a single owltable", false),
    ERROR_UNKNOWN_ENUM_TYPE             (3022, "Invalid ENUM code specified"),
    ERROR_INVALID_TYPE                  (3023, "Invalid type encountered"),
    ERROR_MISMATCH_PARTITION_KEYVALUES  (3024, "Partition key values provided for Data Element"
                                                + " does not match the partition keys for the given table"),
    ERROR_UNKNOWN_DATAELEMENT_PARTITION (3025, "Unable to map given data element partition keyvalue pairs"
                                                + " to an appropriate partition"),
    ERROR_UNKNOWN_PROPERTY_KEY          (3026, "Unable to locate property key with given name in given OwlTable"),
    ERROR_UNKNOWN_PROPERTY_KEY_TYPE     (3027, "Property key specified is not a regular property key or a global key"),
    ERROR_UNKNOWN_PARTITION_KEY         (3028, "Unable to locate partition key with given name in given OwlTable"),
    ERROR_OWLTABLE_PROPERTY_INVALID     (3029, "OwlTable level property keys can only be used for OwlTable operations"),
    ERROR_INVALID_VALUE_DATATYPE        (3030, "Invalid datatype for bounded key value"),
    ERROR_UNKNOWN_OPERATOR_TYPE         (3031, "Unknown operator type"),

    ERROR_DUPLICATE_PROPERTY_KEY        (3032, "Duplicate property key found"),
    ERROR_UNKNOWN_KEY_NAME_OR_TYPE      (3033, "Key name or type not specified"),
    ERROR_UNKNOWN_GLOBAL_KEY            (3034, "Unknown Global Key"),
    ERROR_DUPLICATE_LEAF_PARTITION      (3035, "Leaf partition matching criteria already exists"),
    ERROR_DUPLICATE_PUBLISH             (3036, "Data already published for given OwlTable"),

    /* Config/Setup related errors, range 4000-4999 */
    
    ERROR_UNABLE_TO_LOAD_CONFIG         (4000, "Unable to load config"),
    ERROR_UNABLE_TO_STORE_CONFIG        (4001, "Unable to store config"),
    ERROR_UNKNOWN_CONFIG_PARAMETER      (4002, "Unknown config parameter"),
    ERROR_UNABLE_TO_SYNC_PREFS          (4003, "Unable to synchronize preferences with backend preferences store"),
    ERROR_UNABLE_TO_WRITE_TO_LOGFILE    (4004, "Unable to write to output logfile"),
        
    /* User input related errors, range 5000 - 5999 */
    INVALID_UPDATE_PARAM                (5000, "Invalid parameter for resource update"),
    INVALID_MESSAGE                     (5001, "Invalid input message"),
    INVALID_MESSAGE_HEADER              (5002, "Input message does not have a header"),
    INVALID_MESSAGE_BODY                (5003, "Input message does not have a body"),
    INVALID_MESSAGE_CREATE              (5004, "Input message body does not an object to create"),
    INVALID_MESSAGE_DELTA               (5005, "Input message body does not have the delta object"),
    INVALID_DATABASE_ID                 (5006, "Invalid parent database id specified"),
    INVALID_UPDATE_FILTER_NOMATCH       (5007, "The specified filter did not match any resources to update"),
    INVALID_DELETE_FILTER_NOMATCH       (5008, "The specified filter did not match any resources to delete"),
    INVALID_UPDATE_DELETE_FILTER        (5009, "Filter cannot be empty for delete operations"),
    INVALID_PARENTID_VALUE              (5010, "Invalid value specified for parent id"),
    INVALID_RESOURCE_NAME               (5011, "The specified value for resource name is invalid"),
    INVALID_RESOURCE_OWNER              (5012, "The specified value for resource owner is invalid"),
    INVALID_RESOURCE_DESCRIPTION        (5013, "The specified value for description is invalid"),
    INVALID_RESOURCE_ID_CREATE          (5014, "Resource id should not be specified during create operation"),
    INVALID_MESSAGE_BULKCREATE          (5015, "Bulk creates are not supported currently"),
    INVALID_FIELD_VALUE                 (5016, "Invalid value specified for field"),
    INVALID_MESSAGE_COMMAND             (5017, "Input message should have a command to execute"),
    INVALID_DUPLICATE_KEY_NAME          (5018, "Duplicate key name specified"),
    INVALID_FILTER_OPERATOR             (5019, "Operation is not supported for specified data type"),
    INVALID_STORAGE_LOCATION            (5020, "Invalid value for the OwlDatabase storage location"),
    INVALID_FILTER_DATATYPE             (5021, "Invalid datatype specified for value of key"),
    INVALID_ARGUMENT_VALUE              (5022, "Invalid value specified for argument"),
    INVALID_OPERATOR_ORDER              (5023, "The value should be on the right for IN and LIKE operators"),
    INVALID_OWL_SCHEMA                  (5024, "The OwlSchema specified is invalid"),
    INVALID_SCHEMA_PARTITION_MISSING    (5025, "The OwlSchema specified does not contain the OwlTable partition key"),
    INVALID_SCHEMA_PARTITION_DATATYPE   (5026, "The datatype for the partitioning column in the OwlSchema does not match the partition key datatype"),
    INVALID_TABLE_NAME                  (5027, "The given table name is invalid"),
    MISSING_PROPERTY_KEY                (5028, "No property key is specified"),
    MISSING_PARTITION_KEY               (5029, "No partition key is specified"),
    
    /* Parser errors, ranger 6000 - 6999 */
    PARSE_EXCEPTION                     (6000, "Error occurred while parsing the command"),
    TOKENMRGERROR_EXCEPTION             (6001, "Token manager error from the owl parser"),
    ZEBRA_SCHEMA_EXCEPTION              (6002, "Error occurred while processing schema string"),
    ZEBRA_TOKENMRGERROR_EXCEPTION       (6003, "Token manager error from the zebra parser"),
    ERROR_PARSING_FILTER                (6004, "Error occurred while parsing the filter"),
    NO_SCHEMA_PROVIDED                  (6005, "Schema is required for Create OwlTable and Publish operations"),
    

    /* REST/JSON errors, range 7000 - 7499 */
    ERROR_JSON_SERIALIZATION            (7000, "Error serializing to JSON"),
    ERROR_JSON_DESERIALIZATION          (7001, "Error de-serializing from JSON"),
    ERROR_UNRECOGNIZED_RESOURCE_TYPE    (7002, "Unrecognized resource type", false),
    ERROR_HANDLING_EXCEPTION            (7003, "Error in processing exception"),
    ERROR_INVALID_REQUEST_TYPE          (7004, "Specified command does not match with the HTTP request type"),

    
    /* Client errors, range 7500 - 7999 */
    ERROR_UNSUPPORTED_ENCODING          (7500, "Unsupported encoding"),
    ERROR_FILE_NOT_FOUND                (7501, "File not found"),
    ERROR_READING_FILE                  (7502, "Error reading file contents"),
    ERROR_INVALID_SERVER_RESPONSE       (7503, "Invalid ressponse received from the server"),
    ERROR_READING_OBJECT_TYPE           (7504, "Error reading object type from the header in server response"),
    ERROR_SERVER_CONNECTION             (7505, "Error connecting to server, check if server is running and URI is correct"),
    ERROR_SERVER_COMMUNICATION          (7506, "Error communicating with server"),
    INVALID_COMMAND_EXCEPTION           (7507, "Invalid command line arguments"),
    ERROR_UNRECOGNIZED_COMMAND          (7508, "Specified command type is not recognized"),
    ERROR_INCONSISTANT_DATA_TYPE        (7509, "Inconsistant data type from input"),
    ERROR_MISSING_SUBSCHEMA             (7510, "Subschema is required for columns of type RECORD and COLLECTION"),
    ERROR_MISSING_COLUMNNAME            (7511, "Missing column name"),

    /* Data Access layer errors, range 8000 - 8999 */
    ERROR_INPUT_UNINITIALIZED           (8000, "OwlInputFormat is not initialized. OwlInputFormat.setInput must be the first function called on OwlInputFormat"),
    ERROR_SERIALIZING_DATA              (8001, "Error serializing object to string format"),
    ERROR_DESERIALIZING_DATA            (8002, "Error deserializing object from string format"),
    ERROR_CREATE_STORAGE_DRIVER         (8003, "Could not create storage driver instance from given class name"),
    ERROR_CREATE_INPUT_SPLIT            (8004, "Could not create InputSplit instance from given class name"),
    ERROR_CREATE_STORAGE_DRIVER_INSTANCE    (8005, "Could not create  storage driver instance from given class name"),
    ERROR_STORAGE_DRIVER_EXCEPTION          (8006, "Error occured in storage driver"),
    
    /* Miscellaneous errors, range 9000 - 9998 */
    ERROR_UNIMPLEMENTED                 (9000, "Functionality currently unimplemented"),
    ERROR_IDENTIFIER_LENGTH_VALIDATION_FAILED   (9001, "Identifier length validation failed"),
    ERROR_DUPLICATED_RESOURCENAME       (9002, "Validation failed due to duplicated resource name");
    

    
    
    
    /** The error code. */
    private int errorCode;
    
    /** The error message. */
    private String errorMessage;
    
    /** Should the causal exception message be appended to the error message, yes by default*/
    private boolean appendCauseMessage = true;
    
    /** Is this a retriable error, no by default. */
    private boolean isRetriable = false;

    /**
     * Instantiates a new error type.
     * 
     * @param errorCode
     *            the error code
     * @param errorMessage
     *            the error message
     */
    private ErrorType(int errorCode, String errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    /**
     * Instantiates a new error type.
     * 
     * @param errorCode
     *            the error code
     * @param errorMessage
     *            the error message
     * @param appendCauseMessage
     *            should causal exception message be appended to error message
     */
    private ErrorType(int errorCode, String errorMessage, boolean appendCauseMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.appendCauseMessage = appendCauseMessage;
    }
    
    /**
     * Instantiates a new error type.
     * 
     * @param errorCode
     *            the error code
     * @param errorMessage
     *            the error message
     * @param appendCauseMessage
     *            should causal exception message be appended to error message
     * @param isRetriable
     *            is this a retriable error
     */
    private ErrorType(int errorCode, String errorMessage, boolean appendCauseMessage, boolean isRetriable) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.appendCauseMessage = appendCauseMessage;
        this.isRetriable = isRetriable;
    }

    /**
     * Gets the error code.
     * 
     * @return the error code
     */
    public int getErrorCode() {
        return errorCode;
    }

    /**
     * Gets the error message.
     * 
     * @return the error message
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Checks if this is a retriable error.
     * 
     * @return true, if is a retriable error, false otherwise
     */
    public boolean isRetriable() {
        return isRetriable;
    }

    /**
     * Whether the cause of the exception should be added to the error message.
     * 
     * @return true, if the cause should be added to the message, false otherwise
     */
    public boolean appendCauseMessage() {
        return appendCauseMessage;
    }
}
