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
import org.apache.hadoop.owl.common.OwlUtil.Verb;
import org.apache.hadoop.owl.entity.OwlResourceEntity;

/** A utility class for functions which do validations on inputs provided by the user. This class
 * has static functions called from other places.
 */
public class ValidateInput {

    /** Regex used to validate whether an identifier (name,owner etc) is valid */
    private static String identifierRegex = "[\\w\\.\\-_]*";

    /** Regex used to validate whether the text (like description) is valid, this regex allows spaces */
    private static String textRegex =  "[\\w\\.\\-\\s\\(\\)_,:'\"\\/]*";


    /**
     * Check for unsafe update/delete, update and delete operations are not allowed with null/empty filters.
     * 
     * @param filter the filter
     * @throws OwlException the meta data exception
     */
    public static void checkUnsafeUpdate(String filter) throws OwlException {
        if( filter == null || filter.trim().length() == 0 ) {
            OwlException mex = new OwlException(ErrorType.INVALID_UPDATE_DELETE_FILTER);
            LogHandler.getLogger("server").debug(mex.getMessage());
            throw mex;
        }
    }


    /**
     * Validate if the name, owner and description are set to proper values in the input resource.
     * 
     * @param inputResource the input object being validated
     * @param opType the type of operation being performed
     * @throws OwlException the meta data exception
     */
    public static void validateResource(
            OwlResourceEntity inputResource,
            Verb opType) throws OwlException {

        if( opType == Verb.CREATE ) {
            if( inputResource.getId() > 0 ) {
                OwlException mex = new OwlException(ErrorType.INVALID_RESOURCE_ID_CREATE,
                        Integer.toString(inputResource.getId()));
                LogHandler.getLogger("server").debug(mex.getMessage());
                throw mex;
            }
        }

        //validate description, can be null
        if( ! validateText(inputResource.getDescription(), true) ) {
            OwlException mex = new OwlException(ErrorType.INVALID_RESOURCE_DESCRIPTION,
                    inputResource.getDescription());
            LogHandler.getLogger("server").debug(mex.getMessage());
            throw mex;
        }

        //validate owner, can be null
        if( ! validateIdentifier(inputResource.getOwner(), true) ) {
            OwlException mex = new OwlException(ErrorType.INVALID_RESOURCE_OWNER,
                    inputResource.getOwner());
            LogHandler.getLogger("server").debug(mex.getMessage());
            throw mex;
        }
    }


    /**
     * Validate whether the input is a valid string as per the specified regex value.
     * 
     * @param input the input string
     * @param regex the regex to check against
     * @param allowNulls is null a valid value
     * 
     * @return true, if valid
     */
    private static boolean validateString(String input, String regex, boolean allowNulls) {

        //Check if null is allowed
        if( input == null ) {
            if( allowNulls == false ) {
                return false;
            } else {
                return true;
            }
        }

        //Check if the input has any invalid characters
        if( input.matches(regex)) {
            return true;
        } else {
            return false;
        }
    }


    /**
     * Validate whether the input is a valid identifier value.
     * 
     * @param input the input string
     * @param allowNulls is null a valid value
     * 
     * @return true, if valid
     */
    private static boolean validateIdentifier(String input, boolean allowNulls) {
        return validateString(input, identifierRegex, allowNulls);
    }

    /**
     * Validate whether the input is a valid text value.
     * 
     * @param input the input string
     * @param allowNulls is null a valid value
     * 
     * @return true, if valid
     */
    private static boolean validateText(String input, boolean allowNulls) {
        return validateString(input, textRegex, allowNulls);
    }

    /**
     * Check whether specified field is null or empty.
     * 
     * @param fieldName
     *            the field name, appended to exception thrown
     * @param fieldValue
     *            the field value to check
     * 
     * @throws OwlException
     *             the meta data exception
     */
    public static void checkForEmpty(String fieldName, String fieldValue) throws OwlException {
        if( fieldValue == null || fieldValue.trim().length() == 0 ) {
            throw new OwlException(ErrorType.INVALID_FIELD_VALUE, fieldName);
        }
    }
}
