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

package org.apache.hadoop.owl.rest;

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.common.OwlUtil.Verb;
import org.apache.hadoop.owl.protocol.OwlRestBody;
import org.apache.hadoop.owl.protocol.OwlRestMessage;

/**
 * This class defined the static functions used to validate input REST messages.
 */
public class ValidateMessage {

    /**
     * Validate request, check if request has all required fields and is of property type .
     * 
     * @param inputMessage the input message
     * @param inputString the input string
     * @param opType the operation type
     * @throws OwlException the owl exception
     */
    public static void validateRequest(
            OwlRestMessage inputMessage,
            String inputString,
            Verb opType) throws OwlException {

        if( inputMessage == null ) {
            throw new OwlException(ErrorType.INVALID_MESSAGE, inputString);
        }

        if( inputMessage.getH() == null ) {
            throw new OwlException(ErrorType.INVALID_MESSAGE_HEADER, inputString);
        }

        OwlRestBody body = inputMessage.getB();

        if( body == null ) {
            throw new OwlException(ErrorType.INVALID_MESSAGE_BODY, inputString);
        }

        if( body.getCommand() == null ) {
            throw new OwlException(ErrorType.INVALID_MESSAGE_COMMAND, inputString);
        }

        validateRequestType(body.getCommand(), opType);

    }

    /**
     * Validate request type, checks if command matches with HTTP request type (GET/PUT/POST/DELETE).
     * 
     * @param commandString the command string
     * @param opType the operation type
     * 
     * @throws OwlException the owl exception
     */
    public static void validateRequestType(String commandString, Verb opType) throws OwlException {

        String commandToken = OwlUtil.getCommandFromString(commandString);
        Verb commandVerb = OwlUtil.getVerbForCommand(commandToken);

        if( commandVerb != opType ) {
            throw new OwlException(ErrorType.ERROR_INVALID_REQUEST_TYPE,
                    "Expected <" + commandVerb.getHttpRequestType() + "> request for command <" + commandToken + 
                    ">, got <" + opType.getHttpRequestType() + "> request");
        }
    }
}
