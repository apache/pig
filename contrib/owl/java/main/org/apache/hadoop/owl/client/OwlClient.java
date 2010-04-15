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
package org.apache.hadoop.owl.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlRestMessage;
import org.apache.hadoop.owl.protocol.OwlResultObject;

/** This class provides the interface to execute Owl commands from a Java client program */
public class OwlClient {

    /** The handler for executing requests */
    private ResourceHandler handler;

    /**
     * Instantiates a new owl client.
     * 
     * @param serverURL
     *            the server url
     */
    public OwlClient(String serverURL) {
        handler = new ResourceHandler(serverURL);
    }

    /**
     * Execute the specified command.
     * 
     * @param command
     *            the command
     * 
     * @return the owl result object
     * 
     * @throws OwlException
     *             the owl exception
     */
    public OwlResultObject execute(String command) throws OwlException {

        long t1 = System.currentTimeMillis();

        String commandToken = OwlUtil.getCommandFromString(command);

        //Create input message and execute request
        OwlRestMessage inputMessage = createRequestMessage(command);
        OwlRestMessage outputMessage = handler.performRequest(inputMessage, OwlUtil.getVerbForCommand(commandToken)); 

        //Create output object
        OwlResultObject result = new OwlResultObject();

        result.setErrorMessage(outputMessage.getH().getError());
        result.setStatus(outputMessage.getH().getStatus());

        //Copy results array into result object
        List<? extends OwlObject> outputList = new ArrayList<OwlObject>();
        if( outputMessage.getB().getResults() != null ) {
            outputList = Arrays.asList(outputMessage.getB().getResults());
        }
        result.setOutput(outputList);

        long t2 = System.currentTimeMillis();
        result.setExecutionTime(t2 - t1);

        return result;
    }

    /**
     * Creates the request message.
     * 
     * @param command
     *            the command
     * 
     * @return the owl rest message
     */
    private OwlRestMessage createRequestMessage(String command) {
        OwlRestMessage message = new OwlRestMessage();
        message.setH(handler.createHeader());
        message.getB().setCommand(command);
        return message;
    }

}
