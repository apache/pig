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

import java.io.ByteArrayInputStream;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.owl.backend.OwlBackend;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.LogHandler;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.common.OwlUtil.Verb;
import org.apache.hadoop.owl.logical.Command;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlRestMessage;
import org.apache.hadoop.owl.protocol.OwlResultObject;
import org.apache.hadoop.owl.parser.OwlParser;
import org.apache.hadoop.owl.parser.ParseException;


@Path("/rest")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
/**
 * Class which exposes Owl REST interfaces using Jersey.
 */
public class OwlREST {


    /**
     * The function to handle all PUT requests.
     * 
     * @param inputMessageString
     *            the input message string
     * @return the response
     * @throws OwlException the owl exception
     */
    @PUT
    public  Response putHandler(String inputMessageString) throws OwlException {
        return handleRequest(inputMessageString, Verb.CREATE);
    }

    /**
     * The function to handle all POST requests.
     * 
     * @param inputMessageString
     *            the input message string
     * @return the response
     * @throws OwlException the owl exception
     */
    @POST
    public Response postHandler(String inputMessageString) throws OwlException {
        return handleRequest(inputMessageString, Verb.UPDATE);
    }


    /**
     * The function to handle all DELETE requests.
     * 
     * @param inputMessageString
     *            the input message string
     * @return the response
     * @throws OwlException the owl exception
     */
    @DELETE
    public Response deleteHandler(
            @QueryParam("message") String inputMessageString
    ) throws OwlException {
        return handleRequest(inputMessageString, Verb.DELETE);
    }


    /**
     * The function to handle all GET requests.
     * 
     * @param inputMessageString
     *            the input message string
     * @return the response
     * @throws OwlException the owl exception
     */
    @GET
    public Response getHandler(
            @QueryParam("message") String inputMessageString
    )  throws OwlException {
        return handleRequest(inputMessageString, Verb.READ);
    }


    /**
     * Handle the specified request, call the parser, create a backend, begin transaction, 
     * execute the command, commit/rollback transaction and generate the output response.
     * @param inputMessageString the input message string
     * @param opType the operation type
     * @return the response
     * @throws OwlException the owl exception
     */
    public Response handleRequest(String inputMessageString,Verb opType) throws OwlException {
        OwlRestMessage outputMessage = new OwlRestMessage();
        OwlBackend backend = null;
        String command = null;

        try {
            LogHandler.getLogger("server").debug(
                    "OwlRequest : " + opType + " : Message [" + inputMessageString + "]");

            OwlRestMessage inputMessage = initializeMessage(
                    inputMessageString,
                    outputMessage,
                    opType);

            command = inputMessage.getB().getCommand();
            LogHandler.getLogger("server").debug("Command " + command);

            ByteArrayInputStream bi = new ByteArrayInputStream(command.getBytes());

            //Parse the input command
            OwlParser parser = new OwlParser(bi);
            Command cmd = parser.Start();

            //Create a backend, begin transaction, execute the Command 
            backend = new OwlBackend();
            backend.beginTransaction();

            List<? extends OwlObject> results = cmd.execute(backend);

            //For selects, rollback transaction, otherwise commit transaction
            //before closing backend
            if( opType == Verb.READ ) {
                backend.rollbackTransaction();
            } else {
                backend.commitTransaction();
            }

            backend.close();

            //Copy response into outputMessage
            OwlObject[] resultArray = null;
            if( results != null ) {
                resultArray = new OwlObject[results.size()];
                results.toArray(resultArray);

                if( results.size() > 0 ) {
                    //Set the object bean type in the output message
                    String objectType = results.get(0).getClass().getSimpleName();
                    outputMessage.getH().setObjectType(objectType);
                }
            }
            outputMessage.getB().setResults(resultArray);
            outputMessage.getH().setStatus(OwlResultObject.Status.SUCCESS);

            //Convert to JSON and return response
            String outputJson = OwlUtil.getJSONFromObject(outputMessage);
            return Response.ok(outputJson).build();

        }
        catch(Throwable e) {

            if( backend != null ) {
                try {
                    backend.close();
                } catch(Exception ce) {
                    //Exception while cleaning up, return the original error,
                    //just log this exception
                    if( LogHandler.getLogger("server").isInfoEnabled() ) {
                        LogHandler.getLogger("server").info(
                                OwlUtil.getStackTraceString(ce)); 
                    }
                }
            }

            //Update error description in output message
            String outputJson = handleRESTException(e, outputMessage, command);
            return Response.ok(outputJson).build();
        }
    }


    /**
     * Handle rest exception.
     * @param throwable the original throwable
     * @param outputMessage the output message
     * @param command the command being executed
     * @return the JSON string for the output rest message
     */
    public String handleRESTException(Throwable throwable,
            OwlRestMessage outputMessage, String command) {
        try {
            if( LogHandler.getLogger("server").isInfoEnabled() ) {
                LogHandler.getLogger("server").info(OwlUtil.getStackTraceString(throwable));
            }

            String exceptionMessage = throwable.toString();

            outputMessage.getH().setStatus(OwlResultObject.Status.FAILURE);
            if( throwable instanceof OwlException ) {
                if( command != null && 
                        ((OwlException) throwable).getErrorType() == ErrorType.PARSE_EXCEPTION) {
                    //If parse exception, add the command to it
                    exceptionMessage = new OwlException(ErrorType.PARSE_EXCEPTION,
                            "\"" + command + "\"", throwable.getCause()).toString();
                }

                outputMessage.getH().setErrorType(((OwlException) throwable).getErrorType());
            } else if( throwable instanceof ParseException ) {
                outputMessage.getH().setErrorType(ErrorType.PARSE_EXCEPTION);
                if( command != null ) {
                    exceptionMessage =  exceptionMessage + " : \"" + command + "\"";
                }
            }
            else {
                outputMessage.getH().setErrorType(null);
            }

            outputMessage.getH().setError(exceptionMessage);

            String outputString = OwlUtil.getJSONFromObject(outputMessage);
            return outputString;

        }catch (Exception e) {
            //Cannot return a valid JSON, just returning the error message as a string 
            return new OwlException(ErrorType.ERROR_HANDLING_EXCEPTION, throwable.toString(), e).toString();
        }
    }


    /**
     * Initialize output message.
     * 
     * @param inputMessage
     *            the input message
     * @param outputMessage
     *            the output message
     */
    public void initializeOutputMessage(
            OwlRestMessage inputMessage,
            OwlRestMessage outputMessage ) {

        //Copy input message id into output message
        if( inputMessage!= null && inputMessage.getH() != null ) {
            outputMessage.getH().setId( inputMessage.getH().getId() );
        }

        //Initialize to OwlObject as default, will be updated for select operations 
        outputMessage.getH().setObjectType(OwlObject.class.getSimpleName());
    }


    /**
     * Initialize message.
     * 
     * @param inputMessageString
     *            the input message string
     * @param outputMessage
     *            the output message
     * @param opType
     *            the op type
     * 
     * @return the owl rest message
     * 
     * @throws OwlException
     *             the owl exception
     */
    private OwlRestMessage initializeMessage(
            String inputMessageString,
            OwlRestMessage outputMessage,
            Verb opType) throws OwlException {
        //Convert input from JSON to Bean
        OwlRestMessage inputMessage = (OwlRestMessage) OwlUtil.getObjectFromJSON(
                OwlRestMessage.class,
                null,
                inputMessageString);

        //Populate output message with defaults
        initializeOutputMessage(
                inputMessage,
                outputMessage);

        //Validate input message
        ValidateMessage.validateRequest(
                inputMessage,
                inputMessageString,
                opType);

        return inputMessage;
    }
}
