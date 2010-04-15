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

import java.net.URL;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.LogHandler;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlRestHeader;
import org.apache.hadoop.owl.protocol.OwlRestMessage;
import org.apache.hadoop.owl.protocol.OwlResultObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.api.client.config.DefaultClientConfig;

/**
 * This class implements the JSON message handling and Jersey Client API calls for Owl client.
 */

class ResourceHandler {

    protected Client client;
    protected String baseUri;

    /**
     * Returns an instance of a logger (<code>org.apache.commons.logging.Log</code>) for the client
     */
    protected Log getLogger(){
        return LogHandler.getLogger("client");
    }

    /**
     * Constructor for ResourceHandler
     * @param uri URI where Owl Server is installed and running
     */
    ResourceHandler(String uri){
        DefaultClientConfig clientConfig = new DefaultClientConfig();
        this.client = Client.create(clientConfig);

        //Remove trailing / character if any
        if( uri.endsWith("/") ) {
            uri = uri.substring(0, uri.length() - 1);
        }

        this.baseUri = uri;
    }


    /**
     * Creates a unique identifier identifying the client and the unique call.
     * @return A string identifier identifying the client and the call
     */
    protected String generateId(){
        String host;
        try {
            host = java.net.InetAddress.getLocalHost().toString();
        } catch (UnknownHostException e) {
            host = "unknown";
        }
        String id = host + "." + Long.toString(System.currentTimeMillis());
        return id;
    }

    /**
     * Constructs a <code>OwlRestHeader</code> given request type
     * @return A constructed <code>OwlRestHeader</code> based on the given request type
     */
    protected OwlRestHeader createHeader(){
        OwlRestHeader header = new OwlRestHeader();
        header.setAuth(null);
        header.setId(generateId());
        header.setVersion("2");
        return header;
    }

    /**
     * Internal debug logging routine used by <code>performGet</code>/<code>performPut</code>/<code>performPost</code>/<code>performDelete</code>
     * @param functionName The function that called the debug log routine
     * @param prefix A prefix before the message being logged
     * @param message The <code>OwlRestMessage</code> object being logged
     * @throws OwlException
     */
    protected void logMessageIntoDebugLog(String functionName, String prefix, OwlRestMessage message) throws OwlException{
        getLogger().debug("["+functionName+"] "+prefix+ "[" + OwlUtil.getJSONFromObject(message) +"]");
    }

    /**
     * Validates responses from the server for error conditions and converts them to exceptions
     * @throws OwlException
     */
    private void validateResponse(OwlRestMessage response) throws OwlException{
        getLogger().debug("Hmmm... validating response");
        if (response == null){
            getLogger().debug("response was null");
            throw new OwlException(ErrorType.ERROR_INVALID_SERVER_RESPONSE, "Null response from server");
        }
        if (response.getH() == null){
            getLogger().debug("response header was null! [" + OwlUtil.getJSONFromObject(response)+"]");
            throw new OwlException(ErrorType.ERROR_INVALID_SERVER_RESPONSE, "Null header in response from server");
        }
        if (response.getH().getStatus() == OwlResultObject.Status.FAILURE ){
            getLogger().debug("Error from server! [" + OwlUtil.getJSONFromObject(response.getH()) + "]");
            ErrorType errorType = response.getH().getErrorType();

            throw OwlException.createException(response.getH().getError(), errorType);
        }
    }


    /**
     * performs a operation on the predefined server URI and the specified resource
     * @param message The <code>OwlRestMessage</code> that we have constructed to send along
     * @param verb the type of operation being performed
     * @return A <code>OwlRestMessage</code> response from the server.
     * @throws OwlException the owl exception
     */
    OwlRestMessage performRequest(OwlRestMessage message, OwlUtil.Verb verb) throws OwlException{

        String uri = baseUri;
        String messageJSON = OwlUtil.getJSONFromObject(message);

        if( verb == OwlUtil.Verb.READ || verb == OwlUtil.Verb.DELETE ) {
            //For select and delete, the message goes as part of the url
            uri += "?message=" + OwlUtil.encodeURICompatible(messageJSON);
        }

        logMessageIntoDebugLog(verb.toString(), "sending ", message);

        String serverResponse = null;
        try {
            getLogger().debug("accessing uri:[" + uri + "]");

            Builder builder = client.resource(uri)
                .accept("application/json")
                .type("application/json");

            //Perform http request as per the command type
            switch(verb) {
            case READ:
                serverResponse = builder.get(String.class);
                break;
            case DELETE:
                serverResponse = builder.delete(String.class);
                break;
            case UPDATE:
                serverResponse = builder.post(String.class, messageJSON);
                break;
            case CREATE:
                serverResponse = builder.put(String.class, messageJSON);
                break;
            }

        } catch (ClientHandlerException e) {
            getLogger().error("Whoops, there wasn't a server running, or you gave a bad uri");
            OwlException me = new OwlException(ErrorType.ERROR_SERVER_CONNECTION, baseUri, e); 
            throw me;
        } catch (Exception e) {
            getLogger().error("Whoops, I got an exception " + e);
            OwlException me = new OwlException(ErrorType.ERROR_SERVER_COMMUNICATION, e);
            throw me;
        }

        Class <? extends OwlObject> owlClass = OwlUtil.getObjectType(serverResponse);

        OwlRestMessage response = (OwlRestMessage) OwlUtil.getObjectFromJSON (
                message.getClass(),
                owlClass,
                serverResponse);

        logMessageIntoDebugLog(verb.toString(), "received", response);

        validateResponse(response);
        return response;
    }
}
