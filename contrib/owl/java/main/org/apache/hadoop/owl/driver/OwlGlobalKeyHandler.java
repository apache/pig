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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.owl.client.OwlClient;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.OwlGlobalKey;
import org.apache.hadoop.owl.protocol.OwlResultObject;

/**
 * This class handles the OwlGlobalKey object access operations for the OwlDriver.
 */
class OwlGlobalKeyHandler {

    /**
     * The client to talk to the Owl server.
     */
    private OwlClient client;

    /**
     * Instantiates a new database handler.
     * 
     * @param client the client
     */
    OwlGlobalKeyHandler(OwlClient client) {
        this.client = client;
    }

    /**
     * Fetch all global keys in the Owl metadata.
     * @return the list of global key names
     * @throws OwlException the owl exception
     */
    List<String> fetchAll() throws OwlException {
        String command = "SELECT GLOBALKEY OBJECTS";
        OwlResultObject result = client.execute(command);

        @SuppressWarnings("unchecked") List<OwlGlobalKey> gkeys = (List<OwlGlobalKey>) result.getOutput();

        List<String> gkeyNames = new ArrayList<String>();
        for(OwlGlobalKey gkey : gkeys) {
            gkeyNames.add(gkey.getName());
        }

        return gkeyNames;
    }

    /**
     * Fetch the OwlGlobalKey instance corresponding to given name.
     * @param gkeyName the global key name
     * @return the owl global key object
     * @throws OwlException the owl exception
     */
    OwlGlobalKey fetch(String gkeyName) throws OwlException {
        OwlTableHandler.validateStringValue("GlobalKeyName", gkeyName);

        String command = "select globalkey objects where globalkey in ( " + gkeyName + ")";
        OwlResultObject result = client.execute(command);

        OwlGlobalKey gkey = (OwlGlobalKey)result.getOutput().get(0);
        return gkey;
    }
}
