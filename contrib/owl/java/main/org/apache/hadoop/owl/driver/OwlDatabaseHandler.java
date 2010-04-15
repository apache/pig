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
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.protocol.OwlDatabase;
import org.apache.hadoop.owl.protocol.OwlResultObject;

/**
 * This class handles the OwlDatabase object access operations for the OwlDriver.
 */
class OwlDatabaseHandler {

    /**
     * The client to talk to the Owl server.
     */
    private OwlClient client;

    /**
     * Instantiates a new database handler.
     * 
     * @param client the client
     */
    OwlDatabaseHandler(OwlClient client) {
        this.client = client;
    }

    /**
     * Creates the OwlDatabase.
     * @param database the database
     * @throws OwlException the owl exception
     */
    void create(OwlDatabase database) throws OwlException {
        OwlTableHandler.validateValue("Database", database);
        OwlTableHandler.validateStringValue("DatabaseName", database.getName());

        StringBuffer command = new StringBuffer("create owldatabase " + database.getName() + " identified by storage directory ");

        if( database.getStorageLocation() != null && database.getStorageLocation().length() > 0 ) {
            command.append("\"" + database.getStorageLocation() + "\"");
        } else {
            throw new OwlException(ErrorType.INVALID_STORAGE_LOCATION);
        }

        client.execute(command.toString());
    }

    /**
     * Drops the OwlDatabase.
     * @param database the database
     * @throws OwlException the owl exception
     */
    void drop(OwlDatabase database) throws OwlException {
        OwlTableHandler.validateValue("Database", database);
        OwlTableHandler.validateStringValue("DatabaseName", database.getName());

        String command = "drop owldatabase " + database.getName();
        client.execute(command);
    }

    /**
     * Alter the OwlDatabase.
     * @param alterCommand the alter command object
     */
    void alter(AlterOwlDatabaseCommand alterCommand) {
        //No-op for Owlbase 2
    }


    /**
     * Fetch all database in the Owl metadata.
     * @return the list of database names
     * @throws OwlException the owl exception
     */
    List<String> fetchAll() throws OwlException {
        String command = "select owldatabase objects";
        OwlResultObject result = client.execute(command);

        @SuppressWarnings("unchecked") List<OwlDatabase> databases = (List<OwlDatabase>) result.getOutput();

        List<String> databaseNames = new ArrayList<String>();
        for(OwlDatabase database : databases) {
            databaseNames.add(database.getName());
        }

        return databaseNames;
    }

    /**
     * Fetch the OwlDatabase corresponding to given name.
     * @param databaseName the database name
     * @return the owl database object
     * @throws OwlException the owl exception
     */
    OwlDatabase fetch(String databaseName) throws OwlException {
        OwlTableHandler.validateStringValue("DatabaseName", databaseName);

        String command = "select owldatabase objects where owldatabase in ( " + databaseName + ")";
        OwlResultObject result = client.execute(command);

        OwlDatabase database = (OwlDatabase) result.getOutput().get(0);
        return database;
    }
}
