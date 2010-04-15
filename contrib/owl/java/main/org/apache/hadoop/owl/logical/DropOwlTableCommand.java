
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

package org.apache.hadoop.owl.logical;

import java.util.List;

import org.apache.hadoop.owl.backend.OwlBackend;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.common.OwlUtil.Verb;
import org.apache.hadoop.owl.entity.DatabaseEntity;
import org.apache.hadoop.owl.entity.OwlTableEntity;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlTable;

public class DropOwlTableCommand extends Command {

    String name = null;
    String database = null;

    DropOwlTableCommand() {
        this.noun = Noun.OWLTABLE;
        this.verb = Verb.DELETE;
    }

    public String getName(){
        return this.name;
    }


    public String getParentDatabaseName(){
        return this.database;
    }

    @Override
    public void setName(String name){
        this.name = OwlUtil.toLowerCase( name );
    }

    @Override
    public void setParentDatabase(String databaseName){
        this.database = OwlUtil.toLowerCase( databaseName );
    }

    @Override
    public List<? extends OwlObject> execute(OwlBackend backend) throws OwlException{

        //Get the owltable so that drop fails for invalid table name
        int dbId = getBackendDatabaseId(backend, database);
        OwlTableEntity table = getBackendOwlTable(backend, dbId, name);

        backend.delete(OwlTableEntity.class, "id = " + table.getId());
        return null;
    }


}
