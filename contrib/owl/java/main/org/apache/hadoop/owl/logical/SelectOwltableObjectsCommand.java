
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.common.OwlUtil.Verb;

import org.apache.hadoop.owl.protocol.*;
import org.apache.hadoop.owl.entity.*;
import org.apache.hadoop.owl.backend.OwlBackend;


public class SelectOwltableObjectsCommand extends Command {
    List<String> databases = new ArrayList<String>();
    String likeTableNameStr = null; 
    String databaseName = null;

    SelectOwltableObjectsCommand() {
        this.noun = Noun.OBJECTS;
        this.verb = Verb.READ;
    }

    @Override
    public void inDatabase(String databaseName){
        databases.add( OwlUtil.toLowerCase( databaseName ) );
    }
    @Override 
    public void likeTables(String likeTableNameStr){
        this.likeTableNameStr = OwlUtil.toLowerCase( likeTableNameStr );
    }

    @Override
    public void setParentDatabase(String databaseName) throws OwlException {
        this.databaseName = OwlUtil.toLowerCase( databaseName );
    }

    @SuppressWarnings("boxing")
    public List<OwlTable> getBackendLikeOwlTables(OwlBackend backend) throws OwlException {
        List<OwlTable> owlTableList = new ArrayList<OwlTable>();
        List<OwlTableEntity> owlTableEntityList;

        List<GlobalKeyEntity> globalKeys = backend.find(GlobalKeyEntity.class, null);

        if(databaseName != null)
        {
            int likeDatabaseId = getBackendDatabaseId(backend, databaseName);
            owlTableEntityList = backend.find(
                    OwlTableEntity.class,
                    "databaseId = "+ likeDatabaseId +" and name like \"" + likeTableNameStr + "\""
            );

        }
        else
        {
            owlTableEntityList = backend.find(
                    OwlTableEntity.class,
                    "name like \"" + likeTableNameStr + "\""
            );
        }

        if (owlTableEntityList.size() == 0 ){
            return owlTableList;
        } 

        HashMap<Integer, String> dbIdtoNameMap = new HashMap<Integer, String>();

        for (OwlTableEntity ot : owlTableEntityList){

            String dbName = databaseName;

            if( databaseName == null ) {
                dbName = dbIdtoNameMap.get(ot.getDatabaseId());

                if( dbName == null ) {
                    List<DatabaseEntity> dbEntities = backend.find(DatabaseEntity.class, "id = " + ot.getDatabaseId());
                    dbName = dbEntities.get(0).getName();
                    dbIdtoNameMap.put(ot.getDatabaseId(), dbName);
                }
            }

            owlTableList.add(ConvertEntity.convert(ot, dbName, globalKeys, backend));
        }

        return owlTableList;
    }


    @Override
    public List<? extends OwlObject> execute(OwlBackend backend) throws OwlException{

        List<OwlTable> retval = null;

        if(likeTableNameStr != null)
        {  
            retval = getBackendLikeOwlTables(backend); 
        }
        else
        { 
            retval = new ArrayList<OwlTable>();
            List<GlobalKeyEntity> globalKeys = backend.find(GlobalKeyEntity.class, null);
            List<String> catList = new ArrayList<String>();
            if ( databases.isEmpty()){
                // get all the database entities in the system
                List<DatabaseEntity> parentDatabaseList= backend.find(DatabaseEntity.class, null);
                // get the database names for each database entity
                for ( DatabaseEntity cat : parentDatabaseList){
                    catList.add(cat.getName());
                }
            }else{
                catList = this.databases;
            }

            for (String ocat : catList ){
                int databaseId = getBackendDatabaseId(backend, ocat);
                List<OwlTableEntity> owlTableList = backend.find(OwlTableEntity.class, "databaseId = "+ databaseId );
                for (OwlTableEntity ot : owlTableList){
                    System.out.println("ot " + ot.getDescription());
                    // convert DatabaseEntity to Database bean object
                    retval.add(ConvertEntity.convert(ot, ocat, globalKeys, backend));
                }
            }

        }

        return retval;

    }  
}
