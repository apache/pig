
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
import java.util.List;

import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.common.OwlUtil.Verb;

import org.apache.hadoop.owl.protocol.*;
import org.apache.hadoop.owl.entity.*;
import org.apache.hadoop.owl.backend.OwlBackend;


public class SelectDatabaseObjectsCommand extends Command {
    List<String> databases = new ArrayList<String>();

    SelectDatabaseObjectsCommand() {
        this.noun = Noun.OBJECTS;
        this.verb = Verb.READ;
    }

    @Override
    public void inDatabase(String databaseName){
        databases.add( OwlUtil.toLowerCase( databaseName ) );
    }

    @Override
    public List<? extends OwlObject> execute(OwlBackend backend) throws OwlException{

        List<OwlDatabase> retval = new ArrayList<OwlDatabase>();
        if ( databases.isEmpty() ){
            List<DatabaseEntity> parentDatabaseList = backend.find(DatabaseEntity.class, null);
            for (DatabaseEntity db : parentDatabaseList ){
                // convert DatabaseEntity to Database bean object
                retval.add( ConvertEntity.convert(db) );
            }
        }else{
            for (String odb : databases ){
                DatabaseEntity odatabase = getBackendDatabase(backend, odb);
                // convert DatabaseEntity to Database bean object
                retval.add( ConvertEntity.convert(odatabase) );
            }
        }
        return retval;
    }        
}
