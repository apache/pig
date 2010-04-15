
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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.common.OwlUtil.Verb;

import org.apache.hadoop.owl.logical.CommandInfo.FilterString;
import org.apache.hadoop.owl.protocol.*;
import org.apache.hadoop.owl.entity.*;
import org.apache.hadoop.owl.backend.OwlBackend;



public class SelectPartitionpropertyObjectsCommand extends Command {

    CommandInfo info = null;
    String owltableName = null;
    String database = null;
    List<String> partitionkeySet = new ArrayList<String>();
    List<String> propertykeySet = new ArrayList<String>();

    SelectPartitionpropertyObjectsCommand() {
        this.noun = Noun.OBJECTS;
        this.verb = Verb.READ;
        info = new CommandInfo();
    }

    public String getOwltableName(){
        return this.owltableName;
    }

    public String getParentDatabaseName(){
        return this.database;
    }

    @Override
    public void setParentDatabase(String databaseName){
        this.database = OwlUtil.toLowerCase( databaseName );
    }


    @Override
    public void addPartitionKeyValue(String keyName, String value) throws OwlException {
        info.addPartitionKeyValue(keyName, value);
    }

    @Override
    public void addPropertyKeyValue(String keyName, String value) throws OwlException {
        info.addPropertyKeyValue(keyName, value);
    }

    @Override
    public void addPartitionFilter(String keyName, String operator, String value) throws OwlException {
        info.addPartitionFilter(keyName, operator, value);
    }

    @Override
    public void addPropertyFilter(String keyName, String operator, String value) throws OwlException {
        info.addPropertyFilter(keyName, operator, value);
    }

    @Override
    public void addAdditionalActionInfo(String additionalAction) throws OwlException {
        this.owltableName = OwlUtil.toLowerCase( additionalAction );
    }

    @Override
    public void inPropertyKeySet(String partitionName) throws OwlException {
        propertykeySet.add( OwlUtil.toLowerCase( partitionName ) );
    }

    @Override
    public CommandInfo getCommandInfo(){
        return info;
    }

    @Override
    public List<? extends OwlObject> execute(OwlBackend backend) throws OwlException{

        OwlTableEntity otable = getBackendOwlTable(backend, database, owltableName);
        List<OwlPartitionProperty> retval = new ArrayList<OwlPartitionProperty>();
        List<GlobalKeyEntity> globalKeys = backend.find(GlobalKeyEntity.class, null);

        String filter = generateFilterForKeys(backend, otable,
                info.ptnKeyFilters, info.propKeyFilters, globalKeys,
                false, false);  

        List<PartitionEntity> partitions = backend.find(PartitionEntity.class,filter);
        // for each PartitionEntity, we return property properties for it
        for(PartitionEntity part : partitions){
            OwlPartitionProperty opp = ConvertEntity.convert(part, otable, globalKeys);
            retval.add(opp);
        }

        return retval;
    }        
}
