
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

import org.apache.hadoop.owl.backend.BackendUtil;
import org.apache.hadoop.owl.backend.OwlBackend;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.common.OwlUtil.Verb;
import org.apache.hadoop.owl.entity.DataElementEntity;
import org.apache.hadoop.owl.entity.GlobalKeyEntity;
import org.apache.hadoop.owl.entity.KeyBaseEntity;
import org.apache.hadoop.owl.entity.OwlTableEntity;
import org.apache.hadoop.owl.entity.PartitionEntity;
import org.apache.hadoop.owl.entity.PartitionKeyEntity;
import org.apache.hadoop.owl.protocol.OwlObject;
import org.apache.hadoop.owl.protocol.OwlPartition;

class SelectPartitionObjectsCommand extends Command {

    ExpressionTree filter;
    CommandInfo info = null;
    String owltableName = null;
    String database = null;
    String partitionKey = null;

    SelectPartitionObjectsCommand() {
        this.noun = Noun.OBJECTS;
        this.verb = Verb.READ;
    }

    public String getOwltableName(){
        return owltableName;
    }

    public String getParentDatabaseName(){
        return database;
    }

    @Override
    public void setParentDatabase(String databaseName){
        database = OwlUtil.toLowerCase( databaseName );
    }

    @Override
    public void addAdditionalActionInfo(String additionalAction) throws OwlException {
        owltableName = OwlUtil.toLowerCase( additionalAction );
    }

    @Override
    public void setFilter(ExpressionTree filter) throws OwlException {
        this.filter = filter;
    }

    @Override
    public void setPartitionLevel(String partitionKey) throws OwlException {
        this.partitionKey = partitionKey.toLowerCase();
    }

    @Override
    public List<? extends OwlObject> execute(OwlBackend backend) throws OwlException{

        OwlTableEntity otable = getBackendOwlTable(backend, database, owltableName);

        List<OwlPartition> retval = new ArrayList<OwlPartition>();
        List<GlobalKeyEntity> globalKeys = backend.find(GlobalKeyEntity.class, null);

        if( otable.getPartitionKeys().size() == 0 ) {
            return selectNonPartitionedTable(backend, otable, globalKeys);
        }

        StringBuffer filterString = new StringBuffer("owlTableId = " + otable.getId());
        if( filter != null ) {
            filterString.append(" and (" + filter.getFilterString(backend, otable, globalKeys) + ")");  
        }

        if( partitionKey != null ) {
            KeyBaseEntity key = BackendUtil.getKeyForName(partitionKey, otable, globalKeys);

            //Only partition key names should be given for level
            if(! (key instanceof PartitionKeyEntity) ) {
                throw new OwlException(ErrorType.ERROR_UNKNOWN_PARTITION_KEY, partitionKey);
            }

            filterString.append(" and partitionLevel = " + ((PartitionKeyEntity) key).getPartitionLevel());
        } else {
            filterString.append(" and isLeaf = 'Y'");
        }

        List<PartitionEntity> partitions = backend.find(PartitionEntity.class, filterString.toString());

        // for each PartitionEntity, return partition bean for it
        for(PartitionEntity part : partitions){
            List<OwlPartition> op = ConvertEntity.convertPartition(backend, part, otable, globalKeys);
            retval.addAll(op);
        }

        return retval;
    }

    private List<? extends OwlObject> selectNonPartitionedTable(
            OwlBackend backend, OwlTableEntity otable,
            List<GlobalKeyEntity> globalKeys) throws OwlException {
        List<DataElementEntity> deList = backend.find(
                DataElementEntity.class, "owlTableId = " + otable.getId());

        List<OwlPartition> retval = new ArrayList<OwlPartition>();

        // for each DataElementEntity, return partition bean for it
        for(DataElementEntity de : deList){
            OwlPartition op = ConvertEntity.convertPartition(backend, de, otable, globalKeys);
            retval.add(op);
        }

        return retval;
    }
}
