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

package org.apache.hadoop.owl.backend;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.entity.GlobalKeyEntity;
import org.apache.hadoop.owl.entity.KeyValueEntity;
import org.apache.hadoop.owl.entity.OwlEntity;
import org.apache.hadoop.owl.entity.OwlTableEntity;
import org.apache.hadoop.owl.entity.PartitionEntity;
import org.apache.hadoop.owl.entity.OwlResourceEntity;
import org.apache.hadoop.owl.orm.OwlEntityManager;

/**
 * This class implements the services exposed for Partition resource.
 */
public class PartitionBackend extends OwlGenericBackend<PartitionEntity> {

    /** Whether partition deletes should be done recursively. Set to false during OwlTable delete, since the
     *  table deletion deletes all partitions, so each partition delete need not be recursive in itself. */
    private boolean recursiveDelete = true;

    /**
     * Instantiates a new partition backend.
     * 
     * @param sessionBackend the backend to use for session context
     */
    public PartitionBackend(OwlGenericBackend<? extends OwlResourceEntity> sessionBackend) {
        super(OwlEntityManager.createEntityManager(PartitionEntity.class, sessionBackend.getEntityManager()));
    }

    /**
     * Instantiates a new partition backend.
     * 
     * @param sessionBackend the backend to use for session context
     * @param recursiveDelete Whether deletes should be done recursively or not
     */
    public PartitionBackend(
            OwlGenericBackend<? extends OwlResourceEntity> sessionBackend,
            boolean recursiveDelete) {
        super(OwlEntityManager.createEntityManager(PartitionEntity.class, sessionBackend.getEntityManager()));
        this.recursiveDelete = recursiveDelete;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlBackend#validateCreate(org.apache.hadoop.owl.entity.OwlResourceEntity)
     */
    @Override
    protected void validateCreate(OwlResourceEntity entity) throws OwlException {
        validateListValues(entity);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlBackend#validateUpdate(org.apache.hadoop.owl.entity.OwlResourceEntity)
     */
    @Override
    protected void validateUpdate(OwlResourceEntity entity) throws OwlException {
        validateListValues(entity);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlGenericBackend#createResource(org.apache.hadoop.owl.entity.OwlResourceEntity)
     */
    @Override
    protected void createResource(OwlResourceEntity entity) throws OwlException {
        copyParentKeys(entity);
        updateLevelInfo(entity);
    }


    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlGenericBackend#updateResource(org.apache.hadoop.owl.entity.OwlResourceEntity)
     */
    @Override
    protected void updateResource(OwlResourceEntity entity) throws OwlException {
        //The logical layer sends the updated keyValue list, with the parent partitions keyValues already present
        //So copyParentKeys is not required for update 
    }

    /**
     * Copy the key values from the parent partition to the current partition, required for partition pruning queries
     * @param entity the partition entity being updated/created
     * @throws OwlException the owl exception
     */
    private void copyParentKeys(OwlResourceEntity entity) throws OwlException {
        PartitionEntity partition = (PartitionEntity) entity;

        if( partition.getParentPartitionId() == null || partition.getParentPartitionId().intValue() <= 0) {
            //Parent partition will be null for first level partition
            return;
        }

        //Get parent partition and its key values
        OwlEntityManager<PartitionEntity> em = OwlEntityManager.createEntityManager(PartitionEntity.class, entityManager);
        PartitionEntity parentPartition = em.fetchById(partition.getParentPartitionId().intValue());

        if( parentPartition.getKeyValues() != null ) {
            //Add parent partitions partition key values to current partition
            List<KeyValueEntity> newValues = new ArrayList<KeyValueEntity>();

            if( partition.getKeyValues() != null ) {
                for(KeyValueEntity kv : partition.getKeyValues()) {
                    KeyValueEntity keyValue = new KeyValueEntity(kv);
                    keyValue.setPartition(partition); // required to make this key value point to the current partition 
                    newValues.add(keyValue);
                }
            }

            for(KeyValueEntity kv : parentPartition.getKeyValues()) {
                if( kv.getPartitionKeyId() != null ) {
                    KeyValueEntity newKeyValue = new KeyValueEntity(kv);
                    newKeyValue.setPartition(partition); // required to make this key value point to the current partition 
                    newValues.add(newKeyValue);
                }
            }

            partition.setKeyValues(newValues);
        }
    }


    /**
     * Update the partition level and isLeaf information.
     * 
     * @param entity
     *            the entity
     * 
     * @throws OwlException
     *             the owl exception
     */
    private void updateLevelInfo(OwlResourceEntity entity) throws OwlException {
        PartitionEntity partition = (PartitionEntity) entity;

        if( partition.getParentPartitionId() == null ) {
            partition.setPartitionLevel(1);
        } else {
            //Get parent partition and its key values
            OwlEntityManager<PartitionEntity> em = OwlEntityManager.createEntityManager(PartitionEntity.class, entityManager);
            PartitionEntity parentPartition = em.fetchById(partition.getParentPartitionId().intValue());

            //Set partition level to previous partitions level plus one
            partition.setPartitionLevel(parentPartition.getPartitionLevel() + 1);
        }

        //Fetch the table for this partition
        OwlEntityManager<OwlTableEntity> tableEM = OwlEntityManager.createEntityManager(OwlTableEntity.class, getEntityManager());
        OwlTableEntity table = tableEM.fetchById(partition.getOwlTableId());

        if( partition.getPartitionLevel() == table.getPartitionKeys().size() ) {
            //Last partition level, set isLeaf to true
            partition.setIsLeaf(OwlEntity.isTrue(true));
        } else {
            partition.setIsLeaf(OwlEntity.isTrue(false));
        }
    }


    /**
     * Validate whether key values specified satisfy the bounded list.
     * 
     * @param entity the entity
     * @throws OwlException 
     */
    private void validateListValues(OwlResourceEntity entity) throws OwlException {
        PartitionEntity partition = (PartitionEntity) entity;

        //Fetch the table for this partition
        OwlEntityManager<OwlTableEntity> tableEM = OwlEntityManager.createEntityManager(OwlTableEntity.class, getEntityManager());
        OwlTableEntity table = tableEM.fetchById(partition.getOwlTableId());

        if( partition.getKeyValues() != null ) {
            GlobalKeyBackend gkBackend = new GlobalKeyBackend(this);
            List<GlobalKeyEntity> globalKeys = gkBackend.find(null);

            //Check each key value if it satisfies the bounds 
            for(KeyValueEntity kv : partition.getKeyValues()) {
                BackendUtil.validateKeyValue(kv, table, globalKeys);
            }
        }
    }


    /* (non-Javadoc)
     * @see org.apache.hadoop.owl.backend.OwlBackend#deleteResource(org.apache.hadoop.owl.entity.OwlResourceEntity)
     */
    @Override
    protected void deleteResource(OwlResourceEntity entity) throws OwlException {
        PartitionEntity partition = (PartitionEntity) entity;

        if( recursiveDelete ) {
            //Delete all child partitions
            delete("owlTableId = " + partition.getOwlTableId() + " and parentPartitionId = " + partition.getId());
        }

        //Delete all data elements within this partition
        DataElementBackend deBackend = new DataElementBackend(this);
        deBackend.delete("partitionId = " + partition.getId());
    }
}

