/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.owl.orm;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.owl.common.LogHandler;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.entity.OwlEntity;
import org.apache.hadoop.owl.entity.OwlResourceEntity;

/**
 * This class implements prefetching of data to improve performance. For example, when DataUnitEntities are
 * fetched, the getFacets call on every DataUnitEntity will cause a DB lookup. This class does a single
 * query to get all DataUnitFacetEntities for every dataunit in the list.
 */
public class PrefetchData<T extends OwlEntity> {

    /** The threshold below which prefetching is disabled. */
    private static final int PREFETCH_THRESHOLD = 3;

    /** The maximum length for the filter generated, to avoid JPQL and database query limits */ 
    private static final int QUERY_SIZE_LIMIT = 5000;

    /** The maximum number of entries in the IN LIST, to avoid JPQL and database query limits */ 
    private static final int MAX_IN_LIST_COUNT = 1000;

    /** The entity class to be prefetched. */
    Class<? extends OwlEntity> entityClass;

    /** The entity manager for the entities to fetch. */
    OwlEntityManager<T> entityManager;

    /**
     * Instantiates a new prefetcher instance.
     * 
     * @param entityClass
     *            the entity class
     * @param entityManager
     *            the entity manager
     */
    @SuppressWarnings("unchecked")
    public PrefetchData(
            Class<? extends OwlEntity> entityClass, 
            OwlEntityManager<? extends OwlEntity> entityManager) {
        this.entityClass = entityClass;
        this.entityManager = (OwlEntityManager<T>) OwlEntityManager.createEntityManager(entityClass, entityManager);
    }

    /**
     * Do the prefetching of data. This returns a list of prefetched entities,
     * the caller has to populate the resources with the prefetched values.
     * 
     * @param resourceList
     *            the list of resources which need to be processed
     * @param resourceFieldName
     *            the field name of the resource within the prefetched entity
     * 
     * @return the list of prefetched entities, ordered as in the resource list
     * 
     * @throws OwlException
     *             the meta data exception
     */
    public List<List<T>> fetchData(
            List<? extends OwlResourceEntity> resourceList,
            String resourceFieldName) throws OwlException {
        //Don't do prefetch if resource count is very less
        if( resourceList.size() < PREFETCH_THRESHOLD ) {
            return null;
        }

        List<T> fetchedEntities = new ArrayList<T>();
        int nextIndex = 0;

        //Loop to handle a very large IN list, in which case it will be split into multiple queries
        while(nextIndex < resourceList.size()) {

            //Build a filter and run the DB query to get the prefetched entities
            StringBuffer buffer = new StringBuffer();
            nextIndex = buildPrefetchFilter(
                    resourceList,
                    resourceFieldName,
                    buffer,
                    nextIndex);

            List<T> results = entityManager.fetchByFilter(buffer.toString());
            fetchedEntities.addAll(results);
        }

        //Create a map distributing the prefetched values as per their parent id
        Map<Integer, List<T>> map = buildMap(fetchedEntities);

        //Convert the map to a list in the same order as the input resource list
        List<List<T>> resultList = buildList(map, resourceList);
        return resultList;
    }

    /**
     * Builds the filter for doing the database query to prefetch the data. If the resourceList is
     * too big, generates a partial filter such that the filter does not become too big.
     * 
     * @param resourceList
     *            the list of resources
     * @param resourceFieldName
     *            the field name of the resource within the prefetched entity
     * @param buffer
     *            the buffer into which the generated filter should be written 
     * @param startIndex
     *            the index within resourceList from which to process
     * 
     * @return the index from which the next iteration should start
     */
    private int buildPrefetchFilter(
            List<? extends OwlResourceEntity> resourceList,
            String resourceFieldName,
            StringBuffer buffer,
            int startIndex) {

        buffer.append(resourceFieldName + ".id IN ");
        buffer.append("("); //the beginning bracket for IN list

        int size = resourceList.size();
        int currIndex;

        for(currIndex = startIndex;currIndex < size;currIndex++) {
            buffer.append(resourceList.get(currIndex).getId());

            //Prevent the query from becoming too big
            if( buffer.length() > QUERY_SIZE_LIMIT || 
                    (currIndex - startIndex) > MAX_IN_LIST_COUNT) {
                currIndex++;  //current resource is already added to the filter
                break;
            }
            if( currIndex != (size - 1) ) {
                buffer.append(",");
            }
        }

        buffer.append(")"); //the trailing bracket for the IN list
        return currIndex;
    }

    /**
     * Builds a map between the parent resource id to a list of prefetched
     * entities for that resource.
     * 
     * @param fetchedEntities
     *            the entities fetched by prefetching
     * 
     * @return the map of resource id to list of prefetched entities
     */
    private Map<Integer, List<T>> buildMap(List<T> fetchedEntities) {

        Map<Integer, List<T>> map = new HashMap<Integer, List<T>>();

        for(T row : fetchedEntities) {
            //Build a map on every prefetched entities parent resource id 
            Integer parentId = Integer.valueOf(row.parentResource().getId());

            List<T> data = map.get(parentId);

            if( data == null ) {
                //First entry for this resource, create a new list
                data = new ArrayList<T>();
                data.add(row);
                map.put(parentId, data);
            } else {
                //Add to existing list for this resource
                data.add(row);
            }
        }

        return map;
    }

    /**
     * Builds the list of prefetched entities, ordering it the same as the input resource list.
     * 
     * @param map
     *            the map of resource id to list of prefetched entities
     * @param resourceList
     *            the list of resources
     * 
     * @return the list of prefetched entities ordered as in the input resource list
     */
    private List<List<T>> buildList(
            Map<Integer, List<T>> map,
            List<? extends OwlResourceEntity> resourceList) {
        List<List<T>> resultList = new ArrayList<List<T>>();

        for(OwlResourceEntity resource : resourceList) {
            List<T> data = map.get(Integer.valueOf(resource.getId()));

            if( data == null ) {
                //Data units without any facets. This means something was wrong with
                //the loading of data, not erroring out here currently
                LogHandler.getLogger("server").warn("No " + entityClass.getSimpleName() +
                        " entities prefetched for resource " + resource.getId());
                resultList.add(new ArrayList<T>());

            } else {
                resultList.add(data);
            }
        }
        return resultList;
    }
}
