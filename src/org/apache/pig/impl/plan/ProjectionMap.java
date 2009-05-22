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

package org.apache.pig.impl.plan;

import java.lang.StringBuilder;
import java.util.List;

import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.Pair;

/**
 * A struct detailing how a projection is altered by an operator.
 */
public class ProjectionMap {
    /**
     * Quick way for an operator to note that its input and output are the same.
     */
    private boolean mChanges = true;

    /**
     * Map of field changes, with keys being the output fields of the operator
     * and values being the input fields. Fields are numbered from 0. So for a
     * foreach operator derived from 'B = foreach A generate $0, $2, $3,
     * udf($1)' would produce a mapping of 0->(0, 0), 2->(0, 1), 3->(0, 2)
     */
    private MultiMap<Integer, Pair<Integer, Integer>> mMappedFields;

    /**
     * List of fields removed from the input. This includes fields that were
     * transformed, and thus are no longer the same fields. Using the example
     * foreach given under mappedFields, this list would contain '(0,1)'.
     */
    private List<Pair<Integer, Integer>> mRemovedFields;

    /**
     * List of fields in the output of this operator that were created by this
     * operator. Using the example foreach given under mappedFields, this list
     * would contain '3'.
     */
    private List<Integer> mAddedFields;

    /**
     * 
     * @param changes
     *            to indicate if this projection map changes its input or not
     */
    public ProjectionMap(boolean changes) {
        this(null, null, null, changes);
    }

    /**
     * 
     * @param mapFields
     *            the mapping of input column to output column
     * @param removedFields
     *            the list of input columns that are removed
     * @param addedFields
     *            the list of columns that are added to the output
     */
    public ProjectionMap(MultiMap<Integer, Pair<Integer, Integer>> mapFields,
            List<Pair<Integer, Integer>> removedFields,
            List<Integer> addedFields) {
        this(mapFields, removedFields, addedFields, true);
    }

    /**
     * 
     * @param mapFields
     *            the mapping of input column to output column
     * @param removedFields
     *            the list of input columns that are removed
     * @param addedFields
     *            the list of columns that are added to the output
     * @param changes
     *            to indicate if this projection map changes its input or not
     */
    private ProjectionMap(MultiMap<Integer, Pair<Integer, Integer>> mapFields,
            List<Pair<Integer, Integer>> removedFields,
            List<Integer> addedFields, boolean changes) {
        mMappedFields = mapFields;
        mAddedFields = addedFields;
        mRemovedFields = removedFields;
        mChanges = changes;
    }

    /**
     * 
     * @return the mapping of input column to output column
     */
    public MultiMap<Integer, Pair<Integer, Integer>> getMappedFields() {
        return mMappedFields;
    }

    /**
     * 
     * @param fields
     *            the mapping of input column to output column
     */
    public void setMappedFields(MultiMap<Integer, Pair<Integer, Integer>> fields) {
        mMappedFields = fields;
    }

    /**
     * 
     * @return the list of input columns that are removed
     */
    public List<Pair<Integer, Integer>> getRemovedFields() {
        return mRemovedFields;
    }

    /**
     * 
     * @param fields
     *            the list of input columns that are removed
     */
    public void setRemovedFields(List<Pair<Integer, Integer>> fields) {
        mRemovedFields = fields;
    }

    /**
     * 
     * @return the list of columns that are added to the output
     */
    public List<Integer> getAddedFields() {
        return mAddedFields;
    }

    /**
     * 
     * @param fields
     *            the list of columns that are added to the output
     */
    public void setAddedFields(List<Integer> fields) {
        mAddedFields = fields;
    }

    /**
     * 
     * @return if this projection map changes its input or not
     */
    public boolean changes() {
        return getChanges();
    }


    /**
     * 
     * @return if this projection map changes its input or not
     */
    public boolean getChanges() {
        return mChanges;
    }

    /**
     * 
     * @param changes
     *            if this projection map changes its input or not
     */
    public void setChanges(boolean changes) {
        mChanges = changes;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("changes: " + mChanges);
        sb.append(" mapped fields: " + mMappedFields);
        sb.append(" added fields: " + mAddedFields);
        sb.append(" removed fields: " + mRemovedFields);
        return sb.toString();
    }
}