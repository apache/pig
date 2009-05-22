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

import org.apache.pig.impl.util.Pair;

/**
 * A struct detailing how a projection is altered by an operator.
 */
public class RequiredFields {
    /**
     * Quick way for an operator to note that all columns from an input are required.
     */
    private boolean mNeedAllFields = false;
    
    /**
     * Quick way for an operator to note that no columns from an input are required.
     */
    private boolean mNeedNoFields = false;


    /**
     * List of fields required from the input. This includes fields that are
     * transformed, and thus are no longer the same fields. Using the example 'B
     * = foreach A generate $0, $2, $3, udf($1)' would produce the list (0, 0),
     * (0, 2), (0, 3), (0, 1). Note that the order is not guaranteed.
     */
    private List<Pair<Integer, Integer>> mFields;

    /**
     * 
     * @param needAllFields
     *            to indicate if this required fields needs all the fields from
     *            its input
     */
    public RequiredFields(boolean needAllFields) {
        this(null, needAllFields, false);
    }
    
    /**
     * 
     * @param needAllFields
     *            to indicate if this required fields needs no fields from
     *            its input
     */
    public RequiredFields(boolean needAllFields, boolean needNoFields) {
        this(null, needAllFields, needNoFields);
    }

    /**
     * 
     * @param fields
     *            the list of input columns that are required
     */
    public RequiredFields(List<Pair<Integer, Integer>> fields) {
        this(fields, false, false);
    }

    /**
     * 
     * @param fields
     *            the list of input columns that are required
     * @param needAllFields
     *            to indicate if this required fields needs all the fields from
     *            its input; cannot be true if needNoFields is true
     * @param needNoFields
     *            to indicate if this required fields needs no fields from
     *            its input; cannot be true if needAllFields is true
     */
    private RequiredFields(List<Pair<Integer, Integer>> removedFields,
            boolean needAllFields,
            boolean needNoFields) {
        mFields = removedFields;
        if(needAllFields && needNoFields) {
            //both cannot be true
            //set both of them to false
            mNeedAllFields = false;
            mNeedNoFields = false;
        } else {
            mNeedAllFields = needAllFields;
            mNeedNoFields = needNoFields;
        }
    }

    /**
     * 
     * @return the list of input columns that are required
     */
    public List<Pair<Integer, Integer>> getFields() {
        return mFields;
    }

    /**
     * 
     * @param fields
     *            the list of input columns that are required
     */
    public void setFields(List<Pair<Integer, Integer>> fields) {
        mFields = fields;
    }

    /**
     * 
     * @return if this required fields needs all the fields from its input(s)
     */
    public boolean needAllFields() {
        return getNeedAllFields();
    }

    /**
     * 
     * @return if this required fields needs all the fields from its input(s)
     */
    public boolean getNeedAllFields() {
        return mNeedAllFields;
    }

    /**
     * 
     * @param needAllFields
     *            to indicate if this required fields needs all the fields from
     *            its input; cannot be true if needNoFields() is true
     */
    public void setNeedAllFields(boolean needAllFields) {
        if(needAllFields && needNoFields()) return;
        mNeedAllFields = needAllFields;
    }

    /**
     * 
     * @return if this required fields needs no fields from its input(s)
     */
    public boolean needNoFields() {
        return getNeedNoFields();
    }

    /**
     * 
     * @return if this required fields needs no fields from its input(s)
     */
    public boolean getNeedNoFields() {
        return mNeedNoFields;
    }

    /**
     * 
     * @param needNoFields
     *            to indicate if this required fields needs no fields from
     *            its input; cannot be true if needAllFields() is true
     */
    public void setNeedNoFields(boolean needNoFields) {
        if(needNoFields && needAllFields()) return;
        mNeedNoFields = needNoFields;
    }
    


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("needAllFields: " + mNeedAllFields);
        sb.append(" needNoFields: " + mNeedNoFields);
        sb.append(" fields: " + mFields);
        return sb.toString();
    }
}