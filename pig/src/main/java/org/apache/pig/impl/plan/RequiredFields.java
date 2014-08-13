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
import java.util.ArrayList;
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
    
    /*
     * Required map keys information
     */
    private List<MapKeysInfo> mMapKeysInfoList;

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
    private RequiredFields(List<Pair<Integer, Integer>> fields,
            boolean needAllFields,
            boolean needNoFields) {
        mFields = fields;
        if(needAllFields && needNoFields) {
            //both cannot be true
            //set both of them to false
            mNeedAllFields = false;
            mNeedNoFields = false;
        } else {
            mNeedAllFields = needAllFields;
            mNeedNoFields = needNoFields;
        }
        if (mFields!=null)
        {
            mMapKeysInfoList = new ArrayList<MapKeysInfo>();
            for (int i=0;i<mFields.size();i++)
                mMapKeysInfoList.add(null);
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
     * @param i
     *         the index of the required field
     * @return the ith required input column
     */
    public Pair<Integer, Integer> getField(int i) {
        return mFields.get(i);
    }
    
    /**
     * 
     * @return the size of required input columns
     */
    public int size() {
        if (mFields==null)
            return 0;
        return mFields.size();
    }

    /**
     * 
     * @param fields
     *            the list of input columns that are required
     */
    public void setFields(List<Pair<Integer, Integer>> fields) {
        mFields = fields;
        if (mFields!=null)
        {
            mMapKeysInfoList = new ArrayList<MapKeysInfo>();
            for (int i=0;i<mFields.size();i++)
                mMapKeysInfoList.add(null);
        }
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

    /**
     * Merge with RequiredFields r2. Merge both required fields and required map keys 
     * @param r2
     *            Required fields to merge
     */
    public void merge(RequiredFields r2) {
        if (r2 == null)
            return;
        if (getNeedAllFields())
            return;
        if (r2.getNeedAllFields()) {
            mNeedAllFields = true;
            return;
        }
        if (getNeedNoFields() && !r2.getNeedNoFields()) {
            mNeedNoFields = false;
        }
        if (r2.getFields() == null)
            return;
        for (int i=0;i<r2.size();i++) {
            Pair<Integer, Integer> f = r2.getField(i);
            MapKeysInfo m = r2.getMapKeysInfo(i);
            if (mFields == null)
            {
                mFields = new ArrayList<Pair<Integer, Integer>>();
                mMapKeysInfoList = new ArrayList<MapKeysInfo>();
            }
            if (!mFields.contains(f)) {
                mFields.add(f);
                mMapKeysInfoList.add(m);
                mNeedNoFields = false;
            }
            else
            {
                if (m!=null)
                {
                    int index = mFields.indexOf(new Pair<Integer, Integer>(f.first, f.second));
                    MapKeysInfo mapKeys = mMapKeysInfoList.get(index);
                    if (mapKeys == null)
                        mapKeys = new MapKeysInfo();
                    if (m.needAllKeys)
                    {
                        mapKeys.needAllKeys = true;
                        mapKeys.keys = null;
                    }
                    else if (mapKeys.keys!=null)
                    {
                        if (m.keys!=null)
                            mapKeys.keys.addAll(m.keys);
                    }
                    else
                    {
                        mapKeys.keys = m.keys;
                    }
                    mMapKeysInfoList.set(index, mapKeys);
                }
            }
        }
        return;
    }
    
    /**
     * Set the index of all fields to i  
     * @param i
     *            New index to set
     */
    public void reIndex(int i)
    {
        if (mFields!=null)
        {
            for (Pair<Integer, Integer> p:mFields)
            {
                p.first = i;
            }
        }
    }
    
    /**
     * Merge another map key into existing required map keys list
     * @param input
     *            The input of the field to merge
     * @param column
     *            The column of the field to merge
     * @param key
     *            The key to merge
     */
    public void mergeMapKey(int input, int column, String key)
    {
        int index = mFields.indexOf(new Pair<Integer, Integer>(input, column));
        if (index==-1)
            return;
        if (mMapKeysInfoList.get(index)==null)
            mMapKeysInfoList.set(index, new MapKeysInfo());
        MapKeysInfo keysInfo = mMapKeysInfoList.get(index);
        if (keysInfo.needAllKeys)
            return;
        if (!keysInfo.keys.contains(key))
            keysInfo.keys.add(key);
    }
    
    /**
     * Merge a MapKeysInfo structure to existing required map keys list
     * @param input
     *            The input of the field to merge
     * @param column
     *            The column of the field to merge
     * @param mapKeysInfo
     *            The MapKeysInfo structure to merge
     */
    public void mergeMapKeysInfo(int input, int column, MapKeysInfo mapKeysInfo)
    {
        if (mapKeysInfo == null)
            return;
        int index = mFields.indexOf(new Pair<Integer, Integer>(input, column));
        if (index==-1)
            return;
        if (mMapKeysInfoList.get(index)==null)
        {
            mMapKeysInfoList.set(index, mapKeysInfo);
            return;
        }
        if (mapKeysInfo.needAllKeys)
        {
            mMapKeysInfoList.get(index).needAllKeys = true;
            mMapKeysInfoList.get(index).keys = null;
        }
        if (mapKeysInfo.keys!=null)
        {
            for (String key : mapKeysInfo.keys)
                mergeMapKey(input, column, key);
        }
    }
    
    /**
     * Set a MapKeysInfo structure to the required map keys list
     * @param input
     *            The input of the field to set
     * @param column
     *            The column of the field to set
     * @param mapKeysInfo
     *            The MapKeysInfo structure to set
     */
    public void setMapKeysInfo(int input, int column, MapKeysInfo mapKeysInfo)
    {
        int index = mFields.indexOf(new Pair<Integer, Integer>(input, column));
        if (index==-1)
            return;
        mMapKeysInfoList.set(index, mapKeysInfo);
    }
    
    /**
     * Get the ith MapKeysInfo structure 
     * @param i
     *            The index of the MapKeysInfo, the index of MapKeysInfo is synchronized with mFields
     */
    public MapKeysInfo getMapKeysInfo(int i)
    {
        return mMapKeysInfoList.get(i);
    }

    /**
     * Set the ith MapKeysInfo structure 
     * @param i
     *            The index of the MapKeysInfo, the index of MapKeysInfo is synchronized with mFields
     * @param mapKeysInfo
     *            The MapKeysInfo to set
     */
    public void setMapKeysInfo(int i, MapKeysInfo mapKeysInfo)
    {
        mMapKeysInfoList.set(i, mapKeysInfo);
    }
}