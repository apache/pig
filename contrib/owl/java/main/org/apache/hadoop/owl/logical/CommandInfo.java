
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
import java.util.Map;

import org.apache.hadoop.owl.backend.IntervalUtil;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.protocol.OwlKey;
import org.apache.hadoop.owl.protocol.OwlKeyListValue;
import org.apache.hadoop.owl.protocol.OwlPartitionKey;
import org.apache.hadoop.owl.protocol.OwlPropertyKey;
import org.apache.hadoop.owl.protocol.OwlPartitionKey.IntervalFrequencyUnit;
import org.apache.hadoop.owl.protocol.OwlPartitionKey.PartitioningType;


public class CommandInfo {

    private String owlTableName;
    private String databaseName;

    protected int insertToLevel = 0;
    protected List <OwlPropertyKey> propKeys = null;
    protected List <OwlPartitionKey> ptnKeys = null;
    protected Map<String,String> propKeyValues = null;
    protected Map<String,String> ptnKeyValues = null;
    protected Map<String,FilterString> propKeyFilters = null;
    protected Map<String,FilterString> ptnKeyFilters = null;

    public enum Operator {
        EQUAL  ("="),
        GREATERTHAN  (">"),
        LESSTHAN  ("<"),
        LESSTHANOREQUALTO ("<="),
        GREATERTHANOREQUALTO (">="),
        LIKE ("LIKE"),
        IN ("IN"),
        NOTEQUALS ("<>");

        private String op;

        // private constructor
        private Operator (String inputOperator){
            op = inputOperator;
        }

        public String getOp() {
            return op;
        }

        public static Operator fromString (String inputOperator) throws OwlException{
            for(Operator op : Operator.values()) {
                if( op.getOp().equals(inputOperator) ) {
                    return op;
                }
            }

            throw new OwlException(ErrorType.ERROR_UNKNOWN_ENUM_TYPE, "Value " + inputOperator + " for " + Operator.class.getSimpleName());
        }
    }

    // Inner class Filter String that stores operator and value
    public static class FilterString{
        Operator filterOp;
        String filterValue;

        public FilterString(Operator inputOp, String inputValue){
            filterOp = inputOp;
            filterValue = inputValue;
        }

        public void setFilterOperator(Operator inputOp){
            filterOp = inputOp;
        }

        public void setFilterValue(String inputValue){
            filterValue = inputValue;
        }

        public Operator getFilterOperator(){
            return filterOp;
        }

        public String getFilterValue(){
            return this.filterValue;
        }
    }


    public CommandInfo(){
        propKeys = new ArrayList<OwlPropertyKey>();
        ptnKeys = new ArrayList<OwlPartitionKey>();
        propKeyValues = new HashMap<String,String>();
        ptnKeyValues = new HashMap<String,String>();
        propKeyFilters = new HashMap<String, FilterString>();
        ptnKeyFilters = new HashMap<String, FilterString>();
    }

    public String getOwlTableName(){
        return this.owlTableName;
    }

    public String getDatabaseName(){
        return this.databaseName;
    }

    public List<OwlPropertyKey> getPropertyKeys(){
        return propKeys;
    }

    public List<OwlPartitionKey> getPartitionKeys(){
        return ptnKeys;
    }

    public Map<String,String> getPropertyKeyValues(){
        return propKeyValues;
    }

    public Map<String,String> getPartitionKeyValues(){
        return ptnKeyValues;
    }

    public Map<String,FilterString> getPropertyFilters(){
        return propKeyFilters;
    }

    public Map<String,FilterString> getPartitionFilters(){
        return ptnKeyFilters;
    }

    public CommandInfo setOwlTableName(String owlTableName){
        this.owlTableName = OwlUtil.toLowerCase( owlTableName );
        return this;
    }

    public CommandInfo setParentDatabase(String databaseName){
        this.databaseName = OwlUtil.toLowerCase( databaseName );
        return this;
    }

    public CommandInfo addPropertyKey(String keyName, String keyType) {
        if( keyType.toUpperCase().equals("INTEGER") ) {
            keyType = OwlKey.DataType.INT.name();
        }

        propKeys.add(new OwlPropertyKey(keyName, OwlKey.DataType.valueOf(keyType.toUpperCase())));
        // add enum type-safety checking for .valueOf
        return this;
    }

    public CommandInfo addPartitionKey(String keyName, String keyType, String partitioningType,
            List<? extends Object> listValues,
            String intervalStart, Integer freq, IntervalFrequencyUnit freqUnit) throws OwlException {

        if( keyType.toUpperCase().equals("INTEGER") ) {
            keyType = OwlKey.DataType.INT.name();
        }

        insertToLevel++;

        //Check if valid list values are specified
        List<OwlKeyListValue> keyListValues = new ArrayList<OwlKeyListValue>();
        if( listValues != null ) {
            for(Object value : listValues) {
                if( value instanceof String ) {
                    keyListValues.add(new OwlKeyListValue(null, (String) value));
                } else if (value instanceof Integer ) {
                    keyListValues.add(new OwlKeyListValue((Integer) value, null));
                } else {
                    throw new OwlException(ErrorType.ERROR_INVALID_KEY_DATATYPE, value.getClass().getSimpleName());
                }
            }
        }

        OwlPartitionKey partitionKey;
        if( intervalStart == null ) {

            partitionKey = new OwlPartitionKey(
                    keyName, OwlKey.DataType.valueOf(keyType.toUpperCase()),
                    insertToLevel,
                    PartitioningType.valueOf(partitioningType.toUpperCase()),
                    keyListValues
            );
        } else {
            partitionKey = new OwlPartitionKey(
                    keyName, OwlKey.DataType.valueOf(keyType.toUpperCase()),
                    insertToLevel,
                    PartitioningType.valueOf(partitioningType.toUpperCase()),
                    IntervalUtil.parseDateString(intervalStart),
                    freq,
                    freqUnit,
                    keyListValues
            );

        }
        // add enum type-safety checking for .valueOf

        ptnKeys.add(partitionKey);

        return this;
    }

    public CommandInfo addPropertyKeyValue(String keyName, String value) {
        propKeyValues.put( OwlUtil.toLowerCase( keyName ), value );
        return this;
    }

    public CommandInfo addPartitionKeyValue(String keyName, String value) {
        ptnKeyValues.put(OwlUtil.toLowerCase( keyName ), value);
        return this;
    }

    public CommandInfo addPropertyFilter(String keyName, String operator, String value) throws OwlException {
        propKeyFilters.put(OwlUtil.toLowerCase( keyName ), new FilterString(Operator.fromString(operator.toUpperCase()), value));
        return this;
    }

    public CommandInfo addPartitionFilter(String keyName, String operator, String value) throws OwlException {
        ptnKeyFilters.put(OwlUtil.toLowerCase( keyName ), new FilterString(Operator.fromString(operator.toUpperCase()), value));
        return this;
    }
}
