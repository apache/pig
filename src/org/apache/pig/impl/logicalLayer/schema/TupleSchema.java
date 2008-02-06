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
package org.apache.pig.impl.logicalLayer.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * A SchemaItemList encapuslates a group of schema items which may be SchemaFields
 * (data atoms) or complex items, such as bags or tuples. A SchemaItemList will 
 * recursively search itself for the proper column number for a requested alias, matching
 * against its own alias with priority.
 * @author dnm
 *
 */
public class TupleSchema extends Schema implements Serializable {
    
    private static final long serialVersionUID = 1L;
    private static final String QUALIFIER = "::";
    
    public List<Schema>        fields  = new ArrayList<Schema>();
    public Map<String, Integer> mapping = new HashMap<String, Integer>();
    private boolean                isBag   = false;
    
    
                @Override
    public int colFor(String alias) {        
        if (mapping.containsKey(alias)) {            
            return mapping.get(alias);
        }
        else if(alias.matches(".*::.*")) {
            String[] temp = alias.split("::");
            if(mapping.containsKey(temp[temp.length-1]))
                return mapping.get(temp[temp.length-1]);
        }
        return -1;
    }

    @Override
    public Schema schemaFor(int col) {
        if (col < fields.size()) {
            return fields.get(col);
        }
        return null;
    }

    
    public void add(Schema sc){
        add(sc,false);
    }
    
    
    public void add(Schema sc, boolean ignoreConflicts) {
        
        int pos = fields.size();
        
        fields.add(sc);
     
        for (String alias: sc.aliases){
            if (mapping.containsKey(alias)){
                if (!ignoreConflicts)
                    throw new RuntimeException("Duplicate alias: " + sc.getAlias());
                else
                    mapping.remove(alias);
                    
            }else{
                mapping.put(alias, pos);
            }
        }
    }

    public int numFields() {
        return fields.size();
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        if (getAlias()!=null){
            buf.append(getAlias());
            buf.append(": ");
        }
        
        buf.append( "(" );
        boolean first = true;
        for (Schema item : fields) {
            if (!first) {
                buf.append(", ");
            }
            first = false;
            buf.append(item.toString());
        }
        buf.append(" )");
        return buf.toString();
    }

    @Override
    public TupleSchema copy(){
        return (TupleSchema)super.copy();
    }
    
    @Override
    public List<Schema> flatten(){
        for (Schema item: fields){
            for (String parentAlias: aliases){
                String[] childAliases = item.aliases.toArray(new String[0]);
                for (String childAlias: childAliases){
                    item.aliases.add(parentAlias+QUALIFIER+childAlias);
                }
            }
        }
        return fields;
    }
    
    public List<Schema> getFields(){
        return fields;
    }
    
    
    public boolean isBag() {
        return isBag;
    }

    public boolean isTuple() {
        return !isBag();
    }


}
