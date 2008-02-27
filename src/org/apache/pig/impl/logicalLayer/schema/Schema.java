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

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.util.ObjectSerializer;



public abstract class Schema implements Serializable {

    private final Log log = LogFactory.getLog(getClass());
    
    protected Set<String> aliases = new HashSet<String>();    

    public Schema copy(){
        try{
            return (Schema)ObjectSerializer.deserialize(ObjectSerializer.serialize(this));
        }catch (IOException e){
            log.error(e);
            throw new RuntimeException(e);
        }
    }
    public abstract int colFor(String alias);
    public abstract Schema schemaFor(int col);
    
   
    public abstract List<Schema> flatten();

    public void setAlias(String alias) {
        if (alias!=null)
            aliases.add(alias);
    }
    
    public void removeAlias(String alias){
        aliases.remove(alias);
    }
    
    public void removeAllAliases(){
        aliases.clear();
    }
    public Set<String> getAliases() {
        return aliases;
    }
    public String getAlias() {
        Iterator<String> iter = aliases.iterator();
        if (iter.hasNext())
            return iter.next();
        else
            return null;
    }
}
