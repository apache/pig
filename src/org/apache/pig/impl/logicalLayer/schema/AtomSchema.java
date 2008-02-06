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

import java.util.ArrayList;
import java.util.List;

/**
 * A SchemaField encapsulates a column alias
 * and its numeric column value.
 * @author dnm
 *
 */
public class AtomSchema extends Schema {
    private static final long serialVersionUID = 1L;
    
    public AtomSchema(String alias) {
        getAliases().add(alias);
    }
    
    @Override
    public int colFor(String alias) {
        return -1;
    }
    
    @Override
    public Schema schemaFor(int col) {
        return null;
    }
    
    @Override
    public AtomSchema copy(){
        return (AtomSchema)super.copy();
    }
    
    @Override
    public List<Schema> flatten(){
        List<Schema> ret = new ArrayList<Schema>();
        ret.add(this);
        return ret;
    }
    
    @Override
    public String toString(){
        return getAlias();
    }
}
