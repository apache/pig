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

import java.io.Serializable;

public class OperatorKey implements Serializable, Comparable<OperatorKey> {
    private static final long serialVersionUID = 1L;
    
    public String scope;
    public long id;

    public OperatorKey() {
        this("", -1);
    }
    
    public OperatorKey(String scope, long id) {
        this.scope = scope;
        this.id = id;
    }
    
    @Override
    public String toString() {
        return scope + "-" + id;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof OperatorKey)) {
            return false;
        }
        else {
            OperatorKey otherKey = (OperatorKey) obj;
            return this.scope.equals(otherKey.scope) &&
                   this.id == otherKey.id;
        }
    }
    
    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
    
    public String getScope() {
        return scope;
    }
    
    public long getId() {
        return id;
    }

    public int compareTo(OperatorKey o) {
        int scCmp = scope.compareTo(o.scope);
        if(scCmp!=0)
            return scCmp;
        else{
            if(id>o.id)
                return 1;
            else if(id==o.id)
                return 0;
            else
                return -1;
        }
    }

    /**
     * Utility function for creating operator keys.
     * @param scope Scope to use in creating the key.
     * @return new operator key.
     */
    public static OperatorKey genOpKey(String scope) {
        return new OperatorKey(scope,
            NodeIdGenerator.getGenerator().getNextNodeId(scope));
    }


}
