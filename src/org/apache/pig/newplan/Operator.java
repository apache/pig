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

package org.apache.pig.newplan;

import java.util.HashMap;
import java.util.Map;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.parser.SourceLocation;

public abstract class Operator {
    protected SourceLocation location; // The location of the operator in the original pig script.
    
    protected String name;
    protected OperatorPlan plan; // plan that contains this operator
    protected Map<String, Object> annotations;
    protected final int hashPrime = 31;

    public Operator(String n, OperatorPlan p) {
        name = n;
        plan = p;
        annotations = new HashMap<String, Object>();
        location = new SourceLocation();
    }

    /**
     * Accept a visitor at this node in the graph.
     * @param v Visitor to accept.
     * @throws FrontendException 
     */
    public abstract void accept(PlanVisitor v) throws FrontendException;

    public String getName() {
        return name;
    }
    
    public SourceLocation getLocation() {
        return location;
    }
    
    public void setLocation(SourceLocation loc) {
        location = loc;
    }
    
    /**
     * Get the plan associated with this operator.
     * @return plan
     */
    public OperatorPlan getPlan() {
        return plan;
    }
    
    /**
     * Add an annotation to a node in the plan.
     * @param key string name of this annotation
     * @param val value, as an Object
     */
    public void annotate(String key, Object val) {
        annotations.put(key, val);
    }
    
    /**
     * Look to see if a node is annotated.
     * @param key string name of annotation to look for
     * @return value of the annotation, as an Object, or null if the key is
     * not present in the map.
     */
    public Object getAnnotation(String key) {
        return annotations.get(key);
    }

    /**
     * Remove an annotation
     * @param key the key of the annotation
     * @return the original value of the annotation
     */
    public Object removeAnnotation(String key) {
        return annotations.remove(key);
    }
    
    public void setPlan(OperatorPlan p) {
        plan = p;
    }
    
    /**
     * This is like a shallow equals comparison.
     * It returns true if two operators have equivalent properties even if they are 
     * different objects. Here properties mean equivalent plan and equivalent name.
     * @param operator
     * @return true if two object have equivalent properties, else false
     */
    public abstract boolean isEqual(Operator operator) throws FrontendException;
}
