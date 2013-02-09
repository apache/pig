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

package org.apache.pig.pen;

import java.util.LinkedList;
import java.util.ArrayList;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;

import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.pen.util.LineageTracer;
import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

/**
 * Class used by physical operators to generate example tuples for the ILLUSTRATE
 * purpose
 */

public class Illustrator {

    private LineageTracer lineage;
    private LinkedList<IdentityHashSet<Tuple>> equivalenceClasses;
    // all input tuples for an expression
    private IdentityHashSet<Tuple> inputs = null;
    
    private DataBag data;
    private int maxRecords = -1;
    private int recCounter = 0;
    private IllustratorAttacher attacher;
    private ArrayList<Boolean[]> subExpResults;
    private Boolean[] subExpResult;
    private boolean eqClassesShared;
    private long oriLimit = -1;
    private LogicalSchema schema;

    public Illustrator(LineageTracer lineage, LinkedList<IdentityHashSet<Tuple>> equivalenceClasses, IllustratorAttacher attacher, PigContext hadoopPigContext) {
        this.lineage = lineage;
        this.equivalenceClasses = equivalenceClasses;
        data = BagFactory.getInstance().newDefaultBag();
        this.attacher = attacher;
        subExpResults = new ArrayList<Boolean[]>();
        subExpResult = new Boolean[1];
        schema = null;
    }
    
    public Illustrator(LineageTracer lineage, LinkedList<IdentityHashSet<Tuple>> equivalenceClasses, int maxRecords, IllustratorAttacher attacher,
        LogicalSchema schema, PigContext hadoopPigContext) {
        this(lineage, equivalenceClasses, attacher, hadoopPigContext);
        this.maxRecords = maxRecords;
        this.schema = schema;
    }
    
    public ArrayList<Boolean[]> getSubExpResults() {
        return subExpResults;
    }
    
    Boolean[] getSubExpResult() {
        return subExpResult;
    }
    
    public LineageTracer getLineage() {
        return lineage;
    }
    
    public LinkedList<IdentityHashSet<Tuple>> getEquivalenceClasses() {
        return equivalenceClasses;
    }
    
    public void setSubExpResult(boolean result) {
        subExpResult[0] = result;
    }
    
    public void setEquivalenceClasses(LinkedList<IdentityHashSet<Tuple>> eqClasses, PhysicalOperator po) {
        equivalenceClasses = eqClasses;
        attacher.poToEqclassesMap.put(po, eqClasses);
    }
    
    public boolean ceilingCheck() {
        if (maxRecords != -1 && ++recCounter > maxRecords)
            return false;
        else
          return true;
    }
    
    public IdentityHashSet<Tuple> getInputs() {
        return inputs;      
    }
    
    public void addInputs(IdentityHashSet<Tuple> inputs) {
        if (this.inputs == null)
          this.inputs = new IdentityHashSet<Tuple>();
        this.inputs.addAll(inputs);
    }
    
    public void addData(Tuple t) {
      data.add(t);
    }
    
    public DataBag getData() {
      return data;
    }
    
    public long getOriginalLimit() {
        return oriLimit;
    }
    
    public void setOriginalLimit(long oriLimit) {
        this.oriLimit = oriLimit;
    }
    
    public void setEqClassesShared() {
        eqClassesShared = true;
    }
    
    public boolean getEqClassesShared() {
        return eqClassesShared;
    }
    
    public LogicalSchema getSchema() {
        return schema;
    }
}
