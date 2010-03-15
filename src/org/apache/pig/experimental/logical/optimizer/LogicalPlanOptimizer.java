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
package org.apache.pig.experimental.logical.optimizer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.experimental.logical.rules.AddForEach;
import org.apache.pig.experimental.logical.rules.ColumnMapKeyPrune;
import org.apache.pig.experimental.logical.rules.FilterAboveForeach;
import org.apache.pig.experimental.logical.rules.MergeFilter;
import org.apache.pig.experimental.logical.rules.PushUpFilter;
import org.apache.pig.experimental.logical.rules.SplitFilter;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.optimizer.PlanOptimizer;
import org.apache.pig.experimental.plan.optimizer.Rule;

public class LogicalPlanOptimizer extends PlanOptimizer {

    public LogicalPlanOptimizer(OperatorPlan p, int iterations) {    	
        super(p, null, iterations);
        ruleSets = buildRuleSets();
        addListeners();
    }

    protected List<Set<Rule>> buildRuleSets() {
        List<Set<Rule>> ls = new ArrayList<Set<Rule>>();	    
         
        // Split Set
        // This set of rules does splitting of operators only.
        // It does not move operators
        Set<Rule> s = new HashSet<Rule>();
        ls.add(s);
        // add split filter rule
        Rule r = new SplitFilter("SplitFilter");
        s.add(r);
                
         
        
        
        // Push Set,
        // This set does moving of operators only.
        s = new HashSet<Rule>();
        ls.add(s);
        // add push up filter rule
        r = new PushUpFilter("PushUpFilter");
        s.add(r);
        r = new FilterAboveForeach("FilterAboveForEachWithFlatten");
        s.add(r);
        
        
        
        
        // Merge Set
        // This Set merges operators but does not move them.
        s = new HashSet<Rule>();
        ls.add(s);
        // add merge filter rule
        r = new MergeFilter("MergeFilter");        
        s.add(r);	    
        
        
        // Prune Set Marker
        // This set is used for pruning columns and maps
      
        s = new HashSet<Rule>();
        ls.add(s);
        // Add the PruneMap Filter
        r = new ColumnMapKeyPrune("ColumnMapKeyPrune");
        s.add(r);
        
        // Add LOForEach operator to trim off columns
        
        s = new HashSet<Rule>();
        ls.add(s);
        // Add the AddForEach
        r = new AddForEach("AddForEach");
        s.add(r);
        
        return ls;
    }
    
    private void addListeners() {
        addPlanTransformListener(new SchemaPatcher());
        addPlanTransformListener(new ProjectionPatcher());
    }
}
