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

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.Operator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.*;


//These methods are used to generate equivalence classes given the operator name and the output from the operator
//For example, it gives out 2 eq. classes for filter, one that passes the filter and one that doesn't
public class EquivalenceClasses {
    
    public static Map<LogicalRelationalOperator, Collection<IdentityHashSet<Tuple>>> getLoToEqClassMap(PhysicalPlan plan,
        LogicalPlan lp, Map<Operator, PhysicalOperator> logToPhyMap,
        Map<Operator, DataBag> logToDataMap,
        Map<LOForEach, Map<LogicalRelationalOperator, PhysicalOperator>> forEachInnerLogToPhyMap,
        final HashMap<PhysicalOperator, Collection<IdentityHashSet<Tuple>>> poToEqclassesMap)
    {
        Map<LogicalRelationalOperator, Collection<IdentityHashSet<Tuple>>> ret =
          new HashMap<LogicalRelationalOperator, Collection<IdentityHashSet<Tuple>>>();
        List<Operator> roots = lp.getSources();
        HashSet<Operator> seen = new HashSet<Operator>();
        for(Operator lo: roots) {
            getEqClasses(plan, lo, lp, logToPhyMap, ret, poToEqclassesMap, logToDataMap, forEachInnerLogToPhyMap, seen);
        }
        return ret;
    }
    
    private static void getEqClasses(PhysicalPlan plan, Operator parent, LogicalPlan lp,
        Map<Operator, PhysicalOperator> logToPhyMap, Map<LogicalRelationalOperator,
        Collection<IdentityHashSet<Tuple>>> result,
        final HashMap<PhysicalOperator, Collection<IdentityHashSet<Tuple>>> poToEqclassesMap,
        Map<Operator, DataBag> logToDataMap,
        Map<LOForEach, Map<LogicalRelationalOperator, PhysicalOperator>> forEachInnerLogToPhyMap,
        HashSet<Operator> seen) {
        if (parent instanceof LOForEach) {
            if (poToEqclassesMap.get(logToPhyMap.get(parent)) != null) {
                LinkedList<IdentityHashSet<Tuple>> eqClasses = new LinkedList<IdentityHashSet<Tuple>>();
                eqClasses.addAll(poToEqclassesMap.get(logToPhyMap.get(parent)));
                for (Map.Entry<LogicalRelationalOperator, PhysicalOperator> entry : forEachInnerLogToPhyMap.get(parent).entrySet()) {
                    if (poToEqclassesMap.get(entry.getValue()) != null)
                        eqClasses.addAll(poToEqclassesMap.get(entry.getValue()));
                }
                result.put((LogicalRelationalOperator) parent, eqClasses);
            }
        } else if (parent instanceof LOCross) {
            boolean ok = true; 
            for (Operator input : ((LOCross) parent).getInputs()) {
                if (logToDataMap.get(input).size() < 2) {
                    // only if all inputs have at least more than two tuples will all outputs be added to the eq. class
                    ok = false;
                    break;
                }
            }
            if (ok) {
                LinkedList<IdentityHashSet<Tuple>> eqClasses = new LinkedList<IdentityHashSet<Tuple>>();
                IdentityHashSet<Tuple> eqClass = new IdentityHashSet<Tuple>();
                for (Iterator<Tuple> it = logToDataMap.get(parent).iterator(); it.hasNext();) {
                    eqClass.add(it.next());
                }
                eqClasses.add(eqClass);
                result.put((LogicalRelationalOperator) parent, eqClasses);
            } else {
                LinkedList<IdentityHashSet<Tuple>> eqClasses = new LinkedList<IdentityHashSet<Tuple>>();
                IdentityHashSet<Tuple> eqClass = new IdentityHashSet<Tuple>();
                eqClasses.add(eqClass);
                result.put((LogicalRelationalOperator)parent, eqClasses);
            }
        } else {
            Collection<IdentityHashSet<Tuple>> eqClasses = poToEqclassesMap.get(logToPhyMap.get(parent));
            if (eqClasses == null) {
                eqClasses = new LinkedList<IdentityHashSet<Tuple>>();
                int size = ((POPackage)logToPhyMap.get(parent)).getNumInps();
                for (int i = 0; i < size; i++) {
                    eqClasses.add(new IdentityHashSet<Tuple>());
                }
            }
            result.put((LogicalRelationalOperator)parent, eqClasses);
        }
        // result.put(parent, getEquivalenceClasses(plan, parent, lp, logToPhyMap, poToEqclassesMap));
        if (lp.getSuccessors(parent) != null) {
            for (Operator lo : lp.getSuccessors(parent)) {
                if (!seen.contains(lo)) {
                    seen.add(lo);
                    getEqClasses(plan, lo, lp, logToPhyMap, result, poToEqclassesMap, logToDataMap, forEachInnerLogToPhyMap, seen);
                }
            }
        }
    }
}
