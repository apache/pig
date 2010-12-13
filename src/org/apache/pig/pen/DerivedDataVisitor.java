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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PORead;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOCross;
import org.apache.pig.impl.logicalLayer.LODistinct;
import org.apache.pig.impl.logicalLayer.LOFilter;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LOLimit;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOUnion;
import org.apache.pig.impl.logicalLayer.LOVisitor;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.PlanSetter;
import org.apache.pig.impl.logicalLayer.validators.LogicalPlanValidationExecutor;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.IdentityHashSet;
import org.apache.pig.pen.util.DependencyOrderLimitedWalker;
import org.apache.pig.pen.util.LineageTracer;


//This class is used to pass data through the entire plan and save the intermediates results.
public class DerivedDataVisitor {

    Map<LogicalOperator, DataBag> derivedData = new HashMap<LogicalOperator, DataBag>();
    PhysicalPlan physPlan = null;
    Map<LOLoad, DataBag> baseData = null;

    Map<LogicalOperator, PhysicalOperator> LogToPhyMap = null;
    Log log = LogFactory.getLog(getClass());

    Map<LogicalOperator, Collection<IdentityHashSet<Tuple>>> OpToEqClasses = null;
    Collection<IdentityHashSet<Tuple>> EqClasses = null;

    LineageTracer lineage = new LineageTracer();

    public DerivedDataVisitor(LogicalPlan plan, PigContext pc,
            Map<LOLoad, DataBag> baseData,
            PhysicalPlan physPlan) {

        this.baseData = baseData;

        OpToEqClasses = new HashMap<LogicalOperator, Collection<IdentityHashSet<Tuple>>>();
        EqClasses = new LinkedList<IdentityHashSet<Tuple>>();

        this.physPlan = physPlan;
        // if(logToPhyMap == null)
        // compilePlan(plan);
        // else
        // LogToPhyMap = logToPhyMap;

    }

    public DerivedDataVisitor(LogicalOperator op, PigContext pc,
            Map<LOLoad, DataBag> baseData,
            Map<LogicalOperator, PhysicalOperator> logToPhyMap,
            PhysicalPlan physPlan) {
        this.baseData = baseData;

        OpToEqClasses = new HashMap<LogicalOperator, Collection<IdentityHashSet<Tuple>>>();
        EqClasses = new LinkedList<IdentityHashSet<Tuple>>();

        LogToPhyMap = logToPhyMap;
        this.physPlan = physPlan;
    }
}
