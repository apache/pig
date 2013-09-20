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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.Iterator;
import java.io.IOException;
import org.apache.pig.impl.util.IdentityHashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;


import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.PigException;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.Operator;
import org.apache.pig.pen.util.DisplayExamples;
import org.apache.pig.pen.util.LineageTracer;

/**
 *   This class is used to generate example tuples for the ILLUSTRATE purpose 
 * 
 *
 */
public class ExampleGenerator {

    LogicalPlan plan;
    LogicalPlan newPlan;
    Map<LOLoad, DataBag> baseData = null;
    PigContext pigContext;

    PhysicalPlan physPlan;
    PhysicalPlanResetter physPlanReseter;
    private MRExecutionEngine execEngine;
    private LocalMapReduceSimulator localMRRunner;

    Log log = LogFactory.getLog(getClass());

    private int MAX_RECORDS = 10000;
    
    private Map<Operator, PhysicalOperator> logToPhyMap;
    private Map<PhysicalOperator, Operator> poLoadToLogMap;
    private Map<PhysicalOperator, Operator> poToLogMap;
    private HashMap<PhysicalOperator, Collection<IdentityHashSet<Tuple>>> poToEqclassesMap;
    private LineageTracer lineage;
    private Map<Operator, DataBag> logToDataMap = null;
    private Map<LOForEach, Map<LogicalRelationalOperator, DataBag>> forEachInnerLogToDataMap;
    Map<LOForEach, Map<LogicalRelationalOperator, PhysicalOperator>> forEachInnerLogToPhyMap;
    Map<LOLimit, Long> oriLimitMap = null;
    Map<POLoad, LogicalSchema> poLoadToSchemaMap;

    public ExampleGenerator(LogicalPlan plan, PigContext hadoopPigContext) {
        this.plan = plan;
//        pigContext = new PigContext(ExecType.LOCAL, hadoopPigContext
//                .getProperties());
        pigContext = hadoopPigContext;
        // pigContext.setExecType(ExecType.LOCAL);
        FileLocalizer.setInitialized(false);
        try {
            pigContext.connect();
        } catch (ExecException e) {
            log.error("Error connecting to the cluster "
                    + e.getLocalizedMessage());

        }
        execEngine = new MRExecutionEngine(pigContext);
        localMRRunner = new LocalMapReduceSimulator();
        poLoadToSchemaMap = new HashMap<POLoad, LogicalSchema>();
    }

    public LineageTracer getLineage() {
      return lineage;
    }
    
    public Map<Operator, PhysicalOperator> getLogToPhyMap() {
        return logToPhyMap;
    }
    
    public void setMaxRecords(int max) {
        MAX_RECORDS = max;
    }

    public Map<Operator, DataBag> getExamples() throws IOException, InterruptedException {
        if (pigContext.getProperties().getProperty("pig.usenewlogicalplan", "true").equals("false"))
            throw new ExecException("ILLUSTRATE must use the new logical plan!");
        pigContext.inIllustrator = true;
        physPlan = compilePlan(plan);
        physPlanReseter = new PhysicalPlanResetter(physPlan);
        List<Operator> loads = newPlan.getSources();
        List<PhysicalOperator> pRoots = physPlan.getRoots();
        if (loads.size() != pRoots.size())
            throw new ExecException("Logical and Physical plans have different number of roots");
        logToPhyMap = execEngine.getLogToPhyMap();
        forEachInnerLogToPhyMap = execEngine.getForEachInnerLogToPhyMap(plan);
        poLoadToLogMap = new HashMap<PhysicalOperator, Operator>();
        logToDataMap = new HashMap<Operator, DataBag>();
        poToLogMap = new HashMap<PhysicalOperator, Operator>();
        
        // set up foreach inner data map
        forEachInnerLogToDataMap = new HashMap<LOForEach, Map<LogicalRelationalOperator, DataBag>>();
        for (Map.Entry<LOForEach, Map<LogicalRelationalOperator, PhysicalOperator>> entry : forEachInnerLogToPhyMap.entrySet()) {
            Map<LogicalRelationalOperator, DataBag> innerMap = new HashMap<LogicalRelationalOperator, DataBag>();
            forEachInnerLogToDataMap.put(entry.getKey(), innerMap);
        }

        for (Operator load : loads)
        {
            poLoadToLogMap.put(logToPhyMap.get(load), load);
        }

        boolean hasLimit = false;
        for (Operator lo : logToPhyMap.keySet()) {
            poToLogMap.put(logToPhyMap.get(lo), lo);
            if (!hasLimit && lo instanceof LOLimit)
                hasLimit = true;
        }
        
        try {
            readBaseData(loads);
        } catch (ExecException e) {
            log.error("Error reading data. " + e.getMessage());
            throw e;
        } catch (FrontendException e) {
            log.error("Error reading data. " + e.getMessage());
            throw new RuntimeException(e.getMessage());
        }

        Map<Operator, DataBag> derivedData = null;

        // create derived data and trim base data
        LineageTrimmingVisitor trimmer = new LineageTrimmingVisitor(newPlan,
                baseData, this, logToPhyMap, physPlan, pigContext);
        trimmer.visit();
        baseData = trimmer.getBaseData();
        // System.out.println(
        // "Obtained the first level derived and trimmed data");
        // create new derived data from trimmed basedata
        derivedData = getData(physPlan);

        // System.out.println(
        // "Got new derived data from the trimmed base data");
        // augment base data
        AugmentBaseDataVisitor augment = new AugmentBaseDataVisitor(newPlan,
                logToPhyMap, baseData, derivedData);
        augment.visit();
        this.baseData = augment.getNewBaseData();
        // System.out.println("Obtained augmented base data");
        // create new derived data and trim the base data after augmenting
        // base data with synthetic tuples
        trimmer = new LineageTrimmingVisitor(newPlan, baseData, this,
                logToPhyMap, physPlan, pigContext);
        trimmer.visit();
        baseData = trimmer.getBaseData();
        // System.out.println("Final trimming");
        // create the final version of derivedData to give to the output
        derivedData = getData(physPlan);
        // System.out.println("Obtaining final derived data for output");
        
        if (hasLimit)
        {
            augment.setLimit();
            augment.visit();
            this.baseData = augment.getNewBaseData();
            oriLimitMap = augment.getOriLimitMap();
            derivedData = getData();
        }

        // DisplayExamples.printSimple(plan.getLeaves().get(0),
        // derivedData.derivedData);
        System.out.println(DisplayExamples.printTabular(newPlan,
                derivedData, forEachInnerLogToDataMap));
        pigContext.inIllustrator = false;
        return derivedData;
    }

    private void readBaseData(List<Operator> loads) throws IOException, InterruptedException, FrontendException, ExecException {
        PhysicalPlan thisPhyPlan = new PhysicalPlan();
        for (Operator op : loads) {
            LogicalSchema schema = ((LOLoad) op).getSchema();
            if(schema == null) {
                throw new ExecException("Example Generator requires a schema. Please provide a schema while loading data.");
            }
            poLoadToSchemaMap.put((POLoad)logToPhyMap.get(op), schema);
            thisPhyPlan.add(logToPhyMap.get(op));
        }
        baseData = null;
        Map<Operator, DataBag> result = getData(thisPhyPlan);
        baseData = new HashMap<LOLoad, DataBag>();
        for (Operator lo : result.keySet()) {
            if (lo instanceof LOLoad) {
                baseData.put((LOLoad) lo, result.get(lo));
            }
        }
    }

    PhysicalPlan compilePlan(LogicalPlan plan) throws ExecException, FrontendException {
        PhysicalPlan result = execEngine.compile(plan, null);
        newPlan = execEngine.getNewPlan();
        return result;
    }
    
    public Map<Operator, DataBag> getData() throws IOException, InterruptedException {
      return getData(physPlan);
    }
    
    private Map<Operator, DataBag> getData(PhysicalPlan plan) throws PigException, IOException, InterruptedException
    {
        // get data on a physical plan possibly trimmed of one branch 
        lineage = new LineageTracer();
        IllustratorAttacher attacher = new IllustratorAttacher(plan, lineage, MAX_RECORDS, poLoadToSchemaMap, pigContext);
        attacher.visit();
        if (oriLimitMap != null) {
            for (Map.Entry<LOLimit, Long> entry : oriLimitMap.entrySet()) {
                logToPhyMap.get(entry.getKey()).getIllustrator().setOriginalLimit(entry.getValue());
            }
        }
        getLogToDataMap(attacher.getDataMap());
        if (baseData != null ) {
            setLoadDataMap();
            physPlanReseter.visit();
        }
        localMRRunner.launchPig(plan, baseData, lineage, attacher, this, pigContext);
        if (baseData == null)
            poToEqclassesMap = attacher.poToEqclassesMap;
        else {
            for (Map.Entry<PhysicalOperator, Collection<IdentityHashSet<Tuple>>> entry : attacher.poToEqclassesMap.entrySet()) {
                if(!(entry.getKey() instanceof POLoad))
                  poToEqclassesMap.put(entry.getKey(), entry.getValue());
            }
        }
        if (baseData != null)
            // only for non derived data generation
            phyToMRTransform(plan, attacher.getDataMap());
        return logToDataMap;
    }
    
    public Map<Operator, DataBag> getData(Map<LOLoad, DataBag> newBaseData) throws Exception 
    {
        baseData = newBaseData;
        return getData(physPlan);
    }
    
    private void phyToMRTransform(PhysicalPlan plan, Map<PhysicalOperator, DataBag> phyToDataMap) {
        // remap the LO to PO as result of the MR compilation may have changed PO in the MR plans
        Map<PhysicalOperator, PhysicalOperator> phyToMRMap = localMRRunner.getPhyToMRMap();
        for (Map.Entry<PhysicalOperator, Operator> entry : poToLogMap.entrySet()) {
            if (phyToMRMap.get(entry.getKey()) != null) {
                PhysicalOperator poInMR = phyToMRMap.get(entry.getKey());
                logToDataMap.put(entry.getValue(), phyToDataMap.get(poInMR));
                poToEqclassesMap.put(entry.getKey(), poToEqclassesMap.get(poInMR));
            }
        }
    }
    
    private void getLogToDataMap(Map<PhysicalOperator, DataBag> phyToDataMap) {
        logToDataMap.clear();
        for (Operator lo : logToPhyMap.keySet()) {
            if (logToPhyMap.get(lo) != null)
                logToDataMap.put(lo, phyToDataMap.get(logToPhyMap.get(lo)));
        }
        
        // set the LO-to-Data mapping for the ForEach inner plans
        for (Map.Entry<LOForEach, Map<LogicalRelationalOperator, DataBag>> entry : forEachInnerLogToDataMap.entrySet()) {
            entry.getValue().clear();
            for (Map.Entry<LogicalRelationalOperator, PhysicalOperator>  innerEntry : forEachInnerLogToPhyMap.get(entry.getKey()).entrySet()) {
                entry.getValue().put(innerEntry.getKey(), phyToDataMap.get(innerEntry.getValue()));
            }
        }
    }
    
    private void setLoadDataMap() {
        // This function sets up the LO-TO-Data map, eq. class, and lineage for the base data used in the coming runner
        // this must be called after logToDataMap has been properly (re)set and before the runner is started
        if (baseData != null) {
            if (poToEqclassesMap == null)
                poToEqclassesMap = new HashMap<PhysicalOperator, Collection<IdentityHashSet<Tuple>>>();
            else
                poToEqclassesMap.clear();
            for (LOLoad lo : baseData.keySet()) {
                logToDataMap.get(lo).addAll(baseData.get(lo));
                LinkedList<IdentityHashSet<Tuple>> equivalenceClasses = new LinkedList<IdentityHashSet<Tuple>>();
                IdentityHashSet<Tuple> equivalenceClass = new IdentityHashSet<Tuple>();
                equivalenceClasses.add(equivalenceClass);
                for (Tuple t : baseData.get(lo)) {
                    lineage.insert(t);
                    equivalenceClass.add(t);
                }
                poToEqclassesMap.put(logToPhyMap.get(lo), equivalenceClasses);
            }
        }
    }
    
    public Collection<IdentityHashSet<Tuple>> getEqClasses() {
        Map<LogicalRelationalOperator, Collection<IdentityHashSet<Tuple>>> logToEqclassesMap = getLoToEqClassMap();
        LinkedList<IdentityHashSet<Tuple>> ret = new LinkedList<IdentityHashSet<Tuple>>();
        for (Map.Entry<LogicalRelationalOperator, Collection<IdentityHashSet<Tuple>>> entry :
            logToEqclassesMap.entrySet()) {
            if (entry.getValue() != null)
                ret.addAll(entry.getValue());
        }
        return ret;
    }

    public Map<LogicalRelationalOperator, Collection<IdentityHashSet<Tuple>>> getLoToEqClassMap() {
        Map<LogicalRelationalOperator, Collection<IdentityHashSet<Tuple>>> ret =
          EquivalenceClasses.getLoToEqClassMap(physPlan, newPlan, logToPhyMap, logToDataMap, forEachInnerLogToPhyMap, poToEqclassesMap);
        // eq classes adjustments based upon logical operators
        
        for (Map.Entry<LogicalRelationalOperator, Collection<IdentityHashSet<Tuple>>> entry :ret.entrySet())
        {
            if (entry.getKey() instanceof LOSort) {
                Collection<IdentityHashSet<Tuple>> eqClasses = entry.getValue();
                for (Iterator<IdentityHashSet<Tuple>> it = eqClasses.iterator(); it.hasNext(); ) {
                    Object t = null;
                    IdentityHashSet<Tuple> eqClass = it.next();
                    if (eqClass.size() == 1) {
                        eqClass.clear();
                        continue;
                    }
                    boolean first = true, allIdentical = true;
                    for (Iterator<Tuple> it1 = eqClass.iterator(); it1.hasNext();)
                    {
                        if (first) {
                            first = false;
                            t = it1.next();
                        } else {
                            if (!it1.next().equals(t)) {
                                allIdentical = false;
                                break;
                            }
                        }
                    }
                    if (allIdentical)
                        eqClass.clear();
                }
            }
        }
        
        return ret;
    }
}
