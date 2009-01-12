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
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.local.executionengine.physicalLayer.LocalLogToPhyTranslationVisitor;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.PlanSetter;
import org.apache.pig.impl.logicalLayer.optimizer.LogicalOptimizer;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.validators.LogicalPlanValidationExecutor;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.DisplayExamples;
import org.apache.pig.pen.util.FunctionalLogicalOptimizer;
import org.apache.pig.pen.util.LineageTracer;

public class ExampleGenerator {

    LogicalPlan plan;
    Map<LOLoad, DataBag> baseData;
    PigContext pigContext;

    Map<LogicalOperator, PhysicalOperator> LogToPhyMap;
    PhysicalPlan physPlan;

    Log log = LogFactory.getLog(getClass());

    private int MAX_RECORDS = 10000;

    public ExampleGenerator(LogicalPlan plan, PigContext hadoopPigContext) {
        this.plan = plan;
//        pigContext = new PigContext(ExecType.LOCAL, hadoopPigContext
//                .getProperties());
        pigContext = hadoopPigContext;
        try {
            pigContext.connect();
        } catch (ExecException e) {
            log.error("Error connecting to the cluster "
                    + e.getLocalizedMessage());

        }

    }

    public void setMaxRecords(int max) {
        MAX_RECORDS = max;
    }

    public Map<LogicalOperator, DataBag> getExamples() {

        compilePlan(plan);

        List<LogicalOperator> loads = plan.getRoots();

        try {
            readBaseData(loads);
        } catch (ExecException e) {
            // TODO Auto-generated catch block
            log.error("Error reading data. " + e.getMessage());
            throw new RuntimeException(e.getMessage());
        } catch (FrontendException e) {
            // TODO Auto-generated catch block
            log.error("Error reading data. " + e.getMessage());
        }

        DerivedDataVisitor derivedData = null;
        try {

            // create derived data and trim base data
            LineageTrimmingVisitor trimmer = new LineageTrimmingVisitor(plan,
                    baseData, LogToPhyMap, physPlan, pigContext);
            trimmer.visit();
            // System.out.println(
            // "Obtained the first level derived and trimmed data");
            // create new derived data from trimmed basedata
            derivedData = new DerivedDataVisitor(plan, null, baseData,
                    trimmer.LogToPhyMap, physPlan);
            derivedData.visit();

            // System.out.println(
            // "Got new derived data from the trimmed base data");
            // augment base data
            AugmentBaseDataVisitor augment = new AugmentBaseDataVisitor(plan,
                    baseData, derivedData.derivedData);
            augment.visit();
            this.baseData = augment.getNewBaseData();
            // System.out.println("Obtained augmented base data");
            // create new derived data and trim the base data after augmenting
            // base data with synthetic tuples
            trimmer = new LineageTrimmingVisitor(plan, baseData,
                    derivedData.LogToPhyMap, physPlan, pigContext);
            trimmer.visit();
            // System.out.println("Final trimming");
            // create the final version of derivedData to give to the output
            derivedData = new DerivedDataVisitor(plan, null, baseData,
                    trimmer.LogToPhyMap, physPlan);
            derivedData.visit();
            // System.out.println("Obtaining final derived data for output");

        } catch (VisitorException e) {
            // TODO Auto-generated catch block
            log.error("Visitor exception while creating example data "
                    + e.getMessage());
        }

        // DisplayExamples.PrintSimple(plan.getLeaves().get(0),
        // derivedData.derivedData);
        System.out.println(DisplayExamples.PrintTabular(plan,
                derivedData.derivedData));
        return derivedData.derivedData;
    }

    private void readBaseData(List<LogicalOperator> loads) throws ExecException, FrontendException {
        baseData = new HashMap<LOLoad, DataBag>();
        for (LogicalOperator op : loads) {
            Schema schema = op.getSchema();
            if(schema == null) {
                throw new ExecException("Example Generator requires a schema. Please provide a schema while loading data.");
            }
            
            DataBag opBaseData = BagFactory.getInstance().newDefaultBag();

            POLoad poLoad = (POLoad) LogToPhyMap.get(op);
//            PigContext oldPC = poLoad.getPc();
//            poLoad.setPc(pigContext);

            poLoad.setLineageTracer(new LineageTracer());

            Tuple t = null;
            int count = 0;
            for (Result res = poLoad.getNext(t); res.returnStatus != POStatus.STATUS_EOP
                    && count < MAX_RECORDS; res = poLoad.getNext(t)) {
                if (res.returnStatus == POStatus.STATUS_NULL)
                    continue;
                if (res.returnStatus == POStatus.STATUS_ERR) {
                    log.error("Error reading Tuple");
                } else {
                    opBaseData.add((Tuple) res.result);
                    count++;
                }

            }
            baseData.put((LOLoad) op, opBaseData);
            // poLoad.setPc(oldPC);
            poLoad.setLineageTracer(null);
        }

    }

    private void compilePlan(LogicalPlan plan) {

        plan = refineLogicalPlan(plan);

        LocalLogToPhyTranslationVisitor visitor = new LocalLogToPhyTranslationVisitor(
                plan);
        visitor.setPigContext(pigContext);
        try {
            visitor.visit();
        } catch (VisitorException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            log.error("Error visiting the logical plan in ExampleGenerator");
        }
        physPlan = visitor.getPhysicalPlan();
        LogToPhyMap = visitor.getLogToPhyMap();
    }

    private LogicalPlan refineLogicalPlan(LogicalPlan plan) {
        PlanSetter ps = new PlanSetter(plan);
        try {
            ps.visit();

        } catch (VisitorException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // run through validator
        CompilationMessageCollector collector = new CompilationMessageCollector();
        FrontendException caught = null;
        try {
            LogicalPlanValidationExecutor validator = new LogicalPlanValidationExecutor(
                    plan, pigContext);
            validator.validate(plan, collector);

            FunctionalLogicalOptimizer optimizer = new FunctionalLogicalOptimizer(
                    plan);
            optimizer.optimize();
        } catch (FrontendException fe) {
            // Need to go through and see what the collector has in it. But
            // remember what we've caught so we can wrap it into what we
            // throw.
            caught = fe;
        }

        return plan;

    }
}