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

package org.apache.pig.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.local.executionengine.physicalLayer.LocalLogToPhyTranslationVisitor;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LODefine;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.impl.logicalLayer.PlanSetter;
import org.apache.pig.impl.logicalLayer.validators.LogicalPlanValidationExecutor;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.FunctionalLogicalOptimizer;
import org.junit.Test;

public class TestLocalPOSplit extends TestCase {

    Random r = new Random();

    Log log = LogFactory.getLog(getClass());

    PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());

    @Test
    public void testSplit() throws IOException, VisitorException, ExecException {
        pigContext.connect();
        File datFile = File.createTempFile("tempA", ".dat");

        FileOutputStream dat = new FileOutputStream(datFile);

        for (int i = 0; i < 100; i++) {
            String str = r.nextInt(10) + "\n";
            dat.write(str.getBytes());

        }

        dat.close();

        String query = "split (load '" + Util.encodeEscape(datFile.getAbsolutePath())
                + "') into a if $0 == 2, b if $0 == 9, c if $0 == 7 ;";

        LogicalPlan plan = buildPlan(query);
        PhysicalPlan pp = buildPhysicalPlan(plan);

        DataBag[] bag = new DataBag[pp.getLeaves().size()];

        for (int i = 0; i < bag.length; i++) {
            bag[i] = BagFactory.getInstance().newDefaultBag();
        }

        for (int i = 0; i < pp.getLeaves().size(); i++) {
            Tuple t = null;
            for (Result res = pp.getLeaves().get(i).getNext(t); res.returnStatus != POStatus.STATUS_EOP; res = pp
                    .getLeaves().get(i).getNext(t)) {
                if (res.returnStatus == POStatus.STATUS_OK)
                    bag[i].add((Tuple) res.result);
            }
        }
        
        // Depending on how the "maps" in the physical plan are
        // built the leaves could be in different order between different runs.
        // lets test the first tuple out of each leaf to 
        // 1) ensure the value was not seen before
        // 2) all the remaining tuples from that leaf are same
        //    as the first value
        Map<DataByteArray, Boolean> seen = new HashMap<DataByteArray, Boolean>();
        seen.put(new DataByteArray("7".getBytes()), false);
        seen.put(new DataByteArray("9".getBytes()), false);
        seen.put(new DataByteArray("2".getBytes()), false);
        
        for (int i = 0; i < bag.length; i++) {
            DataByteArray firstValue = null;
            Iterator<Tuple> it = bag[i].iterator();
            if (it.hasNext()) {
                // check that we have not seen this value before
                Tuple t = it.next();
                System.out.println(t);
                firstValue = (DataByteArray) t.get(0);
                assertFalse((Boolean) seen.get(firstValue));
                seen.put(firstValue, true);

            }
            // check that all remaining tuples from this 
            // leaf have the same values as the first value
            for (; it.hasNext();) {
                Tuple t = it.next();
                System.out.println(t);
                assertEquals(t.get(0), firstValue);
            }
        }
    }

    public PhysicalPlan buildPhysicalPlan(LogicalPlan lp)
            throws VisitorException {
        LocalLogToPhyTranslationVisitor visitor = new LocalLogToPhyTranslationVisitor(
                lp);
        visitor.setPigContext(pigContext);
        visitor.visit();
        return visitor.getPhysicalPlan();
    }

    public LogicalPlan buildPlan(String query) {
        return buildPlan(query, LogicalPlanBuilder.class.getClassLoader());
    }

    public LogicalPlan buildPlan(String query, ClassLoader cldr) {
        LogicalPlanBuilder.classloader = cldr;

        LogicalPlanBuilder builder = new LogicalPlanBuilder(pigContext); //

        try {
            LogicalPlan lp = builder.parse("Test-Plan-Builder", query, aliases,
                                           logicalOpTable, aliasOp, fileNameMap);
            List<LogicalOperator> roots = lp.getRoots();

            if (roots.size() > 0) {
                for (LogicalOperator op : roots) {
                    if (!(op instanceof LOLoad) && !(op instanceof LODefine)) {
                        throw new Exception(
                                "Cannot have a root that is not the load or define operator. Found "
                                        + op.getClass().getName());
                    }
                }
            }

            System.err.println("Query: " + query);

            // Just the top level roots and their children
            // Need a recursive one to travel down the tree

            for (LogicalOperator op : lp.getRoots()) {
                System.err.println("Logical Plan Root: "
                        + op.getClass().getName() + " object " + op);

                List<LogicalOperator> listOp = lp.getSuccessors(op);

                if (null != listOp) {
                    Iterator<LogicalOperator> iter = listOp.iterator();
                    while (iter.hasNext()) {
                        LogicalOperator lop = iter.next();
                        System.err.println("Successor: "
                                + lop.getClass().getName() + " object " + lop);
                    }
                }
            }
            lp = refineLogicalPlan(lp);
            assertTrue(lp != null);
            return lp;
        } catch (IOException e) {
            // log.error(e);
            // System.err.println("IOException Stack trace for query: " +
            // query);
            // e.printStackTrace();
            fail("IOException: " + e.getMessage());
        } catch (Exception e) {
            log.error(e);
            // System.err.println("Exception Stack trace for query: " + query);
            // e.printStackTrace();
            fail(e.getClass().getName() + ": " + e.getMessage() + " -- "
                    + query);
        }
        return null;
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

    Map<LogicalOperator, LogicalPlan> aliases = new HashMap<LogicalOperator, LogicalPlan>();
    Map<OperatorKey, LogicalOperator> logicalOpTable = new HashMap<OperatorKey, LogicalOperator>();
    Map<String, LogicalOperator> aliasOp = new HashMap<String, LogicalOperator>();
    Map<String, String> fileNameMap = new HashMap<String, String>();
}
