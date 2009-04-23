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

package org.apache.pig.test.utils;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.logicalLayer.optimizer.LogicalOptimizer;
import org.apache.pig.impl.logicalLayer.validators.TypeCheckingValidator;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.optimizer.OptimizerException;
import org.apache.pig.ExecType;
import static org.apache.pig.test.utils.TypeCheckingTestUtil.* ;
import org.apache.pig.test.utils.dotGraph.LogicalPlanLoader;
import org.apache.pig.test.utils.planComparer.LogicalPlanComparer;
import org.apache.pig.test.utils.dotGraph.DotGraphReader;
import org.apache.pig.test.utils.dotGraph.DotGraph;

import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.io.IOException;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

/***
 * This class is used for logical plan testing
 */
public class LogicalPlanTester {

    static final String SCOPE = "scope" ;

    private Map<LogicalOperator, LogicalPlan> aliases  = null ;
    private Map<OperatorKey, LogicalOperator> logicalOpTable = null ;
    private Map<String, LogicalOperator> aliasOp = null ;
    private Map<String, String> fileNameMap = null ;

    public LogicalPlanTester() {
        reset() ;
    }

    /***
     * Reset state
     */
    public void reset() {
        aliases = new HashMap<LogicalOperator, LogicalPlan>();
        logicalOpTable = new HashMap<OperatorKey, LogicalOperator>();
        aliasOp = new HashMap<String, LogicalOperator>();
        fileNameMap = new HashMap<String, String>();
        NodeIdGenerator.reset(SCOPE);
    }

    /***
     * Build plan by the given query string (Pig script)
     * @param query
     * @return
     */
    public LogicalPlan buildPlan(String query) {
        return buildPlan(query, LogicalPlanBuilder.class.getClassLoader());
    }


    /***
     * Type check the given plan
     * @param plan
     * @throws PlanValidationException
     */
    public void typeCheckPlan(LogicalPlan plan) throws PlanValidationException {
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        printMessageCollector(collector) ;
        System.out.println("Actual plan after type check:") ;
        printTypeGraph(plan) ;
    }

    public void optimizePlan(LogicalPlan plan) throws OptimizerException {
        LogicalOptimizer optimizer = new LogicalOptimizer(plan);
        optimizer.optimize();
        System.out.println("Actual plan after after optimization:") ;
        printTypeGraph(plan) ;
    }

    /***
     * Run type checking and compare the result with  plan structure
     * stored in Dot file
     * @param plan
     * @param file
     * @throws PlanValidationException
     */
    public void typeCheckAgainstDotFile(
            LogicalPlan plan,
            String file) throws PlanValidationException, OptimizerException {
        typeCheckAgainstDotFile(plan, file, false);
    }

    /***
     * Run type checking and compare the result with  plan structure
     * stored in Dot file
     * @param plan
     * @param file
     * @param optimize if true, the plan will be run through the optimizer
     * @throws PlanValidationException
     */
    public void typeCheckAgainstDotFile(
            LogicalPlan plan,
            String file,
            boolean optimize) throws PlanValidationException,
                                     OptimizerException {
        // validate the given plan
        typeCheckPlan(plan);

        if (optimize) optimizePlan(plan);

        // load the expected plan from file
        LogicalPlanLoader planLoader = new LogicalPlanLoader() ;
        LogicalPlan expectedPlan = planLoader.loadFromFile(file, LogicalPlan.class) ;
        System.out.println("Expected plan:") ;
        printTypeGraph(expectedPlan) ;

        // do the comparison
        LogicalPlanComparer comparer = new LogicalPlanComparer() ;
        StringBuilder errMsg = new StringBuilder() ;
        boolean result = comparer.structurallyEquals(plan, expectedPlan, errMsg) ;

        // check
        System.out.println(errMsg.toString()) ;
        assertTrue("The expected plan is different", result);
        System.out.println("Checking DONE!") ;
    }

    public void typeCheckUsingDotFile(
            String file) throws PlanValidationException, OptimizerException {
        typeCheckUsingDotFile(file, false);
    }

    public void typeCheckUsingDotFile(
            String file,
            boolean optimize) throws PlanValidationException,
                                     OptimizerException {
        DotGraphReader reader = new DotGraphReader() ;
        DotGraph graph = reader.loadFromFile(file) ;
        if (!graph.attributes.containsKey("pigScript")) {
            throw new AssertionError("pigScript attribute doesn't exist"
                                     + " in Dot file") ;
        }

        String script = graph.attributes.get("pigScript") ;
        // TODO: Script splitting here is a quick hack.
        String[] queries = script.split(";") ;
        LogicalPlan plan = null ;
        for(String query : queries) {
            if (!query.trim().equals("")) {
                plan = buildPlan(query + ";") ;
            }
        }
        typeCheckAgainstDotFile(plan, file, optimize) ;

    }

    public void printPlan(LogicalPlan lp, String title) {
        try {
            System.err.println(title);
            LOPrinter lv = new LOPrinter(System.err, lp);
            lv.visit();
            System.err.println();
        } catch (Exception e) {
        }
    }

    ////////////// Helpers ////////////////

    // The actual plan builder
    private LogicalPlan buildPlan(String query, ClassLoader cldr) {

        LogicalPlanBuilder.classloader = LogicalPlanTester.class.getClassLoader() ;
        PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());
        try {
            pigContext.connect();
        } catch (ExecException e1) {
            fail(e1.getClass().getName() + ": " + e1.getMessage() + " -- " + query);
        }
        LogicalPlanBuilder builder = new LogicalPlanBuilder(pigContext);

        try {
            LogicalPlan lp = builder.parse(SCOPE,
                                           query,
                                           aliases,
                                           logicalOpTable,
                                           aliasOp,
                                           fileNameMap);

            List<LogicalOperator> roots = lp.getRoots();

            if(roots.size() > 0) {
                if (logicalOpTable.get(roots.get(0)) instanceof LogicalOperator){
                    System.out.println(query);
                    System.out.println(logicalOpTable.get(roots.get(0)));
                }
                if ((roots.get(0)).getAlias()!=null){
                    aliases.put(roots.get(0), lp);
                }
            }

            assertTrue(lp != null);

            return lp ;
        }
        catch (IOException e) {
            fail("IOException: " + e.getMessage());
        }
        catch (Exception e) {
            e.printStackTrace();
            fail(e.getClass().getName() + ": " + e.getMessage() + " -- " + query);
        }
        return null;
    }


}
