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

import junit.framework.TestCase;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.logicalLayer.validators.TypeCheckingValidator;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.ExecType;
import org.apache.pig.test.utils.TypeCheckingTestUtil;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;

public class TestTypeChecking extends TestCase {

    @Test
    public void testValidation1() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        buildPlan("a = load 'a' as (field1: chararray, field2: tuple(inner1 : bytearray, inner2 : int));");
        LogicalPlan plan = buildPlan("b = group a by field1;");
        validatePlan(plan) ;
    }

    @Test
    public void testValidation2() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        buildPlan("a = load 'a' as (field1: long, field2: tuple(inner1 : bytearray, inner2 : float));");
        LogicalPlan plan = buildPlan("b = group a by field2;");
        validatePlan(plan) ;
    }


    @Test
    public void testValidation3() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        buildPlan("a = load 'a' as (field1: int, field2: float, field3: chararray );");
        buildPlan("b = order a by $0 ;");
        LogicalPlan plan = buildPlan("c = distinct b ;");
        validatePlan(plan) ;
    }

    @Test
    public void testValidation4() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        buildPlan("a = load 'a' as (field1: chararray, field2: int);");
        LogicalPlan plan = buildPlan("b = group a by (field1, field2) ;");
        validatePlan(plan) ;
    }

    @Test
    public void testValidation5() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        buildPlan("a = load 'a' as (field1: bytearray, field2: int);");
        LogicalPlan plan = buildPlan("b = group a by field1+field2 ;");
        validatePlan(plan) ;
    }

    @Test
    public void testValidation6() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        buildPlan("a = load 'a' as (field1: bytearray, field2: int, field3: double);");
        LogicalPlan plan = buildPlan("b = group a by field1 % field2  ;");
        validatePlan(plan) ;
    }

    @Test
    public void testValidation7() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        buildPlan("a = load 'a' as (field1: int, field2: tuple(inner1 : bytearray, inner2 : int));");
        buildPlan("b = load 'a' as (field1: long, field2: tuple(inner1 : bytearray, inner2 : int));");
        LogicalPlan plan = buildPlan("c = group a by field1, b by field1;");
        validatePlan(plan) ;
    }

    @Test
    public void testValidation8() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        buildPlan("a = load 'a' as (field1: int, field2: long);");
        buildPlan("b = load 'a' as (field1: bytearray, field2: double);");
        LogicalPlan plan = buildPlan("c = group a by (field1,field2) , b by (field1,field2) ;");
        validatePlan(plan) ;
    }

    @Test
    public void testValidation10() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        buildPlan("a = load 'a' as (name: chararray, details: tuple(age, gpa), mymap: map[]);");
		LogicalPlan plan = buildPlan("e = foreach a generate name, details;");
        validatePlan(plan) ;
    }

    @Test
    public void testValidation11() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        buildPlan("a = load 'a' as (name: chararray, details: tuple(age, gpa), field3: tuple(a,b));");
		LogicalPlan plan = buildPlan("e = foreach a generate name, details.(age, gpa), field3.(a,b) ;");
        validatePlan(plan) ;
    }
    
    @Test
    public void testValidation11_2() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        buildPlan("a = load 'a' as (field1: tuple(a, b), field2: tuple(a, b), field3: tuple(a,b));");
		LogicalPlan plan = buildPlan("e = foreach a generate field1.(a,b) , field2.(a, b), field3.(a,b) ;");
        validatePlan(plan) ;
    }

    @Test
    public void testValidation12() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        buildPlan("a = load 'a' as (field1: int, field2: long);");
        buildPlan("b = load 'a' as (field1: bytearray, field2: double);");
        buildPlan("c = group a by field1, b by field1  ;");
        LogicalPlan plan = buildPlan("d = foreach c generate a.(field1, field2), b.(field1, field2)  ;");
        validatePlan(plan) ;
    }

    // I suspect there is something wrong in Schema.reconcile()
    /*
    @Test
    public void testValidation13() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        buildPlan("a = load 'a' as (field1: integer, field2: long);");
        buildPlan("b = load 'a' as (field1: bytearray, field2: double);");
        buildPlan("c = group a by field1, b by field1  ;");
        LogicalPlan plan = buildPlan("d = foreach c generate b.field2, flattten(a)  ;");
        validatePlan(plan) ;
    }
    */

    // The parser still has a bug dealing with this
    /*
    @Test
    public void testValidation13_2() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        buildPlan("a = load 'a' as (field1: integer, field2: long);");
        buildPlan("b = load 'a' as (field1: bytearray, field2: double);");
        buildPlan("c = group a by (field1+field2)*field1, b by field1  ;");
        LogicalPlan plan = buildPlan("d = foreach c generate b.field2, flattten(a)  ;");
        validatePlan(plan) ;
    }
    */

   @Test
    public void testQuery20() throws Throwable {
       TypeCheckingTestUtil.printCurrentMethodName() ;
        buildPlan("a = load 'a' as (field1: long, field2: long, field3: int);");
        String query = "b = foreach a generate ($1+$2), ($1-$2), ($1*$2), ($1/$2), ($1%$2), -($1), ($2*$2) ;";
        validatePlan(buildPlan(query));
    }

    @Test
    public void testQuery21() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        buildPlan("a = load 'a' as (field1: long, field2: long, field3: int);");
        String query = "b = foreach a generate (field1+field2)*(field1-field2) ;";
        validatePlan(buildPlan(query));
    }
    
    /***************** Helpers ******************/

    public LogicalPlan buildPlan(String query) {
        return buildPlan(query, LogicalPlanBuilder.class.getClassLoader());
    }

    public LogicalPlan buildPlan(String query, ClassLoader cldr) {

        LogicalPlanBuilder.classloader = TestTypeChecking.class.getClassLoader() ;
        PigContext pigContext = new PigContext(ExecType.LOCAL);
        LogicalPlanBuilder builder = new LogicalPlanBuilder(pigContext);

        try {
            LogicalPlan lp = builder.parse("Test-Plan-Builder",
                                           query,
                                           aliases,
                                           logicalOpTable,
                                           aliasOp,
                                           defineAliases);

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
            fail(e.getClass().getName() + ": " + e.getMessage() + " -- " + query);
        }
        return null;
    }

    public void validatePlan(LogicalPlan plan) throws PlanValidationException {
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        TypeCheckingTestUtil.printMessageCollector(collector) ;
        TypeCheckingTestUtil.printTypeGraph(plan) ;
    }

    Map<LogicalOperator, LogicalPlan> aliases = new HashMap<LogicalOperator, LogicalPlan>();
    Map<OperatorKey, LogicalOperator> logicalOpTable = new HashMap<OperatorKey, LogicalOperator>();
    Map<String, LogicalOperator> aliasOp = new HashMap<String, LogicalOperator>();
    Map<String, ExpressionOperator> defineAliases = new HashMap<String, ExpressionOperator>();
}
