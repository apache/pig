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
import org.apache.pig.test.utils.TypeCheckingTestUtil;
import org.apache.pig.test.utils.LogicalPlanTester;
import org.junit.Test;
import org.junit.Before;

public class TestTypeChecking extends TestCase {

    final String FILE_BASE_LOCATION = "test/org/apache/pig/test/data/DotFiles/" ;

    LogicalPlanTester planTester = new LogicalPlanTester() ;

    @Before
    public void setUp() {
        planTester.reset();
    }


    @Test
    public void testSimple1() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        planTester.buildPlan("a = load 'a' as (field1: int, field2: float, field3: chararray );") ;
        LogicalPlan plan = planTester.buildPlan("b = distinct a ;") ;
        planTester.typeCheckAgainstDotFile(plan, FILE_BASE_LOCATION + "plan1.dot");
    }


    @Test
    public void testByScript1() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        planTester.typeCheckUsingDotFile(FILE_BASE_LOCATION + "testScript1.dot");
    }

    @Test
    public void testByScript2() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        planTester.typeCheckUsingDotFile(FILE_BASE_LOCATION + "testScript2.dot");
    }

    // Problem with "group" keyword in QueryParser
    /*
    @Test
    public void testByScript4() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        planTester.typeCheckUsingDotFile(FILE_BASE_LOCATION + "testScript4.dot");
    }
    */

    /*
    @Test
    public void testByScript3() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        planTester.typeCheckUsingDotFile(FILE_BASE_LOCATION + "testScript3.dot");
    }
    */

    @Test
    public void testByScript5() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        planTester.typeCheckUsingDotFile(FILE_BASE_LOCATION + "testScript5.dot");
    }
    
    @Test
    public void testByScript6() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        planTester.typeCheckUsingDotFile(FILE_BASE_LOCATION + "testScript6.dot");
    }

    // TODO: Convert all of these to dot files

    /*
    @Test
    public void testValidation1() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        planTester.buildPlan("a = load 'a' as (" 
                            + "group: tuple(field1:int, field2:double) ,"
                            + "a: bag{tuple1:tuple(field1: int,field2: long)},"
                            + "b: bag{tuple1:tuple(field1: bytearray,field2: double)} "
                            + " ) ;") ;
        LogicalPlan plan = planTester.buildPlan("b = group a by field1;");
        planTester.typeCheckPlan(plan);


    }
    */

    /*
    @Test
    public void testValidation1() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        planTester.buildPlan("a = load 'a' as (field1: chararray, field2: tuple(inner1 : bytearray, inner2 : int));");
        LogicalPlan plan = planTester.buildPlan("b = group a by field1;");
        planTester.typeCheckPlan(plan);
    }
    */

    /*
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
    public void testValidation12() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        buildPlan("a = load 'a' as (field1: int, field2: long);");
        buildPlan("b = load 'a' as (field1: bytearray, field2: double);");
        buildPlan("c = group a by field1, b by field1  ;");
        LogicalPlan plan = buildPlan("d = foreach c generate a.(field1, field2), b.(field1, field2)  ;");
        validatePlan(plan) ;
    }
    */

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

    /*
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

    */

    public void testSUM1() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        planTester.buildPlan("a = load '/user/pig/tests/data/singlefile/studenttab10k' as (name:chararray, age:int, gpa:double);") ;
        LogicalPlan plan1 = planTester.buildPlan("b = foreach a generate (long)age as age:long, (int)gpa as gpa:int;") ;
        LogicalPlan plan2 = planTester.buildPlan("c = foreach b generate SUM(age), SUM(gpa);") ;
        planTester.typeCheckPlan(plan2);
    }

    public void testSUM2() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        planTester.buildPlan("a = group (load '\" + tmpFile + \"') by ($0,$1);") ;
        LogicalPlan plan1 = planTester.buildPlan("b = foreach a generate flatten(group), SUM($1.$2);") ;
        planTester.typeCheckPlan(plan1);
    }


    public void testGenerate1() throws Throwable {
        TypeCheckingTestUtil.printCurrentMethodName() ;
        planTester.buildPlan("a = load '/user/pig/tests/data/singlefile/studenttab10k' as (name:chararray, age:int, gpa:double);") ;
        LogicalPlan plan1 = planTester.buildPlan("b = foreach a generate 1 + 0.2f + 253645L, gpa+1; ") ;
        planTester.typeCheckPlan(plan1);
    }



}
