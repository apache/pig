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
import org.junit.Test;
import org.apache.pig.FuncSpec;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.logicalLayer.validators.TypeCheckingValidator;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema ;
import static org.apache.pig.test.utils.TypeCheckingTestUtil.* ;
import org.apache.pig.test.utils.TypeCheckingTestUtil;

import java.util.List;
import java.util.ArrayList;

public class TestTypeCheckingValidatorNoSchema  extends TestCase {


    @Test
    public void testUnion1() throws Throwable {

        printCurrentMethodName();
        LogicalPlan plan = new LogicalPlan() ;

        LOLoad load1 = genDummyLOLoad(plan) ;
        LOLoad load2 = genDummyLOLoad(plan) ;

        // set schemas
        load1.setEnforcedSchema(null) ;
        load2.setEnforcedSchema(null) ;

        // create union operator
        ArrayList<LogicalOperator> inputList = new ArrayList<LogicalOperator>() ;
        inputList.add(load1) ;
        inputList.add(load2) ;
        LOUnion union = new LOUnion(plan, genNewOperatorKey()) ;

        // wiring
        plan.add(load1) ;
        plan.add(load2) ;
        plan.add(union) ;

        plan.connect(load1, union);
        plan.connect(load2, union);

        // validate
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        printMessageCollector(collector) ;
        printTypeGraph(plan) ;

        // check end result schema
        Schema outputSchema = union.getSchema() ;
        assertEquals(outputSchema, null);

    }


    @Test
    public void testUnion2() throws Throwable {

        printCurrentMethodName();
        LogicalPlan plan = new LogicalPlan() ;

        LOLoad load1 = genDummyLOLoad(plan) ;
        LOLoad load2 = genDummyLOLoad(plan) ;

        String[] aliases = new String[]{ "a", "b", "c" } ;
        byte[] types = new byte[] { DataType.INTEGER, DataType.LONG, DataType.BYTEARRAY } ;
        Schema schema1 = genFlatSchema(aliases, types) ;

        // set schemas
        load1.setEnforcedSchema(schema1) ;
        load2.setEnforcedSchema(null) ;

        // create union operator
        ArrayList<LogicalOperator> inputList = new ArrayList<LogicalOperator>() ;
        inputList.add(load1) ;
        inputList.add(load2) ;
        LOUnion union = new LOUnion(plan, genNewOperatorKey()) ;

        // wiring
        plan.add(load1) ;
        plan.add(load2) ;
        plan.add(union) ;

        plan.connect(load1, union);
        plan.connect(load2, union);

        // validate
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        printMessageCollector(collector) ;
        printTypeGraph(plan) ;

        // check end result schema
        Schema outputSchema = union.getSchema() ;
        assertEquals(outputSchema, null);

    }


    // Positive expression cond columns
    @Test
    public void testSplitWithInnerPlan1() throws Throwable {

        printCurrentMethodName();
        // Create outer plan
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = genDummyLOLoad(plan) ;

        // set schemas
        load1.setEnforcedSchema(null) ;

        // Create expression inner plan #1
        LogicalPlan innerPlan1 = new LogicalPlan() ;
        LOProject project11 = new LOProject(innerPlan1, genNewOperatorKey(), load1, 0) ;
        project11.setSentinel(true);
        LOProject project12 = new LOProject(innerPlan1, genNewOperatorKey(), load1, 1) ;
        project11.setSentinel(true);
        LONotEqual notequal1 = new LONotEqual(innerPlan1, genNewOperatorKey()) ;

        innerPlan1.add(project11) ;
        innerPlan1.add(project12) ;
        innerPlan1.add(notequal1) ;

        innerPlan1.connect(project11, notequal1);
        innerPlan1.connect(project12, notequal1);

        // Create expression inner plan #2
        LogicalPlan innerPlan2 = new LogicalPlan() ;
        LOProject project21 = new LOProject(innerPlan2, genNewOperatorKey(), load1, 0) ;
        project21.setSentinel(true);
        LOConst const21 = new LOConst(innerPlan2, genNewOperatorKey(), 26) ;
        const21.setType(DataType.LONG);
        LOLesserThanEqual lesser21 = new LOLesserThanEqual(innerPlan2,
                                                           genNewOperatorKey()) ;

        innerPlan2.add(project21) ;
        innerPlan2.add(const21) ;
        innerPlan2.add(lesser21) ;

        innerPlan2.connect(project21, lesser21);
        innerPlan2.connect(const21, lesser21) ;

        // List of innerplans
        List<LogicalPlan> innerPlans = new ArrayList<LogicalPlan>() ;
        innerPlans.add(innerPlan1) ;
        innerPlans.add(innerPlan2) ;

        // split
        LOSplit split1 = new LOSplit(plan,
                                     genNewOperatorKey(),
                                     new ArrayList<LogicalOperator>());

        // output1
        LOSplitOutput splitOutput1 = new LOSplitOutput(plan, genNewOperatorKey(), 0, innerPlan1) ;
        split1.addOutput(splitOutput1);

        // output2
        LOSplitOutput splitOutput2 = new LOSplitOutput(plan, genNewOperatorKey(), 1, innerPlan2) ;
        split1.addOutput(splitOutput2);

        plan.add(load1);
        plan.add(split1);
        plan.add(splitOutput1);
        plan.add(splitOutput2);

        plan.connect(load1, split1) ;
        plan.connect(split1, splitOutput1) ;
        plan.connect(split1, splitOutput2) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        printMessageCollector(collector) ;
        printTypeGraph(plan) ;

        if (collector.hasError()) {
            throw new AssertionError("Expect no error") ;
        }

        // check split itself
        assertEquals(split1.getSchema(), null) ;

        // check split output #1
        assertEquals(splitOutput1.getSchema(), null) ;
        assertEquals(splitOutput2.getSchema(), null) ;

        // inner conditions: all have to be boolean
        assertEquals(innerPlan1.getSingleLeafPlanOutputType(), DataType.BOOLEAN);
        assertEquals(innerPlan2.getSingleLeafPlanOutputType(), DataType.BOOLEAN);

    }


    // Negative test in cond plan
    @Test
    public void testSplitWithInnerPlan2() throws Throwable {

        printCurrentMethodName();
        // Create outer plan
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = genDummyLOLoad(plan) ;

        // set schemas
        load1.setEnforcedSchema(null) ;

        // Create expression inner plan #1
        LogicalPlan innerPlan1 = new LogicalPlan() ;
        LOProject project11 = new LOProject(innerPlan1, genNewOperatorKey(), load1, 0) ;
        project11.setSentinel(true);
        LOProject project12 = new LOProject(innerPlan1, genNewOperatorKey(), load1, 1) ;
        project11.setSentinel(true);
        LONotEqual notequal1 = new LONotEqual(innerPlan1, genNewOperatorKey()) ;

        innerPlan1.add(project11) ;
        innerPlan1.add(project12) ;
        innerPlan1.add(notequal1) ;

        innerPlan1.connect(project11, notequal1);
        innerPlan1.connect(project12, notequal1);

        // Create expression inner plan #2
        LogicalPlan innerPlan2 = new LogicalPlan() ;
        LOProject project21 = new LOProject(innerPlan2, genNewOperatorKey(), load1, 0) ;
        project21.setSentinel(true);
        LOConst const21 = new LOConst(innerPlan2, genNewOperatorKey(), 26) ;
        const21.setType(DataType.LONG);
        LOAdd add21 = new LOAdd(innerPlan2, genNewOperatorKey()) ;
        LOConst const22 = new LOConst(innerPlan2, genNewOperatorKey(), "hoho") ;
        const22.setType(DataType.CHARARRAY);
        LOSubtract subtract21 = new LOSubtract(innerPlan2, genNewOperatorKey()) ;

        innerPlan2.add(project21) ;
        innerPlan2.add(const21) ;
        innerPlan2.add(add21) ;
        innerPlan2.add(const22) ;
        innerPlan2.add(subtract21) ;

        innerPlan2.connect(project21, add21);
        innerPlan2.connect(const21, add21) ;       
        innerPlan2.connect(add21, subtract21) ;
        innerPlan2.connect(const22, subtract21) ;

        // List of innerplans
        List<LogicalPlan> innerPlans = new ArrayList<LogicalPlan>() ;
        innerPlans.add(innerPlan1) ;
        innerPlans.add(innerPlan2) ;

        // split
        LOSplit split1 = new LOSplit(plan,
                                     genNewOperatorKey(),
                                     new ArrayList<LogicalOperator>());

        // output1
        LOSplitOutput splitOutput1 = new LOSplitOutput(plan, genNewOperatorKey(), 0, innerPlan1) ;
        split1.addOutput(splitOutput1);

        // output2
        LOSplitOutput splitOutput2 = new LOSplitOutput(plan, genNewOperatorKey(), 1, innerPlan2) ;
        split1.addOutput(splitOutput2);

        plan.add(load1);
        plan.add(split1);
        plan.add(splitOutput1);
        plan.add(splitOutput2);

        plan.connect(load1, split1) ;
        plan.connect(split1, splitOutput1) ;
        plan.connect(split1, splitOutput2) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        try {
            TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
            typeValidator.validate(plan, collector) ;
            fail("Exception expected") ;
        }
        catch(PlanValidationException pve) {
            // good
        }

        printMessageCollector(collector) ;
        printTypeGraph(plan) ;

    }


    @Test
    public void testDistinct1() throws Throwable {

        printCurrentMethodName();
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = genDummyLOLoad(plan) ;

        // set schemas
        load1.setEnforcedSchema(null) ;

        // create union operator
        ArrayList<LogicalOperator> inputList = new ArrayList<LogicalOperator>() ;
        inputList.add(load1) ;
        LODistinct distinct1 = new LODistinct(plan, genNewOperatorKey()) ;

        // wiring
        plan.add(load1) ;
        plan.add(distinct1) ;

        plan.connect(load1, distinct1);

        // validate
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        printMessageCollector(collector) ;
        printTypeGraph(plan) ;

        // check end result schema
        assertEquals(distinct1.getSchema(), null);
    }

    // Positive expression sort columns
    @Test
    public void testSort1() throws Throwable {

        printCurrentMethodName();
        // Create outer plan
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = genDummyLOLoad(plan) ;

        // set schemas
        load1.setEnforcedSchema(null) ;

        // Create expression inner plan #1
        LogicalPlan innerPlan1 = new LogicalPlan() ;
        LOProject project11 = new LOProject(innerPlan1, genNewOperatorKey(), load1, 0) ;
        project11.setSentinel(true);
        LOProject project12 = new LOProject(innerPlan1, genNewOperatorKey(), load1, 1) ;
        project11.setSentinel(true);
        LOMultiply mul1 = new LOMultiply(innerPlan1, genNewOperatorKey()) ;

        innerPlan1.add(project11) ;
        innerPlan1.add(project12) ;
        innerPlan1.add(mul1) ;

        innerPlan1.connect(project11, mul1);
        innerPlan1.connect(project12, mul1);

        // Create expression inner plan #2
        LogicalPlan innerPlan2 = new LogicalPlan() ;
        LOProject project21 = new LOProject(innerPlan2, genNewOperatorKey(), load1, 0) ;
        project21.setSentinel(true);
        LOConst const21 = new LOConst(innerPlan2, genNewOperatorKey(), 26) ;
        const21.setType(DataType.LONG);
        LOMod mod21 = new LOMod(innerPlan2, genNewOperatorKey()) ;

        innerPlan2.add(project21) ;
        innerPlan2.add(const21) ;
        innerPlan2.add(mod21) ;

        innerPlan2.connect(project21, mod21);
        innerPlan2.connect(const21, mod21) ;

        // List of innerplans
        List<LogicalPlan> innerPlans = new ArrayList<LogicalPlan>() ;
        innerPlans.add(innerPlan1) ;
        innerPlans.add(innerPlan2) ;

        // List of ASC flags
        List<Boolean> ascList = new ArrayList<Boolean>() ;
        ascList.add(true);
        ascList.add(true);

        // Sort
        LOSort sort1 = new LOSort(plan, genNewOperatorKey(), innerPlans,  ascList, null) ;


        plan.add(load1);
        plan.add(sort1);
        plan.connect(load1, sort1) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        TypeCheckingTestUtil.printMessageCollector(collector) ;
        TypeCheckingTestUtil.printTypeGraph(plan) ;

        if (collector.hasError()) {
            throw new AssertionError("Expect no error") ;
        }

        assertEquals(sort1.getSchema(), null) ;

    }

    // Positive expression column
    @Test
    public void testFilter1() throws Throwable {

        printCurrentMethodName();
        // Create outer plan
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", new FuncSpec(pigStorage)),
                                  null, null, true) ;

        // set schemas
        load1.setEnforcedSchema(null) ;

        // Create inner plan
        LogicalPlan innerPlan = new LogicalPlan() ;
        LOProject project1 = new LOProject(innerPlan, genNewOperatorKey(), load1, 0) ;
        project1.setSentinel(true);
        LOProject project2 = new LOProject(innerPlan, genNewOperatorKey(), load1, 1) ;
        project2.setSentinel(true);
        LOAdd add1 = new LOAdd(innerPlan, genNewOperatorKey()) ;
        LOConst const1  = new LOConst(innerPlan, genNewOperatorKey(), 10) ;
        const1.setType(DataType.LONG);

        LOGreaterThan gt1 = new LOGreaterThan(innerPlan,
                                              genNewOperatorKey()) ;

        innerPlan.add(project1) ;
        innerPlan.add(project2) ;
        innerPlan.add(add1) ;
        innerPlan.add(const1) ;
        innerPlan.add(gt1) ;

        innerPlan.connect(project1, add1) ;
        innerPlan.connect(project2, add1) ;
        innerPlan.connect(add1, gt1) ;
        innerPlan.connect(const1, gt1) ;

        // filter
        LOFilter filter1 = new LOFilter(plan, genNewOperatorKey(), innerPlan) ;

        plan.add(load1);
        plan.add(filter1);
        plan.connect(load1, filter1) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        TypeCheckingTestUtil.printMessageCollector(collector) ;
        TypeCheckingTestUtil.printTypeGraph(plan) ;

        if (collector.hasError()) {
            throw new AssertionError("Expect no error") ;
        }

        assertEquals(filter1.getSchema(), null) ;

    }


    // Negative expression column
    @Test
    public void testFilter2() throws Throwable {

        printCurrentMethodName();
        // Create outer plan
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", new FuncSpec(pigStorage)),
                                  null, null, true) ;

        // set schemas
        load1.setEnforcedSchema(null) ;

        // Create inner plan
        LogicalPlan innerPlan = new LogicalPlan() ;
        LOProject project1 = new LOProject(innerPlan, genNewOperatorKey(), load1, 0) ;
        project1.setSentinel(true);
        LOProject project2 = new LOProject(innerPlan, genNewOperatorKey(), load1, 1) ;
        project2.setSentinel(true);
        LOAdd add1 = new LOAdd(innerPlan, genNewOperatorKey()) ;
        LOConst const1  = new LOConst(innerPlan, genNewOperatorKey(), "10") ;
        const1.setType(DataType.CHARARRAY);

        LOGreaterThan gt1 = new LOGreaterThan(innerPlan,
                                              genNewOperatorKey()) ;

        innerPlan.add(project1) ;
        innerPlan.add(project2) ;
        innerPlan.add(add1) ;
        innerPlan.add(const1) ;
        innerPlan.add(gt1) ;

        innerPlan.connect(project1, add1) ;
        innerPlan.connect(project2, add1) ;
        innerPlan.connect(add1, gt1) ;
        innerPlan.connect(const1, gt1) ;

        // filter
        LOFilter filter1 = new LOFilter(plan, genNewOperatorKey(), innerPlan) ;

        plan.add(load1);
        plan.add(filter1);
        plan.connect(load1, filter1) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        try {
            typeValidator.validate(plan, collector) ;
            fail("Exception expected") ;
        }
        catch(PlanValidationException pve) {
            // good
        }

        TypeCheckingTestUtil.printMessageCollector(collector) ;
        TypeCheckingTestUtil.printTypeGraph(plan) ;

        if (!collector.hasError()) {
            throw new AssertionError("Expect an error") ;
        }

    }

    @Test
    public void testCross1() throws Throwable {

        printCurrentMethodName();
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = genDummyLOLoad(plan) ;
        LOLoad load2 = genDummyLOLoad(plan) ;

        // schema for input#1
        Schema inputSchema1 = null ;
        {
            List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
            fsList1.add(new FieldSchema("field1a", DataType.INTEGER)) ;
            fsList1.add(new FieldSchema("field2a", DataType.BYTEARRAY)) ;
            inputSchema1 = new Schema(fsList1) ;
        }

        // set schemas
        load1.setEnforcedSchema(inputSchema1) ;
        load2.setEnforcedSchema(null) ;

        // create union operator
        ArrayList<LogicalOperator> inputList = new ArrayList<LogicalOperator>() ;
        inputList.add(load1) ;
        inputList.add(load2) ;
        LOCross cross = new LOCross(plan, genNewOperatorKey()) ;

        // wiring
        plan.add(load1) ;
        plan.add(load2) ;
        plan.add(cross) ;

        plan.connect(load1, cross);
        plan.connect(load2, cross);

        // validate
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;

        printMessageCollector(collector) ;
        printTypeGraph(plan) ;

        assertEquals(cross.getSchema(), null);

    }


    @Test
    public void testCross2() throws Throwable {

        printCurrentMethodName();
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = genDummyLOLoad(plan) ;
        LOLoad load2 = genDummyLOLoad(plan) ;

        // set schemas
        load1.setEnforcedSchema(null) ;
        load2.setEnforcedSchema(null) ;

        // create union operator
        ArrayList<LogicalOperator> inputList = new ArrayList<LogicalOperator>() ;
        inputList.add(load1) ;
        inputList.add(load2) ;
        LOCross cross = new LOCross(plan, genNewOperatorKey()) ;

        // wiring
        plan.add(load1) ;
        plan.add(load2) ;
        plan.add(cross) ;

        plan.connect(load1, cross);
        plan.connect(load2, cross);

        // validate
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;

        printMessageCollector(collector) ;
        printTypeGraph(plan) ;

        assertEquals(cross.getSchema(), null);

    }


    // Positive test
    @Test
    public void testCOGroupByAtom1() throws Throwable {

        printCurrentMethodName();
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = genDummyLOLoad(plan) ;
        LOLoad load2 = genDummyLOLoad(plan) ;

        // set schemas
        load1.setEnforcedSchema(null) ;
        load2.setEnforcedSchema(null) ;

        // Create expression inner plan #1 of input #1
        LogicalPlan innerPlan11 = new LogicalPlan() ;
        LOProject project111 = new LOProject(innerPlan11, genNewOperatorKey(), load1, 0) ;
        project111.setSentinel(true);
        LOConst const111 = new LOConst(innerPlan11, genNewOperatorKey(), 26F) ;
        const111.setType(DataType.FLOAT);
        LOSubtract subtract111 = new LOSubtract(innerPlan11,
                                                genNewOperatorKey()) ;

        innerPlan11.add(project111) ;
        innerPlan11.add(const111) ;
        innerPlan11.add(subtract111) ;

        innerPlan11.connect(project111, subtract111);
        innerPlan11.connect(const111, subtract111) ;

        // Create expression inner plan #1 of input #2
        LogicalPlan innerPlan12 = new LogicalPlan() ;
        LOProject project121 = new LOProject(innerPlan12, genNewOperatorKey(), load2, 0) ;
        project121.setSentinel(true);
        LOConst const121 = new LOConst(innerPlan12, genNewOperatorKey(), 26) ;
        const121.setType(DataType.INTEGER);
        LOSubtract subtract121 = new LOSubtract(innerPlan12,
                                                genNewOperatorKey()) ;

        innerPlan12.add(project121) ;
        innerPlan12.add(const121) ;
        innerPlan12.add(subtract121) ;

        innerPlan12.connect(project121, subtract121);
        innerPlan12.connect(const121, subtract121) ;

        // Create Cogroup
        ArrayList<LogicalOperator> inputs = new ArrayList<LogicalOperator>() ;
        inputs.add(load1) ;
        inputs.add(load2) ;

        MultiMap<LogicalOperator, LogicalPlan> maps
                            = new MultiMap<LogicalOperator, LogicalPlan>() ;
        maps.put(load1, innerPlan11);
        maps.put(load2, innerPlan12);

        boolean[] isInner = new boolean[inputs.size()] ;
        for (int i=0; i < isInner.length ; i++) {
            isInner[i] = false ;
        }

        LOCogroup cogroup1 = new LOCogroup(plan,
                                           genNewOperatorKey(),
                                           maps,
                                           isInner) ;

        // construct the main plan
        plan.add(load1) ;
        plan.add(load2) ;
        plan.add(cogroup1) ;

        plan.connect(load1, cogroup1);
        plan.connect(load2, cogroup1);

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        printMessageCollector(collector) ;
        printTypeGraph(plan) ;

        if (collector.hasError()) {
            throw new AssertionError("Expect no error") ;
        }

        // check outer schema
        Schema endResultSchema = cogroup1.getSchema() ;

        // Tuple group column
        assertEquals(endResultSchema.getField(0).type, DataType.FLOAT) ;

        // check inner schema1
        assertEquals(endResultSchema.getField(1).schema, null);
        assertEquals(endResultSchema.getField(2).schema, null);

        // check group by col end result
        assertEquals(innerPlan11.getSingleLeafPlanOutputType(), DataType.FLOAT) ;
        assertEquals(innerPlan12.getSingleLeafPlanOutputType(), DataType.FLOAT) ;
    }


    // Positive test
    @Test
    public void testCOGroupByTuple1() throws Throwable {

        printCurrentMethodName();
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = genDummyLOLoad(plan) ;
        LOLoad load2 = genDummyLOLoad(plan) ;

        // set schemas
        load1.setEnforcedSchema(null) ;
        load2.setEnforcedSchema(null) ;

        // Create expression inner plan #1 of input #1
        LogicalPlan innerPlan11 = new LogicalPlan() ;
        LOProject project111 = new LOProject(innerPlan11, genNewOperatorKey(), load1, 0) ;
        project111.setSentinel(true);
        LOConst const111 = new LOConst(innerPlan11, genNewOperatorKey(), 26F) ;
        const111.setType(DataType.FLOAT);
        LOSubtract subtract111 = new LOSubtract(innerPlan11,
                                                genNewOperatorKey()) ;

        innerPlan11.add(project111) ;
        innerPlan11.add(const111) ;
        innerPlan11.add(subtract111) ;

        innerPlan11.connect(project111, subtract111);
        innerPlan11.connect(const111, subtract111) ;

        // Create expression inner plan #2 of input #1
        LogicalPlan innerPlan21 = new LogicalPlan() ;
        LOProject project211 = new LOProject(innerPlan21, genNewOperatorKey(), load1, 0) ;
        project211.setSentinel(true);
        LOProject project212 = new LOProject(innerPlan21, genNewOperatorKey(), load1, 1) ;
        project212.setSentinel(true);

        LOAdd add211 = new LOAdd(innerPlan21,
                                 genNewOperatorKey()) ;

        innerPlan21.add(project211) ;
        innerPlan21.add(project212) ;
        innerPlan21.add(add211) ;

        innerPlan21.connect(project211, add211);
        innerPlan21.connect(project212, add211) ;


        // Create expression inner plan #1 of input #2
        LogicalPlan innerPlan12 = new LogicalPlan() ;
        LOProject project121 = new LOProject(innerPlan12, genNewOperatorKey(), load2, 0) ;
        project121.setSentinel(true);
        LOConst const121 = new LOConst(innerPlan12, genNewOperatorKey(), 26) ;
        const121.setType(DataType.INTEGER);
        LOSubtract subtract121 = new LOSubtract(innerPlan12,
                                                genNewOperatorKey()) ;

        innerPlan12.add(project121) ;
        innerPlan12.add(const121) ;
        innerPlan12.add(subtract121) ;

        innerPlan12.connect(project121, subtract121);
        innerPlan12.connect(const121, subtract121) ;

        // Create expression inner plan #2 of input #2
        LogicalPlan innerPlan22 = new LogicalPlan() ;
        LOConst const122 = new LOConst(innerPlan22, genNewOperatorKey(), 26) ;
        const122.setType(DataType.INTEGER);
        innerPlan22.add(const122) ;

        // Create Cogroup
        ArrayList<LogicalOperator> inputs = new ArrayList<LogicalOperator>() ;
        inputs.add(load1) ;
        inputs.add(load2) ;

        MultiMap<LogicalOperator, LogicalPlan> maps
                            = new MultiMap<LogicalOperator, LogicalPlan>() ;
        maps.put(load1, innerPlan11);
        maps.put(load1, innerPlan21);
        maps.put(load2, innerPlan12);
        maps.put(load2, innerPlan22);

        boolean[] isInner = new boolean[inputs.size()] ;
        for (int i=0; i < isInner.length ; i++) {
            isInner[i] = false ;
        }

        LOCogroup cogroup1 = new LOCogroup(plan,
                                           genNewOperatorKey(),
                                           maps,
                                           isInner) ;

        // construct the main plan
        plan.add(load1) ;
        plan.add(load2) ;
        plan.add(cogroup1) ;

        plan.connect(load1, cogroup1);
        plan.connect(load2, cogroup1);

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        TypeCheckingTestUtil.printMessageCollector(collector) ;
        TypeCheckingTestUtil.printTypeGraph(plan) ;

        if (collector.hasError()) {
            throw new AssertionError("Expect no error") ;
        }

        // check outer schema
        Schema endResultSchema = cogroup1.getSchema() ;

        // Tuple group column
        assertEquals(endResultSchema.getField(0).type, DataType.TUPLE) ;
        assertEquals(endResultSchema.getField(0).schema.getField(0).type, DataType.FLOAT) ;
        assertEquals(endResultSchema.getField(0).schema.getField(1).type, DataType.DOUBLE);

        assertEquals(endResultSchema.getField(1).type, DataType.BAG) ;
        assertEquals(endResultSchema.getField(2).type, DataType.BAG) ;

        // check inner schema1
        assertEquals(endResultSchema.getField(1).schema, null);
        assertEquals(endResultSchema.getField(2).schema, null);

        // check group by col end result
        assertEquals(innerPlan11.getSingleLeafPlanOutputType(), DataType.FLOAT) ;
        assertEquals(innerPlan21.getSingleLeafPlanOutputType(), DataType.DOUBLE) ;
        assertEquals(innerPlan12.getSingleLeafPlanOutputType(), DataType.FLOAT) ;
        assertEquals(innerPlan22.getSingleLeafPlanOutputType(), DataType.DOUBLE) ;
    }



    // Positive test
    @Test
    public void testForEachGenerate1() throws Throwable {

        printCurrentMethodName() ;

        LogicalPlan plan = new LogicalPlan() ;
        LOLoad load1 = genDummyLOLoad(plan) ;

        // set schemas
        load1.setEnforcedSchema(null) ;

        // Create expression inner plan #1
        LogicalPlan innerPlan1 = new LogicalPlan() ;
        LOProject project11 = new LOProject(innerPlan1, genNewOperatorKey(), load1, 0) ;
        project11.setSentinel(true);
        LOConst const11 = new LOConst(innerPlan1, genNewOperatorKey(), 26F) ;
        const11.setType(DataType.FLOAT);
        LOSubtract subtract11 = new LOSubtract(innerPlan1,
                                                genNewOperatorKey()) ;

        innerPlan1.add(project11) ;
        innerPlan1.add(const11) ;
        innerPlan1.add(subtract11) ;

        innerPlan1.connect(project11, subtract11);
        innerPlan1.connect(const11, subtract11) ;

        // Create expression inner plan #2
        LogicalPlan innerPlan2 = new LogicalPlan() ;
        LOProject project21 = new LOProject(innerPlan2, genNewOperatorKey(), load1, 0) ;
        project21.setSentinel(true);
        LOProject project22 = new LOProject(innerPlan2, genNewOperatorKey(), load1, 1) ;
        project21.setSentinel(true);
        LOAdd add21 = new LOAdd(innerPlan2,
                                genNewOperatorKey()) ;

        innerPlan2.add(project21) ;
        innerPlan2.add(project22) ;
        innerPlan2.add(add21) ;

        innerPlan2.connect(project21, add21);
        innerPlan2.connect(project22, add21);

        // List of plans
        ArrayList<LogicalPlan> generatePlans = new ArrayList<LogicalPlan>() ;
        generatePlans.add(innerPlan1);
        generatePlans.add(innerPlan2);

        // List of flatten flags
        ArrayList<Boolean> flattens = new ArrayList<Boolean>() ;
        flattens.add(true) ;
        flattens.add(false) ;

        // Create LOForEach
        LOForEach foreach1 = new LOForEach(plan, genNewOperatorKey(), generatePlans, flattens) ;

        // construct the main plan
        plan.add(load1) ;
        plan.add(foreach1) ;

        plan.connect(load1, foreach1);

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        printMessageCollector(collector) ;
        printTypeGraph(plan) ;

        if (collector.hasError()) {
            throw new AssertionError("Expect no error") ;
        }

        // check outer schema
        Schema endResultSchema = foreach1.getSchema() ;

        assertEquals(endResultSchema.getField(0).type, DataType.FLOAT) ;
        assertEquals(endResultSchema.getField(1).type, DataType.DOUBLE) ;
    }
}
