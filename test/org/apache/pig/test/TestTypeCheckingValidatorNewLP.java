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

import static org.apache.pig.test.utils.TypeCheckingTestUtil.genDummyLOLoadNewLP;
import static org.apache.pig.test.utils.TypeCheckingTestUtil.genFlatSchema;
import static org.apache.pig.test.utils.TypeCheckingTestUtil.genFlatSchemaInTuple;
import static org.apache.pig.test.utils.TypeCheckingTestUtil.printCurrentMethodName;
import static org.apache.pig.test.utils.TypeCheckingTestUtil.printMessageCollector;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.logicalLayer.validators.TypeCheckerException;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.CompilationMessageCollector.Message;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.Util;
import org.apache.pig.newplan.logical.expression.AddExpression;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.BinCondExpression;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.DereferenceExpression;
import org.apache.pig.newplan.logical.expression.DivideExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanExpression;
import org.apache.pig.newplan.logical.expression.LessThanEqualExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.MapLookupExpression;
import org.apache.pig.newplan.logical.expression.ModExpression;
import org.apache.pig.newplan.logical.expression.MultiplyExpression;
import org.apache.pig.newplan.logical.expression.NegativeExpression;
import org.apache.pig.newplan.logical.expression.NotEqualExpression;
import org.apache.pig.newplan.logical.expression.NotExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.RegexExpression;
import org.apache.pig.newplan.logical.expression.SubtractExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.newplan.logical.visitor.CastLineageSetter;
import org.apache.pig.newplan.logical.visitor.ColumnAliasConversionVisitor;
import org.apache.pig.newplan.logical.visitor.TypeCheckingExpVisitor;
import org.apache.pig.newplan.logical.visitor.TypeCheckingRelVisitor;
import org.apache.pig.parser.ParserTestingUtils;
import org.apache.pig.test.utils.LogicalPlanTester;
import org.junit.Before;
import org.junit.Test;

public class TestTypeCheckingValidatorNewLP {

    PigContext pc = new PigContext(ExecType.LOCAL, new Properties());
    LogicalPlanTester planTester;
    private static final String CAST_LOAD_NOT_FOUND = 
        "Cannot resolve load function to use for casting from bytearray";
    
    
    /* (non-Javadoc)
     * @see junit.framework.TestCase#setUp()
     */
    @Before
    public void setUp() throws Exception {
        // create a new instance of the plan tester
        // for each test so that different tests do not
        // interact with each other's plans
        planTester = new LogicalPlanTester(pc) ;
    }

    private static final String simpleEchoStreamingCommand;
    static {
        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
            simpleEchoStreamingCommand = "perl -ne 'print \\\"$_\\\"'";
        else
            simpleEchoStreamingCommand = "perl -ne 'print \"$_\"'";
        File fileA = new File("a");
        File fileB = new File("b");
        try {
            fileA.delete();
            fileB.delete();
            if(!fileA.createNewFile() || !fileB.createNewFile())
                fail("Unable to create input files");
        } catch (IOException e) {
            fail("Unable to create input files:" + e.getMessage());
        }
        fileA.deleteOnExit();
        fileB.deleteOnExit();
    }

    @Test
    public void testExpressionTypeChecking1() throws Throwable {
        LogicalExpressionPlan expPlan = new LogicalExpressionPlan();

        ConstantExpression constant1 = new ConstantExpression(expPlan, 10);

        ConstantExpression constant2 =  new ConstantExpression(expPlan, 20D) ;
        ConstantExpression constant3 =  new ConstantExpression(expPlan, 123f) ;

        AddExpression add1 = new AddExpression(expPlan, constant1, constant2) ;
        CastExpression cast1 = new CastExpression(expPlan,constant3, createFS(DataType.DOUBLE)) ;
        MultiplyExpression mul1 = new MultiplyExpression(expPlan, add1, cast1) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingExpVisitor expTypeChecker = new TypeCheckingExpVisitor(expPlan, collector, null);
        expTypeChecker.visit();
        printMessageCollector(collector) ;
        //printTypeGraph(plan) ;

        if (collector.hasError()) {
            throw new Exception("Error during type checking") ;
        }       

        // Induction check
        assertEquals(DataType.DOUBLE, add1.getType()) ;
        assertEquals(DataType.DOUBLE, mul1.getType()) ;

        // Cast insertion check
        assertEquals(DataType.DOUBLE, add1.getLhs().getType()) ;
        assertEquals(DataType.DOUBLE, mul1.getRhs().getType()) ;

    }

    private LogicalFieldSchema createFS(byte datatype) {
        return new LogicalFieldSchema(null, null, datatype);
    }

    @Test
    public void testExpressionTypeCheckingFail1() throws Throwable {
        LogicalExpressionPlan plan = new LogicalExpressionPlan() ;
        ConstantExpression constant1 = new ConstantExpression(plan, 10) ;
        ConstantExpression constant2 =  new ConstantExpression(plan, 20D) ;
        ConstantExpression constant3 =  new ConstantExpression(plan, "123") ;

        AddExpression add1 = new AddExpression(plan, constant1, constant2) ;
        CastExpression cast1 = new CastExpression(plan, constant3,  createFS(DataType.BYTEARRAY)) ;
        MultiplyExpression mul1 = new MultiplyExpression(plan, add1, cast1) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingExpVisitor expTypeChecker = new TypeCheckingExpVisitor(plan, collector, null);
        try {
            expTypeChecker.visit();
            fail("Exception expected") ;
        }
        catch (TypeCheckerException pve) {
            // good
        }
        printMessageCollector(collector) ;
        //printTypeGraph(plan) ;

        if (!collector.hasError()) {
            throw new Exception("Error during type checking") ;
        }       
    }

    @Test
    public void testExpressionTypeChecking2() throws Throwable {
        LogicalExpressionPlan plan = new LogicalExpressionPlan() ;
        ConstantExpression constant1 = new ConstantExpression(plan, 10) ;
        ConstantExpression constant2 =  new ConstantExpression(plan,  new DataByteArray()) ;
        ConstantExpression constant3 =  new ConstantExpression(plan,  123L) ;
        ConstantExpression constant4 =  new ConstantExpression(plan,  true) ;

        SubtractExpression sub1 = new SubtractExpression(plan, constant1, constant2) ;
        GreaterThanExpression gt1 = new GreaterThanExpression(plan, sub1, constant3) ;
        AndExpression and1 = new AndExpression(plan, gt1, constant4) ;
        NotExpression not1 = new NotExpression(plan, and1) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingExpVisitor expTypeChecker = new TypeCheckingExpVisitor(plan, collector, null);
        expTypeChecker.visit();


        printMessageCollector(collector) ;
        //printTypeGraph(plan) ;

        if (collector.hasError()) {
            throw new Exception("Error not expected during type checking") ;
        }       


        // Induction check   
        assertEquals(DataType.INTEGER, sub1.getType()) ;    
        assertEquals(DataType.BOOLEAN, gt1.getType()) ;     
        assertEquals(DataType.BOOLEAN, and1.getType()) ;    
        assertEquals(DataType.BOOLEAN, not1.getType()) ;    

        // Cast insertion check     
        assertEquals(DataType.INTEGER, sub1.getRhs().getType()) ;    
        assertEquals(DataType.LONG, gt1.getLhs().getType()) ;

    }


    @Test
    public void testExpressionTypeChecking3() throws Throwable {
        LogicalExpressionPlan plan = new LogicalExpressionPlan() ;
        ConstantExpression constant1 = new ConstantExpression(plan, 10) ;
        ConstantExpression constant2 =  new ConstantExpression(plan, 20L) ;
        ConstantExpression constant3 =  new ConstantExpression(plan, 123) ;

        ModExpression mod1 = new ModExpression(plan, constant1, constant2) ;
        EqualExpression equal1 = new EqualExpression(plan, mod1, constant3) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingExpVisitor expTypeChecker = new TypeCheckingExpVisitor(plan, collector, null);
        expTypeChecker.visit();

        plan.explain(System.out, "text", true);

        printMessageCollector(collector) ;
        //printTypeGraph(plan) ;

        if (collector.hasError()) {
            throw new Exception("Error during type checking") ;
        }      

        // Induction check
        assertEquals(DataType.LONG, mod1.getType()) ;
        assertEquals(DataType.BOOLEAN, equal1.getType()) ;

        // Cast insertion check
        assertEquals(DataType.LONG, mod1.getLhs().getType()) ;
        assertEquals(DataType.LONG, equal1.getRhs().getType()) ;

    }

    @Test
    public void testExpressionTypeChecking4() throws Throwable {
        LogicalExpressionPlan plan = new LogicalExpressionPlan() ;
        ConstantExpression constant1 = new ConstantExpression(plan, 10) ;
        ConstantExpression constant2 =  new ConstantExpression(plan, 20D) ;
        ConstantExpression constant3 =  new ConstantExpression(plan,  123f) ;

        DivideExpression div1 = new DivideExpression(plan, constant1, constant2) ;
        CastExpression cast1 = new CastExpression(plan, constant3, createFS(DataType.DOUBLE));
        NotEqualExpression notequal1 = new NotEqualExpression(plan, div1, cast1) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingExpVisitor expTypeChecker = new TypeCheckingExpVisitor(plan, collector, null);
        expTypeChecker.visit();        
        printMessageCollector(collector) ;
        //printTypeGraph(plan) ;

        if (collector.hasError()) {
            throw new Exception("Error during type checking") ;
        }  

        // Induction check
        assertEquals(DataType.DOUBLE, div1.getType()) ;
        assertEquals(DataType.BOOLEAN, notequal1.getType()) ;

        // Cast insertion check
        assertEquals(DataType.DOUBLE, div1.getLhs().getType()) ;
        assertEquals(DataType.DOUBLE, notequal1.getRhs().getType()) ;

    }

    @Test
    public void testExpressionTypeCheckingFail4() throws Throwable {
        LogicalExpressionPlan plan = new LogicalExpressionPlan() ;
        ConstantExpression constant1 = new ConstantExpression(plan, 10) ;
        ConstantExpression constant2 =  new ConstantExpression(plan, 20D) ;
        ConstantExpression constant3 =  new ConstantExpression(plan, "123") ;

        DivideExpression div1 = new DivideExpression(plan, constant1, constant2) ;
        CastExpression cast1 = new CastExpression(plan,  constant3, createFS(DataType.BYTEARRAY)) ;
        NotEqualExpression notequal1 = new NotEqualExpression(plan, div1, cast1) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingExpVisitor expTypeChecker = new TypeCheckingExpVisitor(plan, collector, null);

        try{
            expTypeChecker.visit();        
            fail("Exception expected") ;
        }
        catch (TypeCheckerException pve) {
            // good
        }
        printMessageCollector(collector) ;
        //printTypeGraph(plan) ;

        if (!collector.hasError()) {
            throw new Exception("Error during type checking") ;
        }  
    }

    @Test
    public void testExpressionTypeChecking5() throws Throwable {
        LogicalExpressionPlan plan = new LogicalExpressionPlan() ;
        ConstantExpression constant1 = new ConstantExpression(plan, 10F) ;
        ConstantExpression constant2 =  new ConstantExpression(plan, 20L) ;
        ConstantExpression constant3 =  new ConstantExpression(plan, 123F) ;
        ConstantExpression constant4 =  new ConstantExpression(plan, 123D) ;

        LessThanEqualExpression lesser1 = new LessThanEqualExpression(plan, constant1, constant2) ;
        BinCondExpression bincond1 = new BinCondExpression(plan, lesser1, constant3, constant4) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingExpVisitor expTypeChecker = new TypeCheckingExpVisitor(plan, collector, null);
        expTypeChecker.visit();             
        printMessageCollector(collector) ;
        //printTypeGraph(plan) ;

        if (collector.hasError()) {
            throw new Exception("Error during type checking") ;
        }  

        // Induction check
        assertEquals(DataType.BOOLEAN, lesser1.getType()) ;
        assertEquals(DataType.DOUBLE, bincond1.getType()) ;

        // Cast insertion check
        assertEquals(DataType.FLOAT, lesser1.getLhs().getType()) ;
        assertEquals(DataType.FLOAT, lesser1.getRhs().getType()) ;
        assertEquals(DataType.DOUBLE, bincond1.getLhs().getType()) ;
        assertEquals(DataType.DOUBLE, bincond1.getRhs().getType()) ;

    }

    @Test
    public void testExpressionTypeChecking6() throws Throwable {
        LogicalExpressionPlan plan = new LogicalExpressionPlan() ;
        ConstantExpression constant1 = new ConstantExpression(plan, "10") ;
        ConstantExpression constant2 =  new ConstantExpression(plan, 20L) ;

        AddExpression add1 = new AddExpression(plan, constant1, constant2) ; 

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        LogicalRelationalOperator dummyRelOp = createDummyRelOpWithAlias();
        TypeCheckingExpVisitor expTypeChecker = new TypeCheckingExpVisitor(plan, collector, dummyRelOp);
        try {
            expTypeChecker.visit();        
            fail("Exception expected") ;
        }
        catch (TypeCheckerException pve) {
            // good
        }
        printMessageCollector(collector) ;
        //printTypeGraph(plan) ;

        if (!collector.hasError()) {
            throw new AssertionError("Error expected") ;
        }         

    }

    /**
     * @return a dummy logical relational operator 
     */
    private LogicalRelationalOperator createDummyRelOpWithAlias() {
        class DummyRelOp extends LogicalRelationalOperator{
            DummyRelOp(){
                super("dummy", new LogicalPlan());
                this.alias = "dummy";
            }

            @Override
            public LogicalSchema getSchema() throws FrontendException {
                          return null;
            }

            @Override
            public void accept(PlanVisitor v) throws FrontendException {

            }

            @Override
            public boolean isEqual(Operator operator) throws FrontendException {
                return false;
            }

        }
        return new DummyRelOp();
    }




    @Test
    public void testExpressionTypeChecking7() throws Throwable {
        LogicalExpressionPlan plan = new LogicalExpressionPlan() ;
        ConstantExpression constant1 = new ConstantExpression(plan, 10) ;
        ConstantExpression constant2 =  new ConstantExpression(plan, 20D) ;
        ConstantExpression constant3 =  new ConstantExpression(plan, 123L) ;

        GreaterThanExpression gt1 = new GreaterThanExpression(plan, constant1, constant2) ;
        EqualExpression equal1 = new EqualExpression(plan, gt1, constant3) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        LogicalRelationalOperator dummyRelOp = createDummyRelOpWithAlias();
        TypeCheckingExpVisitor expTypeChecker = new TypeCheckingExpVisitor(plan, collector, dummyRelOp);
        try {
            expTypeChecker.visit();        
            fail("Exception expected") ;
        }
        catch (TypeCheckerException pve) {
            // good
        }      
        printMessageCollector(collector) ;
        //printTypeGraph(plan) ;

        if (!collector.hasError()) {
            throw new AssertionError("Error expected") ;
        }   
    }

    @Test
    public void testExpressionTypeChecking8() throws Throwable {
        LogicalExpressionPlan plan = new LogicalExpressionPlan() ;

        TupleFactory tupleFactory = TupleFactory.getInstance();

        ArrayList<Object> innerObjList = new ArrayList<Object>(); 
        ArrayList<Object> objList = new ArrayList<Object>(); 

        innerObjList.add(10);
        innerObjList.add(3);
        innerObjList.add(7);
        innerObjList.add(17);

        Tuple innerTuple = tupleFactory.newTuple(innerObjList);

        objList.add("World");
        objList.add(42);
        objList.add(innerTuple);

        Tuple tuple = tupleFactory.newTuple(objList);

        ArrayList<Schema.FieldSchema> innerFss = new ArrayList<Schema.FieldSchema>();
        ArrayList<Schema.FieldSchema> fss = new ArrayList<Schema.FieldSchema>();
        ArrayList<Schema.FieldSchema> castFss = new ArrayList<Schema.FieldSchema>();

        Schema.FieldSchema stringFs = new Schema.FieldSchema(null, DataType.CHARARRAY);
        Schema.FieldSchema intFs = new Schema.FieldSchema(null, DataType.INTEGER);

        for(int i = 0; i < innerObjList.size(); ++i) {
            innerFss.add(intFs);
        }

        Schema innerTupleSchema = new Schema(innerFss);

        fss.add(stringFs);
        fss.add(intFs);
        fss.add(new Schema.FieldSchema(null, innerTupleSchema, DataType.TUPLE));

        Schema tupleSchema = new Schema(fss);

        Schema.FieldSchema byteArrayFs = new Schema.FieldSchema(null, DataType.BYTEARRAY);
        for(int i = 0; i < 4; ++i) {
            castFss.add(byteArrayFs);
        }

        Schema castSchema = new Schema(castFss);


        ConstantExpression constant1 = new ConstantExpression(plan, innerTuple) ;
        ConstantExpression constant2 =  new ConstantExpression(plan, tuple) ;
        CastExpression cast1 = new CastExpression(plan, constant1,
                Util.translateFieldSchema(new FieldSchema(null, castSchema, DataType.TUPLE))) ;

        EqualExpression equal1 = new EqualExpression(plan, cast1, constant2) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;

        LogicalRelationalOperator dummyRelOp = createDummyRelOpWithAlias();
        TypeCheckingExpVisitor expTypeChecker = new TypeCheckingExpVisitor(plan, collector, dummyRelOp);
        expTypeChecker.visit();    
        printMessageCollector(collector) ;
        //printTypeGraph(plan) ;

        if (collector.hasError()) {
            throw new Exception("Error during type checking") ;
        }   

        assertEquals(DataType.BOOLEAN, equal1.getType()) ;
        assertEquals(DataType.TUPLE, equal1.getRhs().getType()) ;
        assertEquals(DataType.TUPLE, equal1.getLhs().getType()) ;
    }

    /*
     * chararray can been cast to int when jira-893 been resolved
     */
    @Test
    public void testExpressionTypeChecking9() throws Throwable {
        LogicalExpressionPlan plan = new LogicalExpressionPlan() ;

        TupleFactory tupleFactory = TupleFactory.getInstance();

        ArrayList<Object> innerObjList = new ArrayList<Object>(); 
        ArrayList<Object> objList = new ArrayList<Object>(); 

        innerObjList.add("10");
        innerObjList.add("3");
        innerObjList.add(7);
        innerObjList.add("17");

        Tuple innerTuple = tupleFactory.newTuple(innerObjList);

        objList.add("World");
        objList.add(42);
        objList.add(innerTuple);

        Tuple tuple = tupleFactory.newTuple(objList);

        ArrayList<Schema.FieldSchema> innerFss = new ArrayList<Schema.FieldSchema>();
        ArrayList<Schema.FieldSchema> fss = new ArrayList<Schema.FieldSchema>();
        ArrayList<Schema.FieldSchema> castFss = new ArrayList<Schema.FieldSchema>();

        Schema.FieldSchema stringFs = new Schema.FieldSchema(null, DataType.CHARARRAY);
        Schema.FieldSchema intFs = new Schema.FieldSchema(null, DataType.INTEGER);
        Schema.FieldSchema doubleFs = new Schema.FieldSchema(null, DataType.DOUBLE);

        innerFss.add(stringFs);
        innerFss.add(stringFs);
        innerFss.add(intFs);
        innerFss.add(stringFs);

        Schema innerTupleSchema = new Schema(innerFss);

        fss.add(stringFs);
        fss.add(intFs);
        fss.add(new Schema.FieldSchema(null, innerTupleSchema, DataType.TUPLE));

        Schema tupleSchema = new Schema(fss);

        castFss.add(stringFs);
        castFss.add(stringFs);
        castFss.add(doubleFs);
        castFss.add(intFs);

        Schema castSchema = new Schema(castFss);


        ConstantExpression constant1 = new ConstantExpression(plan, innerTuple) ;
        ConstantExpression constant2 =  new ConstantExpression(plan, tuple) ;
        CastExpression cast1 = new CastExpression(plan, constant1,
                Util.translateFieldSchema(new FieldSchema(null, castSchema, DataType.TUPLE))) ;

        EqualExpression equal1 = new EqualExpression(plan, cast1, constant2) ;
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        LogicalRelationalOperator dummyRelOp = createDummyRelOpWithAlias();
        TypeCheckingExpVisitor expTypeChecker = new TypeCheckingExpVisitor(plan, collector, dummyRelOp);
        try {
            expTypeChecker.visit();        
        }
        catch (TypeCheckerException pve) {
            fail("Exception not expected") ;
        } 

        printMessageCollector(collector) ;
        //printTypeGraph(plan) ;

        if (collector.hasError()) {
            throw new Exception("Error expected") ;
        }   
    }

    @Test
    public void testArithmeticOpCastInsert1() throws Throwable {
        LogicalExpressionPlan plan = new LogicalExpressionPlan() ;
        ConstantExpression constant1 = new ConstantExpression(plan, 10) ;
        ConstantExpression constant2 =  new ConstantExpression(plan, 20D) ;

        MultiplyExpression mul1 = new MultiplyExpression(plan,constant1, constant2) ;

        // Before type checking its set correctly - PIG-421
//        System.out.println(DataType.findTypeName(mul1.getType())) ;
//        assertEquals(DataType.DOUBLE, mul1.getType()) ;    

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingExpVisitor expTypeChecker = new TypeCheckingExpVisitor(plan, collector, null);
        expTypeChecker.visit();       
        printMessageCollector(collector) ;

        //printTypeGraph(plan) ;

        // After type checking
        System.out.println(DataType.findTypeName(mul1.getType())) ;
        assertEquals(DataType.DOUBLE, mul1.getType()) ;
    }

    @Test
    public void testArithmeticOpCastInsert2() throws Throwable {
        LogicalExpressionPlan plan = new LogicalExpressionPlan() ;
        ConstantExpression constant1 = new ConstantExpression(plan, 10) ;
        ConstantExpression constant2 =  new ConstantExpression(plan, 20L) ;

        NegativeExpression neg1 = new NegativeExpression(plan, constant1) ;
        SubtractExpression subtract1 = new SubtractExpression(plan, neg1, constant2) ;

        // Before type checking its set correctly = PIG-421
//        assertEquals(DataType.LONG, subtract1.getType()) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingExpVisitor expTypeChecker = new TypeCheckingExpVisitor(plan, collector, null);
        expTypeChecker.visit();   
        printMessageCollector(collector) ;

        //printTypeGraph(plan) ;

        // After type checking
        System.out.println(DataType.findTypeName(subtract1.getType())) ;
        assertEquals(DataType.LONG, subtract1.getType()) ;

        assertTrue(subtract1.getLhs() instanceof CastExpression);
        assertEquals(((CastExpression)subtract1.getLhs()).getType(), DataType.LONG);
        assertTrue(((CastExpression)subtract1.getLhs()).getExpression() == neg1);
    }

    @Test
    public void testModCastInsert1() throws Throwable {
        LogicalExpressionPlan plan = new LogicalExpressionPlan() ;
        ConstantExpression constant1 = new ConstantExpression(plan, 10) ;
        ConstantExpression constant2 =  new ConstantExpression(plan, 20L) ;

        ModExpression mod1 = new ModExpression(plan, constant1, constant2) ;

        // Before type checking its set correctly = PIG-421
//        assertEquals(DataType.LONG, mod1.getType()) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingExpVisitor expTypeChecker = new TypeCheckingExpVisitor(plan, collector, null);
        expTypeChecker.visit();   
        printMessageCollector(collector) ;

        //printTypeGraph(plan) ;

        // After type checking
        System.out.println(DataType.findTypeName(mod1.getType())) ;
        assertEquals(DataType.LONG, mod1.getType()) ;

        assertTrue(mod1.getLhs() instanceof CastExpression);
        assertEquals(((CastExpression)mod1.getLhs()).getType(), DataType.LONG);
        assertTrue(((CastExpression)mod1.getLhs()).getExpression() == constant1);
    }
    
    
        // Positive case
        @Test
        public void testRegexTypeChecking1() throws Throwable {
            LogicalExpressionPlan plan = new LogicalExpressionPlan() ;
            ConstantExpression constant1 = new ConstantExpression(plan, "10") ;
            ConstantExpression constant2 = new ConstantExpression(plan, "Regex");
    
            RegexExpression regex = new RegexExpression(plan, constant1, constant2) ;
    
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingExpVisitor expTypeChecker = new TypeCheckingExpVisitor(plan, collector, null);
            expTypeChecker.visit();  
            printMessageCollector(collector) ;
    
            //printTypeGraph(plan) ;
    
            // After type checking
            System.out.println(DataType.findTypeName(regex.getType())) ;
            assertEquals(DataType.BOOLEAN, regex.getType()) ;
        }
    
        // Positive case with cast insertion
        @Test
        public void testRegexTypeChecking2() throws Throwable {
            LogicalExpressionPlan plan = new LogicalExpressionPlan() ;
            ConstantExpression constant1 = new ConstantExpression(plan, 
                    new DataByteArray()
            ) ;
            ConstantExpression constant2 = new ConstantExpression(plan, "Regex");
    
            RegexExpression regex = new RegexExpression(plan, constant1, constant2);

            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingExpVisitor expTypeChecker = new TypeCheckingExpVisitor(plan, collector, null);
            expTypeChecker.visit();  
            printMessageCollector(collector) ;
    
            //printTypeGraph(plan) ;
    
            // After type checking
    
            if (collector.hasError()) {
                throw new Exception("Error not expected during type checking") ;
            }       
            
            // check type
            System.out.println(DataType.findTypeName(regex.getType())) ;
            assertEquals(DataType.BOOLEAN, regex.getType()) ;
                                             
            // check wiring      
            CastExpression cast = (CastExpression) regex.getLhs() ;      
            assertEquals(cast.getType(), DataType.CHARARRAY);    
            assertEquals(cast.getExpression(), constant1) ;
        }
    
            // Negative case
        @Test
        public void testRegexTypeChecking3() throws Throwable {
            LogicalExpressionPlan plan = new LogicalExpressionPlan() ;
            ConstantExpression constant1 = new ConstantExpression(plan, 10) ;
            ConstantExpression constant2 = new ConstantExpression(plan, "Regex");
            
            RegexExpression regex = new RegexExpression(plan, constant1, constant2);
    
            try {
                CompilationMessageCollector collector = new CompilationMessageCollector() ;
                TypeCheckingExpVisitor expTypeChecker = new TypeCheckingExpVisitor(plan, collector, null);
                expTypeChecker.visit();  
                printMessageCollector(collector) ;
                fail("Exception expected") ;
            }
            catch (TypeCheckerException pve) {
                // good
            }
    
        }
    
        // Expression plan has to support DAG before this can be used.
        // Currently it supports only tree.
    
        /*
        @Test
        public void testDiamondShapedExpressionPlan1() throws Throwable {
            LogicalExpressionPlan plan = new LogicalExpressionPlan() ;
            ConstantExpression constant1 = new ConstantExpression(plan, 10) ;
            constant1.setType(DataType.LONG) ;
    
            LONegative neg1 = new LONegative(plan, constant1) ;
            LONegative neg2 = new LONegative(plan, constant1) ;
    
            DivideExpression div1 = new DivideExpression(plan, neg1, neg2) ;
    
            LONegative neg3 = new LONegative(plan, div1) ;
            LONegative neg4 = new LONegative(plan, div1) ;
    
            AddExpression add1 = new AddExpression(plan, neg3, neg4) ;
    
            plan.add(constant1) ;
            plan.add(neg1) ;
            plan.add(neg2) ;
            plan.add(div1) ;
            plan.add(neg3) ;
            plan.add(neg4) ;
            plan.add(add1) ;
    
            plan.connect(constant1, neg1) ;
            plan.connect(constant1, neg2) ;
    
            plan.connect(neg1, div1) ;
            plan.connect(neg2, div1) ;
    
            plan.connect(div1, neg3) ;
            plan.connect(div1, neg3) ;
    
            plan.connect(neg3, add1) ;
            plan.connect(neg4, add1) ;
    
            // Before type checking
            assertEquals(DataType.UNKNOWN, add1.getType()) ;
    
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
            typeValidator.validate(plan, collector) ;
            printMessageCollector(collector) ;
    
            //printTypeGraph(plan) ;
    
            // After type checking
            assertEquals(DataType.LONG, div1.getType()) ;
            assertEquals(DataType.LONG, add1.getType()) ;
    
        }
        */
        
        // This tests when both inputs need casting
        //@Test
        public void testUnionCastingInsert1() throws Throwable {
    
            printCurrentMethodName();
            LogicalPlan plan = new LogicalPlan() ;
    
            String pigStorage = PigStorage.class.getName() ;
            
            LOLoad load1 = new LOLoad(
                    new FileSpec("pi", new FuncSpec(pigStorage)),
                    null, plan, null
            );
                
            LOLoad load2 = new LOLoad(
                    new FileSpec("pi", new FuncSpec(pigStorage)),
                    null, plan, null
            );
//                
//                new LOLoad(plan,
//                                      
//                                      new FileSpec("pi", new FuncSpec(pigStorage)),
//                                      null) ;
//            LOLoad load2 = new LOLoad(plan,
//                                      
//                                      new FileSpec("pi", new FuncSpec(pigStorage)),
//                                      null) ;
//    
            // schema for input#1
            Schema inputSchema1 = null ;
            {
                List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
                fsList1.add(new FieldSchema("field1a", DataType.INTEGER)) ;
                fsList1.add(new FieldSchema("field2a", DataType.LONG)) ;
                fsList1.add(new FieldSchema(null, DataType.BYTEARRAY)) ;
                fsList1.add(new FieldSchema(null, DataType.CHARARRAY)) ;
                inputSchema1 = new Schema(fsList1) ;
            }
    
            // schema for input#2
            Schema inputSchema2 = null ;
            {
                List<FieldSchema> fsList2 = new ArrayList<FieldSchema>() ;
                fsList2.add(new FieldSchema("field1b", DataType.DOUBLE)) ;
                fsList2.add(new FieldSchema(null, DataType.INTEGER)) ;
                fsList2.add(new FieldSchema("field3b", DataType.FLOAT)) ;
                fsList2.add(new FieldSchema("field4b", DataType.CHARARRAY)) ;
                inputSchema2 = new Schema(fsList2) ;
            }
    
            // set schemas
            load1.setScriptSchema(Util.translateSchema(inputSchema1));
            load2.setScriptSchema(Util.translateSchema(inputSchema2));
    
            // create union operator
            ArrayList<LogicalRelationalOperator> inputList = new ArrayList<LogicalRelationalOperator>() ;
            inputList.add(load1) ;
            inputList.add(load2) ;
            LOUnion union = new LOUnion(plan, false) ;
    
            // wiring
            plan.add(load1) ;
            plan.add(load2) ;
            plan.add(union) ;
    
            plan.connect(load1, union);
            plan.connect(load2, union);
    
            // validate
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            typeChecker.visit();  
            printMessageCollector(collector) ;
    
            // check end result schema
            Schema outputSchema = Util.translateSchema(union.getSchema()) ;
    
            Schema expectedSchema = null ;
            {
                List<FieldSchema> fsListExpected = new ArrayList<FieldSchema>() ;
                fsListExpected.add(new FieldSchema("field1a", DataType.DOUBLE)) ;
                fsListExpected.add(new FieldSchema("field2a", DataType.LONG)) ;
                fsListExpected.add(new FieldSchema("field3b", DataType.FLOAT)) ;
                fsListExpected.add(new FieldSchema("field4b", DataType.CHARARRAY)) ;
                expectedSchema = new Schema(fsListExpected) ;
            }
    
            assertTrue(Schema.equals(outputSchema, expectedSchema, true, false)) ;
    
            //printTypeGraph(plan) ;
    
            // check the inserted casting of input1
            {
                // Check wiring
                List<Operator> sucList1 = plan.getSuccessors(load1);
                assertEquals(sucList1.size(), 1);
                LOForEach foreach = (LOForEach) sucList1.get(0) ;
                assertTrue(foreach instanceof LOForEach) ;
    
                List<Operator> sucList2 = plan.getSuccessors(foreach);
                assertEquals(sucList2.size(), 1);
                assertTrue(sucList2.get(0) instanceof LOUnion) ;
    
                // Check inserted casting
                checkForEachCasting(foreach, 0, true, DataType.DOUBLE) ;
                checkForEachCasting(foreach, 1, false, DataType.UNKNOWN) ;
                checkForEachCasting(foreach, 2, true, DataType.FLOAT) ;
                checkForEachCasting(foreach, 3, false, DataType.UNKNOWN) ;
    
            }
    
            // check the inserted casting of input2
            {
                // Check wiring
                List<Operator> sucList1 = plan.getSuccessors(load2) ;
                assertEquals(sucList1.size(), 1);
                LOForEach foreach = (LOForEach) sucList1.get(0) ;
                assertTrue(foreach instanceof LOForEach) ;
    
                List<Operator> sucList2 = plan.getSuccessors(foreach) ;
                assertEquals(sucList2.size(), 1);
                assertTrue(sucList2.get(0) instanceof LOUnion) ;
    
                // Check inserted casting
                checkForEachCasting(foreach, 0, false, DataType.UNKNOWN) ;
                checkForEachCasting(foreach, 1, true, DataType.LONG) ;
                checkForEachCasting(foreach, 2, false, DataType.UNKNOWN) ;
                checkForEachCasting(foreach, 3, false, DataType.UNKNOWN) ;
    
            }
    
        }
    //
    //
    //    // This tests when both only on input needs casting
    //    //@Test
    //    public void testUnionCastingInsert2() throws Throwable {
    //
    //        printCurrentMethodName();
    //        LogicalPlan plan = new LogicalPlan() ;
    //
    //        String pigStorage = PigStorage.class.getName() ;
    //
    //        LOLoad load1 = new LOLoad(plan,
    //                                  
    //                                  new FileSpec("pi", new FuncSpec(pigStorage)),
    //                                  null) ;
    //        LOLoad load2 = new LOLoad(plan,
    //                                  
    //                                  new FileSpec("pi", new FuncSpec(pigStorage)),
    //                                  null) ;
    //
    //        // schema for input#1
    //        Schema inputSchema1 = null ;
    //        {
    //            List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
    //            fsList1.add(new FieldSchema("field1a", DataType.INTEGER)) ;
    //            fsList1.add(new FieldSchema("field2a", DataType.BYTEARRAY)) ;
    //            inputSchema1 = new Schema(fsList1) ;
    //        }
    //
    //        // schema for input#2
    //        Schema inputSchema2 = null ;
    //        {
    //            List<FieldSchema> fsList2 = new ArrayList<FieldSchema>() ;
    //            fsList2.add(new FieldSchema("field1b", DataType.DOUBLE)) ;
    //            fsList2.add(new FieldSchema("field2b", DataType.DOUBLE)) ;
    //            inputSchema2 = new Schema(fsList2) ;
    //        }
    //
    //        // set schemas
    //        load1.setEnforcedSchema(inputSchema1) ;
    //        load2.setEnforcedSchema(inputSchema2) ;
    //
    //        // create union operator
    //        ArrayList<LogicalOperator> inputList = new ArrayList<LogicalOperator>() ;
    //        inputList.add(load1) ;
    //        inputList.add(load2) ;
    //        LOUnion union = new LOUnion(plan) ;
    //
    //        // wiring
    //        plan.add(load1) ;
    //        plan.add(load2) ;
    //        plan.add(union) ;
    //
    //        plan.connect(load1, union);
    //        plan.connect(load2, union);
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
    //        typeValidator.validate(plan, collector) ;
    //        printMessageCollector(collector) ;
    //
    //        // check end result schema
    //        Schema outputSchema = union.getSchema() ;
    //
    //        Schema expectedSchema = null ;
    //        {
    //            List<FieldSchema> fsListExpected = new ArrayList<FieldSchema>() ;
    //            fsListExpected.add(new FieldSchema("field1a", DataType.DOUBLE)) ;
    //            fsListExpected.add(new FieldSchema("field2a", DataType.DOUBLE)) ;
    //            expectedSchema = new Schema(fsListExpected) ;
    //        }
    //
    //        assertTrue(Schema.equals(outputSchema, expectedSchema, true, false)) ;
    //
    //        //printTypeGraph(plan) ;
    //
    //        // check the inserted casting of input1
    //        {
    //            // Check wiring
    //            List<LogicalOperator> sucList1 = plan.getSuccessors(load1) ;
    //            assertEquals(sucList1.size(), 1);
    //            LOForEach foreach = (LOForEach) sucList1.get(0) ;
    //            assertTrue(foreach instanceof LOForEach) ;
    //
    //            List<LogicalOperator> sucList2 = plan.getSuccessors(foreach) ;
    //            assertEquals(sucList2.size(), 1);
    //            assertTrue(sucList2.get(0) instanceof LOUnion) ;
    //
    //            // Check inserted casting
    //            checkForEachCasting(foreach, 0, true, DataType.DOUBLE) ;
    //            checkForEachCasting(foreach, 1, true, DataType.DOUBLE) ;
    //
    //        }
    //
    //        // check the inserted casting of input2
    //        {
    //            // Check wiring
    //            List<LogicalOperator> sucList1 = plan.getSuccessors(load2) ;
    //            assertEquals(sucList1.size(), 1);
    //            assertTrue(sucList1.get(0) instanceof LOUnion) ;
    //        }
    //
    //    }
    //    
    //    // This has to fail under strict typing mode
    //    /*
    //    // This is a negative test
    //    // Two inputs cannot be merged due to incompatible schemas
    //    @Test
    //    public void testUnionCastingInsert3() throws Throwable {
    //        LogicalPlan plan = new LogicalPlan() ;
    //
    //        String pigStorage = PigStorage.class.getName() ;
    //
    //        LOLoad load1 = new LOLoad(plan,
    //                                  
    //                                  new FileSpec("pi", new FuncSpec(pigStorage)),
    //                                  null) ;
    //        LOLoad load2 = new LOLoad(plan,
    //                                  
    //                                  new FileSpec("pi", new FuncSpec(pigStorage)),
    //                                  null) ;
    //
    //        // schema for input#1
    //        Schema inputSchema1 = null ;
    //        {
    //            List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
    //            fsList1.add(new FieldSchema("field1a", DataType.INTEGER)) ;
    //            fsList1.add(new FieldSchema("field2a", DataType.BYTEARRAY)) ;
    //            inputSchema1 = new Schema(fsList1) ;
    //        }
    //
    //        // schema for input#2
    //        Schema inputSchema2 = null ;
    //        {
    //            List<FieldSchema> fsList2 = new ArrayList<FieldSchema>() ;
    //            fsList2.add(new FieldSchema("field1b", DataType.CHARARRAY)) ;
    //            fsList2.add(new FieldSchema("field2b", DataType.DOUBLE)) ;
    //            inputSchema2 = new Schema(fsList2) ;
    //        }
    //
    //        // set schemas
    //        load1.setEnforcedSchema(inputSchema1) ;
    //        load2.setEnforcedSchema(inputSchema2) ;
    //
    //        // create union operator
    //        ArrayList<LogicalOperator> inputList = new ArrayList<LogicalOperator>() ;
    //        inputList.add(load1) ;
    //        inputList.add(load2) ;
    //        LOUnion union = new LOUnion(plan, inputList) ;
    //
    //        // wiring
    //        plan.add(load1) ;
    //        plan.add(load2) ;
    //        plan.add(union) ;
    //
    //        plan.connect(load1, union);
    //        plan.connect(load2, union);
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
    //        try {
    //            typeValidator.validate(plan, collector) ;
    //            fail("Exception expected") ;
    //        }
    //        catch (TypeCheckerException pve) {
    //            // good
    //        }
    //        printMessageCollector(collector) ;
    //
    //    }
    //    */
    
        @Test
        public void testDistinct1() throws Throwable {
    
            printCurrentMethodName();
            LogicalPlan plan = new LogicalPlan() ;
    
            String pigStorage = PigStorage.class.getName() ;
    
            LOLoad load1 = new LOLoad(
                    new FileSpec("pi", new FuncSpec(pigStorage)),
                    null, plan, null
            );
    
            // schema for input#1
            Schema inputSchema1 = null ;
            {
                List<FieldSchema> innerList = new ArrayList<FieldSchema>() ;
                innerList.add(new FieldSchema("innerfield1", DataType.BAG)) ;
                innerList.add(new FieldSchema("innerfield2", DataType.FLOAT)) ;
                Schema innerSchema = new Schema(innerList) ;
    
                List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
                fsList1.add(new FieldSchema("field1", DataType.INTEGER)) ;
                fsList1.add(new FieldSchema("field2", DataType.BYTEARRAY)) ;
                fsList1.add(new FieldSchema("field3", innerSchema)) ;
                fsList1.add(new FieldSchema("field4", DataType.BAG)) ;
    
                inputSchema1 = new Schema(fsList1) ;
            }
    
            // set schemas
            load1.setSchema(Util.translateSchema(inputSchema1)) ;
    
            // create union operator
            LODistinct distinct1 = new LODistinct(plan) ;
    
            // wiring
            plan.add(load1) ;
            plan.add(distinct1) ;
    
            plan.connect(load1, distinct1);
    
            // validate
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            typeChecker.visit();  
            printMessageCollector(collector) ;
    
            // check end result schema
            LogicalSchema outputSchema = distinct1.getSchema() ;
            assertTrue(load1.getSchema().isEqual(outputSchema));


        }

        // Positive test
        @Test
        public void testFilterWithInnerPlan1() throws Throwable {
            testFilterWithInnerPlan(DataType.INTEGER, DataType.LONG) ;
        }

        // Positive test
        @Test
        public void testFilterWithInnerPlan2() throws Throwable {
            testFilterWithInnerPlan(DataType.INTEGER, DataType.BYTEARRAY) ;    
        }

        // Filter test helper
        public void testFilterWithInnerPlan(byte field1Type, byte field2Type) throws Throwable {

            // Create outer plan
            printCurrentMethodName();
            LogicalPlan plan = new LogicalPlan() ;

            String pigStorage = PigStorage.class.getName() ;

            LOLoad load1 = new LOLoad(
                    new FileSpec("pi", new FuncSpec(pigStorage)),
                    null, plan, null
            );
            // schema for input#1
            Schema inputSchema1 = null ;
            {
                List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
                fsList1.add(new FieldSchema("field1", field1Type)) ;
                fsList1.add(new FieldSchema("field2", field2Type)) ;

                inputSchema1 = new Schema(fsList1) ;
            }

            // set schemas
            load1.setScriptSchema(Util.translateSchema(inputSchema1)) ;

            // Create inner plan
            LogicalExpressionPlan innerPlan = new LogicalExpressionPlan() ;
            // filter
            LOFilter filter1 = new LOFilter(plan) ;
            filter1.setFilterPlan(innerPlan);
            
            ProjectExpression project1 = new ProjectExpression(innerPlan, 0, 0, filter1);
                
//                new ProjectExpression(innerPlan, load1, 0) ;
  //          project1.setSentinel(true);
            ProjectExpression project2 = new ProjectExpression(innerPlan, 0, 1, filter1); 
                //new ProjectExpression(innerPlan, load1, 1) ;
            //project2.setSentinel(true);

            GreaterThanExpression gt1 = new GreaterThanExpression(innerPlan, project1, project2);
            
            plan.add(load1);
            plan.add(filter1);
            plan.connect(load1, filter1) ;

            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            typeChecker.visit();  
            printMessageCollector(collector) ;
            //printTypeGraph(plan) ;

            if (collector.hasError()) {
                throw new AssertionError("Expect no error") ;
            }

            LogicalSchema endResultSchema = filter1.getSchema() ;
            assertEquals(endResultSchema.getField(0).type, field1Type) ;
            assertEquals(endResultSchema.getField(1).type, field2Type) ;

        }

        // Negative test
        @Test
        public void testFilterWithInnerPlan3() throws Throwable {

            // Create outer plan
            printCurrentMethodName();
            LogicalPlan plan = new LogicalPlan() ;

            String pigStorage = PigStorage.class.getName() ;

            LOLoad load1 = new LOLoad(
                    new FileSpec("pi", new FuncSpec(pigStorage)),
                    null, plan, null
            );
            
            // schema for input#1
            Schema inputSchema1 = null ;
            {
                List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
                fsList1.add(new FieldSchema("field1", DataType.INTEGER)) ;
                fsList1.add(new FieldSchema("field2", DataType.LONG)) ;

                inputSchema1 = new Schema(fsList1) ;
            }

            // set schemas
            load1.setScriptSchema(Util.translateSchema((inputSchema1))) ;

            // Create inner plan
            LogicalExpressionPlan innerPlan = new LogicalExpressionPlan() ;
            
            // filter
            LOFilter filter1 = new LOFilter(plan) ;
            filter1.setFilterPlan(innerPlan);
            
            ProjectExpression project1 = new ProjectExpression(innerPlan, 0, 0, filter1) ;
            ProjectExpression project2 = new ProjectExpression(innerPlan, 0, 1, filter1) ;

            AddExpression add1 = new AddExpression(innerPlan, project1, project2) ;

            plan.add(load1);
            plan.add(filter1);
            plan.connect(load1, filter1) ;

            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            try {
                typeChecker.visit();  
            }
            catch (Exception t) {
                // good
            }
            printMessageCollector(collector) ;
            //printTypeGraph(plan) ;

            if (!collector.hasError()) {
                throw new AssertionError("Expect error") ;
            }

        }
    
    
        // Simple project sort columns
        @Test
        public void testSortWithInnerPlan1() throws Throwable {
    
            // Create outer plan
            printCurrentMethodName();
            LogicalPlan plan = new LogicalPlan() ;
    
            String pigStorage = PigStorage.class.getName() ;
    
            LOLoad load1 = new LOLoad(
                    new FileSpec("pi", new FuncSpec(pigStorage)),
                    null, plan, null
            );

    
            // schema for input#1
            Schema inputSchema1 = null ;
            {
                List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
                fsList1.add(new FieldSchema("field1", DataType.LONG)) ;
                fsList1.add(new FieldSchema("field2", DataType.INTEGER)) ;
    
                inputSchema1 = new Schema(fsList1) ;
            }
    
            // set schemas
            load1.setScriptSchema(Util.translateSchema((inputSchema1))) ; ;
    
            // Create project inner plan #1
            LogicalExpressionPlan innerPlan1 = new LogicalExpressionPlan() ;
            // Sort
            LOSort sort1 = new LOSort(plan);
            ProjectExpression project1 = new ProjectExpression(innerPlan1, 0, 1, sort1) ;
            
            innerPlan1.add(project1) ;
    
            // Create project inner plan #2
            LogicalExpressionPlan innerPlan2 = new LogicalExpressionPlan() ;
            ProjectExpression project2 = new ProjectExpression(innerPlan2, 0, 0, sort1) ;
            
            innerPlan2.add(project2) ;
    
            // List of innerplans
            List<LogicalExpressionPlan> innerPlans = new ArrayList<LogicalExpressionPlan>() ;
            innerPlans.add(innerPlan1) ;
            innerPlans.add(innerPlan2) ;
    
            // List of ASC flags
            List<Boolean> ascList = new ArrayList<Boolean>() ;
            ascList.add(true);
            ascList.add(true);
            
            sort1.setAscendingCols(ascList);
            sort1.setSortColPlans(innerPlans);
    
            plan.add(load1);
            plan.add(sort1);
            plan.connect(load1, sort1) ;
    
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            typeChecker.visit();  
            printMessageCollector(collector) ;
            //printTypeGraph(plan) ;
    
            if (collector.hasError()) {
                throw new AssertionError("Expect no error") ;
            }
    
            LogicalSchema endResultSchema = sort1.getSchema() ;
    
            // outer
            assertEquals(endResultSchema.getField(0).type, DataType.LONG) ;
            assertEquals(endResultSchema.getField(1).type, DataType.INTEGER) ;
    
            // inner
            assertEquals(getSingleOutput(innerPlan1).getType(), DataType.INTEGER);
            assertEquals(getSingleOutput(innerPlan2).getType(), DataType.LONG);
    
        }
        
        
    
        private LogicalExpression getSingleOutput(LogicalExpressionPlan innerPlan1) {
            List<Operator> outputs = innerPlan1.getSources();
            assertEquals("number of outputs in exp plan", outputs.size(),1);
            return (LogicalExpression)outputs.get(0);
        }

        // Positive expression sort columns
        @Test
        public void testSortWithInnerPlan2() throws Throwable {
    
            // Create outer plan
            printCurrentMethodName();
            LogicalPlan plan = new LogicalPlan() ;
    
            String pigStorage = PigStorage.class.getName() ;
    
            LOLoad load1 =  new LOLoad(
                    new FileSpec("pi", new FuncSpec(pigStorage)),
                    null, plan, null
            );

            // schema for input#1
            Schema inputSchema1 = null ;
            {
                List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
                fsList1.add(new FieldSchema("field1", DataType.BYTEARRAY)) ;
                fsList1.add(new FieldSchema("field2", DataType.INTEGER)) ;
    
                inputSchema1 = new Schema(fsList1) ;
            }
    
            // set schemas
            load1.setScriptSchema(Util.translateSchema((inputSchema1))) ; ;
    
            LOSort sort1 = new LOSort(plan) ;

            
            // Create expression inner plan #1
            LogicalExpressionPlan innerPlan1 = new LogicalExpressionPlan() ;
            ProjectExpression project11 = new ProjectExpression(innerPlan1, 0, 0, sort1) ;
            ProjectExpression project12 = new ProjectExpression(innerPlan1, 0, 1, sort1) ;
     
            MultiplyExpression mul1 = new MultiplyExpression(innerPlan1, project11, project12) ;
    
            // Create expression inner plan #2
            LogicalExpressionPlan innerPlan2 = new LogicalExpressionPlan() ;
            ProjectExpression project21 = new ProjectExpression(innerPlan2, 0, 0, sort1) ;

            ConstantExpression const21 = new ConstantExpression(innerPlan2, 26L) ;
            ModExpression mod21 = new ModExpression(innerPlan2, project21, const21) ;
    
            // List of innerplans
            List<LogicalExpressionPlan> innerPlans = new ArrayList<LogicalExpressionPlan>() ;
            innerPlans.add(innerPlan1) ;
            innerPlans.add(innerPlan2) ;
    
            // List of ASC flags
            List<Boolean> ascList = new ArrayList<Boolean>() ;
            ascList.add(true);
            ascList.add(true);
    
            // Sort
            sort1.setAscendingCols(ascList);
            sort1.setSortColPlans(innerPlans);
    
            plan.add(load1);
            plan.add(sort1);
            plan.connect(load1, sort1) ;
    
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            typeChecker.visit();  
            printMessageCollector(collector) ;
            //printTypeGraph(plan) ;
    
            if (collector.hasError()) {
                throw new AssertionError("Expect no error") ;
            }
    
            LogicalSchema endResultSchema = sort1.getSchema() ;
    
            // outer
            assertEquals(endResultSchema.getField(0).type, DataType.BYTEARRAY) ;
            assertEquals(endResultSchema.getField(1).type, DataType.INTEGER) ;
    
            // inner
            assertEquals(getSingleOutput(innerPlan1).getType(), DataType.INTEGER);
            assertEquals(getSingleOutput(innerPlan2).getType(), DataType.LONG);
    
        }
        
        // Negative test on expression sort columns
        @Test
        public void testSortWithInnerPlan3() throws Throwable {
    
            // Create outer plan
            printCurrentMethodName();
            LogicalPlan plan = new LogicalPlan() ;
    
            String pigStorage = PigStorage.class.getName() ;
    
            LOLoad load1 =  new LOLoad(
                    new FileSpec("pi", new FuncSpec(pigStorage)),
                    null, plan, null
            );

    
            // schema for input#1
            Schema inputSchema1 = null ;
            {
                List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
                fsList1.add(new FieldSchema("field1", DataType.BYTEARRAY)) ;
                fsList1.add(new FieldSchema("field2", DataType.INTEGER)) ;
    
                inputSchema1 = new Schema(fsList1) ;
            }
    
            // set schemas
            load1.setScriptSchema(Util.translateSchema((inputSchema1))) ; ;
    
            
            // Sort
            LOSort sort1 = new LOSort(plan) ;
            
            // Create expression inner plan #1
            LogicalExpressionPlan innerPlan1 = new LogicalExpressionPlan() ;
            ProjectExpression project11 = new ProjectExpression(innerPlan1, 0, 0, sort1) ;
            ProjectExpression project12 = new ProjectExpression(innerPlan1, 0, 1, sort1) ;
            MultiplyExpression mul1 = new MultiplyExpression(innerPlan1, project11, project12) ;

    
            // Create expression inner plan #2
            LogicalExpressionPlan innerPlan2 = new LogicalExpressionPlan() ;
            ProjectExpression project21 = new ProjectExpression(innerPlan2, 0, 0, sort1) ;
            ConstantExpression const21 = new ConstantExpression(innerPlan2, "26") ;
            ModExpression mod21 = new ModExpression(innerPlan2, project21, const21) ;
    
            // List of innerplans
            List<LogicalExpressionPlan> innerPlans = new ArrayList<LogicalExpressionPlan>() ;
            innerPlans.add(innerPlan1) ;
            innerPlans.add(innerPlan2) ;
    
            // List of ASC flags
            List<Boolean> ascList = new ArrayList<Boolean>() ;
            ascList.add(true);
            ascList.add(true);

            // Sort
            sort1.setAscendingCols(ascList);
            sort1.setSortColPlans(innerPlans);
    
            plan.add(load1);
            plan.add(sort1);
            plan.connect(load1, sort1) ;
    
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            try {
                typeChecker.visit();  
                fail("Error expected") ;
            }
            catch (Exception t) {
                // good
            }
            printMessageCollector(collector) ;
            //printTypeGraph(plan) ;
    
            if (!collector.hasError()) {
                throw new AssertionError("Error expected") ;
            }
    
        }


        // Positive expression cond columns
        @Test
        public void testSplitWithInnerPlan1() throws Throwable {

            // Create outer plan
            printCurrentMethodName();
            LogicalPlan plan = new LogicalPlan() ;

            String pigStorage = PigStorage.class.getName() ;

            LOLoad load1 =  new LOLoad(
                    new FileSpec("pi", new FuncSpec(pigStorage)),
                    null, plan, null
            );

            // schema for input#1
            Schema inputSchema1 = null ;
            {
                List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
                fsList1.add(new FieldSchema("field1", DataType.BYTEARRAY)) ;
                fsList1.add(new FieldSchema("field2", DataType.INTEGER)) ;

                inputSchema1 = new Schema(fsList1) ;
            }

            // set schemas
            load1.setScriptSchema(Util.translateSchema((inputSchema1))) ; ;

            
            // split
            LOSplit split1 = new LOSplit(plan);

            // output1
            LOSplitOutput splitOutput1 = new LOSplitOutput(plan) ;

            // output2
            LOSplitOutput splitOutput2 = new LOSplitOutput(plan) ;
            
            // Create expression inner plan #1
            LogicalExpressionPlan innerPlan1 = new LogicalExpressionPlan() ;
            ProjectExpression project11 = new ProjectExpression(innerPlan1, 0, 0, splitOutput1) ;
            
            ProjectExpression project12 = new ProjectExpression(innerPlan1, 0, 1, splitOutput1) ;
            
            NotEqualExpression notequal1 = new NotEqualExpression(innerPlan1, project11, project12) ;

//            innerPlan1.add(project11) ;
//            innerPlan1.add(project12) ;
//            innerPlan1.add(notequal1) ;
//
//            innerPlan1.connect(project11, notequal1);
//            innerPlan1.connect(project12, notequal1);

            // Create expression inner plan #2
            LogicalExpressionPlan innerPlan2 = new LogicalExpressionPlan() ;
            ProjectExpression project21 = new ProjectExpression(innerPlan2, 0, 0, splitOutput2) ;
            
            ConstantExpression const21 = new ConstantExpression(innerPlan2, 26L) ;
            LessThanEqualExpression lesser21 = new LessThanEqualExpression(innerPlan2, project21, const21) ;
//
//            innerPlan2.add(project21) ;
//            innerPlan2.add(const21) ;
//            innerPlan2.add(lesser21) ;
//
//            innerPlan2.connect(project21, lesser21);
//            innerPlan2.connect(const21, lesser21) ;


            splitOutput1.setFilterPlan(innerPlan1);
            splitOutput2.setFilterPlan(innerPlan2);

            plan.add(load1);
            plan.add(split1);
            plan.add(splitOutput1);
            plan.add(splitOutput2);

            plan.connect(load1, split1) ;
            plan.connect(split1, splitOutput1) ;
            plan.connect(split1, splitOutput2) ;

            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            typeChecker.visit();  
            printMessageCollector(collector) ;
            //printTypeGraph(plan) ;

            if (collector.hasError()) {
                throw new AssertionError("Expect no error") ;
            }

            // check split itself
            {
                LogicalSchema endResultSchema1 = split1.getSchema() ;
                // outer
                assertEquals(endResultSchema1.getField(0).type, DataType.BYTEARRAY) ;
                assertEquals(endResultSchema1.getField(1).type, DataType.INTEGER) ;
            }

            // check split output #1
            {
                LogicalSchema endResultSchema1 = splitOutput1.getSchema() ;
                // outer
                assertEquals(endResultSchema1.getField(0).type, DataType.BYTEARRAY) ;
                assertEquals(endResultSchema1.getField(1).type, DataType.INTEGER) ;
            }

            // check split output #2
            {
                LogicalSchema endResultSchema2 = splitOutput2.getSchema() ;
                // outer
                assertEquals(endResultSchema2.getField(0).type, DataType.BYTEARRAY) ;
                assertEquals(endResultSchema2.getField(1).type, DataType.INTEGER) ;
            }

            // inner conditions: all have to be boolean
            assertEquals(getSingleOutput(innerPlan1).getType(), DataType.BOOLEAN);
            assertEquals(getSingleOutput(innerPlan2).getType(), DataType.BOOLEAN);

        }

        // Negative test: expression cond columns not evaluate to boolean
        @Test
        public void testSplitWithInnerPlan2() throws Throwable {

            // Create outer plan
            printCurrentMethodName();
            LogicalPlan plan = new LogicalPlan() ;

            String pigStorage = PigStorage.class.getName() ;

            LOLoad load1 =  new LOLoad(
                    new FileSpec("pi", new FuncSpec(pigStorage)),
                    null, plan, null
            );

            // schema for input#1
            Schema inputSchema1 = null ;
            {
                List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
                fsList1.add(new FieldSchema("field1", DataType.BYTEARRAY)) ;
                fsList1.add(new FieldSchema("field2", DataType.INTEGER)) ;

                inputSchema1 = new Schema(fsList1) ;
            }

            // set schemas
            load1.setScriptSchema(Util.translateSchema((inputSchema1))) ; ;

            // split
            LOSplit split1 = new LOSplit(plan);

            // output1
            LOSplitOutput splitOutput1 = new LOSplitOutput(plan) ;

            // output2
            LOSplitOutput splitOutput2 = new LOSplitOutput(plan) ;          
            
            // Create expression inner plan #1
            LogicalExpressionPlan innerPlan1 = new LogicalExpressionPlan() ;
            ProjectExpression project11 = new ProjectExpression(innerPlan1, 0, 0, splitOutput1) ;
            
            ProjectExpression project12 = new ProjectExpression(innerPlan1, 0, 1, splitOutput1) ;
            
            NotEqualExpression notequal1 = new NotEqualExpression(innerPlan1, project11, project12) ;

//            innerPlan1.add(project11) ;
//            innerPlan1.add(project12) ;
//            innerPlan1.add(notequal1) ;
//
//            innerPlan1.connect(project11, notequal1);
//            innerPlan1.connect(project12, notequal1);

            // Create expression inner plan #2
            LogicalExpressionPlan innerPlan2 = new LogicalExpressionPlan() ;
            ProjectExpression project21 = new ProjectExpression(innerPlan2, 0, 0, splitOutput1) ;
            
            ConstantExpression const21 = 
                new ConstantExpression(innerPlan2, 26L) ;
            
            SubtractExpression subtract21 = 
                new SubtractExpression(innerPlan2, project21, const21) ;

//            innerPlan2.add(project21) ;
//            innerPlan2.add(const21) ;
//            innerPlan2.add(subtract21) ;
//
//            innerPlan2.connect(project21, subtract21);
//            innerPlan2.connect(const21, subtract21) ;

            splitOutput1.setFilterPlan(innerPlan1);
            splitOutput2.setFilterPlan(innerPlan2);

            plan.add(load1);
            plan.add(split1);
            plan.add(splitOutput1);
            plan.add(splitOutput2);

            plan.connect(load1, split1) ;
            plan.connect(split1, splitOutput1) ;
            plan.connect(split1, splitOutput2) ;

            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            try {
                typeChecker.visit();  
            }
            catch (Exception t) {
                // good
            }
            printMessageCollector(collector) ;
            //printTypeGraph(plan) ;

            if (!collector.hasError()) {
                throw new AssertionError("Error expected") ;
            }

        }
    
        // Positive test
        @Test
        public void testCOGroupWithInnerPlan1GroupByTuple1() throws Throwable {
    
            printCurrentMethodName();
            LogicalPlan plan = new LogicalPlan() ;
    
            String pigStorage = PigStorage.class.getName() ;
    
            LOLoad load1 =  new LOLoad(
                    new FileSpec("pi", new FuncSpec(pigStorage)),
                    null, plan, null
            );
            
            LOLoad load2 =  new LOLoad(
                    new FileSpec("pi", new FuncSpec(pigStorage)),
                    null, plan, null
            );
              
            // schema for input#1
            Schema inputSchema1 = null ;
            {
                List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
                fsList1.add(new FieldSchema("field1a", DataType.INTEGER)) ;
                fsList1.add(new FieldSchema("field2a", DataType.LONG)) ;
                inputSchema1 = new Schema(fsList1) ;
            }
    
            // schema for input#2
            Schema inputSchema2 = null ;
            {
                List<FieldSchema> fsList2 = new ArrayList<FieldSchema>() ;
                fsList2.add(new FieldSchema("field1b", DataType.DOUBLE)) ;
                fsList2.add(new FieldSchema(null, DataType.INTEGER)) ;
                inputSchema2 = new Schema(fsList2) ;
            }
    
            // set schemas
            load1.setScriptSchema(Util.translateSchema((inputSchema1))) ;
            load2.setScriptSchema(Util.translateSchema((inputSchema2))) ;
            load2.setScriptSchema(Util.translateSchema((inputSchema2))) ;
    
            LOCogroup cogroup1 = new LOCogroup(plan);
            
            // Create expression inner plan #1 of input #1
            LogicalExpressionPlan innerPlan11 = new LogicalExpressionPlan() ;
            ProjectExpression project111 = new ProjectExpression(innerPlan11, 0, 0, cogroup1) ;
            ConstantExpression const111 = new ConstantExpression(innerPlan11, 26F) ;
            SubtractExpression subtract111 = new SubtractExpression(innerPlan11, project111, const111) ;
    
//            innerPlan11.add(project111) ;
//            innerPlan11.add(const111) ;
//            innerPlan11.add(subtract111) ;
//    
//            innerPlan11.connect(project111, subtract111);
//            innerPlan11.connect(const111, subtract111) ;
    
            // Create expression inner plan #2 of input #1
            LogicalExpressionPlan innerPlan21 = new LogicalExpressionPlan() ;
            ProjectExpression project211 = new ProjectExpression(innerPlan21, 0, 0, cogroup1) ;
            ProjectExpression project212 = new ProjectExpression(innerPlan21, 0, 1, cogroup1) ;
    
            AddExpression add211 = new AddExpression(innerPlan21, project211, project212) ;
    
//            innerPlan21.add(project211) ;
//            innerPlan21.add(project212) ;
//            innerPlan21.add(add211) ;
//    
//            innerPlan21.connect(project211, add211);
//            innerPlan21.connect(project212, add211) ;
   
            // Create expression inner plan #1 of input #2
            LogicalExpressionPlan innerPlan12 = new LogicalExpressionPlan() ;
            ProjectExpression project121 = new ProjectExpression(innerPlan12, 1, 0, cogroup1) ;
            ConstantExpression const121 = new ConstantExpression(innerPlan12, 26) ;
            SubtractExpression subtract121 = new SubtractExpression(innerPlan12, project121, const121) ;
    
//            innerPlan12.add(project121) ;
//            innerPlan12.add(const121) ;
//            innerPlan12.add(subtract121) ;
//    
//            innerPlan12.connect(project121, subtract121);
//            innerPlan12.connect(const121, subtract121) ;
    
            // Create expression inner plan #2 of input #2
            LogicalExpressionPlan innerPlan22 = new LogicalExpressionPlan() ;
            ConstantExpression const122 = new ConstantExpression(innerPlan22, 26) ;
//            innerPlan22.add(const122) ;
    
            // Create Cogroup
            ArrayList<LogicalRelationalOperator> inputs = new ArrayList<LogicalRelationalOperator>() ;
            inputs.add(load1) ;
            inputs.add(load2) ;
    
            MultiMap<Integer, LogicalExpressionPlan> maps
                                = new MultiMap<Integer, LogicalExpressionPlan>() ;
            maps.put(0, innerPlan11);
            maps.put(0, innerPlan21);
            maps.put(1, innerPlan12);
            maps.put(1, innerPlan22);
    
            boolean[] isInner = new boolean[inputs.size()] ;
            for (int i=0; i < isInner.length ; i++) {
                isInner[i] = false ;
            }
    
            cogroup1.setInnerFlags(isInner);
            cogroup1.setExpressionPlans(maps);
    
            // construct the main plan
            plan.add(load1) ;
            plan.add(load2) ;
            plan.add(cogroup1) ;
    
            plan.connect(load1, cogroup1);
            plan.connect(load2, cogroup1);
    
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            typeChecker.visit();  
            printMessageCollector(collector) ;
            //printTypeGraph(plan) ;
    
            if (collector.hasError()) {
                throw new AssertionError("Expect no error") ;
            }
            
            // check outer schema
            LogicalSchema endResultSchema = cogroup1.getSchema() ;
    
            // Tuple group column
            assertEquals(endResultSchema.getField(0).type, DataType.TUPLE) ;
            assertEquals(endResultSchema.getField(0).schema.getField(0).type, DataType.DOUBLE) ;
            assertEquals(endResultSchema.getField(0).schema.getField(1).type, DataType.LONG);
    
            assertEquals(endResultSchema.getField(1).type, DataType.BAG) ;
            assertEquals(endResultSchema.getField(2).type, DataType.BAG) ;
    
            // check inner schema1
            LogicalSchema innerSchema1 = endResultSchema.getField(1).schema.getField(0).schema ;
            assertEquals(innerSchema1.getField(0).type, DataType.INTEGER);
            assertEquals(innerSchema1.getField(1).type, DataType.LONG);
    
            // check inner schema2
            LogicalSchema innerSchema2 = endResultSchema.getField(2).schema.getField(0).schema ;
            assertEquals(innerSchema2.getField(0).type, DataType.DOUBLE);
            assertEquals(innerSchema2.getField(1).type, DataType.INTEGER);
    
            // check group by col end result
            assertEquals(getSingleOutput(innerPlan11).getType(), DataType.DOUBLE) ;
            assertEquals(getSingleOutput(innerPlan21).getType(), DataType.LONG) ;
            assertEquals(getSingleOutput(innerPlan12).getType(), DataType.DOUBLE) ;
            assertEquals(getSingleOutput(innerPlan22).getType(), DataType.LONG) ;
        }
    
    
        // Positive test
        @Test
        public void testCOGroupWithInnerPlan1GroupByAtom1() throws Throwable {
    
            printCurrentMethodName();
            LogicalPlan plan = new LogicalPlan() ;
    
            String pigStorage = PigStorage.class.getName() ;
            LOLoad load1 =  new LOLoad(
                    new FileSpec("pi", new FuncSpec(pigStorage)),
                    null, plan, null
            );
            
            LOLoad load2 =  new LOLoad(
                    new FileSpec("pi", new FuncSpec(pigStorage)),
                    null, plan, null
            );
            // schema for input#1
            Schema inputSchema1 = null ;
            {
                List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
                fsList1.add(new FieldSchema("field1a", DataType.INTEGER)) ;
                fsList1.add(new FieldSchema("field2a", DataType.LONG)) ;
                inputSchema1 = new Schema(fsList1) ;
            }
    
            // schema for input#2
            Schema inputSchema2 = null ;
            {
                List<FieldSchema> fsList2 = new ArrayList<FieldSchema>() ;
                fsList2.add(new FieldSchema("field1b", DataType.DOUBLE)) ;
                fsList2.add(new FieldSchema(null, DataType.INTEGER)) ;
                inputSchema2 = new Schema(fsList2) ;
            }
    
            // set schemas
            load1.setScriptSchema(Util.translateSchema((inputSchema1))) ; ;
            load2.setScriptSchema(Util.translateSchema((inputSchema2))) ;
    
            LOCogroup cogroup1 = new LOCogroup(plan);
                    
            // Create expression inner plan #1 of input #1
            LogicalExpressionPlan innerPlan11 = new LogicalExpressionPlan() ;
            ProjectExpression project111 = new ProjectExpression(innerPlan11, 0, 0, cogroup1) ;
            ConstantExpression const111 = new ConstantExpression(innerPlan11, 26F) ;
            SubtractExpression subtract111 = new SubtractExpression(innerPlan11, project111, const111) ;
    
//            innerPlan11.add(project111) ;
//            innerPlan11.add(const111) ;
//            innerPlan11.add(subtract111) ;
//    
//            innerPlan11.connect(project111, subtract111);
//            innerPlan11.connect(const111, subtract111) ;
    
            // Create expression inner plan #1 of input #2
            LogicalExpressionPlan innerPlan12 = new LogicalExpressionPlan() ;
            ProjectExpression project121 = new ProjectExpression(innerPlan12, 1, 0, cogroup1) ;
            ConstantExpression const121 = new ConstantExpression(innerPlan12, 26) ;
            SubtractExpression subtract121 = new SubtractExpression(innerPlan12, project121, const121) ;
    
//            innerPlan12.add(project121) ;
//            innerPlan12.add(const121) ;
//            innerPlan12.add(subtract121) ;
//    
//            innerPlan12.connect(project121, subtract121);
//            innerPlan12.connect(const121, subtract121) ;
    
            // Create Cogroup
            ArrayList<LogicalRelationalOperator> inputs = new ArrayList<LogicalRelationalOperator>() ;
            inputs.add(load1) ;
            inputs.add(load2) ;
    
            MultiMap<Integer, LogicalExpressionPlan> maps
                                = new MultiMap<Integer, LogicalExpressionPlan>() ;
            maps.put(0, innerPlan11);
            maps.put(1, innerPlan12);
    
            boolean[] isInner = new boolean[inputs.size()] ;
            for (int i=0; i < isInner.length ; i++) {
                isInner[i] = false ;
            }
    
            
            cogroup1.setInnerFlags(isInner);
            cogroup1.setExpressionPlans(maps);
    
            // construct the main plan
            plan.add(load1) ;
            plan.add(load2) ;
            plan.add(cogroup1) ;
    
            plan.connect(load1, cogroup1);
            plan.connect(load2, cogroup1);
    
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            typeChecker.visit();  
            printMessageCollector(collector) ;
            //printTypeGraph(plan) ;
    
            if (collector.hasError()) {
                throw new AssertionError("Expect no error") ;
            }
    
            // check outer schema
            LogicalSchema endResultSchema = cogroup1.getSchema() ;
    
            // Tuple group column
            assertEquals(endResultSchema.getField(0).type, DataType.DOUBLE) ;
    
            assertEquals(endResultSchema.getField(1).type, DataType.BAG) ;
            assertEquals(endResultSchema.getField(2).type, DataType.BAG) ;
    
            // check inner schema1
            LogicalSchema innerSchema1 = endResultSchema.getField(1).schema.getField(0).schema ;
            assertEquals(innerSchema1.getField(0).type, DataType.INTEGER);
            assertEquals(innerSchema1.getField(1).type, DataType.LONG);
    
            // check inner schema2
            LogicalSchema innerSchema2 = endResultSchema.getField(2).schema.getField(0).schema ;
            assertEquals(innerSchema2.getField(0).type, DataType.DOUBLE);
            assertEquals(innerSchema2.getField(1).type, DataType.INTEGER);
    
            // check group by col end result
            assertEquals(getSingleOutput(innerPlan11).getType(), DataType.DOUBLE) ;
            assertEquals(getSingleOutput(innerPlan12).getType(), DataType.DOUBLE) ;
        }
    
    
        // Positive test
        @Test
        public void testCOGroupWithInnerPlan1GroupByIncompatibleAtom1() throws Throwable {
    
            printCurrentMethodName();
            LogicalPlan plan = new LogicalPlan() ;
    
            String pigStorage = PigStorage.class.getName() ;
    
            LOLoad load1 =  new LOLoad(
                    new FileSpec("pi", new FuncSpec(pigStorage)),
                    null, plan, null
            );
            
            LOLoad load2 =  new LOLoad(
                    new FileSpec("pi", new FuncSpec(pigStorage)),
                    null, plan, null
            );
    
            // schema for input#1
            Schema inputSchema1 = null ;
            {
                List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
                fsList1.add(new FieldSchema("field1a", DataType.INTEGER)) ;
                fsList1.add(new FieldSchema("field2a", DataType.LONG)) ;
                inputSchema1 = new Schema(fsList1) ;
            }
    
            // schema for input#2
            Schema inputSchema2 = null ;
            {
                List<FieldSchema> fsList2 = new ArrayList<FieldSchema>() ;
                fsList2.add(new FieldSchema("field1b", DataType.DOUBLE)) ;
                fsList2.add(new FieldSchema(null, DataType.INTEGER)) ;
                inputSchema2 = new Schema(fsList2) ;
            }
    
            // set schemas
            load1.setScriptSchema(Util.translateSchema((inputSchema1))) ;
            load2.setScriptSchema(Util.translateSchema((inputSchema2))) ;
    
            LOCogroup cogroup1 = new LOCogroup(plan);
            // Create expression inner plan #1
            LogicalExpressionPlan innerPlan11 = new LogicalExpressionPlan() ;
            ProjectExpression project111 = new ProjectExpression(innerPlan11, 0, 0, cogroup1) ;
            ConstantExpression const111 = new ConstantExpression(innerPlan11, 26F) ;
            SubtractExpression subtract111 = new SubtractExpression(innerPlan11, project111, const111) ;
    
//            innerPlan11.add(project111) ;
//            innerPlan11.add(const111) ;
//            innerPlan11.add(subtract111) ;
//    
//            innerPlan11.connect(project111, subtract111);
//            innerPlan11.connect(const111, subtract111) ;
    
            // Create expression inner plan #2
            LogicalExpressionPlan innerPlan12 = new LogicalExpressionPlan() ;
            ConstantExpression const121 = new ConstantExpression(innerPlan12, 26) ;
//            innerPlan12.add(const121) ;
    
            // Create Cogroup
            ArrayList<LogicalRelationalOperator> inputs = new ArrayList<LogicalRelationalOperator>() ;
            inputs.add(load1) ;
            inputs.add(load2) ;
    
            MultiMap<Integer, LogicalExpressionPlan> maps
                                = new MultiMap<Integer, LogicalExpressionPlan>() ;
            maps.put(0, innerPlan11);
            maps.put(1, innerPlan12);
    
            boolean[] isInner = new boolean[inputs.size()] ;
            for (int i=0; i < isInner.length ; i++) {
                isInner[i] = false ;
            }
    
            
            cogroup1.setInnerFlags(isInner);
            cogroup1.setExpressionPlans(maps);
    
            // construct the main plan
            plan.add(load1) ;
            plan.add(load2) ;
            plan.add(cogroup1) ;
    
            plan.connect(load1, cogroup1);
            plan.connect(load2, cogroup1);
    
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            typeChecker.visit();  
            printMessageCollector(collector) ;
            //printTypeGraph(plan) ;
    
            if (collector.hasError()) {
                throw new AssertionError("Expect no error") ;
            }
    
            // check outer schema
            LogicalSchema endResultSchema = cogroup1.getSchema() ;
    
            // Tuple group column
            assertEquals(endResultSchema.getField(0).type, DataType.FLOAT) ;
    
            assertEquals(endResultSchema.getField(1).type, DataType.BAG) ;
            assertEquals(endResultSchema.getField(2).type, DataType.BAG) ;
    
            // check inner schema1
            LogicalSchema innerSchema1 = endResultSchema.getField(1).schema.getField(0).schema ;
            assertEquals(innerSchema1.getField(0).type, DataType.INTEGER);
            assertEquals(innerSchema1.getField(1).type, DataType.LONG);
    
            // check inner schema2
            LogicalSchema innerSchema2 = endResultSchema.getField(2).schema.getField(0).schema ;
            assertEquals(innerSchema2.getField(0).type, DataType.DOUBLE);
            assertEquals(innerSchema2.getField(1).type, DataType.INTEGER);
    
            // check group by col end result
            assertEquals(getSingleOutput(innerPlan11).getType(), DataType.FLOAT) ;
            assertEquals(getSingleOutput(innerPlan12).getType(), DataType.FLOAT) ;
        }
    
        // Positive test
        @Test
        public void testForEachGenerate1() throws Throwable {
    
            printCurrentMethodName() ;
    
            LogicalPlan plan = new LogicalPlan() ;
            LOLoad load1 = genDummyLOLoadNewLP(plan) ;
    
            // schema for input#1
            Schema inputSchema1 = null ;
            {
                List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
                fsList1.add(new FieldSchema("field1a", DataType.INTEGER)) ;
                fsList1.add(new FieldSchema("field2a", DataType.LONG)) ;
                inputSchema1 = new Schema(fsList1) ;
            }
    
            // set schemas
            load1.setScriptSchema(Util.translateSchema((inputSchema1))) ; ;
            
            LogicalPlan innerRelPlan = new LogicalPlan();
            LOForEach foreach1 = new LOForEach(plan);
            foreach1.setInnerPlan(innerRelPlan);
            
            LOGenerate loGen = new LOGenerate(innerRelPlan);
            innerRelPlan.add(loGen);
            
            LOInnerLoad innerLoad1 = new LOInnerLoad(innerRelPlan, foreach1, 0);
            innerRelPlan.add(innerLoad1);
            innerRelPlan.connect(innerLoad1, loGen);

            // Create expression inner plan #1
            LogicalExpressionPlan innerPlan1 = new LogicalExpressionPlan() ;
            ProjectExpression project11 = new ProjectExpression(innerPlan1, 0, 0, loGen) ;
            
            
            ConstantExpression const11 = new ConstantExpression(innerPlan1, 26F) ;
          //  const11.setType(DataType.FLOAT);
            SubtractExpression subtract11 = new SubtractExpression(innerPlan1, project11, const11) ;
    
//            innerPlan1.add(project11) ;
//            innerPlan1.add(const11) ;
//            innerPlan1.add(subtract11) ;
//    
//            innerPlan1.connect(project11, subtract11);
//            innerPlan1.connect(const11, subtract11) ;
    

            
            // Create expression inner plan #2
            LogicalExpressionPlan innerPlan2 = new LogicalExpressionPlan() ;
            LOInnerLoad innerLoad2 = new LOInnerLoad(innerRelPlan, foreach1, 0);
            innerRelPlan.add(innerLoad2);
            innerRelPlan.connect(innerLoad2, loGen);
            ProjectExpression project21 = new ProjectExpression(innerPlan2, 1, 0, loGen) ;
            
            LOInnerLoad innerLoad3 = new LOInnerLoad(innerRelPlan, foreach1, 1);
            innerRelPlan.add(innerLoad3);
            innerRelPlan.connect(innerLoad3, loGen);

            ProjectExpression project22 = new ProjectExpression(innerPlan2, 2, 0, loGen) ;
            
            AddExpression add21 = new AddExpression(innerPlan2, project21, project22 ) ;
    
//            innerPlan2.add(project21) ;
//            innerPlan2.add(project22) ;
//            innerPlan2.add(add21) ;
//    
//            innerPlan2.connect(project21, add21);
//            innerPlan2.connect(project22, add21);
    
            // List of plans
            ArrayList<LogicalExpressionPlan> generatePlans = new ArrayList<LogicalExpressionPlan>() ;
            generatePlans.add(innerPlan1);
            generatePlans.add(innerPlan2);
    
            // List of flatten flags
            boolean [] flattens = {true, false}; 
//            ArrayList<Boolean> flattens = new ArrayList<Boolean>() ;
//            flattens.add(true) ;
//            flattens.add(false) ;
    
            loGen.setFlattenFlags(flattens);
            loGen.setOutputPlans(generatePlans);
            
    
            // construct the main plan
            plan.add(load1) ;
            plan.add(foreach1) ;
    
            plan.connect(load1, foreach1);
    
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            typeChecker.visit();  
            printMessageCollector(collector) ;
            //printTypeGraph(plan) ;
    
            if (collector.hasError()) {
                throw new AssertionError("Expect no error") ;
            }
    
            // check outer schema
            LogicalSchema endResultSchema = foreach1.getSchema() ;
    
            assertEquals(endResultSchema.getField(0).type, DataType.FLOAT) ;
            assertEquals(endResultSchema.getField(1).type, DataType.LONG) ;
    
        }
    
        // Negative test
        @Test
        public void testForEachGenerate2() throws Throwable {
    
            printCurrentMethodName() ;
    
            LogicalPlan plan = new LogicalPlan() ;
            LOLoad load1 = genDummyLOLoadNewLP(plan) ;
    
            // schema for input#1
            Schema inputSchema1 = null ;
            {
                List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
                fsList1.add(new FieldSchema("field1a", DataType.INTEGER)) ;
                fsList1.add(new FieldSchema("field2a", DataType.LONG)) ;
                inputSchema1 = new Schema(fsList1) ;
            }
    
            // set schemas
            load1.setScriptSchema(Util.translateSchema((inputSchema1))) ; ;
    
            LogicalPlan innerRelPlan = new LogicalPlan();
            LOForEach foreach1 = new LOForEach(plan);
            foreach1.setInnerPlan(innerRelPlan);
            
            LOGenerate loGen = new LOGenerate(innerRelPlan);
            innerRelPlan.add(loGen);
            
            LOInnerLoad innerLoad1 = new LOInnerLoad(innerRelPlan, foreach1, 0);
            innerRelPlan.add(innerLoad1);
            innerRelPlan.connect(innerLoad1, loGen);

            
            // Create expression inner plan #1
            LogicalExpressionPlan innerPlan1 = new LogicalExpressionPlan() ;
            ProjectExpression project11 = new ProjectExpression(innerPlan1, 0, 0, loGen) ;
            
            ConstantExpression const11 = new ConstantExpression(innerPlan1, "26F") ;
            SubtractExpression subtract11 = new SubtractExpression(innerPlan1, project11, const11) ;
    
//            innerPlan1.add(project11) ;
//            innerPlan1.add(const11) ;
//            innerPlan1.add(subtract11) ;
//    
//            innerPlan1.connect(project11, subtract11);
//            innerPlan1.connect(const11, subtract11) ;
    
            // Create expression inner plan #2
            LogicalExpressionPlan innerPlan2 = new LogicalExpressionPlan() ;
            innerLoad1 = new LOInnerLoad(innerRelPlan, foreach1, 0);
            innerRelPlan.add(innerLoad1);
            innerRelPlan.connect(innerLoad1, loGen);
            ProjectExpression project21 = new ProjectExpression(innerPlan2, 1, 0, loGen) ;
            
            
            innerLoad1 = new LOInnerLoad(innerRelPlan, foreach1, 0);
            innerRelPlan.add(innerLoad1);
            innerRelPlan.connect(innerLoad1, loGen);
            ProjectExpression project22 = new ProjectExpression(innerPlan2, 2, 1, loGen) ;
            
            AddExpression add21 = new AddExpression(innerPlan2, project21, project22) ;
    
//            innerPlan2.add(project21) ;
//            innerPlan2.add(project22) ;
//            innerPlan2.add(add21) ;
//    
//            innerPlan2.connect(project21, add21);
//            innerPlan2.connect(project22, add21);
    
            // List of plans
            ArrayList<LogicalExpressionPlan> generatePlans = new ArrayList<LogicalExpressionPlan>() ;
            generatePlans.add(innerPlan1);
            generatePlans.add(innerPlan2);
    
            // List of flatten flags
            boolean [] flattens = {true, false}; 
    
            loGen.setFlattenFlags(flattens);
            loGen.setOutputPlans(generatePlans);
            
            // construct the main plan
            plan.add(load1) ;
            plan.add(foreach1) ;
    
            plan.connect(load1, foreach1);
    
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            try {
                typeChecker.visit();  
                fail("Exception expected") ;
            }
            catch (TypeCheckerException pve) {
                // good
            }
            printMessageCollector(collector) ;
            //printTypeGraph(plan) ;
    
            if (!collector.hasError()) {
                throw new AssertionError("Expect error") ;
            }
    
        }
    
        /*
    
        // Positive test
        // This one does project bag in inner plans
        @Test
        public void testForEachGenerate3() throws Throwable {
    
            printCurrentMethodName() ;
    
            LogicalPlan plan = new LogicalPlan() ;
            LOLoad load1 = genDummyLOLoadNewLP(plan) ;
    
            String[] aliases = new String[]{ "a", "b", "c" } ;
            byte[] types = new byte[] { DataType.INTEGER, DataType.LONG, DataType.BYTEARRAY } ;
            Schema innerSchema1 = genFlatSchema(aliases, types) ;
    
            // schema for input#1
            Schema inputSchema1 = null ;
            {
                List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
                fsList1.add(new FieldSchema("field1a", DataType.INTEGER)) ;
                fsList1.add(new FieldSchema("field2a", DataType.LONG)) ;
                fsList1.add(new FieldSchema("field3a", innerSchema1, DataType.BAG)) ;
                inputSchema1 = new Schema(fsList1) ;
            }
    
            // set schemas
            load1.setScriptSchema(Util.translateSchema((inputSchema1))) ; ;
    
            // Create expression inner plan #1 of input #1
            LogicalExpressionPlan innerPlan1 = new LogicalExpressionPlan() ;
            ProjectExpression project11 = new ProjectExpression(innerPlan1, load1, 2) ;
            
            List<Integer> projections1 = new ArrayList<Integer>() ;
            projections1.add(1) ;
            projections1.add(2) ;
            ProjectExpression project12 = new ProjectExpression(innerPlan1, project11, projections1) ;
            project12.setSentinel(false);
    
            innerPlan1.add(project12) ;
    
            // Create expression inner plan #1 of input #2
            LogicalExpressionPlan innerPlan2 = new LogicalExpressionPlan() ;
            ProjectExpression project21 = new ProjectExpression(innerPlan1, load1, 0) ;
            
            ProjectExpression project22 = new ProjectExpression(innerPlan1, load1, 1) ;
            
            AddExpression add21 = new AddExpression(innerPlan1,
                                    
                                    project21,
                                    project22) ;
    
            innerPlan2.add(project21) ;
            innerPlan2.add(project22) ;
            innerPlan2.add(add21) ;
    
            innerPlan2.connect(project21, add21);
            innerPlan2.connect(project22, add21);
    
            // List of plans
            ArrayList<LogicalExpressionPlan> generatePlans = new ArrayList<LogicalExpressionPlan>() ;
            generatePlans.add(innerPlan1);
            generatePlans.add(innerPlan2);
    
            // List of flatten flags
            ArrayList<Boolean> flattens = new ArrayList<Boolean>() ;
            flattens.add(false) ;
            flattens.add(false) ;
    
            // Create LOGenerate
            LOGenerate generate1 = new LOGenerate(plan, generatePlans, flattens) ;
    
            LogicalExpressionPlan foreachPlan = new LogicalExpressionPlan() ;
            foreachPlan.add(generate1) ;
    
            // Create LOForEach
            LOForEach foreach1 = new LOForEach(plan, foreachPlan) ;
    
            // construct the main plan
            plan.add(load1) ;
            plan.add(foreach1) ;
    
            plan.connect(load1, foreach1);
    
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            typeChecker.visit();  
    
            printMessageCollector(collector) ;
            //printTypeGraph(plan) ;
    
            if (collector.hasError()) {
                throw new AssertionError("Expect no  error") ;
            }
            
            // check outer schema
            Schema endResultSchema = foreach1.getSchema() ;
            assertEquals(endResultSchema.getField(0).type, DataType.BAG) ;
            assertEquals(endResultSchema.getField(1).type, DataType.LONG) ;
    
            // check inner bag schema
            Schema bagSchema = endResultSchema.getField(0).schema ;
            assertEquals(bagSchema.getField(0).type, DataType.LONG) ;
            assertEquals(bagSchema.getField(1).type, DataType.BYTEARRAY) ;
    
        }
    
        */
    
        // Positive test
        // This one does project bag in inner plans with flatten
        @Test
        public void testForEachGenerate4() throws Throwable {
    
            printCurrentMethodName() ;
    
            LogicalPlan plan = new LogicalPlan() ;
            LOLoad load1 = genDummyLOLoadNewLP(plan) ;
    
            String[] aliases = new String[]{ "a", "b", "c" } ;
            byte[] types = new byte[] { DataType.INTEGER, DataType.LONG, DataType.BYTEARRAY } ;
            Schema innerSchema1 = genFlatSchemaInTuple(aliases, types) ;
    
            // schema for input#1
            Schema inputSchema1 = null ;
            {
                List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
                fsList1.add(new FieldSchema("field1a", DataType.INTEGER)) ;
                fsList1.add(new FieldSchema("field2a", DataType.LONG)) ;
                fsList1.add(new FieldSchema("field3a", innerSchema1, DataType.BAG)) ;
                inputSchema1 = new Schema(fsList1) ;
            }
    
            // set schemas
            load1.setScriptSchema(Util.translateSchema((inputSchema1))) ; ;
            LogicalPlan innerRelPlan = new LogicalPlan();
            LOForEach foreach1 = new LOForEach(plan);
            foreach1.setInnerPlan(innerRelPlan);
            
            LOGenerate loGen = new LOGenerate(innerRelPlan);
            innerRelPlan.add(loGen);
            
            LOInnerLoad innerLoad1 = new LOInnerLoad(innerRelPlan, foreach1, 2);
            innerRelPlan.add(innerLoad1);
            innerRelPlan.connect(innerLoad1, loGen);
            
            // Create expression inner plan #1 of input #1
            LogicalExpressionPlan innerPlan1 = new LogicalExpressionPlan() ;
            ProjectExpression project11 = new ProjectExpression(innerPlan1, 0, 2, loGen) ;
            
            List<Integer> projections1 = new ArrayList<Integer>() ;
            projections1.add(1) ;
            projections1.add(2) ;
            //ProjectExpression project12 = new ProjectExpression(innerPlan1, project11, projections1) ;
            //project12.setSentinel(false);
            DereferenceExpression project12 = new DereferenceExpression(innerPlan1, projections1);
           // innerPlan1.connect(project11, project12);
            innerPlan1.connect(project12, project11);
            
            // Create expression inner plan #1 of input #2
            LogicalExpressionPlan innerPlan2 = new LogicalExpressionPlan() ;
            innerLoad1 = new LOInnerLoad(innerRelPlan, foreach1, 0);
            innerRelPlan.add(innerLoad1);
            innerRelPlan.connect(innerLoad1, loGen);
            ProjectExpression project21 = new ProjectExpression(innerPlan2, 1, 0, loGen) ;
            
            innerLoad1 = new LOInnerLoad(innerRelPlan, foreach1, 1);
            innerRelPlan.add(innerLoad1);
            innerRelPlan.connect(innerLoad1, loGen);
            //ProjectExpression project22 = new ProjectExpression(innerPlan2, 2, 1, loGen) ;
            ProjectExpression project22 = new ProjectExpression(innerPlan2, 2, 0, loGen) ;
            
            AddExpression add21 = new AddExpression(innerPlan2, project21, project22) ;
    
//            innerPlan2.add(project21) ;
//            innerPlan2.add(project22) ;
//            innerPlan2.add(add21) ;
//    
//            innerPlan2.connect(project21, add21);
//            innerPlan2.connect(project22, add21);
    
            // List of plans
            ArrayList<LogicalExpressionPlan> generatePlans = new ArrayList<LogicalExpressionPlan>() ;
            generatePlans.add(innerPlan1);
            generatePlans.add(innerPlan2);
    
            // List of flatten flags
            boolean [] flattens = {true, false}; 
    
            loGen.setFlattenFlags(flattens);
            loGen.setOutputPlans(generatePlans);
    
            // construct the main plan
            plan.add(load1) ;
            plan.add(foreach1) ;
    
            plan.connect(load1, foreach1);
    
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            typeChecker.visit();  
    
            printMessageCollector(collector) ;
            //printTypeGraph(plan) ;
    
            if (collector.hasError()) {
                throw new AssertionError("Expect no  error") ;
            }

            // check outer schema
            LogicalSchema endResultSchema = foreach1.getSchema() ;
            assertEquals(endResultSchema.getField(0).type, DataType.LONG) ;
            assertEquals(endResultSchema.getField(1).type, DataType.BYTEARRAY) ;
            assertEquals(endResultSchema.getField(2).type, DataType.LONG) ;
        }
    
        @Test
        public void testCross1() throws Throwable {
    
            printCurrentMethodName();
            LogicalPlan plan = new LogicalPlan() ;
    
            LOLoad load1 = genDummyLOLoadNewLP(plan) ;
            LOLoad load2 = genDummyLOLoadNewLP(plan) ;
    
            String[] aliases1 = new String[]{ "a", "b", "c" } ;
            byte[] types1 = new byte[] { DataType.INTEGER, DataType.LONG, DataType.BYTEARRAY } ;
            Schema schema1 = genFlatSchema(aliases1, types1) ;
    
            String[] aliases2 = new String[]{ "e", "f" } ;
            byte[] types2 = new byte[] { DataType.FLOAT, DataType.DOUBLE } ;
            Schema schema2 = genFlatSchema(aliases2, types2) ;
    
            // set schemas
            load1.setScriptSchema(Util.translateSchema(schema1)) ;
            load2.setScriptSchema(Util.translateSchema(schema2));
    
            LOCross cross = new LOCross(plan) ;
    
            // wiring
            plan.add(load1) ;
            plan.add(load2) ;
            plan.add(cross) ;
    
            plan.connect(load1, cross);
            plan.connect(load2, cross);
    
            // validate
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            typeChecker.visit();  
    
            printMessageCollector(collector) ;
            //printTypeGraph(plan) ;
    
            assertEquals(cross.getSchema().size(), 5) ;
            assertEquals(cross.getSchema().getField(0).type, DataType.INTEGER);
            assertEquals(cross.getSchema().getField(1).type, DataType.LONG);
            assertEquals(cross.getSchema().getField(2).type, DataType.BYTEARRAY);
            assertEquals(cross.getSchema().getField(3).type, DataType.FLOAT);
            assertEquals(cross.getSchema().getField(4).type, DataType.DOUBLE);
    
        }
    
        @Test
        public void testLineage1() throws Throwable {
            String query = "a = load 'a' as (field1: int, field2: float, field3: chararray );" 
                + "b = foreach a generate field1 + 1.0 ;";

            CastExpression cast = getCastFromLastForeach(query);

            assertTrue(cast.getFuncSpec() == null);
    
        }
        
        
        /**
         * Convert query to logical plan, do validations, 
         * return the cast in the expression in the foreach
         * relation which is the final relation in the plan  
         * @param query
         * @return
         * @throws FrontendException
         */
        private CastExpression getCastFromLastForeach(String query) throws FrontendException {
            return getCastFromLastForeach(query, 0);
        }
        
        

        private CastExpression getCastFromLastForeach(String query, int expressionNum) throws FrontendException {
            LogicalPlan plan = createAndProcessLPlan(query);
            LOForEach foreach = (LOForEach)plan.getSinks().get(0);
            LogicalExpressionPlan foreachPlan =
                ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(expressionNum);
    
            LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    
            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    
            assertNotNull(foreachPlan.getPredecessors(exOp));
            return (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
        }

        private LogicalPlan createAndProcessLPlan(String query) throws FrontendException {
            LogicalPlan plan = generateLogicalPlan(query);
            new ColumnAliasConversionVisitor( plan ).visit();
            // validate
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            typeChecker.visit();  
            new CastLineageSetter(plan, collector).visit();
    
            printMessageCollector(collector) ;
            //printTypeGraph(plan) ;
            //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    
            if (collector.hasError()) {
                throw new AssertionError("Expect no  error") ;
            }
            return plan;
        }

        private LogicalPlan generateLogicalPlan(String query) {
            try {
                return ParserTestingUtils.generateLogicalPlan( query );
            } catch(Exception ex) {
                ex.printStackTrace();
                Assert.fail( "Failed to generate logical plan for query [" + query + "] due to exception: " + ex );
            }
            return null;
        }
    
        @Test
        public void testLineage1NoSchema() throws Throwable {
            String query = "a = load 'a';" 
                + "b = foreach a generate $1 + 1.0 ;" ;
            CastExpression cast = getCastFromLastForeach(query);
            
            assertTrue(cast.getFuncSpec().getClassName().startsWith("org.apache.pig.builtin.PigStorage"));
    
        }
    
        @Test
        public void testLineage2() throws Throwable {
            String query = "a = load 'a' as (field1, field2: float, field3: chararray );" 
                + "b = foreach a generate field1 + 1.0 ;" ;
            CastExpression cast = getCastFromLastForeach(query);
            
            assertTrue(cast.getFuncSpec().getClassName().startsWith("org.apache.pig.builtin.PigStorage"));
    
        }
    
        //@Test
        public void testGroupLineage() throws Throwable {
            //FIXME : ColumnAliasConversion exception - in statement d, field1 not found in schema of c?
            String query = "a = load 'a' using PigStorage('a') as (field1 , field2: float, field3: chararray );" 
            + " b = group a by field1 ;"
            + " c = foreach b generate flatten(a) ;"
            + " d = foreach c generate field1 + 1.0 ;";
            checkLastForeachCastLoadFunc(query, "PigStorage('a')");

    
        }
    
        /**
         * check the load function in the cast in the expression in the foreach
         * relation which is the final relation in the plan 
         * @param query
         * @param loadFuncStr
         * @throws FrontendException
         */
        private void checkLastForeachCastLoadFunc(String query, String loadFuncStr)
        throws FrontendException {
            checkLastForeachCastLoadFunc(query, loadFuncStr, 0);

        }
        
        

        private void checkLastForeachCastLoadFunc(String query,
                String loadFuncStr, int expressionNum)
        throws FrontendException {
            CastExpression cast = getCastFromLastForeach(query, expressionNum);
            checkCastLoadFunc(cast, loadFuncStr);            
        }

        private void checkCastLoadFunc(CastExpression cast, String loadFuncStr) {
            assertNotNull(cast);
            if(loadFuncStr == null){
                assertNull(cast.getFuncSpec());
            }
            else{
                assertNotNull("Expecting cast funcspec to be non null",
                        cast.getFuncSpec());
                assertEquals("Load function string", 
                        loadFuncStr, cast.getFuncSpec().toString());
            }            
        }

        @Test
        public void testGroupLineageNoSchema() throws Throwable {
            String query = "a = load 'a' using PigStorage('a') ;" 
                + "b = group a by $0 ;"
                + "c = foreach b generate flatten(a) ;" 
            + "d = foreach c generate $0 + 1.0 ;" ;
            checkLastForeachCastLoadFunc(query, "PigStorage('a')");
    
        }
    
        @Test
        public void testGroupLineage2() throws Throwable {
            String query =  "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
            + " b = group a by field1 ;"
            + " c = foreach b generate group + 1.0 ;";
            checkLastForeachCastLoadFunc(query, "PigStorage('a')");
    
        }
    
        @Test
        public void testGroupLineage2NoSchema() throws Throwable {
            String query =  "a = load 'a' using PigStorage('a') ;"
            + "b = group a by $0 ;"
            + "c = foreach b generate group + 1.0 ;";
            checkLastForeachCastLoadFunc(query, "PigStorage('a')");
    
        }
    
        @Test
        public void testGroupLineageStar() throws Throwable {
            String query = "a = load 'a' using PigStorage('a') as (name, age, gpa);"
            + "b = group a by *;"
            + "c = foreach b generate flatten(group);"
            + "d = foreach c generate $0 + 1;";
    
            checkLastForeachCastLoadFunc(query, "PigStorage('a')");
    
        }
    
        @Test
        public void testGroupLineageStarNoSchema() throws Throwable {
            String query = "a = load 'a' using PigStorage('a');" 
            + "b = group a by *;"
            + "c = foreach b generate flatten(group);"
            + "d = foreach c generate $0 + 1;";
    
            checkLastForeachCastLoadFunc(query, "PigStorage('a')");
    
        }
    
        //@Test
        public void testCogroupLineage() throws Throwable {
            //FIXME : same issue as testGroupLineage
            String query = "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
            + "b = load 'a'  using PigStorage('b') as (field4, field5, field6: chararray );"
            + "c = cogroup a by field1, b by field4 ;"
            + "d = foreach c generate group, flatten(a), flatten(b)  ;"
            + "e = foreach d generate group, field1 + 1, field4 + 2.0  ;";
    
            checkLastForeachCastLoadFunc(query, "PigStorage('a')", 1);
    
        }
    
        //@Test
        public void testCogroupMapLookupLineage() throws Throwable {
            //FIXME - invalid logical plan
            String query =  "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
            + "b = load 'a' using PigStorage('b') as (field4, field5, field6: chararray );"
            + "c = cogroup a by field1, b by field4 ;"
            + "d = foreach c generate group, flatten(a), flatten(b)  ;"
            + "e = foreach d generate group, field1#'key' + 1, field4 + 2.0  ;";
    
            
            LogicalPlan plan = generateLogicalPlan(query);
            new ColumnAliasConversionVisitor( plan ).visit();
            // validate
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            typeChecker.visit();  
            new CastLineageSetter(plan, collector).visit();
            printMessageCollector(collector) ;
            //printTypeGraph(plan) ;
            //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    
            if (collector.hasError()) {
                throw new AssertionError("Expect no  error") ;
            }
    
            LOForEach foreach = (LOForEach)plan.getSinks().get(0);
            LogicalExpressionPlan foreachPlan =
                ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
    
            LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    
            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);

            CastExpression cast1 = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
            MapLookupExpression map = (MapLookupExpression)foreachPlan.getSuccessors(cast1).get(0);
            checkCastLoadFunc(cast1, "PigStorage('a')");
            
            CastExpression cast2 = (CastExpression)foreachPlan.getSuccessors(map).get(0);
            checkCastLoadFunc(cast1, "PigStorage('a')");
            
            foreachPlan =  ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(2);
            exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
            CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
            assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage('b')"));
    
        }
    
        @Test
        public void testCogroupStarLineage() throws Throwable {
            String query = "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
            + "b = load 'b' using PigStorage('b') as (field4, field5, field6: chararray );"
            + "c = cogroup a by *, b by * ;"
            + "d = foreach c generate group, flatten($1), flatten($2);"
            + "e = foreach d generate group, field1 + 1, field4 + 2.0;";
    
            checkLastForeachCastLoadFunc(query, "PigStorage('a')", 1);
            checkLastForeachCastLoadFunc(query, "PigStorage('b')", 2);        
    
        }
    
        @Test
        public void testCogroupStarLineageFail() throws Throwable {
         
            String query = "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
            + "b = load 'b' using PigStorage('b') as (field4, field5, field6: chararray );"
            + "c = cogroup a by *, b by * ;"
            + "d = foreach c generate group, flatten($1), flatten($2);"
            + "e = foreach d generate group + 1, field1 + 1, field4 + 2.0;";
    
            
            LogicalPlan plan = generateLogicalPlan(query);
            new ColumnAliasConversionVisitor( plan ).visit();
            // validate
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            try {
                TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
                typeChecker.visit();
                fail("exception expected");
            } catch(TypeCheckerException pve) {
                //test pass
            }
    
            printMessageCollector(collector) ;
    
            if (!collector.hasError()) {
                throw new AssertionError("Expect error") ;
            }
    
        }
    
        //@Test
        public void testCogroupStarLineage1() throws Throwable {
            //FIXME : parser fails because of a::field1 name - "::" is not working
            String query = "a = load 'a' using PigStorage('x') as (field1, field2: float, field3: chararray );"
            + "b = load 'b' using PigStorage('x') as (field4, field5, field6: chararray );"
            + "c = cogroup a by *, b by * ;"
            + "d = foreach c generate flatten(group), flatten($1), flatten($2);"
            + "e = foreach d generate $0 + 1, a::field1 + 1, field4 + 2.0;";
    
            checkLastForeachCastLoadFunc(query, "PigStorage('x')", 0);
            checkLastForeachCastLoadFunc(query, "PigStorage('x')", 1);
            checkLastForeachCastLoadFunc(query, "PigStorage('x')", 2);
            
//            // validate
//            CompilationMessageCollector collector = new CompilationMessageCollector() ;
//            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
//            try {
//                typeChecker.visit();  
//            }
//            catch (TypeCheckerException pve) {
//                //not good
//            }
//    
//            printMessageCollector(collector) ;
//            //printTypeGraph(plan) ;
//            //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
//    
//            if (collector.hasError()) {
//                throw new AssertionError("Expect no  error") ;
//            }
//    
//    
//            LOForEach foreach = (LOForEach)plan.getSinks().get(0);
//            LogicalExpressionPlan foreachPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
//    
//            LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
//            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
//    
//            CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
//            assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
//    
//            foreachPlan = foreach.getForEachPlans().get(1);
//            exOp = foreachPlan.getRoots().get(0);
//            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
//    
//            cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
//            assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
//    
//            foreachPlan = foreach.getForEachPlans().get(2);
//            exOp = foreachPlan.getRoots().get(0);
//            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
//            cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
//            assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    
        }
    
        //@Test
        public void testCogroupStarLineageNoSchemaFail() throws Throwable {
            //FIXME : expected exception is thrown by ProjectStarTranslator 
            String query = "a = load 'a' using PigStorage('a') ;"
            + "b = load 'b' using PigStorage('b') ;"
            + "c = cogroup a by *, b by *;";
            boolean exceptionThrown = false;
            try {
                createAndProcessLPlan(query);
            } catch(Exception e) {
                assertTrue(e.getMessage().contains("Cogroup/Group by * is only allowed if " +
                "the input has a schema"));
                exceptionThrown = true;
            }
            assertTrue(exceptionThrown);
            
        }
    
        //@Test
        public void testCogroupMultiColumnProjectLineage() throws Throwable {
            //FIXME - invalid logical plan - derefence has not successor (findRereferent) gives NPE
            // usage   a.(field1, field2), b.(field4);"
            String query = "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
            + "b = load 'b' using PigStorage('b') as (field4, field5, field6: chararray );"
            + "c = cogroup a by field1, b by field4 ;"
            + "d = foreach c generate group, a.(field1, field2), b.(field4);"
            + "e = foreach d generate group, flatten($1), flatten($2);"
            + "f = foreach e generate group, field1 + 1, field4 + 2.0;";
    
            checkLastForeachCastLoadFunc(query, "PigStorage('a')", 1);
            checkLastForeachCastLoadFunc(query, "PigStorage('b')", 2);
//            
//            // validate
//            CompilationMessageCollector collector = new CompilationMessageCollector() ;
//            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
//            try {
//                typeChecker.visit();  
//            }
//            catch (TypeCheckerException pve) {
//                //not good
//            }
//    
//            printMessageCollector(collector) ;
//            //printTypeGraph(plan) ;
//            //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
//    
//            if (collector.hasError()) {
//                throw new AssertionError("Expect no  error") ;
//            }
//    
//    
//            LOForEach foreach = (LOForEach)plan.getSinks().get(0);
//            LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
//    
//            LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
//    
//            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
//    
//            CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
//            assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
//    
//            foreachPlan = foreach.getForEachPlans().get(2);
//            exOp = foreachPlan.getRoots().get(0);
//            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
//            cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
//            assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
//    
        }
    
        //@Test
        public void testCogroupProjectStarLineage() throws Throwable {
            //FIXME : column alias finder failure. seems to be a star expansion error
            String query = "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
            + "b = load 'b' using PigStorage('b') as (field4, field5, field6: chararray );"
            + "c = cogroup a by field1, b by field4 ;"
            + "d = foreach c generate * ;"
            + "f = foreach d generate group, flatten(a), flatten(b)  ;"
            + "g = foreach f generate group, field1 + 1, field4 + 2.0  ;";
    
            checkLastForeachCastLoadFunc(query, "PigStorage('a')", 1);
            checkLastForeachCastLoadFunc(query, "PigStorage('b')", 2);
            
//            // validate
//            CompilationMessageCollector collector = new CompilationMessageCollector() ;
//            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
//            try {
//                typeChecker.visit();  
//            }
//            catch (TypeCheckerException pve) {
//                //not good
//            }
//    
//            printMessageCollector(collector) ;
//            //printTypeGraph(plan) ;
//            //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
//    
//            if (collector.hasError()) {
//                throw new AssertionError("Expect no  error") ;
//            }
//    
//    
//            LOForEach foreach = (LOForEach)plan.getSinks().get(0);
//            LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
//    
//            LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
//    
//            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
//    
//            CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
//            assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
//    
//            foreachPlan = foreach.getForEachPlans().get(2);
//            exOp = foreachPlan.getRoots().get(0);
//            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
//            cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
//            assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    
        }
    //
    //    //@Test
    //    public void testCogroupProjectStarLineageNoSchema() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') ;") ;
    //        + "b = load 'b' using PigStorage() ;") ;
    //        + "c = cogroup a by $0, b by $0 ;") ;
    //        + "d = foreach c generate * ;") ;
    //        + "f = foreach d generate group, flatten(a), flatten(b)  ;") ;
    //        + "g = foreach f generate group, $1 + 1, $2 + 2.0  ;") ;
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        try {
    //            typeChecker.visit();  
    //        }
    //        catch (TypeCheckerException pve) {
    //            //not good
    //        }
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no error") ;
    //        }
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(2);
    //        exOp = foreachPlan.getRoots().get(0);
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //        cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //    }
    //
    //    //@Test
    //    public void testCogroupProjectStarLineageMixSchema() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );") ;
    //        + "b = load 'b' using PigStorage() ;") ;
    //        + "c = cogroup a by field1, b by $0 ;") ;
    //        + "d = foreach c generate * ;") ;
    //        + "f = foreach d generate group, flatten(a), flatten(b)  ;") ;
    //        + "g = foreach f generate group, field1 + 1, $4 + 2.0  ;") ;
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        try {
    //            typeChecker.visit();  
    //        }
    //        catch (TypeCheckerException pve) {
    //            //not good
    //        }
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no error") ;
    //        }
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(2);
    //        exOp = foreachPlan.getRoots().get(0);
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //        cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //    }
    //
        //@Test
        public void testCogroupLineageFail() throws Throwable {
          //FIXME : same issue as testGroupLineage
            String query =  "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
            + "b = load 'a' using PigStorage('b') as (field4, field5, field6: chararray );"
            + "c = cogroup a by field1, b by field4 ;"
            + "d = foreach c generate group, flatten(a), flatten(b)  ;"
            + "e = foreach d generate group + 1, field1 + 1, field4 + 2.0  ;";
            
            checkLastForeachCastLoadFunc(query, null, 0);
            
        }

        //@Test
        public void testCogroupLineage2NoSchema() throws Throwable {
            //FIXME seems to be a problem in ForEach.getschema, it returns null
            //schema for alias d, while it should be  {group: bytearray,bytearray,bytearray}
            String query = "a = load 'a' using PigStorage('a') ;"
            + "b = load 'a' using PigStorage('b') ;"
            + "c = cogroup a by $0, b by $0 ;"
            + "d = foreach c generate group, flatten(a), flatten(b)  ;"
            + "e = foreach d generate group, $1 + 1, $2 + 2.0  ;";
    
            checkLastForeachCastLoadFunc(query, "PigStorage('a')", 1);
            checkLastForeachCastLoadFunc(query, "PigStorage('b')", 2);
//            // validate
//            CompilationMessageCollector collector = new CompilationMessageCollector() ;
//            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
//            typeChecker.visit();  
//    
//            printMessageCollector(collector) ;
//            //printTypeGraph(plan) ;
//            //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
//    
//            if (collector.hasError()) {
//                throw new AssertionError("Expect no  error") ;
//            }
//    
//            LOForEach foreach = (LOForEach)plan.getSinks().get(0);
//            LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
//    
//            LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
//    
//            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
//    
//            CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
//            assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
//    
//            foreachPlan = foreach.getForEachPlans().get(2);
//            exOp = foreachPlan.getRoots().get(0);
//            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
//            cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
//            assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    
        }
    
        @Test
        public void testUnionLineage() throws Throwable {
            //here the type checker will insert a cast for the union, converting the column field2 into a float
            String query = "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
            + "b = load 'a' using PigStorage('b') as (field4, field5, field6: chararray );"
            + "c = union a , b ;"
            + "d = foreach c generate field2 + 2.0  ;";
    
           checkLastForeachCastLoadFunc(query, null);
    
        }
   
        @Test
        public void testUnionLineageNoSchema() throws Throwable {
            //if all inputs to union have same load function, set the
            // load function func spec
            String query = "a = load 'a' using PigStorage('a') ;"
            + "b = load 'a' using PigStorage('a') ;"
            + "c = union a , b ;"
            + "d = foreach c generate $1 + 2.0  ;";
    
            checkLastForeachCastLoadFunc(query, "PigStorage('a')");
        }
    
        @Test
        public void testUnionLineageNoSchemaDiffLoadFunc() throws Throwable {
            String query = "a = load 'a' using PigStorage('a') ;"
            + "b = load 'a' using PigStorage('b') ;"
            + "c = union a , b ;"
            + "d = foreach c generate $1 + 2.0  ;";
            
            checkLastForeachCastLoadFunc(query, null);
          
        }
    
        @Test
        public void testUnionLineageDifferentSchema() throws Throwable {
            //here the type checker will insert a cast for the union, converting the column field2 into a float
            String query = "a = load 'a' using PigStorage('\u0001') as (field1, field2: float, field3: chararray );"
            + "b = load 'a' using PigStorage('\u0001') as (field4, field5, field6: chararray, field7 );"
            + "c = union a , b ;"
            + "d = foreach c generate $3 + 2.0  ;";
            
            checkLastForeachCastLoadFunc(query, "PigStorage('\u0001')");
    
        }
    
        @Test
        public void testUnionLineageDifferentSchemaFail() throws Throwable {
            String query = "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
            + "b = load 'a' using PigStorage('b') as (field4, field5, field6: chararray, field7 );"
            + "c = union a , b ;"
            + "d = foreach c generate $3 + 2.0  ;";
    
            checkWarning(query, CAST_LOAD_NOT_FOUND);

    
        }

        private void checkWarning(String query, String warnMsg)
        throws FrontendException {
            LogicalPlan plan = generateLogicalPlan(query);
            new ColumnAliasConversionVisitor( plan ).visit();
            // validate
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
            typeChecker.visit();  
            new CastLineageSetter(plan, collector).visit();

            printMessageCollector(collector) ;
            
            boolean isWarningSeen = false;
            assertTrue("message collector has message", collector.hasMessage());
            
            for(Message msg : collector){
                if(msg.getMessageType() == MessageType.Warning 
                        && msg.getMessage().contains(warnMsg)){
                    isWarningSeen = true;
                }
            }
            
            assertTrue("Expected warning is seen", isWarningSeen);
            

        }

        @Test
        public void testUnionLineageMixSchema() throws Throwable {
            //here the type checker will insert a cast for the union, converting the column field2 into a float
            String query = "a = load 'a' using PigStorage('x') as (field1, field2: float, field3: chararray );"
            + "b = load 'a' using PigStorage('x') ;"
            + "c = union a , b ;"
            + "d = foreach c generate $3 + 2.0  ;";
            
            checkLastForeachCastLoadFunc(query, "PigStorage('x')");
    
        }
    
        @Test
        public void testUnionLineageMixSchemaFail() throws Throwable {
            // different loader func spec associated with each input, so can't determine
            // which one to use on union output
            String query = "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
            + "b = load 'a' using PigStorage('b') ;"
            + "c = union a , b ;"
            + "d = foreach c generate $3 + 2.0  ;";
            
            checkWarning(query, CAST_LOAD_NOT_FOUND);
        }
    
        @Test
        public void testFilterLineage() throws Throwable {
            String query = "a = load 'a' as (field1, field2: float, field3: chararray );"
            + "b = filter a by field1 > 1.0 ;";
    
            checkLastFilterCast(query, "org.apache.pig.builtin.PigStorage");
    
        }
        private void checkLastFilterCast(String query, String loadFuncStr) throws FrontendException {
            LogicalPlan plan = createAndProcessLPlan(query);
            LOFilter filter = (LOFilter)plan.getSinks().get(0);
            LogicalExpressionPlan filterPlan = filter.getFilterPlan();
    
            LogicalExpression exOp = (LogicalExpression)filterPlan.getSinks().get(0);
    
            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) filterPlan.getSinks().get(1);
    
            CastExpression cast = (CastExpression)filterPlan.getPredecessors(exOp).get(0);
            checkCastLoadFunc(cast, loadFuncStr);

        }

        
        @Test
        public void testFilterLineageNoSchema() throws Throwable {
            String query = "a = load 'a' ;"
            + "b = filter a by $0 > 1.0 ;";
            
            checkLastFilterCast(query, "org.apache.pig.builtin.PigStorage");

        }
    
        @Test
        public void testFilterLineage1() throws Throwable {
            String query = "a = load 'a' as (field1, field2: float, field3: chararray );"
            + "b = filter a by field2 > 1.0 ;"
            + "c = foreach b generate field1 + 1.0 ;";
         
            checkLastForeachCastLoadFunc(query, "org.apache.pig.builtin.PigStorage");

        }
    
        @Test
        public void testFilterLineage1NoSchema() throws Throwable {
            String query = "a = load 'a' ;"
            + "b = filter a by $0 > 1.0 ;"
            + "c = foreach b generate $1 + 1.0 ;";
            
            checkLastForeachCastLoadFunc(query, "org.apache.pig.builtin.PigStorage");

        }
    
        //@Test
        public void testCogroupFilterLineage() throws Throwable {
            //FIXME : same issue as testGroupLineage
            String query = "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
            + "b = load 'a' using PigStorage('b') as (field4, field5, field6: chararray );"
            + "c = cogroup a by field1, b by field4 ;"
            + "d = foreach c generate group, flatten(a), flatten(b)  ;"
            + "e = filter d by field4 > 5;"
            + "f = foreach e generate group, field1 + 1, field4 + 2.0  ;";
    
            checkLastForeachCastLoadFunc(query, "PigStorage('a')", 1);
            checkLastForeachCastLoadFunc(query, "PigStorage('b')", 2);
            
//            
//            
//            // validate
//            CompilationMessageCollector collector = new CompilationMessageCollector() ;
//            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
//            typeChecker.visit();  
//    
//            printMessageCollector(collector) ;
//            //printTypeGraph(plan) ;
//            //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
//    
//            if (collector.hasError()) {
//                throw new AssertionError("Expect no  error") ;
//            }
//    
//    
//            LOForEach foreach = (LOForEach)plan.getSinks().get(0);
//            LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
//    
//            LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
//    
//            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
//    
//            CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
//            assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
//    
//            foreachPlan = foreach.getForEachPlans().get(2);
//            exOp = foreachPlan.getRoots().get(0);
//            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
//            cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
//            assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    
        }
    
    //    //@Test
    //    public void testCogroupFilterLineageNoSchema() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') ;") ;
    //        + "b = load 'a' using PigStorage() ;") ;
    //        + "c = cogroup a by $0, b by $0 ;") ;
    //        + "d = foreach c generate group, flatten(a), flatten(b)  ;") ;
    //        + "e = filter d by $2 > 5;") ;
    //        + "f = foreach e generate group, $1 + 1, $2 + 2.0  ;") ;
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(2);
    //        exOp = foreachPlan.getRoots().get(0);
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //        cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //
    //    }
    //
        @Test
        public void testSplitLineage() throws Throwable {
            String query = "a = load 'a' as (field1, field2: float, field3: chararray );"
            + "split a into b if field1 > 1.0, c if field1 <= 1.0 ;";
    
            checkSplitLineage(query,"org.apache.pig.builtin.PigStorage", "org.apache.pig.builtin.PigStorage");

        }
        
    /**
     * Check lineage in each split output (A,B), by checking the func spec
     * for cast in the split output filter expression 
     * @param query
     * @param loadFuncStrA
     * @param loadFuncStrB
     * @throws FrontendException
     */
    private void checkSplitLineage(String query, String loadFuncStrA, String loadFuncStrB)
    throws FrontendException {
        LogicalPlan plan = createAndProcessLPlan(query);
        
        
        LOSplitOutput splitOutputB = (LOSplitOutput)plan.getSinks().get(0);
        LogicalExpressionPlan bPlan = splitOutputB.getFilterPlan();

        LogicalExpression exOp = (LogicalExpression)bPlan.getSinks().get(0);

        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) bPlan.getSinks().get(1);

        CastExpression cast = (CastExpression)bPlan.getPredecessors(exOp).get(0);
        checkCastLoadFunc(cast, loadFuncStrA);
        

        LOSplitOutput splitOutputC = (LOSplitOutput)plan.getSinks().get(0);
        LogicalExpressionPlan cPlan = splitOutputC.getFilterPlan();

        exOp = (LogicalExpression) cPlan.getSinks().get(0);

        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) cPlan.getSinks().get(1);

        cast = (CastExpression)cPlan.getPredecessors(exOp).get(0);
        checkCastLoadFunc(cast, loadFuncStrB);
        
    }

    
        @Test
        public void testSplitLineageNoSchema() throws Throwable {
            //FIXME : split without schema - typechecker bug
            String query =  "a = load 'a' ;"
            + "split a into b if $0 > 1.0, c if $1 <= 1.0 ;";
    
            checkSplitLineage(query,"org.apache.pig.builtin.PigStorage", "org.apache.pig.builtin.PigStorage");
    
        }
    
        //@Test
        public void testSplitLineage1() throws Throwable {
            //FIXME : AstValidator - Alias 'b' is not defined
            String query = "a = load 'a' as (field1, field2: float, field3: chararray );"
            + "split a into b if field2 > 1.0, c if field2 <= 1.0 ;"
            + "c = foreach b generate field1 + 1.0 ;";
    
            checkLastForeachCastLoadFunc(query, "org.apache.pig.builtin.PigStorage");
            
//            // validate
//            CompilationMessageCollector collector = new CompilationMessageCollector() ;
//            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
//            typeChecker.visit();  
//    
//            printMessageCollector(collector) ;
//            //printTypeGraph(plan) ;
//            //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
//    
//            if (collector.hasError()) {
//                throw new AssertionError("Expect no  error") ;
//            }
//    
//            LOForEach foreach = (LOForEach)plan.getSinks().get(0);
//            LogicalExpressionPlan foreachPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
//    
//            LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
//    
//            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
//    
//            CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
//            assertTrue(cast.getFuncSpec().getClassName().startsWith("org.apache.pig.builtin.PigStorage"));
    
        }
    
//        //@Test
//        public void testSplitLineage1NoSchema() throws Throwable {
//            String query =  "a = load 'a' ;"
//            + "split a into b if $0 > 1.0, c if $1 <= 1.0 ;"
//            + "c = foreach b generate $1 + 1.0 ;";
//    
//            // validate
//            CompilationMessageCollector collector = new CompilationMessageCollector() ;
//            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
//            typeChecker.visit();  
//    
//            printMessageCollector(collector) ;
//            //printTypeGraph(plan) ;
//            //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
//    
//            if (collector.hasError()) {
//                throw new AssertionError("Expect no  error") ;
//            }
//    
//            LOForEach foreach = (LOForEach)plan.getSinks().get(0);
//            LogicalExpressionPlan foreachPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
//    
//            LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
//    
//            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
//    
//            CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
//            assertTrue(cast.getFuncSpec().getClassName().startsWith("org.apache.pig.builtin.PigStorage"));
//    
//        }
    //
    //    //@Test
    //    public void testCogroupSplitLineage() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
    //        + "b = load 'a' using PigStorage() as (field4, field5, field6: chararray );"
    //        + "c = cogroup a by field1, b by field4 ;"
    //        + "d = foreach c generate group, flatten(a), flatten(b)  ;"
    //        + "split d into e if field4 > 'm', f if field6 > 'm'  ;"
    //        + "g = foreach e generate group, field1 + 1, field4 + 2.0  ;"
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(2);
    //        exOp = foreachPlan.getRoots().get(0);
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //        cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //
    //    }
    //
    //    //@Test
    //    public void testCogroupSplitLineageNoSchema() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') ;") ;
    //        + "b = load 'a' using PigStorage() ;") ;
    //        + "c = cogroup a by $0, b by $0 ;") ;
    //        + "d = foreach c generate group, flatten(a), flatten(b)  ;") ;
    //        + "split d into e if $1 > 'm', f if $1 > 'm'  ;") ;
    //        + "g = foreach e generate group, $1 + 1, $2 + 2.0  ;") ;
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(2);
    //        exOp = foreachPlan.getRoots().get(0);
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //        cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //
    //    }
    //
        @Test
        public void testDistinctLineage() throws Throwable {
            String query = "a = load 'a' as (field1, field2: float, field3: chararray );"
            + "b = distinct a;"
            + "c = foreach b generate field1 + 1.0 ;";
            
            checkLastForeachCastLoadFunc(query, "org.apache.pig.builtin.PigStorage");

        }
        
    
        @Test
        public void testDistinctLineageNoSchema() throws Throwable {
            String query =  "a = load 'a' ;"
            + "b = distinct a;"
            + "c = foreach b generate $1 + 1.0 ;";
            checkLastForeachCastLoadFunc(query, "org.apache.pig.builtin.PigStorage");
            
//            // validate
//            CompilationMessageCollector collector = new CompilationMessageCollector() ;
//            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
//            typeChecker.visit();  
//    
//            printMessageCollector(collector) ;
//            //printTypeGraph(plan) ;
//            //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
//    
//            if (collector.hasError()) {
//                throw new AssertionError("Expect no  error") ;
//            }
//    
//            LOForEach foreach = (LOForEach)plan.getSinks().get(0);
//            LogicalExpressionPlan foreachPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
//    
//            LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
//    
//            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
//    
//            CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
//            assertTrue(cast.getFuncSpec().getClassName().startsWith("org.apache.pig.builtin.PigStorage"));
    
        }
    //
    //    //@Test
    //    public void testCogroupDistinctLineage() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );") ;
    //        + "b = load 'a' using PigStorage() as (field4, field5, field6: chararray );") ;
    //        + "c = cogroup a by field1, b by field4 ;") ;
    //        + "d = foreach c generate group, flatten(a), flatten(b)  ;") ;
    //        + "e = distinct d ;") ;
    //        + "f = foreach e generate group, field1 + 1, field4 + 2.0  ;") ;
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(2);
    //        exOp = foreachPlan.getRoots().get(0);
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //        cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //
    //    }
    //
    //    //@Test
    //    public void testCogroupDistinctLineageNoSchema() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') ;") ;
    //        + "b = load 'a' using PigStorage() ;") ;
    //        + "c = cogroup a by $0, b by $0 ;") ;
    //        + "d = foreach c generate group, flatten(a), flatten(b)  ;") ;
    //        + "e = distinct d ;") ;
    //        + "f = foreach e generate group, $1 + 1, $2 + 2.0  ;") ;
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(2);
    //        exOp = foreachPlan.getRoots().get(0);
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //        cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //
    //    }
    //
        @Test
        public void testSortLineage() throws Throwable {
            String query =  "a = load 'a' as (field1, field2: float, field3: chararray );"
            + "b = order a by field1;"
            + "c = foreach b generate field1 + 1.0 ;";
    
            checkLastForeachCastLoadFunc(query, "org.apache.pig.builtin.PigStorage");
    
        }
    
        @Test
        public void testSortLineageNoSchema() throws Throwable {
            String query =  "a = load 'a' ;"
            + "b = order a by $1;"
            + "c = foreach b generate $1 + 1.0 ;";
    
            checkLastForeachCastLoadFunc(query, "org.apache.pig.builtin.PigStorage");
        }
        
       
        /**
         * Test nested foreach with sort
         * @throws Throwable
         */
        @Test
        public void testNestedSort() throws Throwable {
            
            String query = "a = load 'x';" +
                    "b = group a by $0;" +
                    "c = foreach b " +
                    "       {c1 = order $1 by $1; generate $0 + 1, flatten(c1); };";
            
            checkLastForeachCastLoadFunc(query, "org.apache.pig.builtin.PigStorage");
        }
        
        
        /**
         * Test nested foreach with distinct
         * @throws Throwable
         */
        @Test
        public void testNestedDistinct() throws Throwable {
            String query = "a = load '/user/pig/tests/data/singlefile/studenttab10k' as (name, age, gpa);" +
            "b = group a by name; " +
            "c = foreach b { aa = distinct a.age; generate group + 1, COUNT(aa); }";

            checkLastForeachCastLoadFunc(query, "org.apache.pig.builtin.PigStorage");
 
        }
        
        
    //
    //    //@Test
    //    public void testCogroupSortLineage() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );") ;
    //        + "b = load 'a' using PigStorage() as (field4, field5, field6: chararray );") ;
    //        + "c = cogroup a by field1, b by field4 ;") ;
    //        + "d = foreach c generate group, flatten(a), flatten(b)  ;") ;
    //        + "e = order d by field4 desc;") ;
    //        + "f = foreach e generate group, field1 + 1, field4 + 2.0  ;") ;
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(2);
    //        exOp = foreachPlan.getRoots().get(0);
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //        cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //
    //    }
    //
    //    //@Test
    //    public void testCogroupSortLineageNoSchema() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') ;") ;
    //        + "b = load 'a' using PigStorage() ;") ;
    //        + "c = cogroup a by $0, b by $0 ;") ;
    //        + "d = foreach c generate group, flatten(a), flatten(b)  ;") ;
    //        + "e = order d by $2 desc;") ;
    //        + "f = foreach e generate group, $1 + 1, $2 + 2.0  ;") ;
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(2);
    //        exOp = foreachPlan.getRoots().get(0);
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //        cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //
    //    }
    //
    //    //@Test
    //    public void testCogroupSortStarLineage() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );") ;
    //        + "b = load 'a' using PigStorage() as (field4, field5, field6: chararray );") ;
    //        + "c = cogroup a by field1, b by field4 ;") ;
    //        + "d = foreach c generate group, flatten(a), flatten(b)  ;") ;
    //        + "e = order d by * desc;") ;
    //        + "f = foreach e generate group, field1 + 1, field4 + 2.0  ;") ;
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(2);
    //        exOp = foreachPlan.getRoots().get(0);
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //        cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //
    //    }
    //
    //    //@Test
    //    public void testCogroupSortStarLineageNoSchema() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') ;") ;
    //        + "b = load 'a' using PigStorage() ;") ;
    //        + "c = cogroup a by $0, b by $0 ;") ;
    //        + "d = foreach c generate group, flatten(a), flatten(b)  ;") ;
    //        + "e = order d by * desc;") ;
    //        + "f = foreach e generate group, $1 + 1, $2 + 2.0  ;") ;
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(2);
    //        exOp = foreachPlan.getRoots().get(0);
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //        cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //
    //    }
    //
        @Test
        public void testCrossLineage() throws Throwable {
            String query =  "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
            + "b = load 'a' using PigStorage('b') as (field4, field5, field6: chararray );"
            + "c = cross a, b ;"
            + "d = foreach c generate field1 + 1, field4 + 2.0  ;";
    
            checkLastForeachCastLoadFunc(query, "PigStorage('a')",0);
            checkLastForeachCastLoadFunc(query, "PigStorage('b')",1);

        }
    
        @Test
        public void testCrossLineageNoSchema() throws Throwable {
            String query =  "a = load 'a' using PigStorage('a') ;"
            + "b = load 'a' using PigStorage('a') ;"
            + "c = cross a , b ;"
            + "d = foreach c generate $1 + 2.0  ;";
    
            checkLastForeachCastLoadFunc(query, "PigStorage('a')");
        }
    
        @Test
        public void testCrossLineageNoSchemaFail() throws Throwable {
            String query = "a = load 'a' using PigStorage('a') ;"
            + "b = load 'a' using PigStorage('b') ;"
            + "c = cross a , b ;"
            + "d = foreach c generate $1 + 2.0  ;";
            
            checkWarning(query, CAST_LOAD_NOT_FOUND);
    
        }
    
        @Test
        public void testCrossLineageMixSchema() throws Throwable {
            //here the type checker will insert a cast for the union, converting the column field2 into a float
            String query = "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
            + "b = load 'a' using PigStorage('a') ;"
            + "c = cross a , b ;"
            + "d = foreach c generate $3 + 2.0  ;";
            
            checkLastForeachCastLoadFunc(query, "PigStorage('a')");
        }
    
        @Test
        public void testCrossLineageMixSchemaFail() throws Throwable {
            String query = "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
            + "b = load 'a' using PigStorage('b') ;"
            + "c = cross a , b ;"
            + "d = foreach c generate $3 + 2.0  ;";
            
            checkWarning(query, CAST_LOAD_NOT_FOUND);
               
        }
    
        @Test
        public void testJoinLineage() throws Throwable {
            String query =  "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
            + "b = load 'a' using PigStorage('b') as (field4, field5, field6: chararray );"
            + "c = join a by field1, b by field4 ;"
            + "d = foreach c generate field1 + 1, field4 + 2.0  ;";
    
            checkLastForeachCastLoadFunc(query, "PigStorage('a')", 0);
            checkLastForeachCastLoadFunc(query, "PigStorage('b')", 1);

        }
    
        @Test
        public void testJoinLineageNoSchema() throws Throwable {
            // same load func for all inputs
            String query =  "a = load 'a' using PigStorage('a') ;"
            + "b = load 'a' using PigStorage('a') ;"
            + "c = join a by $0, b by $0 ;"
            + "d = foreach c generate $1 + 2.0  ;";
            checkLastForeachCastLoadFunc(query, "PigStorage('a')", 0);

        }
    //
    //    //@Test
    //    public void testJoinLineageNoSchemaFail() throws Throwable {
    //        //this test case should change when we decide on what flattening a tuple or bag
    //        //with null schema results in a foreach flatten and hence a join
    //        + "a = load 'a' using PigStorage('a') ;"
    //        + "b = load 'a' using PigStorage() ;"
    //        + "c = join a by $0, b by $0 ;"
    //        + "d = foreach c generate $1 + 2.0  ;"
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //
    //        try {
    //            typeChecker.visit();  
    //        }
    //        catch (TypeCheckerException pve) {
    //            // good
    //        }
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        assertTrue(collector.hasError());
    //    }
    //
    //    //@Test
    //    public void testJoinLineageMixSchema() throws Throwable {
    //        + "a = load 'a' using PigStorage() as (field1, field2: float, field3: chararray );"
    //        + "b = load 'a' using PigStorage() ;"
    //        + "c = join a by field1, b by $0 ;"
    //        + "d = foreach c generate $3 + 2.0  ;"
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //
    //    }
    //
    //    //@Test
    //    public void testJoinLineageMixSchemaFail() throws Throwable {
    //        //this test case should change when we decide on what flattening a tuple or bag
    //        //with null schema results in a foreach flatten and hence a join
    //        + "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
    //        + "b = load 'a' using PigStorage() ;"
    //        + "c = join a by field1, b by $0 ;"
    //        + "d = foreach c generate $3 + 2.0  ;"
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //
    //        try {
    //            typeChecker.visit();  
    //        }
    //        catch (TypeCheckerException pve) {
    //            // good
    //        }
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        assertTrue(collector.hasError());
    //    }
    //
        @Test
        public void testLimitLineage() throws Throwable {
            String query =  "a = load 'a' as (field1, field2: float, field3: chararray );"
            + "b = limit a 100;"
            + "c = foreach b generate field1 + 1.0 ;";
            
            checkLastForeachCastLoadFunc(query, "org.apache.pig.builtin.PigStorage", 0);
            
    
//            // validate
//            CompilationMessageCollector collector = new CompilationMessageCollector() ;
//            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
//            typeChecker.visit();  
//    
//            printMessageCollector(collector) ;
//            //printTypeGraph(plan) ;
//            //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
//    
//            if (collector.hasError()) {
//                throw new AssertionError("Expect no  error") ;
//            }
//    
//            LOForEach foreach = (LOForEach)plan.getSinks().get(0);
//            LogicalExpressionPlan foreachPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
//    
//            LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
//    
//            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
//    
//            CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
//            assertTrue(cast.getFuncSpec().getClassName().startsWith("org.apache.pig.builtin.PigStorage"));
    
        }
    
        @Test
        public void testLimitLineageNoSchema() throws Throwable {
            String query =  "a = load 'a' ;"
            + "b = limit a 100;"
            + "c = foreach b generate $1 + 1.0 ;";
            
            checkLastForeachCastLoadFunc(query, "org.apache.pig.builtin.PigStorage", 0);
            
//            // validate
//            CompilationMessageCollector collector = new CompilationMessageCollector() ;
//            TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
//            typeChecker.visit();  
//    
//            printMessageCollector(collector) ;
//            //printTypeGraph(plan) ;
//            //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
//    
//            if (collector.hasError()) {
//                throw new AssertionError("Expect no  error") ;
//            }
//    
//            LOForEach foreach = (LOForEach)plan.getSinks().get(0);
//            LogicalExpressionPlan foreachPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
//    
//            LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
//    
//            if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
//    
//            CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
//            assertTrue(cast.getFuncSpec().getClassName().startsWith("org.apache.pig.builtin.PigStorage"));
    
        }
    //
    //    //@Test
    //    public void testCogroupLimitLineage() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
    //        + "b = load 'a' using PigStorage() as (field4, field5, field6: chararray );"
    //        + "c = cogroup a by field1, b by field4 ;"
    //        + "d = foreach c generate group, flatten(a), flatten(b)  ;"
    //        + "e = limit d 100;"
    //        + "f = foreach e generate group, field1 + 1, field4 + 2.0  ;"
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(2);
    //        exOp = foreachPlan.getRoots().get(0);
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //        cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //
    //    }
    //
    //    //@Test
    //    public void testCogroupLimitLineageNoSchema() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') ;"
    //        + "b = load 'a' using PigStorage() ;"
    //        + "c = cogroup a by $0, b by $0 ;"
    //        + "d = foreach c generate group, flatten(a), flatten(b)  ;"
    //        + "e = limit d 100;"
    //        + "f = foreach e generate group, $1 + 1, $2 + 2.0  ;"
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(2);
    //        exOp = foreachPlan.getRoots().get(0);
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //        cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //
    //    }
    //
    //    //@Test
    //    public void testCogroupTopKLineage() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );") ;
    //        + "b = load 'a' using PigStorage() as (field4, field5, field6: chararray );"
    //        + "c = cogroup a by field1, b by field4 ;"
    //        + "d = foreach c generate group, flatten(a), flatten(b)  ;"
    //        + "e = order d by field1 desc;"
    //        + "f = limit e 100;"
    //        + "g = foreach f generate group, field1 + 1, field4 + 2.0  ;"
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(2);
    //        exOp = foreachPlan.getRoots().get(0);
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //        cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //
    //    }
    //
    //    //@Test
    //    public void testCogroupTopKLineageNoSchema() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') ;"
    //        + "b = load 'a' using PigStorage() ;"
    //        + "c = cogroup a by $0, b by $0 ;"
    //        + "d = foreach c generate group, flatten(a), flatten(b)  ;"
    //        + "e = order d by $2 desc;"
    //        + "f = limit e 100;"
    //        + "g = foreach f generate group, $1 + 1, $2 + 2.0  ;"
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(2);
    //        exOp = foreachPlan.getRoots().get(0);
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //        cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //
    //    }
    //
    //    //@Test
    //    public void testStreamingLineage1() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') as (field1: int, field2: float, field3: chararray );"
    //        + "b = stream a through `" + simpleEchoStreamingCommand + "`;");
    //        + "c = foreach b generate $1 + 1.0 ;"
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error"
    //        }
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("org.apache.pig.builtin.PigStreaming"));
    //
    //    }
    //
    //    //@Test
    //    public void testStreamingLineage2() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') as (field1: int, field2: float, field3: chararray );"
    //        + "b = stream a through `" + simpleEchoStreamingCommand + "` as (f1, f2: float);");
    //        + "c = foreach b generate f1 + 1.0, f2 + 4 ;"
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("org.apache.pig.builtin.PigStreaming"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        exOp = foreachPlan.getRoots().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        assertTrue(foreachPlan.getSuccessors(exOp).get(0) instanceof AddExpression);
    //    }
    //
    //    //@Test
    //    public void testCogroupStreamingLineage() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
    //        + "b = stream a through `" + simpleEchoStreamingCommand + "` as (field4, field5, field6: chararray);");
    //        + "c = cogroup a by field1, b by field4 ;"
    //        + "d = foreach c generate group, flatten(a), flatten(b)  ;"
    //        + "e = foreach d generate group, field1 + 1, field4 + 2.0  ;"
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(2);
    //        exOp = foreachPlan.getRoots().get(0);
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //        cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("org.apache.pig.builtin.PigStreaming"));
    //
    //    }
    //
    //    //@Test
    //    public void testCogroupStreamingLineageNoSchema() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') ;"
    //        + "b = stream a through `" + simpleEchoStreamingCommand + "` ;");
    //        + "c = cogroup a by $0, b by $0 ;"
    //        + "d = foreach c generate group, flatten(a), flatten(b)  ;"
    //        + "e = foreach d generate group, $1 + 1, $2 + 2.0  ;"
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        CastExpression cast = (CastExpression)foreachPlan.getPredecessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //        foreachPlan = foreach.getForEachPlans().get(2);
    //        exOp = foreachPlan.getRoots().get(0);
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //        cast = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("org.apache.pig.builtin.PigStreaming"));
    //
    //    }
    //
    //    //@Test
    //    public void testMapLookupLineage() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') as (field1, field2: float, field3: chararray );"
    //        + "b = foreach a generate field1#'key1' as map1;"
    //        + "c = foreach b generate map1#'key2' + 1 ;"
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //        // the root would be the project and there would be cast
    //        // to map between the project and MapLookupExpression
    //        CastExpression cast1 = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast1.getFuncSpec().getClassName().startsWith("BinStorage"));
    //        MapLookupExpression map = (MapLookupExpression)foreachPlan.getSuccessors(cast1).get(0);
    //        CastExpression cast = (CastExpression)foreachPlan.getSuccessors(map).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //    }
    //
    //    //@Test
    //    public void testMapLookupLineageNoSchema() throws Throwable {
    //        + "a = load 'a' using PigStorage('a') ;"
    //        + "b = foreach a generate $0#'key1';"
    //        + "c = foreach b generate $0#'key2' + 1 ;"
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = (LogicalExpression) foreachPlan.getSinks().get(1);
    //
    //        // the root would be the project and there would be cast
    //        // to map between the project and MapLookupExpression
    //        CastExpression cast1 = (CastExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast1.getFuncSpec().getClassName().startsWith("BinStorage"));
    //        MapLookupExpression map = (MapLookupExpression)foreachPlan.getSuccessors(cast1).get(0);
    //        CastExpression cast = (CastExpression)foreachPlan.getSuccessors(map).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("BinStorage"));
    //
    //    }
    //
    //    //@Test
    //    public void testMapLookupLineage2() throws Throwable {
    //        + "a = load 'a' as (s, m, l);"
    //        + "b = foreach a generate s#'x' as f1, s#'y' as f2, s#'z' as f3;"
    //        + "c = group b by f1;"
    //        + "d = foreach c {fil = filter b by f2 == 1; generate flatten(group), SUM(fil.f3);};"
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = foreach.getForEachPlans().get(1);
    //
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //
    //        LOFilter filter = (LOFilter)foreachPlan.getSuccessors(exOp).get(0);
    //        LogicalPlan filterPlan = filter.getComparisonPlan();
    //
    //        exOp = filterPlan.getRoots().get(0);
    //
    //        if(! (exOp instanceof ProjectExpression)) exOp = filterPlan.getRoots().get(1);
    //
    //        
    //        CastExpression cast = (CastExpression)filterPlan.getSuccessors(exOp).get(0);
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("org.apache.pig.builtin.PigStorage"));
    //
    //    }
    //
    //    //@Test
    //    public void testMapLookupLineage3() throws Throwable {
    //        + "a = load 'a' as (s, m, l);"
    ////        + "b = foreach a generate s#'src_spaceid' AS vspaceid, flatten(l#'viewinfo') as viewinfo ;"
    ////        + "c = foreach b generate (chararray)vspaceid#'foo', (chararray)viewinfo#'pos' as position;"
    //        
    //        + "b = foreach a generate flatten(l#'viewinfo') as viewinfo ;"
    //      + "c = foreach b generate (chararray)viewinfo#'pos' as position;"
    //
    //        // validate
    //        runTypeCheckingValidator(plan);
    //        
    //        checkLoaderInCasts(plan, "org.apache.pig.builtin.PigStorage");
    //    }
    //    
    //    /**
    //     * test various scenarios with two level map lookup
    //     */
    //    //@Test
    //    public void testTwolevelMapLookupLineage() throws Exception {
    //        List<String[]> queries = new ArrayList<String[]>();
    //        // CASE 1: LOAD -> FILTER -> FOREACH -> LIMIT -> STORE
    //        queries.add(new String[] {"sds = LOAD '/my/data/location' " +
    //                "AS (simpleFields:map[], mapFields:map[], listMapFields:map[]);",
    //                "queries = FILTER sds BY mapFields#'page_params'#'query' " +
    //                "is NOT NULL;",
    //                "queries_rand = FOREACH queries GENERATE " +
    //                "(CHARARRAY) (mapFields#'page_params'#'query') AS query_string;",
    //                "queries_limit = LIMIT queries_rand 100;",
    //                "STORE queries_limit INTO 'out';"});     
    //        // CASE 2: LOAD -> FOREACH -> FILTER -> LIMIT -> STORE
    //        queries.add(new String[]{"sds = LOAD '/my/data/location'  " +
    //                "AS (simpleFields:map[], mapFields:map[], listMapFields:map[]);",
    //                "queries_rand = FOREACH sds GENERATE " +
    //                "(CHARARRAY) (mapFields#'page_params'#'query') AS query_string;",
    //                "queries = FILTER queries_rand BY query_string IS NOT null;",
    //                "queries_limit = LIMIT queries 100;",
    //                "STORE queries_limit INTO 'out';"});
    //        // CASE 3: LOAD -> FOREACH -> FOREACH -> FILTER -> LIMIT -> STORE
    //        queries.add(new String[]{"sds = LOAD '/my/data/location'  " +
    //                "AS (simpleFields:map[], mapFields:map[], listMapFields:map[]);",
    //                "params = FOREACH sds GENERATE " +
    //                "(map[]) (mapFields#'page_params') AS params;",
    //                "queries = FOREACH params " +
    //                "GENERATE (CHARARRAY) (params#'query') AS query_string;",
    //                "queries_filtered = FILTER queries BY query_string IS NOT null;",
    //                "queries_limit = LIMIT queries_filtered 100;",
    //                "STORE queries_limit INTO 'out';"});
    //        // CASE 4: LOAD -> FOREACH -> FOREACH -> LIMIT -> STORE
    //        queries.add(new String[]{"sds = LOAD '/my/data/location'  " +
    //                "AS (simpleFields:map[], mapFields:map[], listMapFields:map[]);",
    //                "params = FOREACH sds GENERATE" +
    //                " (map[]) (mapFields#'page_params') AS params;",
    //                "queries = FOREACH params GENERATE " +
    //                "(CHARARRAY) (params#'query') AS query_string;",
    //                "queries_limit = LIMIT queries 100;",
    //                "STORE queries_limit INTO 'out';"});
    //        // CASE 5: LOAD -> FOREACH -> FOREACH -> FOREACH -> LIMIT -> STORE
    //        queries.add(new String[]{"sds = LOAD '/my/data/location'  " +
    //                "AS (simpleFields:map[], mapFields:map[], listMapFields:map[]);",
    //                "params = FOREACH sds GENERATE " +
    //                "(map[]) (mapFields#'page_params') AS params;",
    //                "queries = FOREACH params GENERATE " +
    //                "(CHARARRAY) (params#'query') AS query_string;",
    //                "rand_queries = FOREACH queries GENERATE query_string as query;",
    //                "queries_limit = LIMIT rand_queries 100;",
    //                "STORE rand_queries INTO 'out';"});
    //        
    //        for (String[] query: queries) {
    //            LogicalPlan lp = null;
    //            for (String queryLine : query) {
    //                lp = + queryLine);    
    //            }
    //            
    //            // validate
    //            runTypeCheckingValidator(lp);
    //            checkLoaderInCasts(lp, "org.apache.pig.builtin.PigStorage");
    //            
    //        }
    //    }
    //    
    //    private void runTypeCheckingValidator(LogicalPlan plan) throws 
    //    TypeCheckerException {
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //    }
    //    
    //    private void checkLoaderInCasts(LogicalPlan plan, String loaderClassName) 
    //    throws VisitorException {
    //        CastFinder cf = new CastFinder(plan);
    //        cf.visit();
    //        List<CastExpression> casts = cf.casts;
    //        for (CastExpression cast : casts) {
    //            assertTrue(cast.getFuncSpec().getClassName().startsWith(
    //                    loaderClassName));    
    //        }
    //    }
    //    
    //    class CastFinder extends LOVisitor {
    //        List<CastExpression> casts = new ArrayList<CastExpression>();
    //        /**
    //         * 
    //         */
    //        public CastFinder(LogicalPlan lp) {
    //            super(lp, new DepthFirstWalker<LogicalOperator, LogicalPlan>(lp));
    //        }
    //        
    //        @Override
    //        protected void visit(CastExpression cast) throws VisitorException {
    //            casts.add(cast);
    //        }
    //    }
    //    
    //    //@Test
    //    public void testBincond() throws Throwable {
    //        + "a = load 'a' as (name: chararray, age: int, gpa: float);"
    //        + "b = group a by name;"
    //        + "c = foreach b generate (IsEmpty(a) ? " + TestBinCondFieldSchema.class.getName() + "(*): a) ;"
    //    
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        
    //        typeChecker.visit();  
    //    
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //    
    //        if (collector.hasError()) {
    //            throw new AssertionError("Did not expect an error") ;
    //        }
    //    
    //    
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        
    //        Schema.FieldSchema charFs = new FieldSchema(null, DataType.CHARARRAY);
    //        Schema.FieldSchema intFs = new FieldSchema(null, DataType.INTEGER);
    //        Schema.FieldSchema floatFs = new FieldSchema(null, DataType.FLOAT);
    //        Schema bagSchema = new Schema();
    //        bagSchema.add(charFs);
    //        bagSchema.add(intFs);
    //        bagSchema.add(floatFs);
    //        Schema.FieldSchema bagFs = null;
    //        try {
    //            bagFs = new Schema.FieldSchema(null, bagSchema, DataType.BAG);
    //        } catch (FrontendException fee) {
    //            fail("Did not expect an error");
    //        }
    //        
    //        Schema expectedSchema = new Schema(bagFs);
    //        
    //        assertTrue(Schema.equals(foreach.getSchema(), expectedSchema, false, true));
    //    
    //    }
    //
    //    //@Test
    //    public void testBinCondForOuterJoin() throws Throwable {
    //        + "a = LOAD 'student_data' AS (name: chararray, age: int, gpa: float);");
    //        + "b = LOAD 'voter_data' AS (name: chararray, age: int, registration: chararray, contributions: float);");
    //        + "c = COGROUP a BY name, b BY name;");
    //        + "d = FOREACH c GENERATE group, flatten((not IsEmpty(a) ? a : (bag{tuple(chararray, int, float)}){(null, null, null)})), flatten((not IsEmpty(b) ? b : (bag{tuple(chararray, int, chararray, float)}){(null,null,null, null)}));");
    //    
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //    
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //    
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //    
    //    
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        String expectedSchemaString = "mygroup: chararray,A::name: chararray,A::age: int,A::gpa: float,B::name: chararray,B::age: int,B::registration: chararray,B::contributions: float";
    //        Schema expectedSchema = Util.getSchemaFromString(expectedSchemaString);
    //        assertTrue(Schema.equals(foreach.getSchema(), expectedSchema, false, true));
    //    
    //    }
    //
    //    //@Test
    //    public void testMapLookupCast() throws Exception {
    //         String input[] = { "[k1#hello,k2#bye]",
    //                 "[k1#good,k2#morning]" };
    //         File f = Util.createInputFile("test", ".txt", input);
    //         String inputFileName = f.getAbsolutePath();
    //         // load as bytearray and use as map
    //         + "a = load 'file://" + inputFileName + "' as (m);");
    //         LogicalPlan lp = + "b = foreach a generate m#'k1';");
    //         // validate
    //         CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //         TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //         typeValidator.validate(lp, collector) ;
    //         
    //         // check that a CastExpression has been introduced
    //         LOForEach foreach = (LOForEach) lp.getSinks().get(0);
    //         LogicalPlan innerPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
    //         MapLookupExpression mapLookup = (MapLookupExpression) innerPlan.getSinks().get(0);
    //         assertEquals(CastExpression.class, mapLookup.getMap().getClass());
    //         assertEquals(DataType.MAP, ((CastExpression)mapLookup.getMap()).getType());
    //         
    //         // load as map and use as map
    //         + "a = load 'file://" + inputFileName + "' as (m:[]);");
    //         lp = + "b = foreach a generate m#'k1';");
    //         // validate
    //         collector = new CompilationMessageCollector() ;
    //         typeValidator = new TypeCheckingValidator() ;
    //         typeValidator.validate(lp, collector) ;
    //         
    //         // check that a CastExpression has NOT been introduced
    //         foreach = (LOForEach) lp.getSinks().get(0);
    //         innerPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
    //         mapLookup = (MapLookupExpression) innerPlan.getSinks().get(0);
    //         assertEquals(ProjectExpression.class, mapLookup.getMap().getClass());
    //         
    //         
    //         PigServer ps = new PigServer(ExecType.LOCAL);
    //         // load as bytearray and use as map
    //         ps.registerQuery("a = load 'file://" + inputFileName + "' as (m);");
    //         ps.registerQuery("b = foreach a generate m#'k1';");
    //         Iterator<Tuple> it = ps.openIterator("b");
    //         String[] expectedResults = new String[] {"(hello)", "(good)"};
    //         int i = 0;
    //         while(it.hasNext()) {
    //             assertEquals(expectedResults[i++], it.next().toString());
    //         }
    //         
    //         // load as map and use as map
    //         ps.registerQuery("a = load 'file://" + inputFileName + "' as (m:[]);");
    //         ps.registerQuery("b = foreach a generate m#'k1';");
    //         it = ps.openIterator("b");
    //         expectedResults = new String[] {"(hello)", "(good)"};
    //         i = 0;
    //         while(it.hasNext()) {
    //             assertEquals(expectedResults[i++], it.next().toString());
    //         }
    //         
    //    }
    //    
    //    /*
    //     * A test UDF that does not data processing but implements the getOutputSchema for
    //     * checking the type checker
    //     */
    //    public static class TestBinCondFieldSchema extends EvalFunc<DataBag> {
    //        //no-op exec method
    //        @Override
    //        public DataBag exec(Tuple input) {
    //            return null;
    //        }
    //        
    //        @Override
    //        public Schema outputSchema(Schema input) {
    //            Schema.FieldSchema charFs = new FieldSchema(null, DataType.CHARARRAY);
    //            Schema.FieldSchema intFs = new FieldSchema(null, DataType.INTEGER);
    //            Schema.FieldSchema floatFs = new FieldSchema(null, DataType.FLOAT);
    //            Schema bagSchema = new Schema();
    //            bagSchema.add(charFs);
    //            bagSchema.add(intFs);
    //            bagSchema.add(floatFs);
    //            Schema.FieldSchema bagFs;
    //            try {
    //                bagFs = new Schema.FieldSchema(null, bagSchema, DataType.BAG);
    //            } catch (FrontendException fee) {
    //                return null;
    //            }
    //            return new Schema(bagFs);
    //        }
    //    }
    //    
        ////////////////////////// Helper //////////////////////////////////
        private void checkForEachCasting(LOForEach foreach, int idx, boolean isCast, byte toType)
        throws FrontendException {
            LOGenerate gen = (LOGenerate) foreach.getInnerPlan().getSinks().get(0) ;
            LogicalExpressionPlan plan = gen.getOutputPlans().get(0);

            if (isCast) {
                List<Operator> leaveList = plan.getSources() ;
                assertEquals(leaveList.size(), 1);
                assertTrue(leaveList.get(0) instanceof CastExpression);
                assertTrue(((LogicalExpression)leaveList.get(0)).getType() == toType) ;
            }
            else {
                List<Operator> leaveList = plan.getSources() ;
                assertEquals(leaveList.size(), 1);
                assertTrue(leaveList.get(0) instanceof ProjectExpression);
            }

        }
    //    
    //    //@Test
    //    public void testLineageMultipleLoader1() throws FrontendException {
    //        +  "A = LOAD 'data1' USING PigStorage() AS (u, v, w);" ) ;
    //        +  "B = LOAD 'data2' USING TextLoader() AS (x, y);" ) ;
    //        + "C = JOIN A BY u, B BY x USING 'replicated';"
    //        + "D = GROUP C BY (u, x);");
    //        LogicalPlan plan = +  "E = FOREACH D GENERATE (chararray)group.u, (int)group.x;" );
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //        // Check group.u
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //        ProjectExpression proj = (ProjectExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        CastExpression cast = (CastExpression)foreachPlan.getSuccessors( proj ).get( 0 );
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //
    //        // Check group.x
    //        foreachPlan = foreach.getForEachPlans().get( 1 );
    //        exOp = foreachPlan.getRoots().get(0);
    //        proj = (ProjectExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        cast = (CastExpression)foreachPlan.getSuccessors( proj ).get( 0 );
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("TextLoader"));
    //    }
    //
    //    /**
    //     * From JIRA 1482
    //     * @throws FrontendException
    //     */
    //    //@Test
    //    public void testLineageMultipleLoader2() throws FrontendException {
    //        +  "A = LOAD 'data1' USING PigStorage() AS (s, m, l);" ) ;
    //        +  "B = FOREACH A GENERATE s#'k1' as v1, m#'k2' as v2, l#'k3' as v3;" ) ;
    //        +  "C = FOREACH B GENERATE v1, (v2 == 'v2' ? 1L : 0L) as v2:long, (v3 == 'v3' ? 1 :0) as v3:int;" ) ;
    //        +  "D = LOAD 'data2' USING TextLoader() AS (a);");
    //        +  "E = JOIN C BY v1, D BY a USING 'replicated';" );
    //        +  "F = GROUP E BY (v1, a);" );
    //        LogicalPlan plan = +  "G = FOREACH F GENERATE (chararray)group.v1, group.a;" );
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //        // Check group.u
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //        ProjectExpression proj = (ProjectExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        CastExpression cast = (CastExpression)foreachPlan.getSuccessors( proj ).get( 0 );
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //    }
    //
    //    /**
    //     * A special invalid case.
    //     */
    //    //@Test
    //    public void testLineageMultipleLoader3() throws FrontendException {
    //        +  "A = LOAD 'data1' USING PigStorage() AS (u, v, w);" ) ;
    //        +  "B = LOAD 'data2' USING TextLoader() AS (x, y);" ) ;
    //        + "C = COGROUP A BY u, B by x;");
    //        LogicalPlan plan = +  "D = FOREACH C GENERATE (chararray)group;" );
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        try {
    //            typeChecker.visit();  
    //        } catch(TypeCheckerException ex) {
    //            assertTrue( ex.getCause().toString().contains( "Cannot resolve load function to use for casting from bytearray to chararray." ) );
    //            return;
    //        }
    //        assertTrue( "Validation failure is expected.", false );
    //    }
    //    
    //    /**
    //     * In case of filter with tuple type
    //     */
    //    //@Test
    //    public void testLineageFilterWithTuple() throws FrontendException {
    //        +  "A = LOAD 'data1' USING PigStorage() AS (u, v, w:tuple(a,b));" ) ;
    //        +  "B = FOREACH A generate v, w;");
    //        +  "C = FILTER B by v < 50;" ) ;
    //        + "D = FOREACH C generate (int)w.a;");
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
    //        LogicalExpression exOp = (LogicalExpression) foreachPlan.getSinks().get(0);
    //        ProjectExpression proj = (ProjectExpression)foreachPlan.getSuccessors(exOp).get(0);
    //        CastExpression cast = (CastExpression)foreachPlan.getSuccessors( proj ).get( 0 );
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //    }
    //    
    //    //@Test
    //    public void testLineageExpressionCasting() throws FrontendException {
    //        +  "A = LOAD 'data1' USING PigStorage() AS (u:int, v);" ) ;
    //        +  "B = FILTER A by u < 50;" ) ;
    //        + "C = FOREACH B generate u + v;");
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
    //        List<LogicalOperator> projs = foreachPlan.getRoots();
    //        LogicalOperator proj = projs.get(1);
    //        LogicalOperator op = foreachPlan.getSuccessors( proj ).get( 0 );
    //        if( !( op instanceof CastExpression ) ) {
    //            proj = projs.get(1);
    //            op = foreachPlan.getSuccessors( proj ).get( 0 );
    //        }
    //        CastExpression cast = (CastExpression)op;
    //        assertTrue(cast.getFuncSpec().getClassName().startsWith("PigStorage"));
    //    }
    //    
    //    //@Test
    //    // See PIG-1741
    //    public void testBagDereference() throws FrontendException {
    //        + "a = load '1.txt' as (a0);");
    //        + "b = foreach a generate flatten((bag{tuple(map[])})a0) as b0:map[];");
    //        + "c = foreach b generate (long)b0#'key1';");
    //
    //        // validate
    //        CompilationMessageCollector collector = new CompilationMessageCollector() ;
    //        TypeCheckingRelVisitor typeChecker = new TypeCheckingRelVisitor(plan, collector);
    //        typeChecker.visit();  
    //
    //        printMessageCollector(collector) ;
    //        //printTypeGraph(plan) ;
    //        //planTester.printPlan(plan, TypeCheckingTestUtil.getCurrentMethodName());
    //
    //        if (collector.hasError()) {
    //            throw new AssertionError("Expect no  error") ;
    //        }
    //
    //        LOForEach foreach = (LOForEach)plan.getSinks().get(0);
    //        LogicalExpressionPlan foreachPlan = ((LOGenerate)foreach.getInnerPlan().getSinks().get(0)).getOutputPlans().get(0);
    //        List<LogicalOperator> projs = foreachPlan.getRoots();
    //        LogicalOperator proj = projs.get(0);
    //        LogicalOperator maplookup = foreachPlan.getSuccessors( proj ).get( 0 );
    //        LogicalOperator cast = foreachPlan.getSuccessors( maplookup ).get( 0 );
    //        
    //        assertTrue(((CastExpression)cast).getFuncSpec().getClassName().equals(PigStorage.class.getName()));
    //    }

}
