package org.apache.pig.test;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.pig.impl.logicalLayer.validators.*;
import org.apache.pig.impl.logicalLayer.* ;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema ;
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.MultiMap;
import org.apache.pig.impl.plan.CompilationMessageCollector.Message;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.parser.NodeIdGenerator;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.builtin.PigStorage;
import org.junit.Test;

public class TestTypeCheckingValidator extends TestCase {

    
    @Test
    public void testExpressionTypeChecking1() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        LOConst constant1 = new LOConst(plan, genNewOperatorKey(), 10) ;
        constant1.setType(DataType.INTEGER) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKey(), 20D) ;
        constant2.setType(DataType.DOUBLE) ;
        LOConst constant3 =  new LOConst(plan, genNewOperatorKey(), "123") ;
        constant3.setType(DataType.CHARARRAY) ;
        
        LOAdd add1 = new LOAdd(plan, genNewOperatorKey(), constant1, constant2) ;
        LOCast cast1 = new LOCast(plan, genNewOperatorKey(), constant3, DataType.BYTEARRAY) ;
        LOMultiply mul1 = new LOMultiply(plan, genNewOperatorKey(), add1, cast1) ;
        
        plan.add(constant1) ;
        plan.add(constant2) ;
        plan.add(constant3) ;
        plan.add(cast1) ;
        plan.add(add1) ;
        plan.add(mul1) ;
        
        plan.connect(constant1, add1) ;
        plan.connect(constant2, add1) ;
        plan.connect(add1, mul1) ;
        plan.connect(constant3, cast1) ;
        plan.connect(cast1, mul1) ;
                          
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;        
        printMessageCollector(collector) ;    
        printTypeGraph(plan) ;
        
        if (collector.hasError()) {
            throw new Exception("Error during type checking") ;
        }       
        
        // Induction check
        assertEquals(DataType.DOUBLE, add1.getType()) ;
        assertEquals(DataType.DOUBLE, mul1.getType()) ;
        
        // Cast insertion check
        assertEquals(DataType.DOUBLE, add1.getLhsOperand().getType()) ;
        assertEquals(DataType.DOUBLE, mul1.getRhsOperand().getType()) ;
        
    }
    
    @Test
    public void testExpressionTypeChecking2() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        LOConst constant1 = new LOConst(plan, genNewOperatorKey(), 10) ;
        constant1.setType(DataType.INTEGER) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKey(), 20D) ;
        constant2.setType(DataType.BYTEARRAY) ;
        LOConst constant3 =  new LOConst(plan, genNewOperatorKey(), 123L) ;
        constant3.setType(DataType.LONG) ;        
        LOConst constant4 =  new LOConst(plan, genNewOperatorKey(), true) ;
        constant4.setType(DataType.BOOLEAN) ;
        
        LOSubtract sub1 = new LOSubtract(plan, genNewOperatorKey(), constant1, constant2) ;
        LOGreaterThan gt1 = new LOGreaterThan(plan, genNewOperatorKey(), sub1, constant3) ;
        LOAnd and1 = new LOAnd(plan, genNewOperatorKey(), gt1, constant4) ;
        LONot not1 = new LONot(plan, genNewOperatorKey(), and1) ;
        
        plan.add(constant1) ;
        plan.add(constant2) ;
        plan.add(constant3) ;
        plan.add(constant4) ;
        
        plan.add(sub1) ;
        plan.add(gt1) ;
        plan.add(and1) ;
        plan.add(not1) ;

        
        plan.connect(constant1, sub1) ;
        plan.connect(constant2, sub1) ;
        plan.connect(sub1, gt1) ;
        plan.connect(constant3, gt1) ;
        plan.connect(gt1, and1) ;
        plan.connect(constant4, and1) ;
        plan.connect(and1, not1) ;
                      
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;        
        printMessageCollector(collector) ;    
        printTypeGraph(plan) ;
        
        if (collector.hasError()) {
            throw new Exception("Error during type checking") ;
        }       
        
        // Induction check
        assertEquals(DataType.INTEGER, sub1.getType()) ;
        assertEquals(DataType.BOOLEAN, gt1.getType()) ;
        assertEquals(DataType.BOOLEAN, and1.getType()) ;
        assertEquals(DataType.BOOLEAN, not1.getType()) ;
        
        // Cast insertion check
        assertEquals(DataType.INTEGER, sub1.getRhsOperand().getType()) ;
        assertEquals(DataType.LONG, gt1.getLhsOperand().getType()) ;
    }
    
    
    @Test
    public void testExpressionTypeChecking3() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        LOConst constant1 = new LOConst(plan, genNewOperatorKey(), 10) ;
        constant1.setType(DataType.BYTEARRAY) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKey(), 20L) ;
        constant2.setType(DataType.LONG) ;
        LOConst constant3 =  new LOConst(plan, genNewOperatorKey(), 123) ;
        constant3.setType(DataType.INTEGER) ;
        
        LOMod mod1 = new LOMod(plan, genNewOperatorKey(), constant1, constant2) ;
        LOEqual equal1 = new LOEqual(plan, genNewOperatorKey(), mod1, constant3) ;
        
        plan.add(constant1) ;
        plan.add(constant2) ;
        plan.add(constant3) ;
        plan.add(equal1) ;
        plan.add(mod1) ;
        
        plan.connect(constant1, mod1) ;
        plan.connect(constant2, mod1) ;
        plan.connect(mod1, equal1) ;
        plan.connect(constant3, equal1) ;
                     
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;        
        printMessageCollector(collector) ;               
        printTypeGraph(plan) ;
        
        if (collector.hasError()) {
            throw new Exception("Error during type checking") ;
        }      
        
        // Induction check
        assertEquals(DataType.LONG, mod1.getType()) ;
        assertEquals(DataType.BOOLEAN, equal1.getType()) ;
        
        // Cast insertion check
        assertEquals(DataType.LONG, mod1.getLhsOperand().getType()) ;
        assertEquals(DataType.LONG, equal1.getRhsOperand().getType()) ;
        
    }
     
    @Test
    public void testExpressionTypeChecking4() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        LOConst constant1 = new LOConst(plan, genNewOperatorKey(), 10) ;
        constant1.setType(DataType.INTEGER) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKey(), 20D) ;
        constant2.setType(DataType.DOUBLE) ;
        LOConst constant3 =  new LOConst(plan, genNewOperatorKey(), "123") ;
        constant3.setType(DataType.CHARARRAY) ;
        
        LODivide div1 = new LODivide(plan, genNewOperatorKey(), constant1, constant2) ;
        LOCast cast1 = new LOCast(plan, genNewOperatorKey(), constant3, DataType.BYTEARRAY) ;
        LONotEqual notequal1 = new LONotEqual(plan, genNewOperatorKey(), div1, cast1) ;
        
        plan.add(constant1) ;
        plan.add(constant2) ;
        plan.add(constant3) ;
        plan.add(div1) ;
        plan.add(cast1) ;
        plan.add(notequal1) ;
        
        plan.connect(constant1, div1) ;
        plan.connect(constant2, div1) ;
        plan.connect(constant3, cast1) ;
        plan.connect(div1, notequal1) ;
        plan.connect(cast1, notequal1) ;
        
                          
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;        
        printMessageCollector(collector) ;             
        printTypeGraph(plan) ;
        
        if (collector.hasError()) {
            throw new Exception("Error during type checking") ;
        }  
        
        // Induction check
        assertEquals(DataType.DOUBLE, div1.getType()) ;
        assertEquals(DataType.BOOLEAN, notequal1.getType()) ;
        
        // Cast insertion check
        assertEquals(DataType.DOUBLE, div1.getLhsOperand().getType()) ;
        assertEquals(DataType.DOUBLE, notequal1.getRhsOperand().getType()) ;
        
    }
    
    @Test
    public void testExpressionTypeChecking5() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        LOConst constant1 = new LOConst(plan, genNewOperatorKey(), 10) ;
        constant1.setType(DataType.FLOAT) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKey(), 20) ;
        constant2.setType(DataType.LONG) ;
        LOConst constant3 =  new LOConst(plan, genNewOperatorKey(), 123F) ;
        constant3.setType(DataType.FLOAT) ;
        LOConst constant4 =  new LOConst(plan, genNewOperatorKey(), 123D) ;
        constant4.setType(DataType.DOUBLE) ;
        
        LOLesserThanEqual lesser1 = new LOLesserThanEqual(plan, genNewOperatorKey(), constant1, constant2) ;
        LOBinCond bincond1 = new LOBinCond(plan, genNewOperatorKey(), lesser1, constant3, constant4) ;
        
        plan.add(constant1) ;
        plan.add(constant2) ;
        plan.add(constant3) ;
        plan.add(constant4) ;
        plan.add(lesser1) ;
        plan.add(bincond1) ;
        
        plan.connect(constant1, lesser1) ;
        plan.connect(constant2, lesser1) ;
        plan.connect(lesser1, bincond1) ;
        plan.connect(constant3, bincond1) ;
        plan.connect(constant4, bincond1) ;
        
                          
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;        
        printMessageCollector(collector) ;             
        printTypeGraph(plan) ;
        
        if (collector.hasError()) {
            throw new Exception("Error during type checking") ;
        }  
        
        // Induction check
        assertEquals(DataType.BOOLEAN, lesser1.getType()) ;
        assertEquals(DataType.DOUBLE, bincond1.getType()) ;
        
        // Cast insertion check
        assertEquals(DataType.FLOAT, lesser1.getLhsOperand().getType()) ;
        assertEquals(DataType.FLOAT, lesser1.getRhsOperand().getType()) ;
        assertEquals(DataType.DOUBLE, bincond1.getLhsOp().getType()) ;
        assertEquals(DataType.DOUBLE, bincond1.getRhsOp().getType()) ;
        
    }
    
    @Test
    public void testExpressionTypeChecking6() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        LOConst constant1 = new LOConst(plan, genNewOperatorKey(), 10) ;
        constant1.setType(DataType.CHARARRAY) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKey(), 20) ;
        constant2.setType(DataType.LONG) ;
        
        LOAdd add1 = new LOAdd(plan, genNewOperatorKey(), constant1, constant2) ;
        
        plan.add(constant1) ;
        plan.add(constant2) ;
        plan.add(add1) ;
        
        plan.connect(constant1, add1) ;
        plan.connect(constant2, add1) ;        
                          
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        try {
            typeValidator.validate(plan, collector) ;    
            fail("Exception expected") ;
        }
        catch (PlanValidationException pve) {
            // good
        }
        printMessageCollector(collector) ;             
        printTypeGraph(plan) ;
        
        if (!collector.hasError()) {
            throw new AssertionError("Error expected") ;
        }         
        
    }

    @Test
    public void testExpressionTypeChecking7() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        LOConst constant1 = new LOConst(plan, genNewOperatorKey(), 10) ;
        constant1.setType(DataType.INTEGER) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKey(), 20D) ;
        constant2.setType(DataType.BYTEARRAY) ;
        LOConst constant3 =  new LOConst(plan, genNewOperatorKey(), 123L) ;
        constant3.setType(DataType.LONG) ;        
        
        LOGreaterThan gt1 = new LOGreaterThan(plan, genNewOperatorKey(), constant1, constant2) ;
        LOEqual equal1 = new LOEqual(plan, genNewOperatorKey(), gt1, constant3) ;
        
        plan.add(constant1) ;
        plan.add(constant2) ;
        plan.add(constant3) ;
        plan.add(gt1) ;
        plan.add(equal1) ;
              
        plan.connect(constant1, gt1) ;
        plan.connect(constant2, gt1) ;
        plan.connect(gt1, equal1) ;
        plan.connect(constant3, equal1) ;
                      
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        try {
            typeValidator.validate(plan, collector) ;    
            fail("Exception expected") ;
        }
        catch (PlanValidationException pve) {
            // good
        }        
        printMessageCollector(collector) ;    
        printTypeGraph(plan) ;
        
        if (!collector.hasError()) {
            throw new AssertionError("Error expected") ;
        }   
    }
    
    @Test
    public void testArithmeticOpCastInsert1() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        LOConst constant1 = new LOConst(plan, genNewOperatorKey(), 10) ;
        constant1.setType(DataType.INTEGER) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKey(), 20D) ;
        constant2.setType(DataType.DOUBLE) ;
        
        LOMultiply mul1 = new LOMultiply(plan, genNewOperatorKey(), constant1, constant2) ;
        
        plan.add(constant1) ;
        plan.add(constant2) ;
        plan.add(mul1) ;
        
        plan.connect(constant1, mul1) ;
        plan.connect(constant2, mul1) ;       
        
        // Before type checking
        System.out.println(DataType.findTypeName(mul1.getType())) ;
        assertEquals(DataType.UNKNOWN, mul1.getType()) ;    
        
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;        
        printMessageCollector(collector) ;       
        
        printTypeGraph(plan) ;
        
        // After type checking
        System.out.println(DataType.findTypeName(mul1.getType())) ;
        assertEquals(DataType.DOUBLE, mul1.getType()) ;
    }

    @Test
    public void testArithmeticOpCastInsert2() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        LOConst constant1 = new LOConst(plan, genNewOperatorKey(), 10) ;
        constant1.setType(DataType.INTEGER) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKey(), 20D) ;
        constant2.setType(DataType.LONG) ;

        LONegative neg1 = new LONegative(plan, genNewOperatorKey(), constant1) ;
        LOSubtract subtract1 = new LOSubtract(plan,
                                              genNewOperatorKey(),
                                              neg1,
                                              constant2) ;

        plan.add(constant1) ;
        plan.add(neg1) ; 
        plan.add(constant2) ;
        plan.add(subtract1) ;

        plan.connect(constant1, neg1) ;
        plan.connect(neg1, subtract1) ;
        plan.connect(constant2, subtract1) ;

        // Before type checking
        assertEquals(DataType.UNKNOWN, subtract1.getType()) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        printMessageCollector(collector) ;

        printTypeGraph(plan) ;

        // After type checking
        System.out.println(DataType.findTypeName(subtract1.getType())) ;
        assertEquals(DataType.LONG, subtract1.getType()) ;

        assertTrue(subtract1.getLhsOperand() instanceof LOCast);
        assertEquals(((LOCast)subtract1.getLhsOperand()).getType(), DataType.LONG);
        assertTrue(((LOCast)subtract1.getLhsOperand()).getExpression() == neg1);
    }

    @Test
    public void testModCastInsert1() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        LOConst constant1 = new LOConst(plan, genNewOperatorKey(), 10) ;
        constant1.setType(DataType.BYTEARRAY) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKey(), 20D) ;
        constant2.setType(DataType.LONG) ;

        LOMod mod1 = new LOMod(plan, genNewOperatorKey(), constant1, constant2) ;

        plan.add(constant1) ;
        plan.add(constant2) ;
        plan.add(mod1) ;

        plan.connect(constant1, mod1) ;
        plan.connect(constant2, mod1) ;

        // Before type checking
        assertEquals(DataType.UNKNOWN, mod1.getType()) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        printMessageCollector(collector) ;

        printTypeGraph(plan) ;

        // After type checking
        System.out.println(DataType.findTypeName(mod1.getType())) ;
        assertEquals(DataType.LONG, mod1.getType()) ;

        assertTrue(mod1.getLhsOperand() instanceof LOCast);
        assertEquals(((LOCast)mod1.getLhsOperand()).getType(), DataType.LONG);
        assertTrue(((LOCast)mod1.getLhsOperand()).getExpression() == constant1);
    }


    // Positive case
    @Test
    public void testRegexTypeChecking1() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        LOConst constant1 = new LOConst(plan, genNewOperatorKey(), 10) ;
        constant1.setType(DataType.CHARARRAY) ;

        LORegexp regex = new LORegexp(plan, genNewOperatorKey(), constant1, "Regex") ;

        plan.add(constant1) ;
        plan.add(regex) ;

        plan.connect(constant1, regex) ;     

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        printMessageCollector(collector) ;

        printTypeGraph(plan) ;

        // After type checking
        System.out.println(DataType.findTypeName(regex.getType())) ;
        assertEquals(DataType.BOOLEAN, regex.getType()) ;
    }

    // Positive case with cast insertion
    @Test
    public void testRegexTypeChecking2() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        LOConst constant1 = new LOConst(plan, genNewOperatorKey(), 10) ;
        constant1.setType(DataType.BYTEARRAY) ;

        LORegexp regex = new LORegexp(plan, genNewOperatorKey(), constant1, "Regex") ;

        plan.add(constant1) ;
        plan.add(regex) ;

        plan.connect(constant1, regex) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        printMessageCollector(collector) ;

        printTypeGraph(plan) ;

        // After type checking

        // check type
        System.out.println(DataType.findTypeName(regex.getType())) ;
        assertEquals(DataType.BOOLEAN, regex.getType()) ;

        // check wiring
        LOCast cast = (LOCast) regex.getOperand() ;
        assertEquals(cast.getType(), DataType.CHARARRAY);

        assertEquals(cast.getExpression(), constant1) ;
    }

        // Negative case
    @Test
    public void testRegexTypeChecking3() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        LOConst constant1 = new LOConst(plan, genNewOperatorKey(), 10) ;
        constant1.setType(DataType.INTEGER) ;

        LORegexp regex = new LORegexp(plan, genNewOperatorKey(), constant1, "Regex") ;

        plan.add(constant1) ;
        plan.add(regex) ;

        plan.connect(constant1, regex) ;

        try {
            CompilationMessageCollector collector = new CompilationMessageCollector() ;
            TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
            typeValidator.validate(plan, collector) ;
            printMessageCollector(collector) ;
            fail("Exception expected") ;
        }
        catch (PlanValidationException pve) {
            // good
        }

    }

    // Expression plan has to support DAG before this can be used.
    // Currently it supports only tree.

    /*
    @Test
    public void testDiamondShapedExpressionPlan1() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        LOConst constant1 = new LOConst(plan, genNewOperatorKey(), 10) ;
        constant1.setType(DataType.LONG) ;

        LONegative neg1 = new LONegative(plan, genNewOperatorKey(), constant1) ;
        LONegative neg2 = new LONegative(plan, genNewOperatorKey(), constant1) ;

        LODivide div1 = new LODivide(plan, genNewOperatorKey(), neg1, neg2) ;

        LONegative neg3 = new LONegative(plan, genNewOperatorKey(), div1) ;
        LONegative neg4 = new LONegative(plan, genNewOperatorKey(), div1) ;

        LOAdd add1 = new LOAdd(plan, genNewOperatorKey(), neg3, neg4) ;

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

        printTypeGraph(plan) ;

        // After type checking
        assertEquals(DataType.LONG, div1.getType()) ;
        assertEquals(DataType.LONG, add1.getType()) ;

    }
    */

    // This tests when both inputs need casting
    @Test
    public void testUnionCastingInsert1() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;
        
        LOLoad load1 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", pigStorage),
                                  null) ;
        LOLoad load2 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", pigStorage),
                                  null) ;

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
        load1.setEnforcedSchema(inputSchema1) ;
        load2.setEnforcedSchema(inputSchema2) ;

        // create union operator
        ArrayList<LogicalOperator> inputList = new ArrayList<LogicalOperator>() ;
        inputList.add(load1) ;
        inputList.add(load2) ;
        LOUnion union = new LOUnion(plan, genNewOperatorKey(), inputList) ;

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

        // check end result schema
        Schema outputSchema = union.getSchema() ;

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

        printTypeGraph(plan) ;

        // check the inserted casting of input1
        {
            // Check wiring
            List<LogicalOperator> sucList1 = plan.getSuccessors(load1) ;
            assertEquals(sucList1.size(), 1);
            LOForEach foreach = (LOForEach) sucList1.get(0) ;
            assertTrue(foreach instanceof LOForEach) ;

            List<LogicalOperator> sucList2 = plan.getSuccessors(foreach) ;
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
            List<LogicalOperator> sucList1 = plan.getSuccessors(load2) ;
            assertEquals(sucList1.size(), 1);
            LOForEach foreach = (LOForEach) sucList1.get(0) ;
            assertTrue(foreach instanceof LOForEach) ;

            List<LogicalOperator> sucList2 = plan.getSuccessors(foreach) ;
            assertEquals(sucList2.size(), 1);
            assertTrue(sucList2.get(0) instanceof LOUnion) ;

            // Check inserted casting
            checkForEachCasting(foreach, 0, false, DataType.UNKNOWN) ;
            checkForEachCasting(foreach, 1, true, DataType.LONG) ;
            checkForEachCasting(foreach, 2, false, DataType.UNKNOWN) ;
            checkForEachCasting(foreach, 3, false, DataType.UNKNOWN) ;

        }

    }


    // This tests when both only on input needs casting
    @Test
    public void testUnionCastingInsert2() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", pigStorage),
                                  null) ;
        LOLoad load2 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", pigStorage),
                                  null) ;

        // schema for input#1
        Schema inputSchema1 = null ;
        {
            List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
            fsList1.add(new FieldSchema("field1a", DataType.INTEGER)) ;
            fsList1.add(new FieldSchema("field2a", DataType.BYTEARRAY)) ;
            inputSchema1 = new Schema(fsList1) ;
        }

        // schema for input#2
        Schema inputSchema2 = null ;
        {
            List<FieldSchema> fsList2 = new ArrayList<FieldSchema>() ;
            fsList2.add(new FieldSchema("field1b", DataType.DOUBLE)) ;
            fsList2.add(new FieldSchema("field2b", DataType.DOUBLE)) ;
            inputSchema2 = new Schema(fsList2) ;
        }

        // set schemas
        load1.setEnforcedSchema(inputSchema1) ;
        load2.setEnforcedSchema(inputSchema2) ;

        // create union operator
        ArrayList<LogicalOperator> inputList = new ArrayList<LogicalOperator>() ;
        inputList.add(load1) ;
        inputList.add(load2) ;
        LOUnion union = new LOUnion(plan, genNewOperatorKey(), inputList) ;

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

        // check end result schema
        Schema outputSchema = union.getSchema() ;

        Schema expectedSchema = null ;
        {
            List<FieldSchema> fsListExpected = new ArrayList<FieldSchema>() ;
            fsListExpected.add(new FieldSchema("field1a", DataType.DOUBLE)) ;
            fsListExpected.add(new FieldSchema("field2a", DataType.DOUBLE)) ;
            expectedSchema = new Schema(fsListExpected) ;
        }

        assertTrue(Schema.equals(outputSchema, expectedSchema, true, false)) ;

        printTypeGraph(plan) ;

        // check the inserted casting of input1
        {
            // Check wiring
            List<LogicalOperator> sucList1 = plan.getSuccessors(load1) ;
            assertEquals(sucList1.size(), 1);
            LOForEach foreach = (LOForEach) sucList1.get(0) ;
            assertTrue(foreach instanceof LOForEach) ;

            List<LogicalOperator> sucList2 = plan.getSuccessors(foreach) ;
            assertEquals(sucList2.size(), 1);
            assertTrue(sucList2.get(0) instanceof LOUnion) ;

            // Check inserted casting
            checkForEachCasting(foreach, 0, true, DataType.DOUBLE) ;
            checkForEachCasting(foreach, 1, true, DataType.DOUBLE) ;

        }

        // check the inserted casting of input2
        {
            // Check wiring
            List<LogicalOperator> sucList1 = plan.getSuccessors(load2) ;
            assertEquals(sucList1.size(), 1);
            assertTrue(sucList1.get(0) instanceof LOUnion) ;
        }

    }

    // This is a negative test
    // Two inputs cannot be merged due to incompatible schemas
    @Test
    public void testUnionCastingInsert3() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", pigStorage),
                                  null) ;
        LOLoad load2 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", pigStorage),
                                  null) ;

        // schema for input#1
        Schema inputSchema1 = null ;
        {
            List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
            fsList1.add(new FieldSchema("field1a", DataType.INTEGER)) ;
            fsList1.add(new FieldSchema("field2a", DataType.BYTEARRAY)) ;
            inputSchema1 = new Schema(fsList1) ;
        }

        // schema for input#2
        Schema inputSchema2 = null ;
        {
            List<FieldSchema> fsList2 = new ArrayList<FieldSchema>() ;
            fsList2.add(new FieldSchema("field1b", DataType.CHARARRAY)) ;
            fsList2.add(new FieldSchema("field2b", DataType.DOUBLE)) ;
            inputSchema2 = new Schema(fsList2) ;
        }

        // set schemas
        load1.setEnforcedSchema(inputSchema1) ;
        load2.setEnforcedSchema(inputSchema2) ;

        // create union operator
        ArrayList<LogicalOperator> inputList = new ArrayList<LogicalOperator>() ;
        inputList.add(load1) ;
        inputList.add(load2) ;
        LOUnion union = new LOUnion(plan, genNewOperatorKey(), inputList) ;

        // wiring
        plan.add(load1) ;
        plan.add(load2) ;
        plan.add(union) ;

        plan.connect(load1, union);
        plan.connect(load2, union);

        // validate
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        try {
            typeValidator.validate(plan, collector) ;
            fail("Exception expected") ;
        }
        catch (PlanValidationException pve) {
            // good
        }
        printMessageCollector(collector) ;

    }

    @Test
    public void testDistinct1() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", pigStorage),
                                  null) ;

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
        load1.setEnforcedSchema(inputSchema1) ;

        // create union operator
        ArrayList<LogicalOperator> inputList = new ArrayList<LogicalOperator>() ;
        inputList.add(load1) ;
        LODistinct distinct1 = new LODistinct(plan, genNewOperatorKey(), load1) ;

        // wiring
        plan.add(load1) ;
        plan.add(distinct1) ;

        plan.connect(load1, distinct1);

        // validate
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        printMessageCollector(collector) ;

        // check end result schema
        Schema outputSchema = distinct1.getSchema() ;
        assertTrue(Schema.equals(load1.getSchema(), outputSchema, false, false)) ;       
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
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", pigStorage),
                                  null) ;

        // schema for input#1
        Schema inputSchema1 = null ;
        {
            List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
            fsList1.add(new FieldSchema("field1", field1Type)) ;
            fsList1.add(new FieldSchema("field2", field2Type)) ;

            inputSchema1 = new Schema(fsList1) ;
        }

        // set schemas
        load1.setEnforcedSchema(inputSchema1) ;

        // Create inner plan
        LogicalPlan innerPlan = new LogicalPlan() ;
        LOProject project1 = new LOProject(innerPlan, genNewOperatorKey(), load1, 0) ;
        project1.setSentinel(true);
        LOProject project2 = new LOProject(innerPlan, genNewOperatorKey(), load1, 1) ;
        project2.setSentinel(true);

        LOGreaterThan gt1 = new LOGreaterThan(innerPlan,
                                              genNewOperatorKey(),
                                              project1,
                                              project2) ;

        innerPlan.add(project1) ;
        innerPlan.add(project2) ;
        innerPlan.add(gt1) ;

        innerPlan.connect(project1, gt1) ;
        innerPlan.connect(project2, gt1) ;

        // filter
        LOFilter filter1 = new LOFilter(plan, genNewOperatorKey(), innerPlan, load1) ;

        plan.add(load1);
        plan.add(filter1);
        plan.connect(load1, filter1) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        printMessageCollector(collector) ;
        printTypeGraph(plan) ;

        if (collector.hasError()) {
            throw new AssertionError("Expect no error") ;
        }

        Schema endResultSchema = filter1.getSchema() ;
        assertEquals(endResultSchema.getField(0).type, field1Type) ;
        assertEquals(endResultSchema.getField(1).type, field2Type) ;

    }

    // Negative test
    @Test
    public void testFilterWithInnerPlan3() throws Throwable {
        // Create outer plan
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", pigStorage),
                                  null) ;

        // schema for input#1
        Schema inputSchema1 = null ;
        {
            List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
            fsList1.add(new FieldSchema("field1", DataType.INTEGER)) ;
            fsList1.add(new FieldSchema("field2", DataType.LONG)) ;

            inputSchema1 = new Schema(fsList1) ;
        }

        // set schemas
        load1.setEnforcedSchema(inputSchema1) ;

        // Create inner plan
        LogicalPlan innerPlan = new LogicalPlan() ;
        LOProject project1 = new LOProject(innerPlan, genNewOperatorKey(), load1, 0) ;
        project1.setSentinel(true);
        LOProject project2 = new LOProject(innerPlan, genNewOperatorKey(), load1, 1) ;
        project2.setSentinel(true);

        LOAdd add1 = new LOAdd(innerPlan, genNewOperatorKey(), project1, project2) ;

        innerPlan.add(project1) ;
        innerPlan.add(project2) ;
        innerPlan.add(add1) ;

        innerPlan.connect(project1, add1) ;
        innerPlan.connect(project2, add1) ;

        // filter
        LOFilter filter1 = new LOFilter(plan, genNewOperatorKey(), innerPlan, load1) ;

        plan.add(load1);
        plan.add(filter1);
        plan.connect(load1, filter1) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        try {
            typeValidator.validate(plan, collector) ;
            fail("Error expected") ;
        }
        catch (Exception t) {
            // good
        }
        printMessageCollector(collector) ;
        printTypeGraph(plan) ;

        if (!collector.hasError()) {
            throw new AssertionError("Expect error") ;
        }

    }


    // Simple project sort columns
    @Test
    public void testSortWithInnerPlan1() throws Throwable {
        // Create outer plan
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", pigStorage),
                                  null) ;

        // schema for input#1
        Schema inputSchema1 = null ;
        {
            List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
            fsList1.add(new FieldSchema("field1", DataType.LONG)) ;
            fsList1.add(new FieldSchema("field2", DataType.INTEGER)) ;

            inputSchema1 = new Schema(fsList1) ;
        }

        // set schemas
        load1.setEnforcedSchema(inputSchema1) ;

        // Create project inner plan #1
        LogicalPlan innerPlan1 = new LogicalPlan() ;
        LOProject project1 = new LOProject(innerPlan1, genNewOperatorKey(), load1, 1) ;
        project1.setSentinel(true);
        innerPlan1.add(project1) ;

        // Create project inner plan #2
        LogicalPlan innerPlan2 = new LogicalPlan() ;
        LOProject project2 = new LOProject(innerPlan2, genNewOperatorKey(), load1, 0) ;
        project1.setSentinel(true);
        innerPlan2.add(project2) ;

        // List of innerplans
        List<LogicalPlan> innerPlans = new ArrayList<LogicalPlan>() ;
        innerPlans.add(innerPlan1) ;
        innerPlans.add(innerPlan2) ;

        // List of ASC flags
        List<Boolean> ascList = new ArrayList<Boolean>() ;
        ascList.add(true);
        ascList.add(true);

        // Sort
        LOSort sort1 = new LOSort(plan, genNewOperatorKey(), load1, innerPlans,  ascList, null) ;


        plan.add(load1);
        plan.add(sort1);
        plan.connect(load1, sort1) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        printMessageCollector(collector) ;
        printTypeGraph(plan) ;

        if (collector.hasError()) {
            throw new AssertionError("Expect no error") ;
        }

        Schema endResultSchema = sort1.getSchema() ;

        // outer
        assertEquals(endResultSchema.getField(0).type, DataType.LONG) ;
        assertEquals(endResultSchema.getField(1).type, DataType.INTEGER) ;

        // inner
        assertEquals(getSingleLeafPlanOutputType(innerPlan1), DataType.INTEGER);
        assertEquals(getSingleLeafPlanOutputType(innerPlan2), DataType.LONG);

    }

    // Positive expression sort columns
    @Test
    public void testSortWithInnerPlan2() throws Throwable {
        // Create outer plan
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", pigStorage),
                                  null) ;

        // schema for input#1
        Schema inputSchema1 = null ;
        {
            List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
            fsList1.add(new FieldSchema("field1", DataType.BYTEARRAY)) ;
            fsList1.add(new FieldSchema("field2", DataType.INTEGER)) ;

            inputSchema1 = new Schema(fsList1) ;
        }

        // set schemas
        load1.setEnforcedSchema(inputSchema1) ;

        // Create expression inner plan #1
        LogicalPlan innerPlan1 = new LogicalPlan() ;
        LOProject project11 = new LOProject(innerPlan1, genNewOperatorKey(), load1, 0) ;
        project11.setSentinel(true);
        LOProject project12 = new LOProject(innerPlan1, genNewOperatorKey(), load1, 1) ;
        project11.setSentinel(true);
        LOMultiply mul1 = new LOMultiply(innerPlan1, genNewOperatorKey(), project11, project12) ;

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
        LOMod mod21 = new LOMod(innerPlan2, genNewOperatorKey(), project21, const21) ;

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
        LOSort sort1 = new LOSort(plan, genNewOperatorKey(), load1, innerPlans,  ascList, null) ;


        plan.add(load1);
        plan.add(sort1);
        plan.connect(load1, sort1) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        typeValidator.validate(plan, collector) ;
        printMessageCollector(collector) ;
        printTypeGraph(plan) ;

        if (collector.hasError()) {
            throw new AssertionError("Expect no error") ;
        }

        Schema endResultSchema = sort1.getSchema() ;

        // outer
        assertEquals(endResultSchema.getField(0).type, DataType.BYTEARRAY) ;
        assertEquals(endResultSchema.getField(1).type, DataType.INTEGER) ;

        // inner
        assertEquals(getSingleLeafPlanOutputType(innerPlan1), DataType.INTEGER);
        assertEquals(getSingleLeafPlanOutputType(innerPlan2), DataType.LONG);

    }
    
    // Negative test on expression sort columns
    @Test
    public void testSortWithInnerPlan3() throws Throwable {
        // Create outer plan
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", pigStorage),
                                  null) ;

        // schema for input#1
        Schema inputSchema1 = null ;
        {
            List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
            fsList1.add(new FieldSchema("field1", DataType.BYTEARRAY)) ;
            fsList1.add(new FieldSchema("field2", DataType.INTEGER)) ;

            inputSchema1 = new Schema(fsList1) ;
        }

        // set schemas
        load1.setEnforcedSchema(inputSchema1) ;

        // Create expression inner plan #1
        LogicalPlan innerPlan1 = new LogicalPlan() ;
        LOProject project11 = new LOProject(innerPlan1, genNewOperatorKey(), load1, 0) ;
        project11.setSentinel(true);
        LOProject project12 = new LOProject(innerPlan1, genNewOperatorKey(), load1, 1) ;
        project11.setSentinel(true);
        LOMultiply mul1 = new LOMultiply(innerPlan1, genNewOperatorKey(), project11, project12) ;

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
        const21.setType(DataType.BYTEARRAY);
        LOMod mod21 = new LOMod(innerPlan2, genNewOperatorKey(), project21, const21) ;

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
        LOSort sort1 = new LOSort(plan, genNewOperatorKey(), load1, innerPlans,  ascList, null) ;


        plan.add(load1);
        plan.add(sort1);
        plan.connect(load1, sort1) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        try {
            typeValidator.validate(plan, collector) ;
            fail("Error expected") ;
        }
        catch (Exception t) {
            // good
        }
        printMessageCollector(collector) ;
        printTypeGraph(plan) ;

        if (!collector.hasError()) {
            throw new AssertionError("Error expected") ;
        }

    }


    // Positive expression cond columns
    @Test
    public void testSplitWithInnerPlan1() throws Throwable {
        // Create outer plan
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", pigStorage),
                                  null) ;

        // schema for input#1
        Schema inputSchema1 = null ;
        {
            List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
            fsList1.add(new FieldSchema("field1", DataType.BYTEARRAY)) ;
            fsList1.add(new FieldSchema("field2", DataType.INTEGER)) ;

            inputSchema1 = new Schema(fsList1) ;
        }

        // set schemas
        load1.setEnforcedSchema(inputSchema1) ;

        // Create expression inner plan #1
        LogicalPlan innerPlan1 = new LogicalPlan() ;
        LOProject project11 = new LOProject(innerPlan1, genNewOperatorKey(), load1, 0) ;
        project11.setSentinel(true);
        LOProject project12 = new LOProject(innerPlan1, genNewOperatorKey(), load1, 1) ;
        project11.setSentinel(true);
        LONotEqual notequal1 = new LONotEqual(innerPlan1, genNewOperatorKey(), project11, project12) ;

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
                                                           genNewOperatorKey(),
                                                           project21,
                                                           const21) ;

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
                                     new ArrayList<LogicalOperator>(),
                                     new HashMap<String,LogicalPlan>());

        // output1
        LOSplitOutput splitOutput1 = new LOSplitOutput(plan, genNewOperatorKey(), 0) ;
        split1.addOutput(splitOutput1);
        split1.addOutputAlias("output1", innerPlan1);

        // output2
        LOSplitOutput splitOutput2 = new LOSplitOutput(plan, genNewOperatorKey(), 1) ;
        split1.addOutput(splitOutput2);
        split1.addOutputAlias("output2", innerPlan2);

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
        {
            Schema endResultSchema1 = split1.getSchema() ;
            // outer
            assertEquals(endResultSchema1.getField(0).type, DataType.BYTEARRAY) ;
            assertEquals(endResultSchema1.getField(1).type, DataType.INTEGER) ;
        }

        // check split output #1
        {
            Schema endResultSchema1 = splitOutput1.getSchema() ;
            // outer
            assertEquals(endResultSchema1.getField(0).type, DataType.BYTEARRAY) ;
            assertEquals(endResultSchema1.getField(1).type, DataType.INTEGER) ;
        }

        // check split output #2
        {
            Schema endResultSchema2 = splitOutput2.getSchema() ;
            // outer
            assertEquals(endResultSchema2.getField(0).type, DataType.BYTEARRAY) ;
            assertEquals(endResultSchema2.getField(1).type, DataType.INTEGER) ;
        }

        // inner conditions: all have to be boolean
        assertEquals(getSingleLeafPlanOutputType(innerPlan1), DataType.BOOLEAN);
        assertEquals(getSingleLeafPlanOutputType(innerPlan2), DataType.BOOLEAN);

    }

    // Negative test: expression cond columns not evaluate to boolean
    @Test
    public void testSplitWithInnerPlan2() throws Throwable {
        // Create outer plan
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", pigStorage),
                                  null) ;

        // schema for input#1
        Schema inputSchema1 = null ;
        {
            List<FieldSchema> fsList1 = new ArrayList<FieldSchema>() ;
            fsList1.add(new FieldSchema("field1", DataType.BYTEARRAY)) ;
            fsList1.add(new FieldSchema("field2", DataType.INTEGER)) ;

            inputSchema1 = new Schema(fsList1) ;
        }

        // set schemas
        load1.setEnforcedSchema(inputSchema1) ;

        // Create expression inner plan #1
        LogicalPlan innerPlan1 = new LogicalPlan() ;
        LOProject project11 = new LOProject(innerPlan1, genNewOperatorKey(), load1, 0) ;
        project11.setSentinel(true);
        LOProject project12 = new LOProject(innerPlan1, genNewOperatorKey(), load1, 1) ;
        project11.setSentinel(true);
        LONotEqual notequal1 = new LONotEqual(innerPlan1, genNewOperatorKey(), project11, project12) ;

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
        LOSubtract subtract21 = new LOSubtract(innerPlan2,
                                               genNewOperatorKey(),
                                               project21,
                                               const21) ;

        innerPlan2.add(project21) ;
        innerPlan2.add(const21) ;
        innerPlan2.add(subtract21) ;

        innerPlan2.connect(project21, subtract21);
        innerPlan2.connect(const21, subtract21) ;

        // List of innerplans
        List<LogicalPlan> innerPlans = new ArrayList<LogicalPlan>() ;
        innerPlans.add(innerPlan1) ;
        innerPlans.add(innerPlan2) ;

        // split
        LOSplit split1 = new LOSplit(plan,
                                     genNewOperatorKey(),
                                     new ArrayList<LogicalOperator>(),
                                     new HashMap<String,LogicalPlan>());

        // output1
        LOSplitOutput splitOutput1 = new LOSplitOutput(plan, genNewOperatorKey(), 0) ;
        split1.addOutput(splitOutput1);
        split1.addOutputAlias("output1", innerPlan1);

        // output2
        LOSplitOutput splitOutput2 = new LOSplitOutput(plan, genNewOperatorKey(), 1) ;
        split1.addOutput(splitOutput2);
        split1.addOutputAlias("output2", innerPlan2);

        plan.add(load1);
        plan.add(split1);
        plan.add(splitOutput1);
        plan.add(splitOutput2);

        plan.connect(load1, split1) ;
        plan.connect(split1, splitOutput1) ;
        plan.connect(split1, splitOutput2) ;

        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        TypeCheckingValidator typeValidator = new TypeCheckingValidator() ;
        try {
            typeValidator.validate(plan, collector) ;
        }
        catch (Exception t) {
            // good
        }
        printMessageCollector(collector) ;
        printTypeGraph(plan) ;

        if (!collector.hasError()) {
            throw new AssertionError("Error expected") ;
        }

    }

    // Positive test
    @Test
    public void testCOGroupWithInnerPlan1() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;

        String pigStorage = PigStorage.class.getName() ;

        LOLoad load1 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", pigStorage),
                                  null) ;
        LOLoad load2 = new LOLoad(plan,
                                  genNewOperatorKey(),
                                  new FileSpec("pi", pigStorage),
                                  null) ;

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
        load1.setEnforcedSchema(inputSchema1) ;
        load2.setEnforcedSchema(inputSchema2) ;

        // Create expression inner plan #1 of input #1
        LogicalPlan innerPlan11 = new LogicalPlan() ;
        LOProject project111 = new LOProject(innerPlan11, genNewOperatorKey(), load1, 0) ;
        project111.setSentinel(true);
        LOConst const111 = new LOConst(innerPlan11, genNewOperatorKey(), 26F) ;
        const111.setType(DataType.FLOAT);
        LOSubtract subtract111 = new LOSubtract(innerPlan11,
                                                genNewOperatorKey(),
                                                project111,
                                                const111) ;

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
                                 genNewOperatorKey(),
                                 project211,
                                 project212) ;

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
                                                genNewOperatorKey(),
                                                project121,
                                                const121) ;

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
        maps.put(load1, innerPlan21);
        maps.put(load2, innerPlan12);

        boolean[] isInner = new boolean[inputs.size()] ;
        for (int i=0; i < isInner.length ; i++) {
            isInner[i] = false ;
        }

        LOCogroup cogroup1 = new LOCogroup(plan,
                                           genNewOperatorKey(),
                                           inputs,
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
        // TODO: First column has to be discussed again
        //assertEquals(endResultSchema.getField(0).type, DataType.BYTEARRAY) ;
        assertEquals(endResultSchema.getField(1).type, DataType.BAG) ;
        assertEquals(endResultSchema.getField(2).type, DataType.BAG) ;

        // check inner schema1
        Schema innerSchema1 = endResultSchema.getField(1).schema ;
        assertEquals(innerSchema1.getField(0).type, DataType.INTEGER);
        assertEquals(innerSchema1.getField(1).type, DataType.LONG);

        // check inner schema2
        Schema innerSchema2 = endResultSchema.getField(2).schema ;
        assertEquals(innerSchema2.getField(0).type, DataType.DOUBLE);
        assertEquals(innerSchema2.getField(1).type, DataType.INTEGER);

        // check group by col end result
        assertEquals(getSingleLeafPlanOutputType(innerPlan11), DataType.FLOAT) ;
        assertEquals(getSingleLeafPlanOutputType(innerPlan21), DataType.LONG) ;
        assertEquals(getSingleLeafPlanOutputType(innerPlan12), DataType.DOUBLE) ;       
    }

    private void checkForEachCasting(LOForEach foreach, int idx, boolean isCast, byte toType) {
        LOGenerate generate = (LOGenerate) foreach.getForEachPlan().getRoots().get(0) ;
        LogicalPlan plan = generate.getGeneratePlans().get(idx) ;

        if (isCast) {
            List<LogicalOperator> leaveList = plan.getLeaves() ;
            assertEquals(leaveList.size(), 1);
            assertTrue(leaveList.get(0) instanceof LOCast);
            assertTrue(leaveList.get(0).getType() == toType) ;
        }
        else {
            List<LogicalOperator> leaveList = plan.getLeaves() ;
            assertEquals(leaveList.size(), 1);
            assertTrue(leaveList.get(0) instanceof LOProject);
        }
        
    }

    private byte getSingleLeafPlanOutputType(LogicalPlan plan) throws Throwable {
        List<LogicalOperator> list = plan.getLeaves() ;
        if (list.size() != 1) {
            throw new AssertionError("The plan has more than one leaf node") ;
        }
        return list.get(0).getType() ;
    }
    
    private OperatorKey genNewOperatorKey() {
        long newId = NodeIdGenerator.getGenerator().getNextNodeId("scope") ;
        return new OperatorKey("scope", newId) ;
    }   
    
    private void printTypeGraph(LogicalPlan plan) {
        System.out.println("*****Type Graph*******") ;
        TypeGraphPrinter printer = new TypeGraphPrinter(plan) ;
        String rep = printer.printToString() ;
        System.out.println(rep) ;
    }
    
    private void printMessageCollector(CompilationMessageCollector collector) {
        if (collector.hasMessage()) {
            System.out.println("*****MessageCollector dump*******") ;
            Iterator<Message> it1 = collector.iterator() ;
            while (it1.hasNext()) {
                Message msg = it1.next() ;
                System.out.println(msg.getMessageType() + ":" + msg.getMessage());
            }
        }
    }

}
