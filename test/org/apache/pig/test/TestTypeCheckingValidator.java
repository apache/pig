package org.apache.pig.test;

import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.pig.impl.logicalLayer.validators.*;
import org.apache.pig.impl.logicalLayer.* ;
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.CompilationMessageCollector.Message;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.parser.NodeIdGenerator;
import org.junit.Test;

public class TestTypeCheckingValidator extends TestCase {

    
    @Test
    public void testExpressionTypeChecking1() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        LOConst constant1 = new LOConst(plan, genNewOperatorKeyId(), 1, 10) ;
        constant1.setType(DataType.INTEGER) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKeyId(), 1, 20D) ;
        constant2.setType(DataType.DOUBLE) ;
        LOConst constant3 =  new LOConst(plan, genNewOperatorKeyId(), 1, "123") ;
        constant3.setType(DataType.CHARARRAY) ;
        
        LOAdd add1 = new LOAdd(plan, genNewOperatorKeyId(),1 , constant1, constant2) ;            
        LOCast cast1 = new LOCast(plan, genNewOperatorKeyId(), 1, constant3, DataType.BYTEARRAY) ;     
        LOMultiply mul1 = new LOMultiply(plan, genNewOperatorKeyId(), 1, add1, cast1) ;
        
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
        LOConst constant1 = new LOConst(plan, genNewOperatorKeyId(), 1, 10) ;
        constant1.setType(DataType.INTEGER) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKeyId(), 1, 20D) ;
        constant2.setType(DataType.BYTEARRAY) ;
        LOConst constant3 =  new LOConst(plan, genNewOperatorKeyId(), 1, 123L) ;
        constant3.setType(DataType.LONG) ;        
        LOConst constant4 =  new LOConst(plan, genNewOperatorKeyId(), 1, true) ;
        constant4.setType(DataType.BOOLEAN) ;
        
        LOSubtract sub1 = new LOSubtract(plan, genNewOperatorKeyId(), 1, constant1, constant2) ;
        LOGreaterThan gt1 = new LOGreaterThan(plan, genNewOperatorKeyId(), 1, sub1, constant3) ;
        LOAnd and1 = new LOAnd(plan, genNewOperatorKeyId(), 1, gt1, constant4) ;
        LONot not1 = new LONot(plan, genNewOperatorKeyId(), 1, and1) ;
        
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
        LOConst constant1 = new LOConst(plan, genNewOperatorKeyId(), 1, 10) ;
        constant1.setType(DataType.BYTEARRAY) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKeyId(), 1, 20L) ;
        constant2.setType(DataType.LONG) ;
        LOConst constant3 =  new LOConst(plan, genNewOperatorKeyId(), 1, 123) ;
        constant3.setType(DataType.INTEGER) ;
        
        LOMod mod1 = new LOMod(plan, genNewOperatorKeyId(),1 , constant1, constant2) ;            
        LOEqual equal1 = new LOEqual(plan, genNewOperatorKeyId(), 1, mod1, constant3) ;      
        
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
        LOConst constant1 = new LOConst(plan, genNewOperatorKeyId(), 1, 10) ;
        constant1.setType(DataType.INTEGER) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKeyId(), 1, 20D) ;
        constant2.setType(DataType.DOUBLE) ;
        LOConst constant3 =  new LOConst(plan, genNewOperatorKeyId(), 1, "123") ;
        constant3.setType(DataType.CHARARRAY) ;
        
        LODivide div1 = new LODivide(plan, genNewOperatorKeyId(),1 , constant1, constant2) ;         
        LOCast cast1 = new LOCast(plan, genNewOperatorKeyId(), 1, constant3, DataType.BYTEARRAY) ;    
        LONotEqual notequal1 = new LONotEqual(plan, genNewOperatorKeyId(), 1, div1, cast1) ;
        
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
        LOConst constant1 = new LOConst(plan, genNewOperatorKeyId(), 1, 10) ;
        constant1.setType(DataType.FLOAT) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKeyId(), 1, 20) ;
        constant2.setType(DataType.LONG) ;
        LOConst constant3 =  new LOConst(plan, genNewOperatorKeyId(), 1, 123F) ;
        constant3.setType(DataType.FLOAT) ;
        LOConst constant4 =  new LOConst(plan, genNewOperatorKeyId(), 1, 123D) ;
        constant4.setType(DataType.DOUBLE) ;
        
        LOLesserThanEqual lesser1 = new LOLesserThanEqual(plan, genNewOperatorKeyId(), 1, constant1, constant2) ;
        LOBinCond bincond1 = new LOBinCond(plan, genNewOperatorKeyId(), 1, lesser1, constant3, constant4) ;
        
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
        LOConst constant1 = new LOConst(plan, genNewOperatorKeyId(), 1, 10) ;
        constant1.setType(DataType.CHARARRAY) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKeyId(), 1, 20) ;
        constant2.setType(DataType.LONG) ;
        
        LOAdd add1 = new LOAdd(plan, genNewOperatorKeyId(),1 , constant1, constant2) ;      
        
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
    
    public void testExpressionTypeChecking7() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        LOConst constant1 = new LOConst(plan, genNewOperatorKeyId(), 1, 10) ;
        constant1.setType(DataType.INTEGER) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKeyId(), 1, 20D) ;
        constant2.setType(DataType.BYTEARRAY) ;
        LOConst constant3 =  new LOConst(plan, genNewOperatorKeyId(), 1, 123L) ;
        constant3.setType(DataType.LONG) ;        
        
        LOGreaterThan gt1 = new LOGreaterThan(plan, genNewOperatorKeyId(), 1, constant1, constant2) ;
        LOEqual equal1 = new LOEqual(plan, genNewOperatorKeyId(),1, gt1, constant3) ;
        
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
        LOConst constant1 = new LOConst(plan, genNewOperatorKeyId(), 1, 10) ;
        constant1.setType(DataType.INTEGER) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKeyId(), 1, 20D) ;
        constant2.setType(DataType.DOUBLE) ;
        
        LOMultiply mul1 = new LOMultiply(plan, genNewOperatorKeyId(), 1, constant1, constant2) ;
        
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
    
    /*
    @Test
    public void testArithmeticOperators2() throws Throwable {
        LogicalPlan plan = new LogicalPlan() ;
        LOConst constant1 = new LOConst(plan, genNewOperatorKeyId(), 1, 10) ;
        constant1.setType(DataType.INTEGER) ;
        LOConst constant2 =  new LOConst(plan, genNewOperatorKeyId(), 1, 20D) ;
        constant2.setType(DataType.DOUBLE) ;
        LOAdd add1 = new LOAdd(plan, genNewOperatorKeyId(),1 , constant1, constant2) ;
        
        plan.add(constant1) ;
        plan.add(constant2) ;
        plan.add(add1) ;
        
        plan.connect(constant1, add1) ;
        plan.connect(constant2, add1) ;
        
        System.out.println(DataType.findTypeName(add1.getType())) ;
        
        TypeCheckingValidator typeValidator = new TypeCheckingValidator(plan) ;
        typeValidator.validate(new CompilationMessageCollector()) ;
        
        System.out.println(DataType.findTypeName(add1.getType())) ;
        
    }
    
    */
    
    
    private OperatorKey genNewOperatorKeyId() {
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
