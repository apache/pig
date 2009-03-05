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

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.logicalLayer.parser.ParseException ;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.* ;

/**
 * This class transforms LogicalPlan to its textual representation
 *
 */
public class TypeGraphPrinter extends LOVisitor {
    
    private StringBuilder sb = null ;
    private int currentTabCount = 0 ;
    
    public TypeGraphPrinter(LogicalPlan plan) {
        super(plan, new DependencyOrderWalker<LogicalOperator, LogicalPlan>(plan));
        sb = new StringBuilder() ;
    }

    private void printTabs() {
        for (int i=0; i< currentTabCount; i++) {
            sb.append("\t") ;
        }
    }
    
    protected void visit(LogicalOperator op) {
        appendOp(op) ;
    }
    
    protected void visit(ExpressionOperator op) {
        appendOp(op) ;
    }
    
    protected void visit(BinaryExpressionOperator op) {
        appendOp(op) ;
    }
    
    protected void visit(UnaryExpressionOperator op) {               
        appendOp(op) ;
    }


    public void visit(LOGreaterThan op) throws VisitorException {
		appendOp(op) ;
	}

	public void visit(LOLesserThan op) throws VisitorException {
		appendOp(op) ;
	}

	public void visit(LOGreaterThanEqual op) throws VisitorException {
		appendOp(op) ;
	}

	public void visit(LOLesserThanEqual op) throws VisitorException {
		appendOp(op) ;
	}

	public void visit(LOEqual op) throws VisitorException {
		appendOp(op) ;
	}

	public void visit(LONotEqual op) throws VisitorException {
		appendOp(op) ;
	}

	public void visit(LOAdd op) throws VisitorException {
		appendOp(op) ;
	}

	public void visit(LOSubtract op) throws VisitorException {
		appendOp(op) ;
	}

	public void visit(LOMultiply op) throws VisitorException {
		appendOp(op) ;
	}

	public void visit(LODivide op) throws VisitorException {
		appendOp(op) ;
	}

	public void visit(LOMod op) throws VisitorException {
		appendOp(op) ;
	}


	public void visit(LONegative op) throws VisitorException {
		appendOp(op) ;
	}

	public void visit(LOMapLookup op) throws VisitorException {
		appendOp(op) ;
	}

	public void visit(LOAnd op) throws VisitorException {
		appendOp(op) ;
	}

	public void visit(LOOr op) throws VisitorException {
		appendOp(op) ;
	}

	public void visit(LONot op) throws VisitorException {
		appendOp(op) ;
	}

	public void visit(LOIsNull op) throws VisitorException {
		appendOp(op) ;
	}
    
    protected void visit(LOCogroup op) throws VisitorException {
        appendOp(op) ;
        List<LogicalOperator> inputs = op.getInputs() ;
        if (inputs != null) {
            for(LogicalOperator input: inputs) {
                List<LogicalPlan> plans
                    = new ArrayList<LogicalPlan>(op.getGroupByPlans().get(input)) ;
                if (plans != null) {
                    for(LogicalPlan plan: plans) {
                        currentTabCount++ ;
                        printTabs() ;
                        sb.append("<COGroup Inner Plan>\n") ;
                        pushWalker(mCurrentWalker.spawnChildWalker(plan)) ;
                        visit();
                        popWalker();
                        currentTabCount-- ;
                    }
                }
            }
        }
    }
    
    protected void visit(LOForEach op) throws VisitorException {
        appendOp(op) ;
        List<LogicalPlan> plans = op.getForEachPlans() ;
        if (plans != null) {
            for (LogicalPlan plan: plans) {
                currentTabCount++ ;
                printTabs() ;
                sb.append("<ForEach Inner Plan>\n") ;
                pushWalker(mCurrentWalker.spawnChildWalker(plan)) ;
                visit();
                popWalker();
                currentTabCount-- ;
            }
        }
    }
    
    protected void visit(LOSort op) {
        appendOp(op) ;
    }
    
    protected void visit(LOFilter op) throws VisitorException {
        appendOp(op) ;
        if (op.getComparisonPlan() != null) {
            currentTabCount++ ;
            printTabs() ;
            sb.append("<Filter Inner Plan>\n") ;
            pushWalker(mCurrentWalker.spawnChildWalker(op.getComparisonPlan())) ;
            visit();
            popWalker();
            currentTabCount-- ;
        }
    }
    
    protected void visit(LOSplit op) {
        appendOp(op) ;
    }
    
    /*
    protected void visit(LOForEach op) throws VisitorException {
        appendOp(op) ;
        if (op.getForEachPlan() != null) {
            currentTabCount++ ;
            printTabs() ;
            sb.append("<ForEach Inner Plan>\n") ;
            pushWalker(mCurrentWalker.spawnChildWalker(op.getForEachPlan())) ;
            visit();
            popWalker();
            currentTabCount-- ;
        }
    }
    */
    
    protected void visit(LOUserFunc op) {
        appendOp(op) ;
    }
    
    protected void visit(LOBinCond op) {
        appendOp(op) ;
    }

    protected void visit(LOCross op) {
        appendOp(op) ;
    }
    
    protected void visit(LOUnion op) {
        appendOp(op) ;
    }
    
    protected void visit(LOCast op) { 
        appendOp(op) ;
    }
      
    protected void visit(LOLoad op) { 
        appendOp(op) ;
    }
    
    protected void visit(LOStore op) {   
        appendOp(op) ;
    }
    
    protected void visit(LOConst op) {  
        appendOp(op) ;
    }

    protected void visit(LORegexp op) {
        appendOp(op) ;
    }

    protected void visit(LOProject op) {
        appendOp(op) ;
    }

    protected void visit(LODistinct op) {
        appendOp(op) ;
    }


    private void appendOp(LogicalOperator op)  {
        printTabs() ;
        sb.append("(") ;
        sb.append(op.getOperatorKey().getId()) ;
        sb.append(":") ;
        sb.append(op.getClass().getSimpleName()) ;
        sb.append("=") ;
        Schema schema = null ;

        if (!DataType.isSchemaType(op.getType())) {
            sb.append(DataType.findTypeName(op.getType())) ;
        }
        else {
            try {
                if (op instanceof ExpressionOperator) {
                    ExpressionOperator eOp = (ExpressionOperator) op ;
                    Schema.FieldSchema fs = eOp.getFieldSchema() ;
                    Schema.stringifySchema(sb, fs.schema, DataType.TUPLE) ;
                }
                else {
                    schema = op.getSchema() ;
                    Schema.stringifySchema(sb, schema, op.getType()) ;
                }
            }
            catch (FrontendException fe) {
                throw new RuntimeException("PROBLEM PRINTING SCHEMA") ;
            }
        }

        sb.append("==>") ;
        List<LogicalOperator> list
                    = mCurrentWalker.getPlan().getSuccessors(op) ;

        if (list!=null) {
            boolean isFirst = true ;
            for(LogicalOperator tmp: list) {         
                if (!isFirst) {
                    sb.append(",") ;
                } 
                else {
                    isFirst = false ;
                }
                sb.append(tmp.getOperatorKey().getId()) ;                
            }
        }
        else {
            sb.append("TERMINAL") ;
        }
        sb.append(")") ;
        sb.append("\n") ;
    }
    
    /***
     * This method has to be called after the visit is totally finished only
     */
    public String printToString() {
        try {
            visit() ;
        }
        catch(VisitorException vse) {
            throw new AssertionError("Error while transforming type graph to text") ;
        }
        return sb.toString() ;
    }
}
