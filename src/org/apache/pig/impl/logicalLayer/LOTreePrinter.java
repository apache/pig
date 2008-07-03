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

package org.apache.pig.impl.logicalLayer;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.eval.cond.Cond; 

import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.eval.EvalSpecPrinter;

public class LOTreePrinter extends LOVisitor {
	
	private static final String LINE_START_SYMBOL = "|---" ;
	private static final String IDENT_SYMBOL = "      " ;
	
	private int currentIdent = 0 ;
	
    private PrintStream mStream = null;
    
	public LOTreePrinter(PrintStream ps) {
        mStream = ps;
    }
	
	public void increaseIdent() {
		currentIdent++ ;
	}
	
	public void decreaseIdent() {
		currentIdent-- ;
		if (currentIdent <0)
		{
			throw new RuntimeException("Invalid LOTreePrinter state. currentIdent < 0") ;
		}
	}
	
    @Override
    public void visitCogroup(LOCogroup g) {
    	printLineHeader(g, false) ;
    	mStream.print(" ( " + g.arguments() + " ) ") ;
    	mStream.println() ;
    	depthFirstSearchVisit(g) ;
    } 
        
    @Override
    public void visitEval(LOEval g) {
    	printLineHeader(g, false) ;
    	mStream.print(" ( " + g.arguments() + " ) ") ;
    	mStream.println() ;
    	depthFirstSearchVisit(g) ;
    }
        
    @Override
    public void visitUnion(LOUnion g) {
    	printLineHeader(g) ;
    	depthFirstSearchVisit(g) ;
    }
        
        
    @Override
    public void visitLoad(LOLoad g) {
    	printLineHeader(g, false) ;
    	mStream.print(" ( file = " + g.getInputFileSpec().getFileName() ) ;
    	
    	if (g.outputSchema().getFields().size() > 0)
    	{
	    	mStream.print(" AS ") ;
	    	boolean isFirst = true ;
	    	for(Schema schema: g.outputSchema().getFields() ) {
	    		if (isFirst) {
	    			isFirst = false ;
	    		} 
	    		else {
	    			mStream.print(",") ;
	    		}
	    		mStream.print(schema.getAlias()) ;
	    	}
    	}
    	
    	mStream.println(" )") ;
    	depthFirstSearchVisit(g) ;
    }
        
    @Override
    public void visitSort(LOSort g) {
    	printLineHeader(g, false) ;
    	mStream.print(" ( BY " + g.arguments() + " ) ") ;
    	mStream.println() ;
    	depthFirstSearchVisit(g) ;
    }
        
    @Override
    public void visitSplit(LOSplit g) {
    	printLineHeader(g, false) ;
    	mStream.print(" ( ") ;
    	boolean isFirst = true ;
    	for(Cond cond:g.getConditions()) {
    		if (isFirst) {
    			isFirst = false ;
    		} 
    		else {
    			mStream.print(",") ;
    		}   		
    		mStream.print(cond) ;
    	}
    	mStream.println(" ) ") ;
    	depthFirstSearchVisit(g) ;
    }
    
    @Override
    public void visitSplitOutput(LOSplitOutput g) {
    	printLineHeader(g, false) ;
    	mStream.print(" ( " + g.arguments() + " ) ") ;
    	mStream.println() ;
    	depthFirstSearchVisit(g) ;
    }
     
    @Override
    public void visitStore(LOStore g) {
    	printLineHeader(g) ;
    	depthFirstSearchVisit(g) ;
    }
    
    private void printLineHeader(LogicalOperator g) {
    	printLineHeader(g, true) ;
    }
    
    private void printLineHeader(LogicalOperator g, boolean appendNewLine) {
    	for(int i=0;i<currentIdent;i++) {
    		mStream.print(IDENT_SYMBOL) ;
    	} 
    	mStream.print(LINE_START_SYMBOL) ;
    	mStream.print(g.getClass().getSimpleName()) ;
    	
    	if (appendNewLine) {
    		mStream.println();
    	}
    }

	private void depthFirstSearchVisit(LogicalOperator lo) {
	    List<OperatorKey> inputs = lo.getInputs();
	    Iterator<OperatorKey> i = inputs.iterator();
	    
	    this.increaseIdent() ;
	    while (i.hasNext()) {
	        LogicalOperator input = lo.getOpTable().get(i.next());
	        input.visit(this);
	    }
	    this.decreaseIdent() ;
	}
}
