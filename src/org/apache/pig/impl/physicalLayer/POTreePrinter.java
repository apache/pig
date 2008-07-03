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

package org.apache.pig.impl.physicalLayer;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.POMapreduce;
import org.apache.pig.backend.local.executionengine.POCogroup;
import org.apache.pig.backend.local.executionengine.POEval;
import org.apache.pig.backend.local.executionengine.POLoad;
import org.apache.pig.backend.local.executionengine.POSort;
import org.apache.pig.backend.local.executionengine.POSplit;
import org.apache.pig.backend.local.executionengine.POStore;
import org.apache.pig.backend.local.executionengine.POUnion;
import org.apache.pig.impl.eval.EvalSpec;
import org.apache.pig.impl.eval.EvalSpecTreePrinter;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.OperatorKey;

public class POTreePrinter extends POVisitor {
	
	private static final String HEADER_START_SYMBOL = "|---" ;
	private static final String CONTENT_START_SYMBOL = "    " ;
	private static final String IDENT_SYMBOL = "      " ;
	
	private int currentIdent = 0 ;
	
    private PrintStream mStream = null;

    public POTreePrinter(Map<OperatorKey, ExecPhysicalOperator> opTable,
                     PrintStream ps) {
        super(opTable);
        mStream = ps;
    }
    
	public void increaseIdent() {
		currentIdent++ ;
	}
	
	public void decreaseIdent() {
		currentIdent-- ;
		if (currentIdent <0)
		{
			throw new RuntimeException("Invalid POTreePrinter state. currentIdent < 0") ;
		}
	}
	
    @Override
    public void visitMapreduce(POMapreduce p) {
    	printLineHeader(p) ;
    	
    	// partition function
        if (p.partitionFunction != null) {
        	adjustContentIdent()  ;
            mStream.println("Partition Function: " + p.partitionFunction.getName());
            mStream.println() ;
        }
        
    	// map line
    	adjustContentIdent()  ;
    	mStream.print("Map : ") ;
        visitSpecs(p.toMap);
    	mStream.println() ;
    	
    	// combine line
    	if (p.toCombine != null) {
	    	adjustContentIdent()  ;
	    	mStream.print("Combine : ") ;
	    	p.toCombine.visit(new EvalSpecTreePrinter(mStream));
	    	mStream.println() ;
    	}
  	
    	// reduce line
    	if (p.toReduce != null) {
	    	adjustContentIdent()  ;
	    	mStream.print("Reduce : ") ;
	    	p.toReduce.visit(new EvalSpecTreePrinter(mStream));
	    	mStream.println() ;
    	}
    	
    	// grouping line
    	if (p.groupFuncs != null) {
	    	adjustContentIdent()  ;
	    	mStream.print("Grouping : ") ;
	    	visitSpecs(p.groupFuncs);
	    	mStream.println() ;
    	}
    	
    	// input  files line
    	adjustContentIdent()  ;
    	mStream.print("Input File(s) : ") ;
        Iterator<FileSpec> i = p.inputFileSpecs.iterator();
        while (i.hasNext()) {
            mStream.print(i.next().getFileName());
            if (i.hasNext()) mStream.print(", ");
        }
    	mStream.println() ;
    	
    	// properties
    	adjustContentIdent();
    	mStream.print("Properties : ");
    	Iterator<Map.Entry<Object, Object>> pi = p.properties.entrySet().iterator();
    	while (pi.hasNext()) {
    	    Map.Entry<Object, Object> e = pi.next();
    	    mStream.print((String)e.getKey() + ":" + (String)e.getValue());
    	    if (pi.hasNext()) {
    	        mStream.print(", ");
    	    }
    	}
        mStream.println() ;

    	depthFirstSearchVisit(p) ;
    }
        
    @Override
    public void visitLoad(POLoad p) {
    	printLineHeader(p) ;
    	depthFirstSearchVisit(p) ;
    }
        
    @Override
    public void visitSort(POSort p) {
    	printLineHeader(p) ;
    	depthFirstSearchVisit(p) ;
    }
        
    @Override
    public void visitStore(POStore p) {
    	printLineHeader(p) ;
    	depthFirstSearchVisit(p) ;
    }

    @Override
    public void visitCogroup(POCogroup p) {
    	printLineHeader(p) ;
    	depthFirstSearchVisit(p) ;
    }

    @Override
    public void visitEval(POEval p) {
    	printLineHeader(p) ;
    	depthFirstSearchVisit(p) ;
    }

    @Override
    public void visitSplit(POSplit p) {
    	printLineHeader(p) ;
    	depthFirstSearchVisit(p) ;
    }

    @Override
    public void visitUnion(POUnion p) {
    	printLineHeader(p) ;
    	depthFirstSearchVisit(p) ;
    }
    
    private void printLineHeader(PhysicalOperator g) {
    	printLineHeader(g, true) ;
    }
    
    private void printLineHeader(PhysicalOperator g, boolean appendNewLine) {
    	for(int i=0;i<currentIdent;i++) {
    		mStream.print(IDENT_SYMBOL) ;
    	} 
    	mStream.print(HEADER_START_SYMBOL) ;
    	mStream.print(g.getClass().getSimpleName()) ;
    	
    	if (appendNewLine) {
    		mStream.println();
    	}
    }
    
    private void adjustContentIdent() {
       	for(int i=0;i<currentIdent;i++) {
    		mStream.print(IDENT_SYMBOL) ;
    	}     	
       	mStream.print(CONTENT_START_SYMBOL) ;
    }
    
	private void depthFirstSearchVisit(PhysicalOperator po) {
	    this.increaseIdent() ; 
	    for(OperatorKey inputKey : po.inputs) {
	    	PhysicalOperator input = null ;
	    	// since the only sub-type of ExecPhysicalOperator is PhysicalOperator
	    	// it is legal to convert this way
	    	input = (PhysicalOperator) mOpTable.get(inputKey) ;    	
	    	if (input==null) {
	    		throw new RuntimeException("Invalid OpKey table found while reading in POTreePrinter") ;
	    	}
	        input.visit(this);
	    }
	    this.decreaseIdent() ;
	}
	
    private void visitSpecs(List<EvalSpec> specs) {
        Iterator<EvalSpec> j = specs.iterator();
        while (j.hasNext()) {
            j.next().visit(new EvalSpecTreePrinter(mStream));
        }
    }

}
