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

package org.apache.pig.impl.eval;

import java.io.PrintStream;
import java.util.Iterator;

import org.apache.pig.impl.eval.cond.AndCond;
import org.apache.pig.impl.eval.cond.CompCond;
import org.apache.pig.impl.eval.cond.FalseCond;
import org.apache.pig.impl.eval.cond.FuncCond;
import org.apache.pig.impl.eval.cond.NotCond;
import org.apache.pig.impl.eval.cond.OrCond;
import org.apache.pig.impl.eval.cond.RegexpCond;
import org.apache.pig.impl.eval.cond.TrueCond;

public class EvalSpecTreePrinter extends EvalSpecVisitor {
	   private PrintStream mStream = null;

	    public EvalSpecTreePrinter(PrintStream ps) {
	        mStream = ps;
	    }

	    @Override
	    public void visitFilter(FilterSpec f) {
	        mStream.print("Filter: ");
	        if (f.cond instanceof AndCond) mStream.print(" AND ");
	        else if (f.cond instanceof CompCond) mStream.print(" COMP ");
	        else if (f.cond instanceof FalseCond) mStream.print(" FALSE ");
	        else if (f.cond instanceof FuncCond) mStream.print(" FUNC ");
	        else if (f.cond instanceof NotCond) mStream.print(" NOT ");
	        else if (f.cond instanceof OrCond) mStream.print(" OR ");
	        else if (f.cond instanceof RegexpCond) mStream.print(" REGEXP ");
	        else if (f.cond instanceof TrueCond) mStream.print(" TRUE ");
	        else throw new AssertionError(" Unknown Cond ");
	    }
	      
	    @Override
	    public void visitSortDistinct(SortDistinctSpec sd) {
	        mStream.print("Sort(") ;
	        if (sd.distinct()) {
	        	mStream.print("Distinct(") ;
	        }
	        sd.getSortSpec().visit(this);
	        if (sd.distinct()) {
	        	mStream.print(")") ;
	        }
	        mStream.print(")") ;
	    }

	    @Override
	    public void visitGenerate(GenerateSpec g) {
	        mStream.print("Generate(");
	        Iterator<EvalSpec> i = g.getSpecs().iterator();
	        boolean isFirst = true ;
	        while (i.hasNext()) {
	        	if (isFirst) {
	        		isFirst = false ;
	        	}
	        	else {
	        		mStream.print(",") ;
	        	}
	            i.next().visit(this);
	        }
	        mStream.print(")") ;
	    }
	        
	    @Override
	    public void visitMapLookup(MapLookupSpec ml) {
	        mStream.print("MapLookup(key=" + ml.key() + ")");
	    }
	      
	    @Override
	    public void visitConst(ConstSpec c) {
	        mStream.print("Const(" + c.value() + ")");
	    }
	      
	    @Override
	    public void visitProject(ProjectSpec p) {
	        mStream.print("Project(");
	        Iterator<Integer> i = p.getCols().iterator();
	        while (i.hasNext()) {
	            mStream.print(i.next().intValue());
	            if (i.hasNext()) mStream.print(",");
	        }
	        mStream.print(")");
	    }
	        
	    @Override
	    public void visitStar(StarSpec s) {
	        mStream.print("*");
	    }
	        
	    @Override
	    public void visitFuncEval(FuncEvalSpec fe) {
	        mStream.print("FuncEval(" + fe.getFuncName() + "(");
	        fe.getArgs().visit(this);
	        mStream.print("))") ;
	    }
	        
	    @Override
	    public void visitCompositeEval(CompositeEvalSpec ce) {
	        mStream.print("Composite(");
	        Iterator<EvalSpec> i = ce.getSpecs().iterator();
	        boolean isFirst = true ;
	        while (i.hasNext()) {
	        	if (isFirst) {
	        		isFirst = false ;
	        	}
	        	else {
	        		mStream.print(",") ;
	        	}
	            i.next().visit(this);
	        }
	        mStream.print(")") ;
	    }

	    @Override
	    public void visitBinCond(BinCondSpec bc) {
	        mStream.print("BinCond(True=>");
	        bc.ifTrue().visit(this);
	        mStream.print (",False=>");
	        bc.ifFalse().visit(this);
	        mStream.print (")");
	    }
}
