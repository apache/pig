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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Collection;

import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.PlanException;

public class LogicalPlan extends OperatorPlan<LogicalOperator> {
    private static final long serialVersionUID = 2L;

    public LogicalPlan() {
        super();
    }
    public LogicalOperator getSingleLeafPlanOutputOp()  {
        List<LogicalOperator> list = this.getLeaves() ;
        if (list.size() != 1) {
            throw new AssertionError("The plan has more than one leaf node") ;
        }
        return list.get(0) ;
    }

    public byte getSingleLeafPlanOutputType()  {
        return getSingleLeafPlanOutputOp().getType() ;
    }

    public void explain(
            OutputStream out,
            PrintStream ps) throws VisitorException, IOException {
        LOPrinter lpp = new LOPrinter(ps, this);

        lpp.print(out);
    }

    public void explain(PrintStream ps, String format, boolean verbose) 
        throws VisitorException, IOException {
        ps.println("#-----------------------------------------------");
        ps.println("# Logical Plan:");
        ps.println("#-----------------------------------------------");
                
        if (format.equals("text")) {
            LOPrinter lpp = new LOPrinter(ps, this);
            lpp.setVerbose(verbose);
            lpp.visit();
        } else if (format.equals("dot")) {
            DotLOPrinter lpp = new DotLOPrinter(this, ps);
            lpp.setVerbose(verbose);
            lpp.dump();
            ps.println("");
        }
    }
    
//    public String toString() {
//        if(mOps.size() == 0)
//            return "Empty Plan!";
//        else{
//            ByteArrayOutputStream baos = new ByteArrayOutputStream();
//            PrintStream ps = new PrintStream(baos);
//            try {
//		explain(baos, ps);
//	    } catch (VisitorException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	    } catch (IOException e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	    }
//            return baos.toString();
//        }
//    }

    /**
     * Do not use the clone method directly. Use {@link LogicalPlanCloner} instead.
     */
    @Override
    public LogicalPlan clone() throws CloneNotSupportedException {
        LogicalPlan clone = new LogicalPlan();

        // Get all the nodes in this plan, and clone them.  As we make
        // clones, create a map between clone and original.  Then walk the
        // connections in this plan and create equivalent connections in the
        // clone.
        Map<LogicalOperator, LogicalOperator> matches = 
            //new HashMap<LogicalOperator, LogicalOperator>(mOps.size());
            LogicalPlanCloneHelper.mOpToCloneMap;
        for (LogicalOperator op : mOps.keySet()) {
            try {
            LogicalOperator c = (LogicalOperator)op.clone();
            clone.add(c);
            matches.put(op, c);
            } catch (CloneNotSupportedException cnse) {
                cnse.printStackTrace();
                throw cnse;
            }
        }

        // Build the edges
        for (LogicalOperator op : mToEdges.keySet()) {
            LogicalOperator cloneTo = matches.get(op);
            if (cloneTo == null) {
                String msg = new String("Unable to find clone for op "
                    + op.name());
                log.error(msg);
                throw new RuntimeException(msg);
            }
            Collection<LogicalOperator> fromOps = mToEdges.get(op);
            for (LogicalOperator fromOp : fromOps) {
                LogicalOperator cloneFrom = matches.get(fromOp);
                if (cloneFrom == null) {
                    String msg = new String("Unable to find clone for op "
                        + fromOp.name());
                    log.error(msg);
                    throw new RuntimeException(msg);
                }
                try {
                    clone.connect(cloneFrom, cloneTo);
                } catch (PlanException pe) {
                    throw new RuntimeException(pe);
                }
            }
        }

        return clone;
    }
    
    /**
     * A utility method to check if a plan contains a chain of projection
     * operators
     * 
     * @param plan
     *            input plan
     * @return true if there is a chain of projection operators; false otherwise
     */
    public static boolean chainOfProjects(LogicalPlan plan) {
        
        if (plan == null) {
            return false;
        }
        
        List<LogicalOperator> leaves = plan.getLeaves();

        if (leaves == null) {
            return false;
        }

        if (leaves.size() > 1) {
            return false;
        }

        LogicalOperator node = leaves.get(0);

        while (true) {
            if ((node == null) || !(node instanceof LOProject)) {
                //not a projection operator
                return false;
            }

            List<LogicalOperator> predecessors = plan.getPredecessors(node);

            if (predecessors == null) {
                //we have reached the root
                return true;
            }

            if (predecessors.size() > 1) {
                //a project cannot have multiple inputs
                return false;
            }

            node = predecessors.get(0);
        }
    }
    
}
