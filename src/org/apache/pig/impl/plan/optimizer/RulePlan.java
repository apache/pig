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
package org.apache.pig.impl.plan.optimizer;

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

public class RulePlan extends OperatorPlan<RuleOperator> {
    private static final long serialVersionUID = 2L;

    public RulePlan() {
        super();
    }

    public void explain(
            OutputStream out,
            PrintStream ps) throws VisitorException, IOException {
        RulePlanPrinter rpp = new RulePlanPrinter(ps, this);

        rpp.print(out);
    }
    
    /**
     * Do not use the clone method directly. Use {@link LogicalPlanCloner} instead.
     */
/*    @Override
    public RulePlan clone() throws CloneNotSupportedException {
        RulePlan clone = new RulePlan();

        // Get all the nodes in this plan, and clone them.  As we make
        // clones, create a map between clone and original.  Then walk the
        // connections in this plan and create equivalent connections in the
        // clone.
        Map<RuleOperator, RuleOperator> matches = 
            //new HashMap<LogicalOperator, LogicalOperator>(mOps.size());
            LogicalPlanCloneHelper.mOpToCloneMap;
        for (RuleOperator op : mOps.keySet()) {
            try {
            RuleOperator c = (RuleOperator)op.clone();
            clone.add(c);
            matches.put(op, c);
            } catch (CloneNotSupportedException cnse) {
                cnse.printStackTrace();
                throw cnse;
            }
        }

        // Build the edges
        for (RuleOperator op : mToEdges.keySet()) {
            RuleOperator cloneTo = matches.get(op);
            if (cloneTo == null) {
                String msg = new String("Unable to find clone for op "
                    + op.name());
                log.error(msg);
                throw new RuntimeException(msg);
            }
            Collection<RuleOperator> fromOps = mToEdges.get(op);
            for (RuleOperator fromOp : fromOps) {
                RuleOperator cloneFrom = matches.get(fromOp);
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
*/    
}
