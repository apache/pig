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

package org.apache.pig.newplan.logical.relational;

import java.util.Collection;
import java.util.List;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;

/** 
 * CUBE operator implementation for data cube computation.
 * <p> 
 * Cube operator syntax
 * <pre>{@code alias = CUBE rel BY (col_ref);}
 * alias - output alias 
 * CUBE - operator
 * rel - input relation
 * BY - operator
 * col_ref - column references or * or range in the schema referred by rel
 * </pre> </p>
 * <p>
 * The cube computation code sample at {@link org.apache.pig.builtin.CubeDimensions} 
 * can now be represented like below
 * <pre>{@code
 * events = LOAD '/logs/events' USING EventLoader() AS (lang, event, app_id, total);
 * eventcube = CUBE events BY (lang, event, app_id);
 * result = FOREACH eventcube GENERATE FLATTEN(group) as (lang, event, app_id),
 *          COUNT_STAR(cube), SUM(cube.total);
 * STORE result INTO 'cuberesult';
 * }</pre>
 * </p>
 */
public class LOCube extends LogicalRelationalOperator {
    private MultiMap<Integer, LogicalExpressionPlan> mExpressionPlans;

    public LOCube(LogicalPlan plan) {
	super("LOCube", plan);
    }

    public LOCube(OperatorPlan plan,
	    MultiMap<Integer, LogicalExpressionPlan> expressionPlans) {
	super("LOCube", plan);
	this.mExpressionPlans = expressionPlans;
    }

    @Override
    public LogicalSchema getSchema() throws FrontendException {
	// TODO: implement when physical operator for CUBE is implemented
	return null;
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
	try {
	    ((LogicalRelationalNodesVisitor) v).visit(this);
	} catch (ClassCastException cce) {
	    throw new FrontendException("Expected LogicalPlanVisitor", cce);
	}
    }

    @Override
    public boolean isEqual(Operator other) throws FrontendException {
	try {
	    LOCube cube = (LOCube) other;
	    for (Integer key : mExpressionPlans.keySet()) {
		if (!cube.mExpressionPlans.containsKey(key)) {
		    return false;
		}
		Collection<LogicalExpressionPlan> lepList1 = mExpressionPlans.get(key);
		Collection<LogicalExpressionPlan> lepList2 = cube.mExpressionPlans.get(key);

		for (LogicalExpressionPlan lep1 : lepList1) {
		    for (LogicalExpressionPlan lep2 : lepList2) {
			if (!lep1.isEqual(lep2)) {
			    return false;
			}
		    }
		}
	    }
	    return checkEquality((LogicalRelationalOperator) other);
	} catch (ClassCastException cce) {
	    throw new FrontendException("Exception while casting CUBE operator", cce);
	}
    }

    public MultiMap<Integer, LogicalExpressionPlan> getExpressionPlans() {
	return mExpressionPlans;
    }

    public void setExpressionPlans(
	    MultiMap<Integer, LogicalExpressionPlan> plans) {
	this.mExpressionPlans = plans;
    }

    @Override
    public void resetUid() {
	//TODO: implement when physical operator for CUBE is implemented
    }

    public List<Operator> getInputs(LogicalPlan plan) {
	return plan.getPredecessors(this);
    }
}
