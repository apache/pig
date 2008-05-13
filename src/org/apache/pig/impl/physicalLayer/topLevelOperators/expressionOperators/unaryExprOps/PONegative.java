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
package org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.unaryExprOps;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.physicalLayer.POStatus;
import org.apache.pig.impl.physicalLayer.Result;
import org.apache.pig.impl.physicalLayer.plans.ExprPlanVisitor;
import org.apache.pig.impl.physicalLayer.topLevelOperators.expressionOperators.ExpressionOperator;
import org.apache.pig.impl.plan.VisitorException;

public class PONegative extends UnaryExpressionOperator {

	public PONegative(OperatorKey k, int rp) {
		super(k, rp);
		
	}

	public PONegative(OperatorKey k) {
		super(k);
		
	}
	
	public PONegative(OperatorKey k, int rp, ExpressionOperator input) {
		super(k, rp);
		this.input = input;
	}

	@Override
	public void visit(ExprPlanVisitor v) throws VisitorException {
		v.visitNegative(this);
	}

	@Override
	public String name() {
		// TODO Auto-generated method stub
		return "PONegative - " + mKey.toString();
	}

	@Override
	public Result getNext(Double d) throws ExecException {
		Result res = input.getNext(d);
		if(res.returnStatus == POStatus.STATUS_OK) {
			res.result = -1*((Double)res.result);
		}
		return res;
	}

	@Override
	public Result getNext(Float f) throws ExecException {
		Result res = input.getNext(f);
		if(res.returnStatus == POStatus.STATUS_OK) {
			res.result = -1*((Float)res.result);
		}
		return res;
	}

	@Override
	public Result getNext(Integer i) throws ExecException {
		Result res = input.getNext(i);
		if(res.returnStatus == POStatus.STATUS_OK) {
			res.result = -1*((Integer)res.result);
		}
		return res;
	}

	@Override
	public Result getNext(Long l) throws ExecException {
		Result res = input.getNext(l);
		if(res.returnStatus == POStatus.STATUS_OK) {
			res.result = -1*((Long)res.result);
		}
		return res;
	}
	
	public void setInput(ExpressionOperator in) {
		this.input = in;
	}
	
	

}
