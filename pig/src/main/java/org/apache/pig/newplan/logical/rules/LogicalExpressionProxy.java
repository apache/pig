/**
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
package org.apache.pig.newplan.logical.rules;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.expression.*;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.rules.LogicalExpressionSimplifier.LogicalExpressionSimplifierTransformer;

/**
 * A proxy for logical expression. Useful if the original plan is not
 * touched while a side plan with the nodes associated with the
 * original is worked on.
 * 
 *
 */
class LogicalExpressionProxy extends LogicalExpression {
    LogicalExpression src;

    LogicalExpressionProxy(OperatorPlan plan, LogicalExpression src) {
        super("Proxy: " + src.getName(), plan);
        this.src = src;
        LogicalExpressionSimplifierTransformer.incrDNFSplitCount(src);
    }

    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        throw new FrontendException("Visitor not accepted by proxy.");
    }

    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (other != null && other instanceof LogicalExpressionProxy) {
            return src.isEqual(((LogicalExpressionProxy) other).src);
        }
        else {
            return false;
        }
    }

    @Override
    public LogicalSchema.LogicalFieldSchema getFieldSchema()
                    throws FrontendException {
        return src.getFieldSchema();
    }

    public void decrSrcDNFSplitCounter() {
      LogicalExpressionSimplifierTransformer.decrDNFSplitCount(src);
        if (LogicalExpressionSimplifierTransformer.getSplitCount(src) == 1)
          LogicalExpressionSimplifierTransformer.decrDNFSplitCount(src);
    }

    public void restoreSrc() {
        while (LogicalExpressionSimplifierTransformer.getSplitCount(src) > 1)
          LogicalExpressionSimplifierTransformer.decrDNFSplitCount(src);
    }
    
    @Override
    public LogicalExpression deepCopy(LogicalExpressionPlan lgExpPlan) throws FrontendException {
        throw new FrontendException("Deepcopy not expected");
    }
}
