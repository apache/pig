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
package org.apache.pig.impl.logicalLayer.validators;

import org.apache.pig.PigException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LOCogroup;
import org.apache.pig.impl.logicalLayer.LOCross;
import org.apache.pig.impl.logicalLayer.LODistinct;
import org.apache.pig.impl.logicalLayer.LOFilter;
import org.apache.pig.impl.logicalLayer.LOForEach;
import org.apache.pig.impl.logicalLayer.LOJoin;
import org.apache.pig.impl.logicalLayer.LOLimit;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOSort;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOSplitOutput;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOStream;
import org.apache.pig.impl.logicalLayer.LOUnion;
import org.apache.pig.impl.logicalLayer.LOVisitor;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.PlanValidationException;

public class SchemaAliasVisitor extends LOVisitor {

    public SchemaAliasVisitor(LogicalPlan plan) {
        super(plan, new DepthFirstWalker<LogicalOperator, LogicalPlan>(plan));
    }

    /***
     * The logic here is to check if we have duplicate alias in each schema
     */
    protected void validate(LogicalOperator lo) throws PlanValidationException {
        try {
            Schema schema = lo.getSchema();
            if (schema != null) {
                for (int i = 0; i < schema.size(); i++)
                    for (int j = i + 1; j < schema.size(); j++) {
                        if (schema.getField(i) != null
                                && schema.getField(j) != null
                                && schema.getField(i).alias != null 
                                && schema.getField(j).alias != null
                                && schema.getField(i).alias.equals(schema
                                        .getField(j).alias)) {
                            int errCode = 1108;
                            String msg = "Duplicate schema alias: "
                                    + schema.getField(i).alias;
                            if (lo.getAlias()!=null)
                                msg = msg + " in \"" + lo.getAlias()+"\"";
                            throw new PlanValidationException(msg, errCode,
                                    PigException.INPUT);
                        }
                    }
            }
        } catch (PlanValidationException e) {
            throw e;
        } catch (FrontendException e) {
            int errCode = 2201;
            String msg = "Could not validate schema alias";
            throw new PlanValidationException(msg, errCode, PigException.INPUT);
        }
    }

    @Override
    protected void visit(LOLoad load) throws PlanValidationException {
        validate(load);
    }

    @Override
    protected void visit(LOCogroup cogroup) throws PlanValidationException {
        validate(cogroup);
    }

    @Override
    protected void visit(LOCross cross) throws PlanValidationException {
        validate(cross);
    }

    @Override
    protected void visit(LODistinct distinct) throws PlanValidationException {
        validate(distinct);
    }

    @Override
    protected void visit(LOFilter filter) throws PlanValidationException {
        validate(filter);
    }

    @Override
    protected void visit(LOForEach foreach) throws PlanValidationException {
        validate(foreach);
    }

    @Override
    protected void visit(LOJoin join) throws PlanValidationException {
        validate(join);
    }

    @Override
    protected void visit(LOLimit limit) throws PlanValidationException {
        validate(limit);
    }

    @Override
    protected void visit(LOSort sort) throws PlanValidationException {
        validate(sort);
    }

    @Override
    protected void visit(LOSplit split) throws PlanValidationException {
        validate(split);
    }

    @Override
    protected void visit(LOSplitOutput splitoutput)
            throws PlanValidationException {
        validate(splitoutput);
    }

    @Override
    protected void visit(LOStore store) throws PlanValidationException {
        validate(store);
        return;
    }

    @Override
    protected void visit(LOStream stream) throws PlanValidationException {
        validate(stream);
        return;
    }

    @Override
    protected void visit(LOUnion union) throws PlanValidationException {
        validate(union);
        return;
    }
}
