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

package org.apache.pig.newplan.logical.visitor;

import java.util.HashSet;
import java.util.Set;

import org.apache.pig.PigException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.plan.PlanValidationException;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LONative;
import org.apache.pig.newplan.logical.relational.LORank;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

public class SchemaAliasVisitor extends LogicalRelationalNodesVisitor {

    public SchemaAliasVisitor(OperatorPlan plan) throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
    }

    /***
     * The logic here is to check if we have duplicate alias in each schema
     * @throws FrontendException
     */
    protected void validate(LogicalRelationalOperator op) throws FrontendException {
        LogicalSchema schema = op.getSchema();

        Set<String> seenAliases = new HashSet<String>();
        if( schema != null){
            for( int i = 0; i < schema.size(); i++){
                if( schema.getField(i) != null &&
                        schema.getField(i).alias != null
                ){
                    String alias = schema.getField(i).alias;
                    if(seenAliases.contains(alias)){
                        int errCode = 1108;
                        String msg = "Duplicate schema alias: " + schema.getField( i ).alias;
                        if( op.getAlias() != null )
                            msg = msg + " in \"" + op.getAlias() + "\"";
                        throw new PlanValidationException( op, msg, errCode, PigException.INPUT );
                    }
                    seenAliases.add(alias);
                }
            }
        }
    }

    @Override
    public void visit(LOStore store) throws FrontendException {
        validate( store );
    }

    @Override
    public void visit(LOFilter filter) throws FrontendException {
        validate( filter );
    }

    @Override
    public void visit(LOJoin join) throws FrontendException {
        validate( join );
    }

    @Override
    public void visit(LOForEach foreach) throws FrontendException {
        new SchemaAliasVisitor( foreach.getInnerPlan() ).visit();
    }

    @Override
    public void visit(LOGenerate gen) throws FrontendException {
        validate( gen );
    }

    @Override
    public void visit(LOInnerLoad load) throws FrontendException {
        validate( load );
    }

    @Override
    public void visit(LOCogroup group) throws FrontendException {
        validate( group );
    }

    @Override
    public void visit(LOSplit split) throws FrontendException {
        validate( split );
    }

    @Override
    public void visit(LOSplitOutput splitOutput) throws FrontendException {
        validate( splitOutput );
    }

    @Override
    public void visit(LOUnion union) throws FrontendException {
        validate( union );
    }

    @Override
    public void visit(LOSort sort) throws FrontendException {
        validate( sort );
    }

    @Override
    public void visit(LORank rank) throws FrontendException {
        validate( rank );
    }

    @Override
    public void visit(LODistinct distinct) throws FrontendException {
        validate( distinct );
    }

    @Override
    public void visit(LOLimit limit) throws FrontendException {
        validate( limit );
    }

    @Override
    public void visit(LOCross cross) throws FrontendException {
        validate( cross );
    }

    @Override
    public void visit(LOStream stream) throws FrontendException {
        validate( stream );
    }

    @Override
    public void visit(LONative nativeMR) throws FrontendException {
        validate( nativeMR );
    }

}
