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

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.DependencyOrderWalker;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalNodesVisitor;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

/**
 * A visitor that modifies the logical plan (if necessary) for union-onschema
 * functionality. It runs logical plan validator so that the correct schema
 * of its inputs is available. It inserts foreach statements in its input
 * if the input operator schema does not match the schema created by 
 * merging all input schemas.
 * 
 * Migrated from the old UnionOnSchemaSetter class.
 * 
 */
public class UnionOnSchemaSetter extends LogicalRelationalNodesVisitor{

    public UnionOnSchemaSetter(OperatorPlan plan)
            throws FrontendException {
        super(plan, new DependencyOrderWalker(plan));
    }

    @Override
    public void visit(LOUnion union) throws FrontendException {
        if( !union.isOnSchema() )
            return;
        
        LogicalSchema outputSchema = union.getSchema();
        int fieldCount = outputSchema.size();
        OperatorPlan plan = union.getPlan();
        List<Operator> preds = new ArrayList<Operator>();
        preds.addAll( plan.getPredecessors( union ) );

        List<LogicalSchema> fieldSchemas = new ArrayList<LogicalSchema>( fieldCount );
        for( LogicalFieldSchema fs : outputSchema.getFields() ) {
            LogicalSchema ls = new LogicalSchema();
            ls.addField( new LogicalFieldSchema( fs.alias, null, DataType.NULL ) );
            fieldSchemas.add( ls );
        }
        
        for( Operator pred : preds ) {
            LogicalRelationalOperator op = (LogicalRelationalOperator)pred;
            LogicalSchema opSchema = op.getSchema();
            if( opSchema.isEqual( outputSchema , true) )
                continue;
            
            LOForEach foreach = new LOForEach( plan );
            LogicalPlan innerPlan = new LogicalPlan();

            LOGenerate gen = new LOGenerate( innerPlan );
            boolean[] flattenFlags = new boolean[fieldCount];
            List<LogicalExpressionPlan> exprPlans = new ArrayList<LogicalExpressionPlan>( fieldCount );
            List<Operator> genInputs = new ArrayList<Operator>();
            
            // Get exprPlans, and genInputs
            for( LogicalFieldSchema fs : outputSchema.getFields() ) {
                LogicalExpressionPlan exprPlan = new LogicalExpressionPlan();
                exprPlans.add( exprPlan );
                int pos = -1;
                //do a match with subname also
                LogicalFieldSchema matchFS = opSchema.getFieldSubNameMatch(fs.alias);
                if(matchFS != null){
                    pos = opSchema.getFieldPosition(matchFS.alias);
                }
                if( pos == -1 ) {
                    ConstantExpression constExp = new ConstantExpression( exprPlan, null);
                    if(fs.type != DataType.BYTEARRAY){
                        LogicalSchema.LogicalFieldSchema constFs = fs.deepCopy();
                        constFs.resetUid();
                        new CastExpression(exprPlan, constExp, constFs);
                    }
                } else {
                    ProjectExpression projExpr = 
                        new ProjectExpression( exprPlan, genInputs.size(), 0, gen );
                    if( fs.type != DataType.BYTEARRAY
                        && opSchema.getField( pos ).type != fs.type ) {
                        new CastExpression( exprPlan, projExpr, fs );
                    }
                    genInputs.add( new LOInnerLoad( innerPlan, foreach, pos ) );
                }
            }
            
            gen.setFlattenFlags( flattenFlags );
            gen.setOutputPlans( exprPlans );
            gen.setUserDefinedSchema( fieldSchemas );
            innerPlan.add( gen );
            for( Operator input : genInputs ) {
                innerPlan.add(input);
                innerPlan.connect( input, gen );
            }
            
            foreach.setInnerPlan( innerPlan );
            foreach.setAlias(union.getAlias());
            Pair<Integer, Integer> pair = plan.disconnect( pred, union );
            plan.add( foreach );
            plan.connect( pred, pair.first, foreach, 0 );
            plan.connect( foreach, 0, union, pair.second );
        }
        
        union.setUnionOnSchema(false);
    }

}
