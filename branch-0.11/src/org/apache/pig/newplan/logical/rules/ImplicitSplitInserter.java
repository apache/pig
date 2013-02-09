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

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.optimizer.SchemaResetter;
import org.apache.pig.newplan.logical.optimizer.UidResetter;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.optimizer.Rule;
import org.apache.pig.newplan.optimizer.Transformer;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LogicalSchema;


/**
 * Super class for all rules that operates on the whole plan. It doesn't look for
 * a specific pattern. An example of such kind rule is ColumnPrune.
 *
 */
public class ImplicitSplitInserter extends Rule {

    public ImplicitSplitInserter(String n) {
        super(n, true);
        // Skip listener, especially, skip ProjectionPatcher so that we can keep column reference for 
        // ProjectExpression (Once ProjectionPatcher is invoked, column reference will be gone in favor of uid reference).
        // There is no need for ProjectionPatcher in this rule since we don't swap columns; however, uid conflict is not solved
        // at this moment (until after DuplicateForEachColumnRewrite), so keep column reference for now
        setSkipListener(true);
    }

    @Override
    public List<OperatorPlan> match(OperatorPlan plan) throws FrontendException {
        // Look to see if this is a non-split node with two outputs.  If so
        // it matches.
        currentPlan = plan;
        List<OperatorPlan> ll = new ArrayList<OperatorPlan>();
        Iterator<Operator> ops = plan.getOperators();
        while (ops.hasNext()) {
            Operator op = ops.next();
            if (op instanceof LOSplit || op instanceof LOStore)
                continue;
            List<Operator> succs = plan.getSuccessors(op);
            if (succs != null && succs.size() >= 2) {
                OperatorPlan match = new LogicalPlan();
                match.add(op);
                ll.add(match);
            }
        }
        return ll;
    }
    
    @Override
    public Transformer getNewTransformer() {
      return new ImplicitSplitInserterTransformer();
    }
    
    public class ImplicitSplitInserterTransformer extends Transformer {
      @Override
      public boolean check(OperatorPlan matched) throws FrontendException {
        return true;
      }
      
      @Override 
      public void transform(OperatorPlan matched) throws FrontendException {
        if (matched == null || matched instanceof LOSplit || matched instanceof LOStore
            || matched.size() != 1)
          throw new FrontendException("Invalid match in ImplicitSplitInserter rule.", 2244);

        // For two successors of op here is a pictorial
        // representation of the change required:
        // BEFORE:
        // Succ1  Succ2
        //  \       /
        //      op
        
        //  SHOULD BECOME:
        
        // AFTER:
        // Succ1          Succ2
        //   |              |
        // SplitOutput SplitOutput
        //      \       /
        //        Split
        //          |
        //          op
        
        Operator op = matched.getSources().get(0);
        List<Operator> succs = currentPlan.getSuccessors(op);
        if (succs == null || succs.size() < 2)
          throw new FrontendException("Invalid match in ImplicitSplitInserter rule.", 2243);
        LOSplit splitOp = new LOSplit(currentPlan);
        splitOp.setAlias(((LogicalRelationalOperator) op).getAlias());
        Operator[] sucs = succs.toArray(new Operator[0]);
        currentPlan.add(splitOp);
        currentPlan.connect(op, splitOp);
        for (Operator suc : sucs) {
          // position is remembered in order to maintain the order of the successors
          Pair<Integer, Integer> pos = currentPlan.disconnect(op, suc);
          LogicalExpressionPlan filterPlan = new LogicalExpressionPlan();
          new ConstantExpression(filterPlan, Boolean.valueOf(true));
          LOSplitOutput splitOutput = new LOSplitOutput((LogicalPlan) currentPlan, filterPlan);
          splitOutput.setAlias(splitOp.getAlias());
          currentPlan.add(splitOutput);
          currentPlan.connect(splitOp, splitOutput);
          currentPlan.connect(splitOutput, pos.first, suc, pos.second);
        }
        
        // Since we adjust the uid layout, clear all cached uids
        UidResetter uidResetter = new UidResetter(currentPlan);
        uidResetter.visit();

        // Manually regenerate schema since we skip listener
        SchemaResetter schemaResetter = new SchemaResetter(currentPlan, true);
        schemaResetter.visit();
      }
      
      @Override
      public OperatorPlan reportChanges() {
        return currentPlan;
      }
    }
    
    @Override
    protected OperatorPlan buildPattern() {
        return null;
    }
}
