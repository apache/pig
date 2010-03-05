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

package org.apache.pig.experimental.logical.expression;

import java.io.IOException;
import java.util.List;

import org.apache.pig.experimental.logical.relational.LogicalSchema;
import org.apache.pig.experimental.logical.relational.LogicalSchema.LogicalFieldSchema;
import org.apache.pig.experimental.plan.Operator;
import org.apache.pig.experimental.plan.OperatorPlan;
import org.apache.pig.experimental.plan.PlanVisitor;
import org.apache.pig.impl.logicalLayer.ExpressionOperator;
import org.apache.pig.impl.logicalLayer.LogicalOperator;

public class MapLookupExpression extends ColumnExpression {

    /**
     * The key to lookup along with the type and schema corresponding to the
     * type and schema of the value linked to the key
     */
    private String mMapKey;
    private LogicalFieldSchema mValueSchema;
    
    public MapLookupExpression(OperatorPlan plan, byte type, String mapKey, 
            LogicalFieldSchema valueSchema ) {
        super("Map", plan, type);
        mMapKey = mapKey;
        mValueSchema = valueSchema;
        plan.add(this);
    }
    /**
     * @link org.apache.pig.experimental.plan.Operator#accept(org.apache.pig.experimental.plan.PlanVisitor)
     */
    @Override
    public void accept(PlanVisitor v) throws IOException {
        if (!(v instanceof LogicalExpressionVisitor)) {
            throw new IOException("Expected LogicalExpressionVisitor");
        }
        ((LogicalExpressionVisitor)v).visitMapLookup(this);
    }
    
    @Override
    public boolean isEqual(Operator other) {
        if (other != null && other instanceof MapLookupExpression) {
            MapLookupExpression po = (MapLookupExpression)other;
            return ( po.mMapKey.compareTo(mMapKey) == 0 ) && 
            po.mValueSchema.isEqual( mValueSchema );
        } else {
            return false;
        }
    }
    
    public LogicalExpression getMap() throws IOException {
        List<Operator> preds = plan.getSuccessors(this);
        if(preds == null) {
            return null;
        }
        return (LogicalExpression)preds.get(0);
    }
    
    public String getLookupKey() {
        return mMapKey;
    }
    
    public LogicalFieldSchema getFieldSchema() {
        return mValueSchema;
    }

    public String toString() {
        StringBuilder msg = new StringBuilder();

        msg.append("(Name: " + name + " Type: " + type + " Uid: " + uid + " Key: " + mMapKey + " Schema: " + mValueSchema + ")");

        return msg.toString();
    }
}
