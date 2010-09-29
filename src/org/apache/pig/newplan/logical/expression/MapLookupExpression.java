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

package org.apache.pig.newplan.logical.expression;

import java.util.List;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanVisitor;
import org.apache.pig.newplan.logical.relational.LogicalSchema;
import org.apache.pig.newplan.logical.relational.LogicalSchema.LogicalFieldSchema;

public class MapLookupExpression extends ColumnExpression {

    /**
     * The key to lookup along with the type and schema corresponding to the
     * type and schema of the value linked to the key
     */
    private String mMapKey;
    private LogicalFieldSchema mValueSchema;
    
    public MapLookupExpression(OperatorPlan plan, String mapKey, 
            LogicalFieldSchema valueSchema ) {
        super("Map", plan);
        mMapKey = mapKey;
        mValueSchema = valueSchema;
        plan.add(this);
    }
    /**
     * @link org.apache.pig.newplan.Operator#accept(org.apache.pig.newplan.PlanVisitor)
     */
    @Override
    public void accept(PlanVisitor v) throws FrontendException {
        if (!(v instanceof LogicalExpressionVisitor)) {
            throw new FrontendException("Expected LogicalExpressionVisitor", 2222);
        }
        ((LogicalExpressionVisitor)v).visit(this);
    }
    
    @Override
    public boolean isEqual(Operator other) throws FrontendException {
        if (other != null && other instanceof MapLookupExpression) {
            MapLookupExpression po = (MapLookupExpression)other;
            return ( po.mMapKey.compareTo(mMapKey) == 0 ) && 
            po.mValueSchema.isEqual( mValueSchema );
        } else {
            return false;
        }
    }
    
    public LogicalExpression getMap() throws FrontendException {
        List<Operator> preds = plan.getSuccessors(this);
        if(preds == null) {
            return null;
        }
        return (LogicalExpression)preds.get(0);
    }
    
    public String getLookupKey() {
        return mMapKey;
    }
    
    public LogicalFieldSchema getFieldSchema() throws FrontendException {
        if (fieldSchema!=null)
            return fieldSchema;
        if (mValueSchema!=null)
            fieldSchema = mValueSchema;
        else
            fieldSchema = new LogicalSchema.LogicalFieldSchema(null, null, DataType.UNKNOWN);
        uidOnlyFieldSchema = fieldSchema.mergeUid(uidOnlyFieldSchema);
        return fieldSchema;
    }

    public String toString() {
        StringBuilder msg = new StringBuilder();
        msg.append("(Name: " + name + " Type: ");
        if (fieldSchema!=null)
            msg.append(DataType.findTypeName(fieldSchema.type));
        else
            msg.append("null");
        msg.append(" Uid: ");
        if (fieldSchema!=null)
            msg.append(fieldSchema.uid);
        else
            msg.append("null");
        msg.append(" Key: " + mMapKey);
        msg.append(")");

        return msg.toString();
    }

    @Override
    public LogicalExpression deepCopy(LogicalExpressionPlan lgExpPlan) throws FrontendException {
        LogicalExpression copy = null;; 
        copy = new MapLookupExpression(
                lgExpPlan,
                this.getLookupKey(),
                this.getFieldSchema().deepCopy());
        
        // Only one input is expected.
        LogicalExpression input = (LogicalExpression) plan.getSuccessors( this ).get( 0 );
        LogicalExpression inputCopy = input.deepCopy( lgExpPlan );
        lgExpPlan.add( inputCopy );
        lgExpPlan.connect( copy, inputCopy );
        
        return copy;
    }

}
