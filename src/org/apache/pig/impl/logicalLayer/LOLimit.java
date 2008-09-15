package org.apache.pig.impl.logicalLayer;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.parser.QueryParser;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

public class LOLimit extends LogicalOperator {
    private static final long serialVersionUID = 2L;
    private long mLimit;
    /**
     * 
     * @param plan
     *            Logical plan this operator is a part of.
     * @param k
     *            Operator key to assign to this node.
     * @param limit
     *            Number of limited outputs
     */

    public LOLimit(LogicalPlan plan, OperatorKey k, long limit) {
        super(plan, k);
        mLimit = limit;
    }

    public LogicalOperator getInput() {
        return mPlan.getPredecessors(this).get(0);
    }

    public long getLimit() {
        return mLimit;
    }

    public void setLimit(long limit) {
    	mLimit = limit;
    }
    @Override
    public Schema getSchema() throws FrontendException {
        if (!mIsSchemaComputed) {
            try {
                mSchema = getInput().getSchema();
                mIsSchemaComputed = true;
            } catch (FrontendException ioe) {
                mSchema = null;
                mIsSchemaComputed = false;
                throw ioe;
            }
        }
        return mSchema;
    }

    @Override
    public String name() {
        return "Limit (" + mLimit + ") " + mKey.scope + "-" + mKey.id;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    @Override
    public byte getType() {
        return DataType.BAG ;
    }
    
    // Shouldn't this be clone?
    public LOLimit duplicate()
    {
    	return new LOLimit(mPlan, OperatorKey.genOpKey(mKey.scope), mLimit);
    }
}
