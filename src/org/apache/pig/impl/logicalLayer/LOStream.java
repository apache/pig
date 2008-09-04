/**
 * 
 */
package org.apache.pig.impl.logicalLayer;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.streaming.ExecutableManager;
import org.apache.pig.impl.streaming.StreamingCommand;

/**
 * {@link LOStream} represents the specification of an external
 * command to be executed in a Pig Query. 
 * 
 * <code>LOStream</code> encapsulates all relevant details of the
 * command specified by the user either directly via the <code>STREAM</code>
 * operator or indirectly via a <code>DEFINE</code> operator. It includes
 * details such as input/output/error specifications and also files to be
 * shipped to the cluster and files to be cached.
 */
public class LOStream extends LogicalOperator {

    /**
     * 
     */
    private static final long serialVersionUID = 2L;
    // the StreamingCommand object for the
    // Stream Operator this operator represents
    private StreamingCommand StrCmd;
    //private LogicalOperator input;
    private ExecutableManager executableManager;
    /**
     * Create a new <code>LOStream</code> with the given command.
     * 
     * @param plan the logical plan this operator is a part of
     * @param k the operator key for this operator
     * @param pigContext the pigContext object
     * @param argv parsed arguments of the <code>command</code>
     */
    public LOStream(LogicalPlan plan, OperatorKey k, LogicalOperator input, ExecutableManager exeManager, StreamingCommand cmd) {
        super(plan, k);
        //this.input = input;
        this.StrCmd = cmd;
        this.executableManager = exeManager;
    }
    
    /**
     * Get the StreamingCommand object associated
     * with this operator
     * 
     * @return the StreamingCommand object
     */
    public StreamingCommand getStreamingCommand() {
        return StrCmd;
    }
    
    /* (non-Javadoc)
     * @see org.apache.pig.impl.logicalLayer.LogicalOperator#getSchema()
     */
    @Override
    public Schema getSchema() throws FrontendException {
        return mSchema;
        /*
        if (!mIsSchemaComputed) {
            /*
            LogicalOperator input = mPlan.getPredecessors(this).get(0);
            ArrayList<Schema.FieldSchema> fss = new ArrayList<Schema.FieldSchema>();
            try {
                mSchema = input.getSchema();
                mIsSchemaComputed = true;
            } catch (FrontendException ioe) {
                mSchema = null;
                mIsSchemaComputed = false;
                throw ioe;
            }
        }
        return mSchema;
        */

    }

    /* (non-Javadoc)
     * @see org.apache.pig.impl.logicalLayer.LogicalOperator#visit(org.apache.pig.impl.logicalLayer.LOVisitor)
     */
    @Override
    public void visit(LOVisitor v) throws VisitorException {
        v.visit(this);
    }

    /* (non-Javadoc)
     * @see org.apache.pig.impl.plan.Operator#name()
     */
    @Override
    public String name() {
        return "Stream (" + StrCmd.toString() + ") " + mKey.scope + "-" + mKey.id;
    }

    /* (non-Javadoc)
     * @see org.apache.pig.impl.plan.Operator#supportsMultipleInputs()
     */
    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    /**
     * @return the ExecutableManager
     */
    public ExecutableManager getExecutableManager() {
        return executableManager;
    }

}
