/**
 * 
 */
package org.apache.pig.impl.logicalLayer;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.streaming.ExecutableManager;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.streaming.StreamingCommand.Handle;
import org.apache.pig.impl.streaming.StreamingCommand.HandleSpec;

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
    private StreamingCommand command;
    private ExecutableManager executableManager;
    /**
     * Create a new <code>LOStream</code> with the given command.
     * 
     * @param plan the logical plan this operator is a part of
     * @param k the operator key for this operator
     * @param input operator that is input to this command
     * @param exeManager ExecutableManager used by this streaming command.
     * @param cmd StreamingCommand for this stream to run.
     */
    public LOStream(LogicalPlan plan, OperatorKey k, LogicalOperator input, ExecutableManager exeManager, StreamingCommand cmd) {
        super(plan, k);
        //this.input = input;
        this.command = cmd;
        this.executableManager = exeManager;
    }
    
    /**
     * Get the StreamingCommand object associated
     * with this operator
     * 
     * @return the StreamingCommand object
     */
    public StreamingCommand getStreamingCommand() {
        return command;
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
    
    /**
     * Set the optimized {@link HandleSpec} for the given {@link Handle} of the 
     * <code>StreamSpec</code>.
     * 
     * @param handle <code>Handle</code> to optimize
     * @param spec optimized specification for the handle
     */ 
    public void setOptimizedSpec(Handle handle, String spec) {

        // The reason we need to clone and optimize the clone is the following:
        // consider a script like this:
        // define CMD1 `perl -ne 'print $_;print STDERR "stderr $_";'`;
        // define CMD2 `cat`;
        // A = load 'bla' split by 'file';
        // B = stream A through CMD1;
        // C = stream B through CMD1;
        // D = stream C through CMD2;
        // store D into 'bla';
        // In this case CMD1 is represented by a single StreamingCommand Object
        // which will be present as the "command" member in both the
        // LOStream operators corresponding to B and C. However we want to
        // optimize only B's input spec since it is immediately following a store
        // and is conducive to optimization. At this point we clone and make
        // sure only B's "command" gets optimized while C's "command" remains
        // untouched.

        StreamingCommand optimizedCommand = (StreamingCommand)command.clone();
        
        if (handle == Handle.INPUT) {
            HandleSpec streamInputSpec = optimizedCommand.getInputSpec();
            streamInputSpec.setSpec(spec);
        } else if (handle == Handle.OUTPUT) {
            HandleSpec streamOutputSpec = optimizedCommand.getOutputSpec();
            streamOutputSpec.setSpec(spec);
        }
        
        command = optimizedCommand;
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
        return "Stream (" + command.toString() + ") " + mKey.scope + "-" + mKey.id;
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
