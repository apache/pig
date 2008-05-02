package org.apache.pig.impl.logicalLayer.validators;

import java.io.IOException;

import org.apache.pig.ExecType; 
import org.apache.pig.impl.PigContext ;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.LOLoad;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LOVisitor;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.backend.datastorage.ElementDescriptor;

/***
 * Visitor for checking input/output files
 * Exceptions in here do not affect later operations
 * so we don't throw any exception but log all of 
 * them in msgCollector.
 * 
 * We assume input/output files can exist only in the top level plan.
 *
 */
public class InputOutputFileVisitor extends LOVisitor {
    
    private PigContext pigCtx = null ;
    private CompilationMessageCollector msgCollector = null ;
    
    public InputOutputFileVisitor(LogicalPlan plan,
                                CompilationMessageCollector messageCollector,
                                PigContext pigContext) {
        super(plan, new DepthFirstWalker<LogicalOperator, LogicalPlan>(plan));
        pigCtx = pigContext ;
        msgCollector = messageCollector ;
    }
   
    /***
     * The logic here is just to check that the file(s) exist
     */
    @Override
    protected void visit(LOLoad load) {
        // make sure that the file exists
        String filename = load.getInputFile().getFileName() ;
        
        try {
            if (!checkFileExists(filename)) {
                msgCollector.collect("The input file(s): " + filename 
                                     + " doesn't exist",
                                     MessageType.Error) ;
            }
        } 
        catch (IOException ioe) {
            msgCollector.collect("Cannot read from the storage where the input " 
                                 + filename + " stay ",
                                 MessageType.Error) ;
        }
    }
    
    /***
     * The logic here is just to check that the file(s) do not exist
     */
    @Override
    protected void visit(LOStore store) {
        // make sure that the file doesn't exist
        String filename = store.getOutputFile().getFileName() ;
        
        try {
            if (checkFileExists(filename)) {
                msgCollector.collect("The output file(s): " + filename 
                                     + " already exists", 
                                     MessageType.Error) ;
            }
        } 
        catch (IOException ioe) {
            msgCollector.collect("Cannot read from the storage where the output " 
                    + filename + " will be stored ",
                    MessageType.Error) ;
        }
    }

    /***
     * Check if the file(s) exist. There are two cases :-
     * 1) Exact match
     * 2) Globbing match
     * TODO: Add globbing support in local execution engine 
     * and then make this check for local FS support too
     */
    private boolean checkFileExists(String filename) throws IOException {
        if (pigCtx.getExecType() == ExecType.LOCAL) {
            ElementDescriptor elem = pigCtx.getLfs().asElement(filename) ;
            return elem.exists() ;
        }
        else if (pigCtx.getExecType() == ExecType.MAPREDUCE) {
            // TODO: Have to put the staging from local to HDFS somewhere else
            // This does actual file check + glob check
            return FileLocalizer.fileExists(filename, pigCtx) ;
        }
        else { // if ExecType is something else) 
            throw new RuntimeException("Undefined state in " + this.getClass()) ;
        }
    }
    
}
