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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigException;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.plan.VisitorException;

/**
 * The store operator which is used in two ways:
 * 1) As a local operator it can be used to store files
 * 2) In the Map Reduce setting, it is used to create jobs
 *    from MapReduce operators which keep the loads and
 *    stores in the Map and Reduce Plans till the job is created
 *
 */
public class POStore extends PhysicalOperator {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    // The user defined load function or a default load function
    private StoreFunc storer;
    // The filespec on which the operator is based
    FileSpec sFile;
    // The stream used to bind to by the loader
    OutputStream os;
    // PigContext passed to us by the operator creator
    PigContext pc;
    
    private final Log log = LogFactory.getLog(getClass());
    
    public POStore(OperatorKey k) {
        this(k, -1, null);
    }

    public POStore(OperatorKey k, int rp) {
        this(k, rp, null);
    }
    
    public POStore(OperatorKey k, int rp, List<PhysicalOperator> inp) {
        super(k, rp, inp);
    }
    
    /**
     * Set up the storer by 
     * 1) Instantiating the store func
     * 2) Opening an output stream to the specified file and
     * 3) Binding to the output stream
     * @throws IOException
     */
    private void setUp() throws IOException{
        storer = (StoreFunc)PigContext.instantiateFuncFromSpec(sFile.getFuncSpec());
        os = FileLocalizer.create(sFile.getFileName(), pc);
        storer.bindTo(os);
    }
    
    /**
     * At the end of processing, the outputstream is closed
     * using this method
     * @throws IOException
     */
    private void tearDown() throws IOException{
        os.close();
    }
    
    /**
     * To perform cleanup when there is an error.
     * Uses the FileLocalizer method which only 
     * deletes the file but not the dirs created
     * with it.
     * @throws IOException
     */
    private void cleanUp() throws IOException{
        String fName = sFile.getFileName();
        os.flush();
        if(FileLocalizer.fileExists(fName,pc))
            FileLocalizer.delete(fName,pc);
    }
    
    /**
     * The main method used by the local execution engine
     * to store tuples into the specified file using the
     * specified store function. One call to this method
     * retrieves all tuples from its predecessor operator
     * and stores it into the file till it recieves an EOP.
     * 
     * If there is an error, the cleanUp routine is called
     * and then the tearDown is called to close the OutputStream
     * 
     * @return Whatever the predecessor returns
     *          A null from the predecessor is ignored
     *          and processing of further tuples continued
     */
    public Result store() throws ExecException{
        try{
            setUp();
        }catch (IOException ioe) {
            int errCode = 2081;
            String msg = "Unable to setup the store function.";            
            throw new ExecException(msg, errCode, PigException.BUG, ioe);
        }
        try{
            Result res;
            Tuple inpValue = null;
            while(true){
                res = processInput();
                if(res.returnStatus==POStatus.STATUS_OK)
                    storer.putNext((Tuple)res.result);
                else if(res.returnStatus==POStatus.STATUS_NULL)
                    continue;
                else
                    break;
            }
            if(res.returnStatus==POStatus.STATUS_EOP){
                storer.finish();
            }
            else{
                cleanUp();
            }
            tearDown();
            return res;
        }catch(IOException e){
            log.error("Received error from storer function: " + e);
            return new Result();
        }
    }

    @Override
    public String name() {
        if(sFile!=null)
            return "Store" + "(" + sFile.toString() + ")" + " - " + mKey.toString();
        else
            return "Store" + "(" + "DummyFil:DummyLdr" + ")" + " - " + mKey.toString();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return false;
    }

    public StoreFunc getStorer() {
        return storer;
    }

    

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitStore(this);
    }

    public FileSpec getSFile() {
        return sFile;
    }

    public void setSFile(FileSpec file) {
        sFile = file;
    }

    public PigContext getPc() {
        return pc;
    }

    public void setPc(PigContext pc) {
        this.pc = pc;
    }

}
