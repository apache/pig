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
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.pen.util.ExampleTuple;

/**
 * The load operator which is used in two ways:
 * 1) As a local operator it can be used to load files
 * 2) In the Map Reduce setting, it is used to create jobs
 *    from MapReduce operators which keep the loads and
 *    stores in the Map and Reduce Plans till the job is created
 *
 */
public class POLoad extends PhysicalOperator {
    /**
     * 
     */
//    private static final long serialVersionUID = 1L;
    // The user defined load function or a default load function
    LoadFunc loader = null;
    // The filespec on which the operator is based
    FileSpec lFile;
    // The stream used to bind to by the loader
    InputStream is;
    // PigContext passed to us by the operator creator
    PigContext pc;
    //Indicates whether the loader setup is done or not
    boolean setUpDone = false;
    // Indicates whether the filespec is splittable
    boolean splittable = true;
    
    private final Log log = LogFactory.getLog(getClass());
    
    public POLoad(OperatorKey k, boolean splittable) {
        this(k,-1, null, splittable);
    }

    
    public POLoad(OperatorKey k, FileSpec lFile, boolean splittable){
        this(k,-1,lFile, splittable);
    }
    
    public POLoad(OperatorKey k, int rp, FileSpec lFile,boolean splittable) {
        super(k, rp);
        this.lFile = lFile;
        this.splittable = splittable;
    }
    
    /**
     * Set up the loader by 
     * 1) Instantiating the load func
     * 2) Opening an input stream to the specified file and
     * 3) Binding to the input stream
     * @throws IOException
     */
    public void setUp() throws IOException{
        String filename = lFile.getFileName();
        loader = (LoadFunc)PigContext.instantiateFuncFromSpec(lFile.getFuncSpec());
        
        is = FileLocalizer.open(filename, pc);
        
        loader.bindTo(filename , new BufferedPositionedInputStream(is), 0, Long.MAX_VALUE);
    }
    
    /**
     * At the end of processing, the inputstream is closed
     * using this method
     * @throws IOException
     */
    public void tearDown() throws IOException{
        is.close();
        setUpDone = false;
    }
    
    /**
     * The main method used by this operator's successor
     * to read tuples from the specified file using the
     * specified load function.
     * 
     * @return Whatever the loader returns
     *          A null from the loader is indicative
     *          of EOP and hence the tearDown of connection
     */
    @Override
    public Result getNext(Tuple t) throws ExecException {
        if(!setUpDone && lFile!=null){
            try {
                setUp();
            } catch (IOException ioe) {
                int errCode = 2081;
                String msg = "Unable to setup the load function.";
                throw new ExecException(msg, errCode, PigException.BUG, ioe);
            }
            setUpDone = true;
        }
        Result res = new Result();
        try {
            res.result = loader.getNext();
            if(res.result==null){
                res.returnStatus = POStatus.STATUS_EOP;
                tearDown();
            }
            else
                res.returnStatus = POStatus.STATUS_OK;
            if(lineageTracer != null) {
        	ExampleTuple tOut = new ExampleTuple((Tuple) res.result);
        	res.result = tOut;
            }
        } catch (IOException e) {
            log.error("Received error from loader function: " + e);
            return res;
        }
        return res;
    }

    @Override
    public String name() {
        if(lFile!=null)
            return "Load" + "(" + lFile.toString() + ")" + " - " + mKey.toString();
        else
            return "Load" + "(" + "DummyFil:DummyLdr" + ")" + " - " + mKey.toString();
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
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitLoad(this);
    }


    public FileSpec getLFile() {
        return lFile;
    }


    public void setLFile(FileSpec file) {
        lFile = file;
    }


    public PigContext getPc() {
        return pc;
    }


    public void setPc(PigContext pc) {
        this.pc = pc;
    }


    /**
     * @return the splittable
     */
    public boolean isSplittable() {
        return splittable;
    }

}
