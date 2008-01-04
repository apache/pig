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
package org.apache.pig.impl.physicalLayer;

import java.io.IOException;
import java.util.Map;

import org.apache.pig.StoreFunc;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.PigFile;
import org.apache.pig.impl.logicalLayer.LOSplit;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.mapreduceExec.MapReduceLauncher;


public class IntermedResult {
    public LogicalPlan  lp          = null;
    public PhysicalPlan pp          = null;
    public DataBag      databag     = null;
    public FileSpec outputFileSpec  = null;
    public String[]     splitOutputs= null;
    boolean             isTemporary = true;
    private boolean     compiled    = false;
    private boolean     executed    = false;
    
    private PigContext pigContext = null;

    public IntermedResult(LogicalPlan lpIn, PigContext pigContext) {
        lp = lpIn;
        if (lp.getRoot() instanceof LOSplit){
        	LOSplit splitOp = (LOSplit)lp.getRoot();
        	splitOutputs = new String[splitOp.getConditions().size()];
        }
        this.pigContext = pigContext;
    }
    
    public IntermedResult() {
        executed = true;
        databag = BagFactory.getInstance().newDefaultBag();
    }
    
    public IntermedResult(DataBag bag) {
        executed = true;
        databag = bag;
    }
    
    public void add(Tuple t) throws IOException {
        if (databag != null) databag.add(t);
        else throw new IOException("Attempt to add tuple to derived relation.");
    }
    
    public void toDFSFile(FileSpec fileSpec, PigContext pigContext) throws IOException {
        if (databag == null) throw new IOException("Internal error: No data to store to file.");

        outputFileSpec = fileSpec;
        
        StoreFunc sf;
        try{
        	sf = (StoreFunc) PigContext.instantiateFuncFromSpec(fileSpec.getFuncSpec());
        }catch (Exception e){
        	throw new IOException(e.getMessage());
        }
                
        PigFile f = new PigFile(outputFileSpec.getFileName(), false);
        f.store(databag, sf, pigContext);
        
        databag = null;
        
        isTemporary = false;
    }
    
    public void compile(Map queryResults) throws IOException {
        pp = new PhysicalPlan(lp, queryResults);
        compiled = true;
    }

    public void exec() throws IOException {
        if (pp == null) {
            throw new IOException("Attempt to execute an uncompiled or already-executed query.");
        } else {
            switch(lp.getPigContext().getExecType()) {
            case MAPREDUCE:
                POMapreduce pom = (POMapreduce) pp.root;

                MapReduceLauncher.initQueryStatus(pom.numMRJobs());  // initialize status, for bookkeeping purposes.
                
                // if the final operator is a MapReduce with no output file, then send to a temp
                // file.
                if (pom.outputFileSpec==null) {
                    pom.outputFileSpec = new FileSpec(PlanCompiler.getTempFile(pigContext),BinStorage.class.getName());
                }

                // remember the name of the file to which the final mapreduce operator writes
                outputFileSpec = pom.outputFileSpec;
                break;
            }

            // xecute the plan
            DataBag results = pp.exec(false);

            if (lp.getPigContext().getExecType() == ExecType.LOCAL) {
                databag = results;
            }

            executed = true;
        }
    }

    public DataBag read() throws IOException {

        if (databag == null) {
            if (outputFileSpec == null)
                throw new IOException("Unable to read result.");
            
            databag = new PhysicalPlan(new POLoad(pigContext,outputFileSpec, getOutputType())).exec(false); // load from temp
        }

        return databag;
    }

    public boolean compiled() {
        return compiled;
    }

    public boolean executed() {
        return executed;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    public void setPermanentFileSpec(FileSpec fileSpec) {
        this.outputFileSpec = fileSpec;
        isTemporary = false;
    }
    
    public int getOutputType(){
        if (lp == null) return LogicalOperator.FIXED;
        else return lp.getOutputType();
    }
}
