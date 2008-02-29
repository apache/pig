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
package org.apache.pig.backend.local.executionengine;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.PigFile;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.OperatorKey;
import org.apache.pig.impl.physicalLayer.PhysicalOperator;
import org.apache.pig.impl.physicalLayer.POVisitor;


public class POStore extends PhysicalOperator {
    private static final long serialVersionUID = 1L;
    
    private final Log log = LogFactory.getLog(getClass());
    
    private PigFile f;
    private String funcSpec;
    boolean append = false;
    PigContext pigContext;
    FileSpec outFileSpec;
    OperatorKey logicalKey;
    Map<OperatorKey, LocalResult> materializedResults;

    public POStore(String scope, 
                   long id, 
                   Map<OperatorKey, ExecPhysicalOperator> opTable,
                   OperatorKey logicalKey,
                   Map<OperatorKey, LocalResult> materializedResults,
                   OperatorKey input, 
                   FileSpec outputFileSpec, 
                   boolean append, 
                   PigContext pigContext) {
        super(scope, id, opTable, LogicalOperator.FIXED);
        funcSpec = outputFileSpec.getFuncSpec();
        inputs = new OperatorKey[1];
        inputs[0] = input;
        System.out.println("Creating " + outputFileSpec.getFileName());
        f = new PigFile(outputFileSpec.getFileName(), append);
        this.append = append;
        this.pigContext = pigContext;
        this.outFileSpec = outputFileSpec;
        this.logicalKey = logicalKey;
        this.materializedResults = materializedResults;
    }

    public POStore(String scope, 
                   long id, 
                   Map<OperatorKey, ExecPhysicalOperator> opTable,
                   OperatorKey logicalKey,
                   Map<OperatorKey, LocalResult> materializedResults,
                   FileSpec outputFileSpec, 
                   boolean append, 
                   PigContext pigContext) {
        super(scope, id, opTable, LogicalOperator.FIXED);
        inputs = new OperatorKey[1];
        inputs[0] = null;
        funcSpec = outputFileSpec.getFuncSpec();
        f = new PigFile(outputFileSpec.getFileName(), append);
        this.append = append;
        this.pigContext = pigContext;
        this.outFileSpec = outputFileSpec;
        this.logicalKey = logicalKey;
        this.materializedResults = materializedResults;
    }

    @Override
    public Tuple getNext() throws IOException {
        // get all tuples from input, and store them.
        DataBag b = BagFactory.getInstance().newDefaultBag();
        Tuple t;
        while ((t = (Tuple) ((PhysicalOperator)opTable.get(inputs[0])).getNext()) != null) {
            b.add(t);
        }
        try {
            StoreFunc func = (StoreFunc) PigContext.instantiateFuncFromSpec(funcSpec);
            f.store(b, func, pigContext);
            
            // a result has materialized, track it!
            LocalResult materializedResult = new LocalResult(this.outFileSpec);

            materializedResults.put(logicalKey, materializedResult);
        } catch(IOException e) {
            throw e;
        } catch(RuntimeException e) {
            throw e;
        } catch(Exception e) {
            IOException ne = new IOException(e.getClass().getName() + ": " + e.getMessage());
            ne.setStackTrace(e.getStackTrace());
            throw ne;
        }

        return null;
    }
    
    @Override
    public int getOutputType(){
        log.error("No one should be asking my output type");
        RuntimeException runtimeException = new RuntimeException();
        log.error(runtimeException);
        throw runtimeException;
    }

    @Override
    public void visit(POVisitor v) {
        v.visitStore(this);
    }

}
