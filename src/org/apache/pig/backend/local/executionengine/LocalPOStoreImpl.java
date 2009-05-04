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
import java.io.OutputStream;

import org.apache.pig.StoreFunc;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStoreImpl;

/**
 * This class implements the behavior of a POStore in the local
 * execution engine. It creates and manages the store function and the
 * output stream of the store.
 */
public class LocalPOStoreImpl extends POStoreImpl {

    private OutputStream os;
    private PigContext pc;
    private StoreFunc storer;
    private FileSpec sFile;
    
    public LocalPOStoreImpl(PigContext pc) {
        this.pc = pc;
    }

    @Override
    public StoreFunc createStoreFunc(FileSpec sFile, Schema schema) 
        throws IOException {
        this.sFile = sFile;
        storer = (StoreFunc)PigContext.instantiateFuncFromSpec(sFile.getFuncSpec());
        os = FileLocalizer.create(sFile.getFileName(), pc);
        storer.bindTo(os);
        return storer;
    }

    @Override
    public void tearDown() throws IOException{
        storer.finish();
        os.close();
    }

    @Override
    public void cleanUp() throws IOException{
        String fName = sFile.getFileName();
        os.flush();
        if(FileLocalizer.fileExists(fName,pc)) {
            FileLocalizer.delete(fName,pc);
        }
    }
}
