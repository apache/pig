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
package org.apache.pig.impl.io;

import java.io.Serializable;

import org.apache.pig.FuncSpec;
import org.apache.pig.builtin.PigStorage;


/**
 * A simple class that specifies a file name and storage function which is used to read/write it
 *
 */
public class FileSpec implements Serializable {
    
    private static final long serialVersionUID = 2L;
    String fileName;

    FuncSpec funcSpec;
    
    public FileSpec(String fileName, FuncSpec funcSpec){
        this.fileName = fileName;
        this.funcSpec = funcSpec != null ? funcSpec : new FuncSpec( PigStorage.class.getName() + "()" );
    }
    
    public String getFileName(){
        return fileName;
    }
    
    public FuncSpec getFuncSpec(){
        return funcSpec;
    }
    
    @Override
    public String toString(){
        return fileName + ":" + funcSpec;
    }

    public String getFuncName(){
            return funcSpec.getClassName();
    }

    public int getSize() {
        throw new UnsupportedOperationException("File Size not implemented yet");
    }
    
    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof FileSpec) {
            FileSpec ofs = (FileSpec)other;
            if (!fileName.equals(ofs.fileName)) return false;
            return funcSpec.equals(ofs.funcSpec);
        } else {
            return false;
        }
    }
    
    @Override
    public int hashCode() {
        return getFuncName().hashCode() + fileName.hashCode();
    }
}
