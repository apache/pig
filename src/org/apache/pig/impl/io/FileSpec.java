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

//import org.apache.pig.impl.PigContext;


/**
 * A simple class that specifies a file name and storage function which is used to read/write it
 * @author utkarsh
 *
 */
public class FileSpec implements Serializable {
    
    private static final long serialVersionUID = 1L;
    String fileName;

    String funcSpec;
    
    public FileSpec(String fileName, String funcSpec){
        this.fileName = fileName;
        this.funcSpec = funcSpec;
    }
    
    public String getFileName(){
        return fileName;
    }
    
    public String getFuncSpec(){
        return funcSpec;
    }
    
    @Override
    public String toString(){
        return fileName + ":" + funcSpec;
    }

    //TODO FIX
    //Commenting out the method getFuncName as it calls getClassNameFromSpec
    //which is part of PigContext. PigContext pulls in HExecutionEngine which
    //is completely commented out. The import org.apache.pig.impl.PigContext
    //is also commented out
    /*    
    public String getFuncName(){
            return PigContext.getClassNameFromSpec(funcSpec);
    }
    */

    public int getSize() {
        throw new UnsupportedOperationException("File Size not implemented yet");
    }
}
