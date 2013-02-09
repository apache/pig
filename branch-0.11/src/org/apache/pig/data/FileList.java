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
package org.apache.pig.data;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class extends ArrayList<File> to add a finalize() that
 * calls delete on the files .
 * This helps in getting rid of the finalize() in the classes such 
 * as DefaultAbstractBag, and they can be freed up without waiting 
 * for finalize to be called. Only if those classes have spilled to
 * disk, there will be a (this) class that needs to be finalized.
 * 
 * CAUTION: if you assign a new value for a variable of this type,
 * the files (if any) in the old object it pointed to will be scheduled for
 * deletion. To avoid that call .clear() before assigning a new value.
 */
public class FileList extends ArrayList<File> {

    private static final long serialVersionUID = 1L;
    private static final Log log = LogFactory.getLog(FileList.class);

    public FileList(int i) {
        super(i);
    }

    public FileList(){
    }
    
    public FileList(LinkedList<File> ll) {
        super(ll);
    }

    @Override
    protected void finalize(){
        for(File f : this){
            if(f.delete() == false){
                log.warn("Failed to delete file: " + f.getPath());
            }
        }
    }
    
}
