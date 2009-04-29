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
package org.apache.pig.builtin;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;


public class PigDump implements StoreFunc {

    public static String recordDelimiter = "\n";
    

    OutputStream os;

    public void bindTo(OutputStream os) throws IOException {
        this.os = os;
    }

    public void finish() throws IOException {
        
    }

    public void putNext(Tuple f) throws IOException {
        os.write((f.toString() + recordDelimiter).getBytes());
    }

    /* (non-Javadoc)
     * @see org.apache.pig.StoreFunc#getStorePreparationClass()
     */
    @Override
    public Class getStorePreparationClass() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

}
