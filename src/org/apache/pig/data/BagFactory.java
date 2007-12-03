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
import java.io.IOException;

public class BagFactory {

    private File              tmpdir;
    private static BagFactory instance = new BagFactory();

    static{
    	init(new File(System.getProperty("java.io.tmpdir")));
    }
    public static BagFactory getInstance() {
        return instance;
    }

    private BagFactory() {
    }

    public static void init(File tmpdir) {
        instance.setTmpDir(tmpdir);
    }

    private void setTmpDir(File tmpdir) {
        this.tmpdir = tmpdir;
        this.tmpdir.mkdirs();
    }
    
    // Get BigBag or Bag, depending on whether the temp directory has been set up
    public DataBag getNewBag() throws IOException {
        if (tmpdir == null) return new DataBag();
        else return getNewBigBag();
    }
    
    // Need a Big Bag, dammit!
    public BigDataBag getNewBigBag() throws IOException {
        if (tmpdir == null) throw new IOException("No temp directory given for BigDataBag.");
        else return new BigDataBag(tmpdir);
    }

}
