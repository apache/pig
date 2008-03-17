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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;


public class PigFile {
    private String file = null;
    boolean append = false;

    public PigFile(String filename, boolean append) {
        file = filename;
        this.append = append;
    }
    
    public PigFile(String filename){
        file = filename;
    }
    
    public DataBag load(LoadFunc lfunc, PigContext pigContext) throws IOException {
        DataBag content = BagFactory.getInstance().newDefaultBag();
        InputStream is = FileLocalizer.open(file, pigContext);
        lfunc.bindTo(file, new BufferedPositionedInputStream(is), 0, Long.MAX_VALUE);
        Tuple f = null;
        while ((f = lfunc.getNext()) != null) {
            content.add(f);
        }
        return content;
    }

    
    public void store(DataBag data, StoreFunc sfunc, PigContext pigContext) throws IOException {
        BufferedOutputStream bos = new BufferedOutputStream(FileLocalizer.create(file, append, pigContext));
        sfunc.bindTo(bos);
        for (Iterator<Tuple> it = data.content(); it.hasNext();) {
            Tuple row = it.next();
            sfunc.putNext(row);
        }
        sfunc.finish();
        bos.close();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PigFile: file: ");
        sb.append(this.file);
        sb.append(", append: ");
        sb.append(this.append);
        return sb.toString();
    }
}
