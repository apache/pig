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
package org.apache.pig.test;


import java.io.File;
import java.io.FileOutputStream;
import java.util.Iterator;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.builtin.DIFF;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCompressedFiles extends PigExecTestCase {
    
    private final Log log = LogFactory.getLog(getClass());

    File datFile;
    File gzFile;
    @Override
	@Before
    protected void setUp() throws Exception {
        super.setUp();
        datFile = File.createTempFile("compTest", ".dat");
        gzFile = File.createTempFile("compTest", ".gz");
        FileOutputStream dat = new FileOutputStream(datFile);
        GZIPOutputStream gz = new GZIPOutputStream(new FileOutputStream(gzFile));
        Random rand = new Random();
        for(int i = 0; i < 1024; i++) {
            StringBuilder sb = new StringBuilder();
            int x = rand.nextInt();
            int y = rand.nextInt();
            sb.append(x);
            sb.append('\t');
            sb.append(y);
            sb.append('\n');
            byte bytes[] = sb.toString().getBytes();
            dat.write(bytes);
            gz.write(bytes);
        }
        dat.close();
        gz.close();
    }

    @Override
	@After
    protected void tearDown() throws Exception {
        datFile.delete();
        gzFile.delete();
        super.tearDown();
    }
    
    @Test
    public void testCompressed1() throws Throwable {
        // FIXME : this should be tested in all modes
        if(execType == ExecType.LOCAL)
            return;
        pigServer.registerQuery("A = foreach (cogroup (load 'file:"+Util.encodeEscape(gzFile.toString())+"') by $1, (load 'file:"+Util.encodeEscape(datFile.toString()) + "') by $1) generate flatten( " + DIFF.class.getName() + "($1.$1,$2.$1)) ;");
        Iterator it = pigServer.openIterator("A");
        boolean success = true;
        while(it.hasNext()) {
            success = false;
            log.info(it.next());
        }
        assertTrue(success);
    }
}
