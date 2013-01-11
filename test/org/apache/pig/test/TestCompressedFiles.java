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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Iterator;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.DIFF;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestCompressedFiles {
    static MiniCluster cluster = MiniCluster.buildCluster();

    File datFile;
    File gzFile;

    @Before
    public void setUp() throws Exception {
        datFile = File.createTempFile("compTest", ".dat");
        gzFile = File.createTempFile("compTest", ".gz");
        FileOutputStream dat = new FileOutputStream(datFile);
        GZIPOutputStream gz = new GZIPOutputStream(new FileOutputStream(gzFile));
        Random rand = new Random();
        for(int i = 0; i < 1024; i++) {
            StringBuffer sb = new StringBuffer();
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

    @After
    public void tearDown() throws Exception {
        datFile.delete();
        gzFile.delete();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testCompressed1() throws Throwable {
        PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pig.registerQuery("A = foreach (cogroup (load '"
                + Util.generateURI(gzFile.toString(), pig.getPigContext())
                + "') by $1, (load '"
                + Util.generateURI(datFile.toString(), pig.getPigContext())
                + "') by $1) generate flatten( " + DIFF.class.getName()
                + "($1.$1,$2.$1)) ;");
        Iterator<Tuple> it = pig.openIterator("A");
        assertFalse(it.hasNext());
    }

    @Test
    public void testCompressed2() throws Throwable {
        PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pig.registerQuery("A = load '"
                + Util.generateURI(gzFile.toString(), pig.getPigContext())
                + "';");

        DataBag dbGz = BagFactory.getInstance().newDefaultBag(), dbDt = BagFactory.getInstance().newDefaultBag();
        {
            Iterator<Tuple> iter = pig.openIterator("A");

            while(iter.hasNext()) {
                dbGz.add(iter.next());
            }
        }
        pig.registerQuery("B = load '"
                + Util.generateURI(datFile.toString(), pig.getPigContext())
                + "';");
        Iterator<Tuple> iter = pig.openIterator("B");

        while(iter.hasNext()) {
            dbDt.add(iter.next());
        }

        assertTrue(dbGz.size() > 0);
        assertTrue(dbDt.size() > 0);
        assertEquals(dbGz.size(), dbDt.size());
        assertTrue(TestHelper.compareBags(dbGz, dbDt));
    }
}