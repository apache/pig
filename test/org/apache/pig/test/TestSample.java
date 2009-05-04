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
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.Iterator;

import junit.framework.TestCase;

import org.junit.Test;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

public class TestSample
extends TestCase
{
    private PigServer pig;
    private File tmpFile;
    private String tmpfilepath;

    private int DATALEN = 1024;

    @Override
    protected void setUp()
    throws Exception
    {
        //pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        pig = new PigServer("local");

        tmpFile = File.createTempFile( this.getName(), ".txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < DATALEN; i++) {
            ps.println(i);
        }
        ps.close();

        tmpfilepath = Util.generateURI(tmpFile.getCanonicalPath());
    }

    protected void tearDown()
    throws Exception
    {
        tmpFile.delete();
    }

    private void verify(String query, int expected_min, int expected_max)
    throws Exception
    {
        System.out.println("[TestSample] Query: "+query);
        pig.registerQuery(query);

        int count = 0;
        Iterator<Tuple> it = pig.openIterator("myid");
        while (it.hasNext()) {
          it.next();
          count ++;
        }

        boolean closeEnough = ((expected_min<=count) && (count<=expected_max));
        System.out.println("[TestSample] Result: "+expected_min+"<="+count+"<="+expected_max+" -> "+closeEnough);
        assertTrue("Count outside expected range", closeEnough);
    }

    @Test
    public void testSample_None()
    throws Exception
    {
        verify("myid = sample (load '"+ tmpfilepath + "') 0.0;", 0, 0);
    }

    @Test
    public void testSample_All()
    throws Exception
    {
        verify("myid = sample (load '"+ tmpfilepath + "') 1.0;", DATALEN, DATALEN);
    }

    @Test
    public void testSample_Some()
    throws Exception
    {
        verify("myid = sample (load '"+ tmpfilepath + "') 0.5;", DATALEN/3, DATALEN*2/3);
    }
}
