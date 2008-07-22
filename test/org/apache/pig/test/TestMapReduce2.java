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

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.utils.TestHelper;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

public class TestMapReduce2 extends TestCase {

    private String initString = "mapreduce";
    MiniCluster cluster = MiniCluster.buildCluster();

    private PigServer pig ;

    public TestMapReduce2() throws Throwable {
        pig = new PigServer(initString) ;
    }


    @Test
    public void testUnion1() throws Exception {
        File tmpFile1 = genDataSetFile1() ;
        File tmpFile2 = genDataSetFile2() ;
        pig.registerQuery("a = load 'file:" + tmpFile1 + "'; ") ;
        pig.registerQuery("b = load 'file:" + tmpFile2 + "'; ") ;
        pig.registerQuery("c = union a, b; ") ;

        Iterator<Tuple> it = pig.openIterator("c");
        Tuple t = null ;
        int count = 0 ;
        while(it.hasNext()) {
            t = it.next() ;
            System.out.println(count + ":" + t) ;
            count++ ;
        }
        Assert.assertEquals(count, 30 + 50);
    }

    @Test
    public void testUnion2() throws Exception {
        File tmpFile1 = genDataSetFile1() ;
        File tmpFile2 = genDataSetFile2() ;
        pig.registerQuery("a = load 'file:" + tmpFile1 + "'; ") ;
        pig.registerQuery("b = load 'file:" + tmpFile2 + "'; ") ;
        pig.registerQuery("a1 = foreach a generate $0, $1; ") ;
        pig.registerQuery("b1 = foreach b generate $0, $1; ") ;
        pig.registerQuery("c = union a1, b1; ") ;

        Iterator<Tuple> it = pig.openIterator("c");
        Tuple t = null ;
        int count = 0 ;
        while(it.hasNext()) {
            t = it.next() ;
            System.out.println(count + ":" + t) ;
            count++ ;
        }
        Assert.assertEquals(count, 30 + 50);
    }

    /***
     * For generating a sample dataset
     */
    private File genDataSetFile1() throws IOException {

        int dataLength = 30;
        String[][] data = new String[dataLength][] ;

        DecimalFormat formatter = new DecimalFormat("0000000");

        for (int i = 0; i < dataLength; i++) {
            data[i] = new String[2] ;
            data[i][0] = formatter.format(i % 10);
            data[i][1] = formatter.format(dataLength - i);
        }

        return TestHelper.createTempFile(data) ;
    }

    private File genDataSetFile2() throws IOException {

        int dataLength = 50;
        String[][] data = new String[dataLength][] ;

        DecimalFormat formatter = new DecimalFormat("0000000");

        for (int i = 0; i < dataLength; i++) {
            data[i] = new String[2] ;
            data[i][0] = formatter.format(i % 10);
            data[i][1] = formatter.format(dataLength - i);
        }

        return TestHelper.createTempFile(data) ;
    }
}
