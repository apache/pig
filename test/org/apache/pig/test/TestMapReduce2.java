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

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.utils.TestHelper;
import org.junit.AfterClass;
import org.junit.Test;

public class TestMapReduce2 {

    static MiniCluster cluster = MiniCluster.buildCluster();

    private PigServer pig ;

    public TestMapReduce2() throws Throwable {
        pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties()) ;
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testUnion1() throws Exception {
        File tmpFile1 = genDataSetFile(false, 30 ) ;
        File tmpFile2 = genDataSetFile(false, 50 ) ;
        pig.registerQuery("a = load '"
                + Util.generateURI(tmpFile1.toString(), pig.getPigContext())
                + "'; ");
        pig.registerQuery("b = load '"
                + Util.generateURI(tmpFile2.toString(), pig.getPigContext())
                + "'; ");
        pig.registerQuery("c = union a, b; ") ;

        verifyUnion( "c", 30 + 50 );
    }

    @Test
    public void testUnion1WithNulls() throws Exception {

        File tmpFile1 = genDataSetFile(true, 30 ) ;
        File tmpFile2 = genDataSetFile(true, 50 ) ;
        pig.registerQuery("a = load '"
                + Util.generateURI(tmpFile1.toString(), pig.getPigContext())
                + "'; ");
        pig.registerQuery("b = load '"
                + Util.generateURI(tmpFile2.toString(), pig.getPigContext())
                + "'; ");
        pig.registerQuery("c = union a, b; ");

        verifyUnion( "c", 30 + 50 );
    }

    @Test
    public void testUnion2() throws Exception {

        File tmpFile1 = genDataSetFile(false, 30) ;
        File tmpFile2 = genDataSetFile(false, 50) ;
        pig.registerQuery("a = load '"
                + Util.generateURI(tmpFile1.toString(), pig.getPigContext())
                + "'; ");
        pig.registerQuery("b = load '"
                + Util.generateURI(tmpFile2.toString(), pig.getPigContext())
                + "'; ");
        pig.registerQuery("a1 = foreach a generate $0, $1; ") ;
        pig.registerQuery("b1 = foreach b generate $0, $1; ") ;
        pig.registerQuery("c = union a1, b1; ") ;

        verifyUnion( "c", 30 + 50 );
    }

    @Test
    public void testUnion2WithNulls() throws Exception {
        File tmpFile1 = genDataSetFile(true, 30) ;
        File tmpFile2 = genDataSetFile(true, 50) ;
        pig.registerQuery("a = load '"
                + Util.generateURI(tmpFile1.toString(), pig.getPigContext())
                + "'; ");
        pig.registerQuery("b = load '"
                + Util.generateURI(tmpFile2.toString(), pig.getPigContext())
                + "'; ");
        pig.registerQuery("a1 = foreach a generate $0, $1; ") ;
        pig.registerQuery("b1 = foreach b generate $0, $1; ") ;
        pig.registerQuery("c = union a1, b1; ") ;

        verifyUnion( "c", 30 + 50 );
    }

    //verifies results
    public void verifyUnion(String id, int actualCount ) throws Exception {

        Iterator<Tuple> it = pig.openIterator(id);
        Tuple t = null ;
        int count = 0 ;

        while(it.hasNext()) {
            t = it.next() ;
            System.out.println(count + ":" + t) ;
            count++ ;
        }

        assertEquals(count, actualCount);
    }


    /***
     * For generating a sample dataset
     */
    private File genDataSetFile(boolean hasNulls, int dataLength ) throws IOException {

        String[][] data = new String[dataLength][] ;

        DecimalFormat formatter = new DecimalFormat("0000000");

        if ( hasNulls == true ) {

                for (int i = 0; i < dataLength; i++) {

                     data[i] = new String[2] ;
                     if ( i % 7  == 0 ) {
                        data[i][0] = "";
                        data[i][1] = formatter.format(dataLength - i);

                     } else if ( i % 10 ==0  ) {

                        data[i][0] = formatter.format(i % 10);
                        data[i][1] = "";

                     } else if ( i % 13 == 0 ) {

                        data[i][0] = "";
                        data[i][1] = "";

                     } else {
                        data[i][0] = formatter.format(i % 10);
                        data[i][1] = formatter.format(dataLength - i);
                     }
             }

        } else {


            for (int i = 0; i < dataLength; i++) {
                data[i] = new String[2] ;
                data[i][0] = formatter.format(i % 10);
                data[i][1] = formatter.format(dataLength - i);
            }

        }
        File tmpFile = TestHelper.createTempFile(data) ;
        tmpFile.deleteOnExit();
        return tmpFile;
    }
}