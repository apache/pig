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

import org.apache.pig.PigServer;
import org.apache.pig.test.utils.TestHelper;
import org.apache.pig.data.Tuple;
import org.junit.Test;
import junit.framework.TestCase;
import junit.framework.Assert;

import java.util.Iterator;
import java.util.Random;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;

public class TestForEachNestedPlan extends TestCase {

    private String initString = "mapreduce";
    MiniCluster cluster = MiniCluster.buildCluster();

    private PigServer pig ;

    public TestForEachNestedPlan() throws Throwable {
        pig = new PigServer(initString) ;
    }

    Boolean[] nullFlags = new Boolean[]{ false, true };
    
    @Test
    public void testInnerOrderBy() throws Exception {
        for (int i = 0; i < nullFlags.length; i++) {
            System.err.println("Running testInnerOrderBy with nullFlags set to :" + nullFlags[i]);
            File tmpFile = genDataSetFile1(nullFlags[i]);
            pig.registerQuery("a = load '" + Util.generateURI(tmpFile.toString()) + "'; ");
            pig.registerQuery("b = group a by $0; ");
            pig.registerQuery("c = foreach b { " + "     c1 = order $1 by *; "
                    + "    generate flatten(c1); " + "};");
            Iterator<Tuple> it = pig.openIterator("c");
            Tuple t = null;
            int count = 0;
            while (it.hasNext()) {
                t = it.next();
                System.out.println(count + ":" + t);
                count++;
            }
            Assert.assertEquals(count, 30);
        }
    }


    /*
    @Test
    public void testInnerDistinct() throws Exception {
        File tmpFile = genDataSetFile1() ;
        pig.registerQuery("a = load 'file:" + tmpFile + "'; ") ;
        pig.registerQuery("b = group a by $0; ");
        pig.registerQuery("c = foreach b { "
                        + "     c1 = distinct $1 ; "
                        +  "    generate flatten(c1); "
                        + "};") ;
        Iterator<Tuple> it = pig.openIterator("c");
        Tuple t = null ;
        int count = 0 ;
        while(it.hasNext()) {
            t = it.next() ;
            System.out.println(count + ":" + t) ;
            count++ ;
        }
        Assert.assertEquals(count, 15);
    }
    */

    /***
     * For generating a sample dataset
     */
    private File genDataSetFile1(boolean withNulls) throws IOException {

        int dataLength = 30;
        String[][] data = new String[dataLength][] ;

        DecimalFormat formatter = new DecimalFormat("0000000");

        Random r = new Random();
        
        for (int i = 0; i < dataLength; i++) {
            data[i] = new String[2] ;
            // inject nulls randomly
            if(withNulls && r.nextInt(dataLength) < 0.3 * dataLength) {
                data[i][0] = "";
            } else {
                data[i][0] = formatter.format(i % 10);
            }
            data[i][1] = formatter.format((dataLength - i)/2);
        }

        return TestHelper.createTempFile(data) ;
    }
}
