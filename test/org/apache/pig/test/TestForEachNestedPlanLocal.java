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
import java.io.*;
import java.text.DecimalFormat;

public class TestForEachNestedPlanLocal extends TestCase {

    private String initString = "local";
    //MiniCluster cluster = MiniCluster.buildCluster();

    private PigServer pig ;

    public TestForEachNestedPlanLocal() throws Throwable {
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

    @Test
    public void testInnerLimit() throws Exception {
        File tmpFile = genDataSetFileOneGroup();
        pig.registerQuery("a = load '" + Util.generateURI(tmpFile.toString()) + "'; ");
        pig.registerQuery("b = group a by $0; ");
        pig.registerQuery("c = foreach b { " + "     c1 = limit $1 5; "
                + "    generate COUNT(c1); " + "};");
        Iterator<Tuple> it = pig.openIterator("c");
        Tuple t = null;
        long count[] = new long[3];
        for (int i = 0; i < 3 && it.hasNext(); i++) {
            t = it.next();
            count[i] = (Long)t.get(0);
        }

        Assert.assertFalse(it.hasNext());

        Assert.assertEquals(3L, count[0]);
        Assert.assertEquals(5L, count[1]);
        Assert.assertEquals(5L, count[2]);
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

    private File genDataSetFileOneGroup() throws IOException {

        File fp1 = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(fp1));

        ps.println("lost\tjack");
        ps.println("lost\tkate");
        ps.println("lost\tsawyer");
        ps.println("lost\tdesmond");
        ps.println("lost\thurley");
        ps.println("lost\tlocke");
        ps.println("lost\tsun");
        ps.println("lost\tcharlie");
        ps.println("lost\tjin");
        ps.println("lost\tben");
        ps.println("lotr\tfrodo");
        ps.println("lotr\tsam");
        ps.println("lotr\tmerry");
        ps.println("lotr\tpippen");
        ps.println("lotr\tbilbo");
        ps.println("3stooges\tlarry");
        ps.println("3stooges\tmoe");
        ps.println("3stooges\tcurly");

        ps.close();

        return fp1;
    }

}
