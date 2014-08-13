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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.test.utils.TestHelper;
import org.junit.Test;

public class TestForEachNestedPlanLocal {

    private PigServer pig ;

    public TestForEachNestedPlanLocal() throws Throwable {
        pig = new PigServer(ExecType.LOCAL) ;
    }

    Boolean[] nullFlags = new Boolean[]{ false, true };

    @Test
    public void testInnerOrderBy() throws Exception {
        for (int i = 0; i < nullFlags.length; i++) {
            System.err.println("Running testInnerOrderBy with nullFlags set to :" + nullFlags[i]);
            File tmpFile = genDataSetFile1(nullFlags[i]);
            pig.registerQuery("a = load '"
                    + Util.generateURI(Util.encodeEscape(tmpFile.toString()), pig.getPigContext())
                    + "'; ");
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
            assertEquals(count, 30);
        }
    }

    @Test
    public void testInnerLimit() throws Exception {
        File tmpFile = genDataSetFileOneGroup();
        pig.registerQuery("a = load '"
                + Util.generateURI(Util.encodeEscape(tmpFile.toString()), pig.getPigContext())
                + "'; ");
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

        assertFalse(it.hasNext());

        // Pig's previous local mode was screwed up correcting that
        assertEquals(5L, count[0]);
        assertEquals(5L, count[1]);
        assertEquals(3L, count[2]);
    }

    @Test
    public void testNestedCrossTwoRelations() throws Exception {
        File[] tmpFiles = generateDataSetFilesForNestedCross();
        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStringAsByteArray(new String[] {
                "({('user1','usa','user1','usa','10'),('user1','usa','user1','usa','30'),('user1','usa','user1','china','20')})",
                "({('user2','usa','user2','usa','20'),('user2','usa','user2','usa','20')})",
                "({('user3','singapore','user3','usa','10'),('user3','singapore','user3','singapore','20')})",
                "({})" });
        pig.registerQuery("user = load '"
                + Util.encodeEscape(Util.generateURI(tmpFiles[0].toString(), pig.getPigContext()))
                + "' as (uid, region);");
        pig.registerQuery("session = load '"
                + Util.encodeEscape(Util.generateURI(tmpFiles[1].toString(), pig.getPigContext()))
                + "' as (uid, region, duration);");
        pig.registerQuery("C = cogroup user by uid, session by uid;");
        pig.registerQuery("D = foreach C {"
                + "crossed = cross user, session;"
                + "generate crossed;" + "}");
        Iterator<Tuple> expectedItr = expectedResults.iterator();
        Iterator<Tuple> actualItr = pig.openIterator("D");
        while (expectedItr.hasNext() && actualItr.hasNext()) {
            Tuple expectedTuple = expectedItr.next();
            Tuple actualTuple = actualItr.next();
            assertEquals(expectedTuple, actualTuple);
        }
        assertEquals(expectedItr.hasNext(), actualItr.hasNext());
    }

    @Test
    public void testNestedCrossTwoRelationsComplex() throws Exception {
        File[] tmpFiles = generateDataSetFilesForNestedCross();
        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStringAsByteArray(new String[] {
                "({('user1','usa','user1','usa','10'),('user1','usa','user1','usa','30')})",
                "({('user2','usa','user2','usa','20')})",
                "({('user3','singapore','user3','singapore','20')})",
                "({})" });
        pig.registerQuery("user = load '"
                + Util.encodeEscape(Util.generateURI(tmpFiles[0].toString(), pig.getPigContext()))
                + "' as (uid, region);");
        pig.registerQuery("session = load '"
                + Util.encodeEscape(Util.generateURI(tmpFiles[1].toString(), pig.getPigContext()))
                + "' as (uid, region, duration);");
        pig.registerQuery("C = cogroup user by uid, session by uid;");
        pig.registerQuery("D = foreach C {"
                + "distinct_session = distinct session;"
                + "crossed = cross user, distinct_session;"
                + "filtered = filter crossed by user::region == distinct_session::region;"
                + "generate filtered;" + "}");
        Iterator<Tuple> expectedItr = expectedResults.iterator();
        Iterator<Tuple> actualItr = pig.openIterator("D");
        while (expectedItr.hasNext() && actualItr.hasNext()) {
            Tuple expectedTuple = expectedItr.next();
            Tuple actualTuple = actualItr.next();
            assertEquals(expectedTuple, actualTuple);
        }
        assertEquals(expectedItr.hasNext(), actualItr.hasNext());
    }

    @Test
    public void testNestedCrossThreeRelations() throws Exception {
        File[] tmpFiles = generateDataSetFilesForNestedCross();
        List<Tuple> expectedResults = Util.getTuplesFromConstantTupleStringAsByteArray(new String[] {
                "({('user1','usa','user1','usa','10','user1','admin','male'),('user1','usa','user1','usa','30','user1','admin','male'),('user1','usa','user1','china','20','user1','admin','male')})",
                "({('user2','usa','user2','usa','20','user2','guest','male'),('user2','usa','user2','usa','20','user2','guest','male')})",
                "({('user3','singapore','user3','usa','10','user3','user','female'),('user3','singapore','user3','singapore','20','user3','user','female')})",
                "({})" });
        pig.registerQuery("user = load '"
                + Util.encodeEscape(Util.generateURI(tmpFiles[0].toString(), pig.getPigContext()))
                + "' as (uid, region);");
        pig.registerQuery("session = load '"
                + Util.encodeEscape(Util.generateURI(tmpFiles[1].toString(), pig.getPigContext()))
                + "' as (uid, region, duration);");
        pig.registerQuery("profile = load '"
                + Util.encodeEscape(Util.generateURI(tmpFiles[2].toString(), pig.getPigContext()))
                + "' as (uid, role, gender);");
        pig.registerQuery("C = cogroup user by uid, session by uid, profile by uid;");
        pig.registerQuery("D = foreach C {"
                + "crossed = cross user, session, profile;"
                + "generate crossed;" + "}");
        Iterator<Tuple> expectedItr = expectedResults.iterator();
        Iterator<Tuple> actualItr = pig.openIterator("D");
        while (expectedItr.hasNext() && actualItr.hasNext()) {
            Tuple expectedTuple = expectedItr.next();
            Tuple actualTuple = actualItr.next();
            assertEquals(expectedTuple, actualTuple);
        }
        assertEquals(expectedItr.hasNext(), actualItr.hasNext());
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
        assertEquals(count, 15);
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

    private File[] generateDataSetFilesForNestedCross() throws IOException {
        File userFile = File.createTempFile("user", "txt");
        PrintStream userPS = new PrintStream(new FileOutputStream(userFile));
        userPS.println("user1\tusa");
        userPS.println("user2\tusa");
        userPS.println("user3\tsingapore");
        userPS.println("user4\tchina");
        userPS.close();
        File sessionFile = File.createTempFile("session", "txt");
        PrintStream sessionPS = new PrintStream(new FileOutputStream(
                sessionFile));
        sessionPS.println("user3\tusa\t10");
        sessionPS.println("user3\tsingapore\t20");
        sessionPS.println("user2\tusa\t20");
        sessionPS.println("user2\tusa\t20");
        sessionPS.println("user1\tusa\t10");
        sessionPS.println("user1\tusa\t30");
        sessionPS.println("user1\tchina\t20");
        sessionPS.close();
        File profileFile = File.createTempFile("profile", "txt");
        PrintStream profilePS = new PrintStream(new FileOutputStream(
                profileFile));
        profilePS.println("user1\tadmin\tmale");
        profilePS.println("user2\tguest\tmale");
        profilePS.println("user3\tuser\tfemale");
        profilePS.println("user4\tuser\tfemale");
        profilePS.close();
        return new File[] { userFile, sessionFile, profileFile };
    }
}