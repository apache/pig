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
import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestOrderBy extends TestCase {
    private static final int DATALEN = 1024;
    private String[][] DATA = new String[2][DATALEN];
    static MiniCluster cluster = MiniCluster.buildCluster();
    
    private PigServer pig;
    private File tmpFile;

    public TestOrderBy() throws Throwable {
        DecimalFormat myFormatter = new DecimalFormat("0000000");
        for (int i = 0; i < DATALEN; i++) {
            DATA[0][i] = myFormatter.format(i);
            DATA[1][i] = myFormatter.format(DATALEN - i - 1);
        }
        pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }
    
    @Before
    public void setUp() throws Exception {
        tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < DATALEN; i++) {
            ps.println("1\t" + DATA[1][i] + "\t" + DATA[0][i]);
        }
        ps.close();
        
        DateTimeZone.setDefault(DateTimeZone.forOffsetMillis(DateTimeZone.UTC.getOffset(null)));
    }
    
    @After
    public void tearDown() throws Exception {
        tmpFile.delete();
    }
    
    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
    
    private void verify(String query, boolean descending) throws Exception {
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("myid");
        int col = (descending ? 1 : 0);
        for(int i = 0; i < DATALEN; i++) {
            Tuple t = (Tuple)it.next();
            int value = DataType.toInteger(t.get(1));
            assertEquals(Integer.parseInt(DATA[col][i]), value);
        }
        assertFalse(it.hasNext());
    }

    @Test
    public void testTopLevelOrderBy_Star_NoUsing() throws Exception {
        verify("myid = order (load 'file:" + tmpFile + "') BY *;", false);
    }

    @Test
    public void testTopLevelOrderBy_Col1_NoUsing() throws Exception {
        verify("myid = order (load 'file:" + tmpFile + "') BY $1;", false);
    }

    @Test
    public void testTopLevelOrderBy_Col2_NoUsing() throws Exception {
        verify("myid = order (load 'file:" + tmpFile + "') BY $2;", true);
    }

    @Test
    public void testTopLevelOrderBy_Col21_NoUsing() throws Exception {
        verify("myid = order (load 'file:" + tmpFile + "') BY $2, $1;", true);
    }

    @Test
    public void testTopLevelOrderBy_Star_Using() throws Exception {
        verify("myid = order (load 'file:" + tmpFile +
            "') BY * USING org.apache.pig.test.OrdAsc;", false);
        verify("myid = order (load 'file:" + tmpFile +
            "') BY * USING org.apache.pig.test.OrdDesc;", true);
        verify("myid = order (load 'file:" + tmpFile +
            "') BY * USING org.apache.pig.test.OrdDescNumeric;", true);
    }

    @Test
    public void testTopLevelOrderBy_Col1_Using() throws Exception {
        verify("myid = order (load 'file:" + tmpFile +
            "') BY $1 USING org.apache.pig.test.OrdAsc;", false);
        verify("myid = order (load 'file:" + tmpFile +
            "') BY $1 USING org.apache.pig.test.OrdDesc;", true);
        verify("myid = order (load 'file:" + tmpFile +
            "') BY $1 USING org.apache.pig.test.OrdDescNumeric;", true);
    }

    @Test
    public void testTopLevelOrderBy_Col2_Using() throws Exception {
        verify("myid = order (load 'file:" + tmpFile +
            "') BY $2 USING org.apache.pig.test.OrdAsc;", true);
        verify("myid = order (load 'file:" + tmpFile +
            "') BY $2 USING org.apache.pig.test.OrdDesc;", false);
        verify("myid = order (load 'file:" + tmpFile +
            "') BY $2 USING org.apache.pig.test.OrdDescNumeric;", false);
    }

    @Test
    public void testTopLevelOrderBy_Col21_Using() throws Exception {
        // col2/col1 ascending - 
        verify("myid = order (load 'file:" + tmpFile +
            "') BY $2, $1 USING org.apache.pig.test.OrdAsc;", true);
        verify("myid = order (load 'file:" + tmpFile +
            "') BY $2, $1 USING org.apache.pig.test.OrdDesc;", false);
        verify("myid = order (load 'file:" + tmpFile +
            "') BY $2, $1 USING org.apache.pig.test.OrdDescNumeric;", false);
    }

    @Test
    public void testNestedOrderBy_Star_NoUsing() throws Exception {
        verify("myid = foreach (group (load 'file:" + tmpFile +
            "') by $0) { D = ORDER $1 BY *; generate flatten(D); };", false);
    }

    @Test
    public void testNestedOrderBy_Col1_NoUsing() throws Exception {
        verify("myid = foreach (group (load 'file:" + tmpFile +
            "') by $0) { D = ORDER $1 BY $1; generate flatten(D); };", false);
    }

    @Test
    public void testNestedOrderBy_Col2_NoUsing() throws Exception {
        verify("myid = foreach (group (load 'file:" + tmpFile +
            "') by $0) { D = ORDER $1 BY $2; generate flatten(D); };", true);
    }

    @Test
    public void testNestedOrderBy_Col21_NoUsing() throws Exception {
        verify("myid = foreach (group (load 'file:" + tmpFile +
            "') by $0) { D = ORDER $1 BY $2, $1; generate flatten(D); };", true);
    }

    @Test
    public void testNestedOrderBy_Star_Using() throws Exception {
        verify("myid = foreach (group (load 'file:" + tmpFile +
            "') by $0) { D = ORDER $1 BY * USING " + 
            "org.apache.pig.test.OrdAsc; generate flatten(D); };", false);
        verify("myid = foreach (group (load 'file:" + tmpFile +
            "') by $0) { D = ORDER $1 BY * USING " + 
            "org.apache.pig.test.OrdDesc; generate flatten(D); };", true);
        verify("myid = foreach (group (load 'file:" + tmpFile +
            "') by $0) { D = ORDER $1 BY * USING " + 
            "org.apache.pig.test.OrdDescNumeric; generate flatten(D); };", true);
    }

    @Test
    public void testNestedOrderBy_Col1_Using() throws Exception {
        verify("myid = foreach (group (load 'file:" + tmpFile +
            "') by $0) { D = ORDER $1 BY $1 USING " + 
            "org.apache.pig.test.OrdAsc; generate flatten(D); };", false);
        verify("myid = foreach (group (load 'file:" + tmpFile +
            "') by $0) { D = ORDER $1 BY $1 USING " + 
            "org.apache.pig.test.OrdDesc; generate flatten(D); };", true);
        verify("myid = foreach (group (load 'file:" + tmpFile +
                "') by $0) { D = ORDER $1 BY $1 USING " + 
                "org.apache.pig.test.OrdDescNumeric; generate flatten(D); };",
                true);
    }

    @Test
    public void testNestedOrderBy_Col2_Using() throws Exception {
        verify("myid = foreach (group (load 'file:" + tmpFile +
                "') by $0) { D = ORDER $1 BY $2 USING " +
                "org.apache.pig.test.OrdAsc; generate flatten(D); };", true);
        verify("myid = foreach (group (load 'file:" + tmpFile +
                "') by $0) { D = ORDER $1 BY $2 USING " +
                "org.apache.pig.test.OrdDesc; generate flatten(D); };", false);
        verify("myid = foreach (group (load 'file:" + tmpFile +
                "') by $0) { D = ORDER $1 BY $2 USING " +
                "org.apache.pig.test.OrdDescNumeric; generate flatten(D); };",
                false);
    }

    @Test
    public void testNestedOrderBy_Col21_Using() throws Exception {
        // col2/col1 ascending - 
        verify("myid = foreach (group (load 'file:" + tmpFile +
                "') by $0) { D = ORDER $1 BY $2, $1 USING " +
                "org.apache.pig.test.OrdAsc; generate flatten(D); };", true);
        verify("myid = foreach (group (load 'file:" + tmpFile +
                "') by $0) { D = ORDER $1 BY $2, $1 USING " +
                "org.apache.pig.test.OrdDesc; generate flatten(D); };", false);
        verify("myid = foreach (group (load 'file:" + tmpFile +
                "') by $0) { D = ORDER $1 BY $2, $1 USING " +
                "org.apache.pig.test.OrdDescNumeric; generate flatten(D); };",
                false);
    }
    
    
    // this test case is for JIRA_1034
    @Test
    public void testOrderByGroup() throws Exception{
    	tmpFile = File.createTempFile("test", "txt");
    	PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
    	for(int i = 0; i < 100; i++) {
    		ps.println(i);
    	}
    	ps.close();
         
    	pig.registerQuery("a = load 'file:" + tmpFile +"' as (f1:int);");
    	pig.registerQuery("b = group a by $0;");
    	pig.registerQuery("c = order b by group;");
    	Iterator<Tuple> iter = pig.openIterator("c");
    	int count = 0;
    	while(iter.hasNext()){
    		Tuple tuple=iter.next();
    		assertEquals(count, tuple.get(0));
    		count++;
    	}
    	assertEquals(count, 100);
    }
    
    @Test
    public void testOrderByBooleanColumn() throws Exception {
        File tmpFile = genDataSetFileForOrderByBooleanColumn();
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(Util.buildTuple("value3", null));
        expectedResults.add(Util.buildTuple("value4", null));
        expectedResults.add(Util.buildTuple("value10", null));
        expectedResults.add(Util.buildTuple("value2", Boolean.FALSE));
        expectedResults.add(Util.buildTuple("value6", Boolean.FALSE));
        expectedResults.add(Util.buildTuple("value7", Boolean.FALSE));
        expectedResults.add(Util.buildTuple("value1", Boolean.TRUE));
        expectedResults.add(Util.buildTuple("value5", Boolean.TRUE));
        expectedResults.add(Util.buildTuple("value8", Boolean.TRUE));
        expectedResults.add(Util.buildTuple("value9", Boolean.TRUE));
        
        pig.registerQuery("blah = load '"
                + Util.generateURI(tmpFile.toString(), pig.getPigContext())
                + "' as (data:chararray, test:boolean);");
        pig.registerQuery("ordered = order blah by test;");
        Iterator<Tuple> expectedItr = expectedResults.iterator();
        Iterator<Tuple> actualItr = pig.openIterator("ordered");
        while (expectedItr.hasNext() && actualItr.hasNext()) {
            Tuple expectedTuple = expectedItr.next();
            Tuple actualTuple = actualItr.next();
            assertEquals(expectedTuple, actualTuple);
        }
        assertEquals(expectedItr.hasNext(), actualItr.hasNext());
    }
    
    private File genDataSetFileForOrderByBooleanColumn() throws IOException {

        File fp1 = File.createTempFile("order_by_boolean", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(fp1));
        ps.println("value1\ttrue");
        ps.println("value2\tfalse");
        ps.println("value3\t");
        ps.println("value4\t");
        ps.println("value5\ttrue");
        ps.println("value6\tfalse");
        ps.println("value7\tfalse");
        ps.println("value8\ttrue");
        ps.println("value9\ttrue");
        ps.println("value10\t");

        ps.close();

        return fp1;
    }

    @Test
    public void testOrderByDateTimeColumn() throws Exception {
        File tmpFile = genDataSetFileForOrderByDateTimeColumn();
        List<Tuple> expectedResults = new ArrayList<Tuple>();
        expectedResults.add(Util.buildTuple("value3", null));
        expectedResults.add(Util.buildTuple("value4", null));
        expectedResults.add(Util.buildTuple("value10", null));
        expectedResults.add(Util.buildTuple("value2", new DateTime("1970-01-01T00:00:00.000Z")));
        expectedResults.add(Util.buildTuple("value6", new DateTime("1970-01-01T00:00:01.000Z")));
        expectedResults.add(Util.buildTuple("value7", new DateTime("1970-01-01T00:00:01.000Z")));
        expectedResults.add(Util.buildTuple("value1", new DateTime("1970-01-01T00:01:00.000Z")));
        expectedResults.add(Util.buildTuple("value5", new DateTime("1970-01-01T01:00:00.000Z")));
        expectedResults.add(Util.buildTuple("value8", new DateTime("1970-01-02T00:00:00.000Z")));
        expectedResults.add(Util.buildTuple("value9", new DateTime("1970-02-01T00:00:00.000Z")));
        
        pig.registerQuery("blah = load '"
                + Util.generateURI(tmpFile.toString(), pig.getPigContext())
                + "' as (data:chararray, test:datetime);");
        pig.registerQuery("ordered = order blah by test;");
        Iterator<Tuple> expectedItr = expectedResults.iterator();
        Iterator<Tuple> actualItr = pig.openIterator("ordered");
        while (expectedItr.hasNext() && actualItr.hasNext()) {
            Tuple expectedTuple = expectedItr.next();
            Tuple actualTuple = actualItr.next();
            assertEquals(expectedTuple, actualTuple);
        }
        assertEquals(expectedItr.hasNext(), actualItr.hasNext());
    }

    private File genDataSetFileForOrderByDateTimeColumn() throws IOException {

        File fp1 = File.createTempFile("order_by_datetime", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(fp1));
        ps.println("value1\t1970-01-01T00:01:00.000Z");
        ps.println("value2\t1970-01-01T00:00:00.000Z");
        ps.println("value3\t");
        ps.println("value4\t");
        ps.println("value5\t1970-01-01T01:00:00.000Z");
        ps.println("value6\t1970-01-01T00:00:01.000Z");
        ps.println("value7\t1970-01-01T00:00:01.000Z");
        ps.println("value8\t1970-01-02T00:00:00.000Z");
        ps.println("value9\t1970-02-01T00:00:00.000Z");
        ps.println("value10\t");

        ps.close();

        return fp1;
    }
}
