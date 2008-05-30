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

import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import static org.apache.pig.PigServer.ExecType.MAPREDUCE;
import static org.apache.pig.PigServer.ExecType.LOCAL;

public class TestOrderBy extends PigExecTestCase {
    private static final int DATALEN = 1024;
    private String[][] DATA = new String[2][DATALEN];

    private File tmpFile;

    public TestOrderBy() throws Throwable {
        DecimalFormat myFormatter = new DecimalFormat("0000000");
        for (int i = 0; i < DATALEN; i++) {
            DATA[0][i] = myFormatter.format(i);
            DATA[1][i] = myFormatter.format(DATALEN - i - 1);
        }
    }
    
    protected void setUp() throws Exception {
        tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        for(int i = 0; i < DATALEN; i++) {
            ps.println("1\t" + DATA[1][i] + "\t" + DATA[0][i]);
        }
        ps.close();
    }
    
    protected void tearDown() throws Exception {
        tmpFile.delete();
    }

    private void verify(String query, boolean descending) throws Exception {
        pigServer.registerQuery(query);
        Iterator<Tuple> it = pigServer.openIterator("myid");
        int col = (descending ? 1 : 0);
        for(int i = 0; i < DATALEN; i++) {
            Tuple t = (Tuple)it.next();
            int value = t.getAtomField(1).numval().intValue();
//            log.info("" + i + "," + DATA[0][i] + "," + DATA[1][i] + "," + value);
            assertEquals(Integer.parseInt(DATA[col][i]), value);
        }
        assertFalse(it.hasNext());
    }

    @Test
    public void testTopLevelOrderBy_Star_NoUsing() throws Exception {
        verify("myid = order (load 'file:" + Util.encodeEscape(tmpFile.toString()) + "') BY *;", false);
    }

    @Test
    public void testTopLevelOrderBy_Col1_NoUsing() throws Exception {
        verify("myid = order (load 'file:" + Util.encodeEscape(tmpFile.toString()) + "') BY $1;", false);
    }

    @Test
    public void testTopLevelOrderBy_Col2_NoUsing() throws Exception {
        verify("myid = order (load 'file:" + Util.encodeEscape(tmpFile.toString()) + "') BY $2;", true);
    }

    @Test
    public void testTopLevelOrderBy_Col21_NoUsing() throws Exception {
        verify("myid = order (load 'file:" + Util.encodeEscape(tmpFile.toString()) + "') BY $2, $1;", true);
    }

    @Test
    public void testTopLevelOrderBy_Star_Using() throws Exception {
        verify("myid = order (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') BY * USING org.apache.pig.test.OrdAsc;", false);
        verify("myid = order (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') BY * USING org.apache.pig.test.OrdDesc;", true);
        verify("myid = order (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') BY * USING org.apache.pig.test.OrdDescNumeric;", true);
    }

    @Test
    public void testTopLevelOrderBy_Col1_Using() throws Exception {
        verify("myid = order (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') BY $1 USING org.apache.pig.test.OrdAsc;", false);
        verify("myid = order (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') BY $1 USING org.apache.pig.test.OrdDesc;", true);
        verify("myid = order (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') BY $1 USING org.apache.pig.test.OrdDescNumeric;", true);
    }

    @Test
    public void testTopLevelOrderBy_Col2_Using() throws Exception {
        verify("myid = order (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') BY $2 USING org.apache.pig.test.OrdAsc;", true);
        verify("myid = order (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') BY $2 USING org.apache.pig.test.OrdDesc;", false);
        verify("myid = order (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') BY $2 USING org.apache.pig.test.OrdDescNumeric;", false);
    }

    @Test
    public void testTopLevelOrderBy_Col21_Using() throws Exception {
        // col2/col1 ascending - 
        verify("myid = order (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') BY $2, $1 USING org.apache.pig.test.OrdAsc;", true);
        verify("myid = order (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') BY $2, $1 USING org.apache.pig.test.OrdDesc;", false);
        verify("myid = order (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') BY $2, $1 USING org.apache.pig.test.OrdDescNumeric;", false);
    }

    @Test
    public void testNestedOrderBy_Star_NoUsing() throws Exception {
        verify("myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') by $0) { D = ORDER $1 BY *; generate flatten(D); };", false);
    }

    @Test
    public void testNestedOrderBy_Col1_NoUsing() throws Exception {
        verify("myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') by $0) { D = ORDER $1 BY $1; generate flatten(D); };", false);
    }

    @Test
    public void testNestedOrderBy_Col2_NoUsing() throws Exception {
        verify("myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') by $0) { D = ORDER $1 BY $2; generate flatten(D); };", true);
    }

    @Test
    public void testNestedOrderBy_Col21_NoUsing() throws Exception {
        verify("myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') by $0) { D = ORDER $1 BY $2, $1; generate flatten(D); };", true);
    }

    @Test
    public void testNestedOrderBy_Star_Using() throws Exception {
        verify("myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') by $0) { D = ORDER $1 BY * USING " + 
            "org.apache.pig.test.OrdAsc; generate flatten(D); };", false);
        verify("myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') by $0) { D = ORDER $1 BY * USING " + 
            "org.apache.pig.test.OrdDesc; generate flatten(D); };", true);
        verify("myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') by $0) { D = ORDER $1 BY * USING " + 
            "org.apache.pig.test.OrdDescNumeric; generate flatten(D); };", true);
    }

    @Test
    public void testNestedOrderBy_Col1_Using() throws Exception {
        verify("myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') by $0) { D = ORDER $1 BY $1 USING " + 
            "org.apache.pig.test.OrdAsc; generate flatten(D); };", false);
        verify("myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
            "') by $0) { D = ORDER $1 BY $1 USING " + 
            "org.apache.pig.test.OrdDesc; generate flatten(D); };", true);
        verify("myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
                "') by $0) { D = ORDER $1 BY $1 USING " + 
                "org.apache.pig.test.OrdDescNumeric; generate flatten(D); };",
                true);
    }

    @Test
    public void testNestedOrderBy_Col2_Using() throws Exception {
        verify("myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
                "') by $0) { D = ORDER $1 BY $2 USING " +
                "org.apache.pig.test.OrdAsc; generate flatten(D); };", true);
        verify("myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
                "') by $0) { D = ORDER $1 BY $2 USING " +
                "org.apache.pig.test.OrdDesc; generate flatten(D); };", false);
        verify("myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
                "') by $0) { D = ORDER $1 BY $2 USING " +
                "org.apache.pig.test.OrdDescNumeric; generate flatten(D); };",
                false);
    }

    @Test
    public void testNestedOrderBy_Col21_Using() throws Exception {
        // col2/col1 ascending - 
        verify("myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
                "') by $0) { D = ORDER $1 BY $2, $1 USING " +
                "org.apache.pig.test.OrdAsc; generate flatten(D); };", true);
        verify("myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
                "') by $0) { D = ORDER $1 BY $2, $1 USING " +
                "org.apache.pig.test.OrdDesc; generate flatten(D); };", false);
        verify("myid = foreach (group (load 'file:" + Util.encodeEscape(tmpFile.toString()) +
                "') by $0) { D = ORDER $1 BY $2, $1 USING " +
                "org.apache.pig.test.OrdDescNumeric; generate flatten(D); };",
                false);
    }

}
