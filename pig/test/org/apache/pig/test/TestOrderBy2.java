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

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestOrderBy2 {
    private PigServer pig;

    @Before
    public void setUp() throws Exception {
        pig = new PigServer(ExecType.LOCAL);

        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        Data data = resetData(pig);
        data.set("foo1", s, genDataSetFile1());

        s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        data.set("foo2", s, genDataSetFile2());

        s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        data.set("foo3", s, genDataSetFile3());
    }

    //////////////////////// Simple Order Tests ///////////////////////

    @Test
    public void testTopLevelOrderBy_Col0_ASC_NoUsing() throws Exception {
        runTest("myid = order (load 'foo1' using mock.Storage()) BY $0;",
                new int[] { 0 } ,
                new boolean[] { false } ) ;
    }

    @Test
    public void testTopLevelOrderBy_Col0_DESC_NoUsing() throws Exception {
        runTest("myid = order (load 'foo1' using mock.Storage()) BY $0 DESC ;",
                new int[] { 0 } ,
                new boolean[] { true } ) ;
    }

    @Test
    public void testTopLeveleeOrderBy_Col1_ASC_NoUsing() throws Exception {
        runTest("myid = order (load 'foo1' using mock.Storage()) BY $1 ASC ;",
                new int[] { 1 } ,
                new boolean[] { false } ) ;
    }

    @Test
    public void testTopLeveleeOrderBy_Col1_DESC_NoUsing() throws Exception {
        runTest("myid = order (load 'foo1' using mock.Storage()) BY $1 DESC ;",
                new int[] { 1 } ,
                new boolean[] { true } ) ;
    }

    @Test
    public void testTopLeveleeOrderBy_Col0Col1_NoUsing() throws Exception {
        runTest("myid = order (load 'foo2' using mock.Storage()) BY $0, $1 ;",
                new int[] { 0, 1 } ,
                new boolean[] { false, false } ) ;
    }


    @Test
    public void testTopLeveleeOrderBy_Col0Col1_DESC_NoUsing() throws Exception {
        runTest("myid = order (load 'foo2' using mock.Storage()) BY $0 DESC, $1 DESC ;",
                new int[] { 0, 1 } ,
                new boolean[] { true, true } ) ;
    }

    @Test
    public void testTopLeveleeOrderBy_Col1Col0_NoUsing() throws Exception {
        runTest("myid = order (load 'foo3' using mock.Storage()) BY $1, $0 ;",
                new int[] { 1, 0 } ,
                new boolean[] { false, false } ) ;
    }


    @Test
    public void testTopLeveleeOrderBy_Col1Col0_DESC_NoUsing() throws Exception {
        runTest("myid = order (load 'foo3' using mock.Storage()) BY $1 DESC, $0 DESC ;",
                new int[] { 1, 0 } ,
                new boolean[] { true, true } ) ;
    }

    @Test
    public void testTopLeveleeOrderBy_Col1Col0_ASCDESC_NoUsing() throws Exception {
        runTest("myid = order (load 'foo3' using mock.Storage()) BY $1 ASC, $0 DESC ;",
                new int[] { 1, 0 } ,
                new boolean[] { false, true } ) ;
    }


    //////////////////////// Star Order Tests ///////////////////////

    @Test
    public void testTopLevelOrderBy_Star_NoUsing() throws Exception {
        runTest("myid = order (load 'foo3' using mock.Storage()) BY * ; ",
                new int[] { 0, 1 } ,
                new boolean[] { false, false } ) ;
    }

    @Test
    public void testTopLevelOrderBy_Star_DESC_NoUsing() throws Exception {
        runTest("myid = order (load 'foo3' using mock.Storage()) BY * DESC ; ",
                new int[] { 0, 1 } ,
                new boolean[] { true, true } ) ;
    }

    //////////////////////// HELPERS ///////////////////////////////

    /***
     * Check if the given dataset is properly sorted
     * @param dataIter the dataset to be checked
     * @param sortCols list of sorted columns
     * @param descFlags flags (true=desc, false=asc)
     */
    private void checkOrder(Iterator<Tuple> dataIter,
                            int[] sortCols,
                            boolean[] descFlags)
                                        throws ExecException {

        assertEquals("checkOrder params have to be of the same size",
                                        sortCols.length, descFlags.length);

        List<String> error = new ArrayList<String>() ;

        Tuple lastTuple = null ;

        while (dataIter.hasNext()) {

            Tuple current = dataIter.next() ;
            System.out.println(current.toString()) ;

            if (lastTuple != null) {
                // do the actual check
                for(int i=0; i < sortCols.length ; i++) {

                    int colIdx = sortCols[i] ;
                    int lastInt = DataType.toInteger(lastTuple.get(colIdx)) ;
                    int curInt = DataType.toInteger(current.get(colIdx)) ;

                    // If it's ascending
                    if (!descFlags[i]) {
                        if (curInt < lastInt) {
                            error.add("Not ASC") ;
                        }
                        // if this happens, no need to check further
                        if (curInt > lastInt) {
                            break ;
                        }
                    }
                    // If it's descending
                    else {
                        if (curInt > lastInt) {
                            error.add("Not DESC") ;
                        }
                        // if this happens, no need to check further
                        if (curInt < lastInt) {
                            break ;
                        }
                    }
                }
            }

            lastTuple = current ;
        }

        assertEquals(0, error.size());

    }

    /***
     * Main helper for running a sort test
     */
    private void runTest(String query,
                         int[] sortCols,
                         boolean[] descFlags)
                                        throws Exception {
        System.out.println(query) ;
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("myid");
        checkOrder(it, sortCols, descFlags) ;
    }


    /***
     * For generating a sample dataset
     */
    private List<Tuple> genDataSetFile1() throws IOException {
        int dataLength = 256;
        List<Tuple> tuples = Lists.newArrayList();

        DecimalFormat formatter = new DecimalFormat("0000000");
        for (int i = 0; i < dataLength; i++) {
            tuples.add(tuple(formatter.format(i), formatter.format(dataLength - i - 1)));
        }

        return tuples;
    }


    /***
     * For generating a sample dataset
     */
    private List<Tuple> genDataSetFile2() throws IOException {
        int dataLength = 256;
        List<Tuple> tuples = Lists.newArrayList();

        DecimalFormat formatter = new DecimalFormat("0000000");
        for (int i = 0; i < dataLength; i++) {
            tuples.add(tuple(formatter.format(i % 20), formatter.format(dataLength - i - 1)));
        }

        return tuples;
    }

    /***
     * For generating a sample dataset
     */
    private List<Tuple> genDataSetFile3() throws IOException {

        int dataLength = 256;
        List<Tuple> tuples = Lists.newArrayList();

        DecimalFormat formatter = new DecimalFormat("0000000");
        for (int i = 0; i < dataLength; i++) {
            tuples.add(tuple(formatter.format(i), formatter.format(i % 20)));
        }

        return tuples;
    }
}