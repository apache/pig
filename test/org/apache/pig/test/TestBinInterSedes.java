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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.BinInterSedes;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.InterSedes;
import org.apache.pig.data.InterSedesFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.InterRecordReader;
import org.apache.pig.impl.util.TupleFormat;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

public class TestBinInterSedes {
    private static final TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static final BinInterSedes bis = new BinInterSedes();
    private static final Random random = new Random(100L);

    @Test
    public void testTupleWriteRead1() throws IOException {
            //create a tuple with columns of different type
            Tuple tuplein = TupleFactory.getInstance().newTuple(7);
            tuplein.set(0, 12);
            Map<String, String> map = new HashMap<String, String>();
            map.put("pig", "scalability");
            tuplein.set(1, map);
            tuplein.set(2, null);
            tuplein.set(3, 12L);
            tuplein.set(4, 1.2F);

            Tuple innerTuple = TupleFactory.getInstance().newTuple(1);
            innerTuple.set(0, "innerTuple");
            tuplein.set(5, innerTuple);
            DataBag bag = BagFactory.getInstance().newDefaultBag();
            bag.add(innerTuple);
            tuplein.set(6, bag);

            testTupleSedes(tuplein);
            
            assertEquals(
                    "(12,[pig#scalability],,12,1.2,(innerTuple),{(innerTuple)})",
                    TupleFormat.format(tuplein));
    }
    
    
    /**
     * test sedes of int of diff sizes
     * @throws IOException
     */
    @Test
    public void testTupleWriteReadIntDiffSizes() throws IOException {
            //create a tuple with integer columns of different sizes
            Tuple tuple = TupleFactory.getInstance().newTuple();
            tuple.append(new Integer(0)); //boolean rep
            tuple.append(new Integer(1)); //boolean rep
            tuple.append(new Integer(125));  //fits into byte
            tuple.append(new Integer(1024)); //fits into short
            tuple.append(new Integer(1024*1024*1024)); //fits into int (=~ 2 ^30)
            
            testTupleSedes(tuple);
    }
    
    /**
     * test sedes of bytearray, string of diff sizes
     * @throws IOException
     */
    @Test
    public void testTupleWriteReadByteArrStringDiffSizes() throws IOException {
            // tuple with ByteArray and strings of different sizes
            Tuple tuple = TupleFactory.getInstance().newTuple();
            byte [] tinyBA = new byte[10];
            byte [] smallBA = new byte[1000];
            byte [] largeBytearray = new byte[80000];
            //init large bytearray with non 0 values, its going to be used as
            //string as well
            for(int i=0; i< largeBytearray.length; i++){
                largeBytearray[i] = '1';
            }
            tuple.append(new DataByteArray(tinyBA));
            tuple.append(new DataByteArray(smallBA));
            tuple.append(new DataByteArray(largeBytearray));

            testTupleSedes(tuple);
            
            // add strings of different sizes
            tuple = TupleFactory.getInstance().newTuple();
            tuple.append(new String(""));
            tuple.append(new String("x"));
            //string larger than 32k
            tuple.append(new String(largeBytearray));

            testTupleSedes(tuple);
        }

    /**
     * test sedes  with bags of diff sizes
     * @throws IOException
     */
    @Test
    public void testTupleWriteReadBagDiffSizes() throws IOException {
            // tuple with ByteArray and strings of different sizes
            Tuple tuple = TupleFactory.getInstance().newTuple();
            DataBag tinyBag = createBag(10); 
            DataBag smallBag = createBag(1000); 
            DataBag largeBag = createBag(100*1024); 

            tuple.append(tinyBag);
            tuple.append(smallBag);
            tuple.append(largeBag);

            testTupleSedes(tuple);
    }

    /*
     * test sedes of long of diff sizes
     * @throws IOException
     */
    @Test
    public void testTupleWriteReadLongDiffSizes() throws IOException {
            Random r = new Random(100L);

            Tuple tuple = TupleFactory.getInstance().newTuple();

            tuple.append(new Long(0));
            tuple.append(new Long(1));
            tuple.append(new Long(-1));
            tuple.append(new Long(300));
            tuple.append(new Long(600));
            tuple.append(new Long(10000));
            tuple.append(new Long(-10000));
            tuple.append(new Long(5000000000000000000L));
            tuple.append(new Long(-5000000000000000000L));

            for (int i = 0; i < 100000; i++) {
                tuple.append(new Long(r.nextLong()));
            }

            testTupleSedes(tuple);
    }

    /**
     * create bag having given number of tuples
     * @param size
     * @return
     */
    private DataBag createBag(int size) {
        Tuple innerTuple = TupleFactory.getInstance().newTuple();
        innerTuple.append(Integer.valueOf(1));
        DataBag bag = BagFactory.getInstance().newDefaultBag();
        for(int i=0; i<size; i++){
            bag.add(innerTuple);
        }
        return bag;
    }

    /**
     * test sedes tuple of diff sizes
     * @throws IOException
     */
    @Test
    public void testTupleWriteReadDiffSizes() throws IOException {
            // tuple with ByteArray and strings of different sizes
            Tuple smallTuple = createTupleWithManyCols(1000);
            testTupleSedes(smallTuple);
            
            Tuple largeTuple = createTupleWithManyCols(100*1000);
            testTupleSedes(largeTuple);
    }

    private Tuple createTupleWithManyCols(int size) {
        Tuple t = TupleFactory.getInstance().newTuple(size);
        Integer col = Integer.valueOf(1);
        for(int i=0; i<size; i++){
            t.append(col);
        }
        return t;
    }
    
    /**
     * test sedes  with maps of diff sizes
     * @throws IOException
     */
    @Test
    public void testTupleWriteReadMapDiffSizes() throws IOException {
            // tuple with ByteArray and strings of different sizes
            Tuple tuple = TupleFactory.getInstance().newTuple();
            Map<String, Object> tinyMap = createMap(10);
            Map<String, Object> smallMap = createMap(1000);
            Map<String, Object> largeMap = createMap(100*1024);
            tuple.append(tinyMap);
            tuple.append(smallMap);
            tuple.append(largeMap);

            testTupleSedes(tuple);
    }

    private Map<String, Object> createMap(int size) {
        Map<String,Object> map = new HashMap<String, Object>(size);
        String key = String.valueOf('A');
        Integer val = Integer.valueOf(1);
        for(int i=0; i<size; i++){
            map.put(key, val);
        }
        return map;
    }

    /**
     * Write the serialized tuple to DataOutputStream and get deserialized tuple
     * compare the given tuple and deserialized tuple to make sure they are
     * equivalent
     * @param tuple
     * @throws IOException
     */
    private void testTupleSedes(Tuple tuple) throws IOException {
        
        InterSedes sedes = InterSedesFactory.getInterSedesInstance();
        
        //write the tuple into a DataOutputStream on bytearray 
        ByteArrayOutputStream bout = new ByteArrayOutputStream(10*1024*1024);// 10 MB
        DataOutputStream out = new DataOutputStream(bout);
        sedes.writeDatum(out, tuple);
        out.flush();
        
        //read tuple back 
        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
        DataInputStream in = new DataInputStream(bin);
        Tuple tupleout = (Tuple)sedes.readDatum(in);
        
        assertEquals(" Tuple before and after serialization are same ",
                tuple, tupleout);
        
    }

    /**
     * See PIG-2936. The purpose of this test is to ensure that Tuples are being serialized in
     * the specific way that we expect.
     */
    @Test
    public void testTupleSerializationSpecific() throws Exception {
        byte[] flags = {
                BinInterSedes.TUPLE_0,
                BinInterSedes.TUPLE_1,
                BinInterSedes.TUPLE_2,
                BinInterSedes.TUPLE_3,
                BinInterSedes.TUPLE_4,
                BinInterSedes.TUPLE_5,
                BinInterSedes.TUPLE_6,
                BinInterSedes.TUPLE_7,
                BinInterSedes.TUPLE_8,
                BinInterSedes.TUPLE_9,
        };

        for (int i = 0; i < flags.length; i++) {
            Tuple t = mTupleFactory.newTuple(i);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutput out = new DataOutputStream(baos);
            out.writeByte(flags[i]);

            for (int j = 0; j < i; j++) {
                Integer val = Integer.valueOf(random.nextInt());
                bis.writeDatum(out, val);
                t.set(j, val);
            }
    
            testSerTuple(t, baos.toByteArray());
        }
    }


    /*
      The following tests are intended to verify the reading and writing of intermediate files of Pig (of InterStorage)
      The test records are 11,14,22,14 bytes long.
      Below I illustrate the splits in rows, records as [] with size and sync markers with [M]
     */

    /**
     * One sync marker only and three splits where the records overlap the splitends.
     * (Reader of 1st split should read every record, readers of 2nd and 3rd splits should read no records.)
     * [M(10)] [11] [11-
     *  -3] [ 22 ] [7-
     *  -7]
     *
     * @throws Exception
     */
    @Test
    public void testSyncMarkerOneMarkerAtBeginningOnly() throws Exception {
        testInterStorageSyncMarker(32, 10, 2000L);
    }

    /**
     * Some sync markers are positioned so that they begin at a split's end and they end in the next split's beginning.
     * (Reader of a split has to read until the next sync marker that has all its bytes in a following split.)
     * @throws Exception
     */
    @Test
    public void testSyncMarkerOverlappingMarker() throws Exception {
        /*
         * [M(16)] [11] [M(16)] [5-
         * -9] [M(16)] [ 22 ] [M(1-
         * -15)] [14]
         */
        testInterStorageSyncMarker(48, 16, 10L);
        /*
         * [M(4)] [ 4-
         *     -7] [1-
         *    - 8 -
         *   -5] [M(3-
         * -1)] [ 7-
         *    - 8 -
         *   -7] [M(1-
         * -3)] [  5-
         *    - 8 -
         *  -1]
         */
        testInterStorageSyncMarker(8, 4, 20L);
    }

    /**
     * No illustration for this one to save characters .. Sync size is over 3 times the size of split size, this is an
     * extremely unlikely scenario. Markers here span over 4 splits.
     * @throws Exception
     */
    @Test
    public void testSyncMarkerLongerMarkerThanSplit() throws Exception {
        testInterStorageSyncMarker(5, 16, 20L);
    }

    /**
     * A sync marker is positioned at exactly the end of the first split without overlapping into the next one.
     * (Reader of the 1st split should read past it and into the 2nd split until next marker.)
     *
     * [M(2)] [11] [14] [M(2)]
     * [  22  ] [M(2)] [ 5-
     * -9]
     * @throws Exception
     */
    @Test
    public void testSyncMarkerMarkerOnSplitEnd() throws Exception {
        testInterStorageSyncMarker(29, 2, 20L);
    }

    /**
     * A sync marker is positioned at exactly the beginning of the 3rd split.
     * (Reader of the 1st split should read 1st and 2nd splits fully, reader of 2nd split should read no records.)
     *
     * [M(3)] [11]
     * [    14   ]
     * [M(3) [11-
     *   -11 ] [3-
     *      -11 ]
     * @throws Exception
     */
    @Test
    public void testSyncMarkerMarkerOnSplitBeginning() throws Exception {
        testInterStorageSyncMarker(14, 3, 25L);
    }

    private void testInterStorageSyncMarker(int maxSplitSize, int syncSize, long syncInterval) throws Exception {
        PigServer pigServer = new PigServer(Util.getLocalTestMode(), new Properties());

        Properties pigProperties = pigServer.getPigContext().getProperties();
        pigProperties.setProperty("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(maxSplitSize));
        pigProperties.setProperty(PigConfiguration.PIG_INTERSTORAGE_SYNCMARKER_SIZE, String.valueOf(syncSize));
        pigProperties.setProperty(PigConfiguration.PIG_INTERSTORAGE_SYNCMARKER_INTERVAL, String.valueOf(syncInterval));

        //Without proper random record markers 0x01020327 would be identified as a marker and 0x50 as an unknown datatype
        //ByteBuffer.wrap(new byte[]{0x01, 0x02, 0x03, 0x27, 0x50, 0x0, 0x0, 0x0}).getLong() => 72624011372134400

        String[] inputData = new String[]{"apple\t1\t1","orange\t2\t2","kiwi\t16909095\t72624011372134400","orange\t4\t4"};
        String[] expected = new String[] {"(apple,1,1)","(orange,2,2)","(kiwi,16909095,72624011372134400)","(orange,4,4)"};
        File inputFile = Util.createInputFile("interStorageInput", "", inputData);
        inputFile.deleteOnExit();

        //Without proper random record markers 0x01020327 would be identified as a marker and although no errors are
        // thrown the result will contain incorrect schema and values past this number
        //ByteBuffer.wrap(new byte[]{0x01, 0x02, 0x03, 0x27, 0x01, 0x0, 0x0, 0x0}).getLong() => 72624010046734336

        String[] inputData2 = new String[]{"apple\t1\t1","orange\t2\t2","kiwi\t16909095\t72624010046734336","orange\t4\t4"};
        String[] expected2 = new String[] {"(apple,1,1)","(orange,2,2)","(kiwi,16909095,72624010046734336)","(orange,4,4)"};
        File inputFile2 = Util.createInputFile("interStorageInput2", "", inputData2);
        inputFile2.deleteOnExit();

        File binOutputdir = new File("build/test/interStorageTest");
        Util.deleteDirectory(binOutputdir);

        String script = "A = LOAD '"+inputFile.getAbsolutePath()+"' AS (name:chararray, cnt:int, cnt2:long);\n" +
                "STORE A INTO '"+binOutputdir.getAbsolutePath()+"' USING org.apache.pig.impl.io.InterStorage();\n" +
                "\n" +
                "B = LOAD '"+binOutputdir.getAbsolutePath()+"' USING org.apache.pig.impl.io.InterStorage();\n";

        pigServer.registerQuery(script);
        Iterator<Tuple> it = pigServer.openIterator("B");
        Util.checkQueryOutputsAfterSortRecursive(it, expected,
                org.apache.pig.newplan.logical.Util.translateSchema(pigServer.dumpSchema("B")));

        Util.deleteDirectory(binOutputdir);

        pigServer.registerQuery(script.replaceAll(inputFile.getAbsolutePath(), inputFile2.getAbsolutePath()));
        it = pigServer.openIterator("B");
        Util.checkQueryOutputsAfterSortRecursive(it, expected2,
                org.apache.pig.newplan.logical.Util.translateSchema(pigServer.dumpSchema("B")));

    }

    /**
     * Tests all combination where:
     * sync marker is {x, y, 4}
     * data is {127, -2, 2, z, x, y, 4, 1, 2, 3}
     * x,y,z in [-128,127]
     * This means that a sync marker has to be found in all iterations (total=16,777,216)
     * @throws Exception
     */
    @Test
    public void testPrefixSyncMarkers() throws Exception {
        long defaultInterval = PigConfiguration.PIG_INTERSTORAGE_SYNCMARKER_INTERVAL_DEFAULT;

        for (int b0 = -128; b0 <= 127; b0++) {
            for (int b1 = -128; b1 <= 127; b1++) {
                for (int b2 = -128; b2 <= 127; b2++) {
                    byte[] syncMarker = new byte[]{(byte) b0, (byte) b1, (byte)4};
                    byte[] data = new byte[]{127, -1, 2, (byte) b2, (byte) b0, (byte) b1, 4, 1, 2, 3};

                    ByteArrayInputStream bi = new ByteArrayInputStream(data);
                    BufferedPositionedInputStream bpi = new BufferedPositionedInputStream(bi);

                    InterRecordReader reader = new InterRecordReader(syncMarker.length, defaultInterval);
                    Whitebox.setInternalState(reader, "syncMarker", syncMarker);
                    Whitebox.setInternalState(reader, "end", data.length);
                    Whitebox.setInternalState(reader, "in", bpi);

                    try {
                        boolean ret = reader.skipUntilMarkerOrSplitEndOrEOF();
                        assertTrue("Marker should have been found: " + "marker: " +
                                Arrays.toString(syncMarker) + " , data: " + Arrays.toString(data),ret);
                    } finally {
                        bpi.close();
                    }

                }
            }
        }
    }

    private void testSerTuple(Tuple t, byte[] expected) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);

        bis.writeDatum(out, t);
    
        Tuple t2 = (Tuple) bis.readDatum(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));

        assertEquals(t, t2);
    }
}
