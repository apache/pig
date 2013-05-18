/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestFRJoin {
    private static final String INPUT_FILE = "testFrJoinInput.txt";
    private static final String INPUT_FILE2 = "testFrJoinInput2.txt";
    private PigServer pigServer;
    private static MiniCluster cluster = MiniCluster.buildCluster();

    public TestFRJoin() throws ExecException, IOException {
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }

    @Before
    public void setUp() throws Exception {
        int LOOP_SIZE = 2;
        String[] input = new String[2 * LOOP_SIZE];
        int k = 0;
        for(int i = 1; i <= LOOP_SIZE; i++) {
            String si = i + "";
            for (int j = 1; j <= LOOP_SIZE; j++)
                input[k++] = si + "\t" + j;
        }
        Util.createInputFile(cluster, INPUT_FILE, input);

        String[] input2 = new String[2 * (LOOP_SIZE / 2)];
        k = 0;
        for (int i = 1; i <= LOOP_SIZE / 2; i++) {
            String si = i + "";
            for (int j = 1; j <= LOOP_SIZE / 2; j++)
                input2[k++] = si + "\t" + j;
        }
        Util.createInputFile(cluster, INPUT_FILE2, input2);
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }

    @After
    public void tearDown() throws Exception {
        Util.deleteFile(cluster, INPUT_FILE);
        Util.deleteFile(cluster, INPUT_FILE2);
    }

    public static class FRJoin extends EvalFunc<DataBag> {
        String repl;
        int keyField;
        boolean isTblSetUp = false;
        Hashtable<String, DataBag> replTbl = new Hashtable<String, DataBag>();

        public FRJoin() {
        }

        public FRJoin(String repl) {
            this.repl = repl;
        }

        @Override
        public DataBag exec(Tuple input) throws IOException {
            if (!isTblSetUp) {
                setUpHashTable();
                isTblSetUp = true;
            }
            String key = (String)input.get(keyField);
            if (!replTbl.containsKey(key))
                return BagFactory.getInstance().newDefaultBag();
            return replTbl.get(key);
        }

        private void setUpHashTable() throws IOException {
            FileSpec replFile = new FileSpec(repl, new FuncSpec(PigStorage.class.getName() + "()"));
            POLoad ld = new POLoad(new OperatorKey("Repl File Loader", 1L), replFile);
            PigContext pc = new PigContext(ExecType.MAPREDUCE, PigMapReduce.sJobConfInternal.get());
            pc.connect();

            ld.setPc(pc);
            Tuple dummyTuple = null;
            for (Result res = ld.getNextTuple(); res.returnStatus != POStatus.STATUS_EOP; res = ld
                    .getNextTuple()) {
                Tuple tup = (Tuple)res.result;
                LoadFunc lf = ((LoadFunc)pc.instantiateFuncFromSpec(ld.getLFile().getFuncSpec()));
                String key = lf.getLoadCaster().bytesToCharArray(
                        ((DataByteArray)tup.get(keyField)).get());
                Tuple csttup = TupleFactory.getInstance().newTuple(2);
                csttup.set(0, key);
                csttup.set(1, lf.getLoadCaster().bytesToInteger(((DataByteArray)tup.get(1)).get()));
                DataBag vals = null;
                if (replTbl.containsKey(key)) {
                    vals = replTbl.get(key);
                }
                else {
                    vals = BagFactory.getInstance().newDefaultBag();
                    replTbl.put(key, vals);
                }
                vals.add(csttup);
            }
        }

    }

    @Test
    public void testSortFRJoin() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("D = ORDER A by y;");
        pigServer.registerQuery("E = ORDER B by y;");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance()
                .newDefaultBag();
        {
            pigServer.registerQuery("C = join D by $0, E by $0 using \'replicated\';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join D by $0, E by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        assertEquals(dbfrj.size(), dbshj.size());
        assertTrue(TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testDistinctFRJoin() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("D = distinct A ;");
        pigServer.registerQuery("E = distinct B ;");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance()
                .newDefaultBag();
        {
            pigServer.registerQuery("C = join D by $0, E by $0 using 'replicated';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join D by $0, E by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        assertEquals(dbfrj.size(), dbshj.size());
        assertTrue(TestHelper.compareBags(dbfrj, dbshj));
      }

    @Test
    public void testUDFFRJ() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:chararray,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (x:chararray,y:int);");

        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance()
                .newDefaultBag();
        {
            String fSpec = FRJoin.class.getName() + "('" + INPUT_FILE + "')";
            pigServer.registerFunction("FRJ", new FuncSpec(fSpec));
            pigServer.registerQuery("C = foreach A generate *, flatten(FRJ(*));");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by $0, B by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        assertTrue(dbfrj.size() > 0);
        assertTrue(dbshj.size() > 0);
        assertTrue(TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testFRJoinOut1() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance()
                .newDefaultBag();
        {
            pigServer.registerQuery("C = join A by $0, B by $0 using 'replicated';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by $0, B by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        assertTrue(dbfrj.size() > 0);
        assertTrue(dbshj.size() > 0);
        assertTrue(TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testFRJoinOut2() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "';");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "';");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance()
                .newDefaultBag();
        {
            pigServer.registerQuery("C = join A by $0, B by $0 using 'replicated';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by $0, B by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        assertTrue(dbfrj.size() > 0);
        assertTrue(dbshj.size() > 0);
        assertTrue(TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testFRJoinOut3() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("C = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance()
                .newDefaultBag();
        {
            pigServer.registerQuery("D = join A by $0, B by $0, C by $0 using 'replicated';");
            Iterator<Tuple> iter = pigServer.openIterator("D");

            while (iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("D = join A by $0, B by $0, C by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("D");

            while (iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        assertTrue(dbfrj.size() > 0);
        assertTrue(dbshj.size() > 0);
        assertTrue(TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testFRJoinOut4() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "';");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "';");
        pigServer.registerQuery("C = LOAD '" + INPUT_FILE + "';");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance()
                .newDefaultBag();
        {
            pigServer.registerQuery("D = join A by $0, B by $0, C by $0 using 'replicated';");
            Iterator<Tuple> iter = pigServer.openIterator("D");

            while (iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("D = join A by $0, B by $0, C by $0;");
            Iterator<Tuple> iter = pigServer.openIterator("D");

            while (iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        assertTrue(dbfrj.size() > 0);
        assertTrue(dbshj.size() > 0);
        assertTrue(TestHelper.compareBags(dbfrj, dbshj));
    }



    @Test
    public void testFRJoinOut5() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance()
                .newDefaultBag();
        {
            pigServer.registerQuery("C = join A by ($0,$1), B by ($0,$1) using 'replicated';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by ($0,$1), B by ($0,$1);");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        assertTrue(dbfrj.size() > 0);
        assertTrue(dbshj.size() > 0);
        assertTrue(TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testFRJoinOut6() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "';");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "';");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance()
                .newDefaultBag();
        {
            pigServer.registerQuery("C = join A by ($0,$1), B by ($0,$1) using 'replicated';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by ($0,$1), B by ($0,$1);");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while (iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        assertTrue(dbfrj.size() > 0);
        assertTrue(dbshj.size() > 0);
        assertTrue(TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testFRJoinOut7() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance()
                .newDefaultBag();
        {
            pigServer.registerQuery("C = join A by $0, B by $0 using 'replicated';");
            pigServer.registerQuery("D = join A by $1, B by $1 using 'replicated';");
            pigServer.registerQuery("E = union C,D;");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while (iter.hasNext()) {
                dbfrj.add(iter.next());
            }
        }
        {
            pigServer.registerQuery("C = join A by $0, B by $0;");
            pigServer.registerQuery("D = join A by $1, B by $1;");
            pigServer.registerQuery("E = union C,D;");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while (iter.hasNext()) {
                dbshj.add(iter.next());
            }
        }
        assertTrue(dbfrj.size() > 0);
        assertTrue(dbshj.size() > 0);
        assertTrue(TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testFRJoinOut8() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE2 + "' as (x:int,y:int);");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance()
                .newDefaultBag();
        Map<String, Tuple> hashFRJoin = new HashMap<String, Tuple>();
        Map<String, Tuple> hashJoin = new HashMap<String, Tuple>();
        {
            pigServer.registerQuery("C = join A by $0 left, B by $0 using 'replicated';");
            pigServer.registerQuery("D = join A by $1 left, B by $1 using 'replicated';");
            pigServer.registerQuery("E = union C,D;");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while (iter.hasNext()) {
                Tuple tuple = iter.next();
                String Key = tuple.toDelimitedString(",");
                hashFRJoin.put(Key, tuple);
                dbfrj.add(tuple);

            }
        }
        {
            pigServer.registerQuery("C = join A by $0 left, B by $0;");
            pigServer.registerQuery("D = join A by $1 left, B by $1;");
            pigServer.registerQuery("E = union C,D;");
            Iterator<Tuple> iter = pigServer.openIterator("E");
            while (iter.hasNext()) {
                Tuple tuple = iter.next();
                String Key = tuple.toDelimitedString(",");
                hashJoin.put(Key, tuple);
                dbshj.add(tuple);
            }
        }
        assertTrue(dbfrj.size() > 0);
        assertTrue(dbshj.size() > 0);
        assertTrue(TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testFRJoinOut9() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE2 + "' as (x:int,y:int);");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance()
                .newDefaultBag();
        Map<String, Tuple> hashFRJoin = new HashMap<String, Tuple>();
        Map<String, Tuple> hashJoin = new HashMap<String, Tuple>();
        {
            pigServer.registerQuery("C = join A by $0 left, B by $0 using 'repl';");
            pigServer.registerQuery("D = join A by $1 left, B by $1 using 'repl';");
            pigServer.registerQuery("E = union C,D;");
            Iterator<Tuple> iter = pigServer.openIterator("E");

            while (iter.hasNext()) {
                Tuple tuple = iter.next();
                String Key = tuple.toDelimitedString(",");
                hashFRJoin.put(Key, tuple);
                dbfrj.add(tuple);

            }
        }
        {
            pigServer.registerQuery("C = join A by $0 left, B by $0;");
            pigServer.registerQuery("D = join A by $1 left, B by $1;");
            pigServer.registerQuery("E = union C,D;");
            Iterator<Tuple> iter = pigServer.openIterator("E");
            while (iter.hasNext()) {
                Tuple tuple = iter.next();
                String Key = tuple.toDelimitedString(",");
                hashJoin.put(Key, tuple);
                dbshj.add(tuple);
            }
        }
        assertTrue(dbfrj.size() > 0);
        assertTrue(dbshj.size() > 0);
        assertTrue(TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testFRJoinSch1() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        Schema frjSch = null, shjSch = null;
        pigServer.registerQuery("C = join A by $0, B by $0 using 'repl';");
        frjSch = pigServer.dumpSchema("C");
        pigServer.registerQuery("C = join A by $0, B by $0;");
        shjSch = pigServer.dumpSchema("C");
        assertEquals(shjSch, frjSch);
    }

    @Test
    public void testFRJoinSch2() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "';");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "';");
        Schema frjSch = null, shjSch = null;
        pigServer.registerQuery("C = join A by $0, B by $0 using 'repl';");
        frjSch = pigServer.dumpSchema("C");
        pigServer.registerQuery("C = join A by $0, B by $0;");
        shjSch = pigServer.dumpSchema("C");
        assertNull(shjSch);
    }

    @Test
    public void testFRJoinSch3() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("C = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        Schema frjSch = null, shjSch = null;
        pigServer.registerQuery("D = join A by $0, B by $0, C by $0 using 'repl';");
        frjSch = pigServer.dumpSchema("D");
        pigServer.registerQuery("D = join A by $0, B by $0, C by $0;");
        shjSch = pigServer.dumpSchema("D");
        assertEquals(shjSch, frjSch);
    }

    @Test
    public void testFRJoinSch4() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "';");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "';");
        pigServer.registerQuery("C = LOAD '" + INPUT_FILE + "';");
        Schema frjSch = null, shjSch = null;
        pigServer.registerQuery("D = join A by $0, B by $0, C by $0 using 'repl';");
        frjSch = pigServer.dumpSchema("D");
        pigServer.registerQuery("D = join A by $0, B by $0, C by $0;");
        shjSch = pigServer.dumpSchema("D");
        assertNull(shjSch);
    }

    @Test
    public void testFRJoinSch5() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        Schema frjSch = null, shjSch = null;
        pigServer.registerQuery("C = join A by ($0,$1), B by ($0,$1) using 'repl';");
        frjSch = pigServer.dumpSchema("C");
        pigServer.registerQuery("C = join A by ($0,$1), B by ($0,$1);");
        shjSch = pigServer.dumpSchema("C");
        assertEquals(shjSch, frjSch);
    }

    @Test
    public void testFRJoinSch6() throws IOException {
        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "';");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "';");
        Schema frjSch = null, shjSch = null;
        pigServer.registerQuery("C = join A by ($0,$1), B by ($0,$1) using 'repl';");
        frjSch = pigServer.dumpSchema("C");
        pigServer.registerQuery("C = join A by ($0,$1), B by ($0,$1);");
        shjSch = pigServer.dumpSchema("C");
        assertNull(shjSch);
    }
}

