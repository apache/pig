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
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.test.utils.GenPhyOp;
import org.apache.pig.test.utils.TestHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *  Start Plan - --4430968173902769765
 *  |
 *  |---Filter - -3398344075398026874
 *  |   |
 *  |   |---For Each - --3361918026493682288
 *  |       |
 *  |       |---Load - --7021057205133896020
 *  |
 *  |---Filter - -4449068980110713814
 *      |
 *      |---For Each - -7192652407897311774
 *          |
 *          |---Load - --3816247505117325386
 *
 *  Tests the Start Plan operator with the above plan.
 *  The verification is done as follows:
 *  Both loads load the same file(/etc/passwd).
 *  The filters cover the input. Here the filters used
 *  are $2<=50 & $2>50
 *  The bags coming out of Start Plan is checked against
 *  the projected input bag.
 *  Since types are not supported yet, there is an explicit
 *  conversion from DataByteArray to native types for computation
 *  and back to DataByteArray for comparison with input.
 */

@RunWith(JUnit4.class)
public class TestUnion extends junit.framework.TestCase {
    POUnion sp;
    DataBag expBag;
    PigContext pc;
    PigServer pigServer;

    @Override
    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL, new Properties());
        pc = pigServer.getPigContext();
        pc.connect();
        GenPhyOp.setPc(pc);
        POLoad ld1 = GenPhyOp.topLoadOp();
        String curDir = System.getProperty("user.dir");
        String inpDir = curDir + File.separatorChar + "test/org/apache/pig/test/data/InputFiles/";
        FileSpec fSpec = new FileSpec(Util.generateURI(Util.encodeEscape(inpDir + "passwd"), pc), new FuncSpec(PigStorage.class.getName() , new String[]{":"}));
        ld1.setLFile(fSpec);

        POLoad ld2 = GenPhyOp.topLoadOp();
        ld2.setLFile(fSpec);

        POFilter fl1 = GenPhyOp.topFilterOpWithProj(1, 50, GenPhyOp.LTE);

        POFilter fl2 = GenPhyOp.topFilterOpWithProj(1, 50, GenPhyOp.GT);

        int[] flds = {0,2};
        Tuple sample = new DefaultTuple();
        sample.append(new String("S"));
        sample.append(new String("x"));
        sample.append(new Integer("10"));
        sample.append(new Integer("20"));
        sample.append(new String("S"));
        sample.append(new String("x"));
        sample.append(new String("S"));
        sample.append(new String("x"));

        POForEach fe1 = GenPhyOp.topForEachOPWithPlan(flds , sample);

        POForEach fe2 = GenPhyOp.topForEachOPWithPlan(flds , sample);

        sp = GenPhyOp.topUnionOp();

        PhysicalPlan plan = new PhysicalPlan();

        plan.add(ld1);
        plan.add(ld2);
        plan.add(fl1);
        plan.add(fl2);
        plan.add(fe1);
        plan.add(fe2);
        plan.add(sp);

        plan.connect(ld1, fe1);
        plan.connect(fe1, fl1);
        plan.connect(ld2, fe2);
        plan.connect(fe2, fl2);
        plan.connect(fl1, sp);
        plan.connect(fl2, sp);

        /*PlanPrinter ppp = new PlanPrinter(plan);
        ppp.visit();*/


        POLoad ld3 = GenPhyOp.topLoadOp();
        ld3.setLFile(fSpec);
        DataBag fullBag = DefaultBagFactory.getInstance().newDefaultBag();
        Tuple t=null;
        for(Result res=ld3.getNext(t);res.returnStatus!=POStatus.STATUS_EOP;res=ld3.getNext(t)){
            fullBag.add((Tuple)res.result);
        }

        int[] fields = {0,2};
        expBag = TestHelper.projectBag(fullBag, fields);
    }

    @Override
    @After
    public void tearDown() throws Exception {
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
    }

    private Tuple castToDBA(Tuple in) throws ExecException{
        Tuple res = new DefaultTuple();
        for (int i=0;i<in.size();i++) {
            DataByteArray dba = new DataByteArray(in.get(i).toString());
            res.append(dba);
        }
        return res;
    }

    @Test
    public void testGetNextTuple() throws ExecException, IOException {
        Tuple t = null;
        DataBag outBag = DefaultBagFactory.getInstance().newDefaultBag();
        for(Result res=sp.getNext(t);res.returnStatus!=POStatus.STATUS_EOP;res=sp.getNext(t)){
            outBag.add(castToDBA((Tuple)res.result));
        }
        assertEquals(true, TestHelper.compareBags(expBag, outBag));
    }

    // Test the case when POUnion is one of the roots in a map reduce
    // plan and the input to it can be null
    // This can happen when we have
    // a plan like below
    // POUnion
    // |
    // |--POLocalRearrange
    // |    |
    // |    |-POUnion (root 2)--> This union's getNext() can lead the code here
    // |
    // |--POLocalRearrange (root 1)

    // The inner POUnion above is a root in the plan which has 2 roots.
    // So these 2 roots would have input coming from different input
    // sources (dfs files). So certain maps would be working on input only
    // meant for "root 1" above and some maps would work on input
    // meant only for "root 2". In the former case, "root 2" would
    // neither get input attached to it nor does it have predecessors
    @Test
    public void testGetNextNullInput() throws Exception {
        File f1 = Util.createInputFile("tmp", "a.txt", new String[] {"1\t2\t3", "4\t5\t6"});
        File f2 = Util.createInputFile("tmp", "b.txt", new String[] {"7\t8\t9", "1\t200\t300"});
        File f3 = Util.createInputFile("tmp", "c.txt", new String[] {"1\t20\t30"});
        //FileLocalizer.deleteTempFiles();
        pigServer.registerQuery("a = load '" + f1.getAbsolutePath() + "' ;");
        pigServer.registerQuery("b = load '" + f2.getAbsolutePath() + "';");
        pigServer.registerQuery("c = union a, b;");
        pigServer.registerQuery("d = load '" + f3.getAbsolutePath() + "' ;");
        pigServer.registerQuery("e = cogroup c by $0 inner, d by $0 inner;");
        pigServer.explain("e", System.err);
        // output should be
        // (1,{(1,2,3),(1,200,300)},{(1,20,30)})
        Tuple expectedResult = new DefaultTuple();
        expectedResult.append(new DataByteArray("1"));
        Tuple[] secondFieldContents = new DefaultTuple[2];
        secondFieldContents[0] = Util.createTuple(Util.toDataByteArrays(new String[] {"1", "2", "3"}));
        secondFieldContents[1] = Util.createTuple(Util.toDataByteArrays(new String[] {"1", "200", "300"}));
        DataBag secondField = Util.createBag(secondFieldContents);
        expectedResult.append(secondField);
        DataBag thirdField = Util.createBag(new Tuple[]{Util.createTuple(Util.toDataByteArrays(new String[]{"1", "20", "30"}))});
        expectedResult.append(thirdField);
        Iterator<Tuple> it = pigServer.openIterator("e");
        assertEquals(expectedResult, it.next());
        assertFalse(it.hasNext());
    }

    // Test schema merge in union when one of the fields is a bag
    @Test
    public void testSchemaMergeWithBag() throws Exception {
        File f1 = Util.createInputFile("tmp", "input1.txt", new String[] {"dummy"});
        File f2 = Util.createInputFile("tmp", "input2.txt", new String[] {"dummy"});
        Util.registerMultiLineQuery(pigServer, "a = load '" + f1.getAbsolutePath() + "';" +
        		"b = load '" + f2.getAbsolutePath() + "';" +
        		"c = foreach a generate 1, {(1, 'str1')};" +
        		"d = foreach b generate 2, {(2, 'str2')};" +
        		"e = union c,d;" +
        		"");
        Iterator<Tuple> it = pigServer.openIterator("e");
        Object[] expected = new Object[] { Util.getPigConstant("(1, {(1, 'str1')})"),
                Util.getPigConstant("(2, {(2, 'str2')})")};
        Object[] results = new Object[2];
        int i = 0;
        while(it.hasNext()) {
            if(i == 2) {
                fail("Got more tuples than expected!");
            }
            Tuple t = it.next();
            if(t.get(0).equals(1)) {
                // this is the first tuple
                results[0] = t;
            } else {
                results[1] = t;
            }
            i++;
        }
        for (int j = 0; j < expected.length; j++) {
            assertTrue(expected[j].equals(results[j]));
        }
    }
    
    @Test
    public void testCastingAfterUnion() throws Exception {

        File f1 = Util.createInputFile("tmp", "i1.txt", new String[] {"aaa\t111"});
        File f2 = Util.createInputFile("tmp", "i2.txt", new String[] {"bbb\t222"});

        PigServer ps = new PigServer(ExecType.LOCAL, new Properties());
        ps.registerQuery("A = load '" + f1.getAbsolutePath() + "' as (a,b);");
        ps.registerQuery("B = load '" + f2.getAbsolutePath() + "' as (a,b);");
        ps.registerQuery("C = union A,B;");
        ps.registerQuery("D = foreach C generate (chararray)a as a,(int)b as b;");

        Schema dumpSchema = ps.dumpSchema("D");
        Schema expected = new Schema ();
        expected.add(new Schema.FieldSchema("a", DataType.CHARARRAY));
        expected.add(new Schema.FieldSchema("b", DataType.INTEGER));
        assertEquals(expected, dumpSchema);

        Iterator<Tuple> itr = ps.openIterator("D");
        int recordCount = 0;
        while(itr.next() != null)
            ++recordCount;
        assertEquals(2, recordCount);

    }


}
