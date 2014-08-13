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
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.test.utils.TestHelper;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStats.JobGraph;
import org.apache.pig.tools.pigstats.mapreduce.MRJobStats;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFRJoin2 {

    // This class contains tests for
    //    - Concatenating small files before adding to DistributedCache (PIG-1458)
    //    - imposing size limit on files being added to DistributedCache
    // Since Replicated join in Tez does not use DistributedCache, these tests are MR specific
    private static MiniGenericCluster cluster = MiniGenericCluster.buildCluster(MiniGenericCluster.EXECTYPE_MR);

    private static final String INPUT_DIR = "frjoin";
    private static final String INPUT_FILE = "input";

    private static final int FILE_MERGE_THRESHOLD = 5;
    private static final int MIN_FILE_MERGE_THRESHOLD = 1;

    //contents of input dir joined by comma
    private static String concatINPUT_DIR = null;
    private File logFile;


    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        StringBuilder strBuilder = new StringBuilder();
        FileSystem fs = cluster.getFileSystem();
        fs.mkdirs(new Path(INPUT_DIR));
        int LOOP_SIZE = 2;
        for (int i=0; i<FILE_MERGE_THRESHOLD; i++) {
            String[] input = new String[2*LOOP_SIZE];
            for (int n=0; n<LOOP_SIZE; n++) {
                for (int j=0; j<LOOP_SIZE;j++) {
                    input[n*LOOP_SIZE + j] = i + "\t" + (j + n);
                }
            }
            String newFile = INPUT_DIR + "/part-0000" + i;
            Util.createInputFile(cluster, newFile, input);
            strBuilder.append(newFile);
            strBuilder.append(",");
        }
        strBuilder.deleteCharAt(strBuilder.length() - 1);
        concatINPUT_DIR = strBuilder.toString();

        String[] input2 = new String[2*(LOOP_SIZE/2)];
        int k = 0;
        for (int i=1; i<=LOOP_SIZE/2; i++) {
            String si = i + "";
            for (int j=0; j<=LOOP_SIZE/2; j++) {
                input2[k++] = si + "\t" + j;
            }
        }
        Util.createInputFile(cluster, INPUT_FILE, input2);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        cluster.shutDown();
    }

    // test simple scalar alias with file concatenation following
    // a MapReduce job
    @Test
    public void testConcatenateJobForScalar() throws Exception {
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");

        // using $0*0, instead of group-all because group-all sets parallelism to 1
        pigServer.registerQuery("B = group A by $0*0 parallel 5;");
        pigServer.registerQuery("C = foreach B generate COUNT(A) as count, MAX(A.y) as max;");

        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.getPigContext().getProperties().setProperty(
                    MRCompiler.FILE_CONCATENATION_THRESHOLD, String.valueOf(FILE_MERGE_THRESHOLD));

            pigServer.registerQuery("D= foreach A generate x / C.count, C.max - y;");
            Iterator<Tuple> iter = pigServer.openIterator("D");

            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }

            JobGraph jGraph = PigStats.get().getJobGraph();
            assertEquals(3, jGraph.size());
            // find added map-only concatenate job
            MRJobStats js = (MRJobStats)jGraph.getSuccessors(jGraph.getSources().get(0)).get(0);
            assertEquals(1, js.getNumberMaps());
            assertEquals(0, js.getNumberReduces());
        }
        {
            pigServer.getPigContext().getProperties().setProperty(
                    "pig.noSplitCombination", "true");

            pigServer.registerQuery("D= foreach A generate x / C.count, C.max - y;");
            Iterator<Tuple> iter = pigServer.openIterator("D");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }

            assertEquals(2, PigStats.get().getJobGraph().size());
        }

        assertEquals(dbfrj.size(), dbshj.size());
        assertEquals(true, TestHelper.compareBags(dbfrj, dbshj));
    }

    // test simple scalar alias with file concatenation following
    // a Map-only job
    @Test
    public void testConcatenateJobForScalar2() throws Exception {
        logFile = Util.resetLog(MRCompiler.class, logFile);
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_DIR +  "/{part-00*}" +"' as (x:int,y:int);");
        pigServer.registerQuery("C = filter B by (x == 3) AND (y == 2);");

        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.getPigContext().getProperties().setProperty(
                    MRCompiler.FILE_CONCATENATION_THRESHOLD, String.valueOf(MIN_FILE_MERGE_THRESHOLD));

            pigServer.registerQuery("D = foreach A generate x / C.x, y + C.y;");
            Iterator<Tuple> iter = pigServer.openIterator("D");

            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }

            JobGraph jGraph = PigStats.get().getJobGraph();
            assertEquals(3, jGraph.size());
            // find added map-only concatenate job
            MRJobStats js = (MRJobStats)jGraph.getSuccessors(jGraph.getSources().get(0)).get(0);
            assertEquals(1, js.getNumberMaps());
            assertEquals(0, js.getNumberReduces());
            Util.checkLogFileMessage(logFile,
                    new String[] {"number of input files: 0", "failed to get number of input files"},
                    false
            );
        }
        {
            pigServer.getPigContext().getProperties().setProperty(
                    "pig.noSplitCombination", "true");

            pigServer.registerQuery("D = foreach A generate x / C.x, y + C.y;");
            Iterator<Tuple> iter = pigServer.openIterator("D");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }

            assertEquals(2, PigStats.get().getJobGraph().size());
        }

        assertEquals(dbfrj.size(), dbshj.size());
        assertEquals(true, TestHelper.compareBags(dbfrj, dbshj));
    }

    // test scalar alias with file concatenation following
    // a multi-query job
    @Test
    public void testConcatenateJobForScalar3() throws Exception {
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("C = group A all parallel 5;");
        pigServer.registerQuery("D = foreach C generate COUNT(A) as count;");
        pigServer.registerQuery("E = foreach C generate MAX(A.x) as max;");

        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.getPigContext().getProperties().setProperty(
                    MRCompiler.FILE_CONCATENATION_THRESHOLD, String.valueOf(MIN_FILE_MERGE_THRESHOLD));

            pigServer.registerQuery("F = foreach B generate x / D.count, y + E.max;");
            Iterator<Tuple> iter = pigServer.openIterator("F");

            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }

            JobGraph jGraph = PigStats.get().getJobGraph();
            assertEquals(4, jGraph.size());
        }
        {
            pigServer.getPigContext().getProperties().setProperty(
                    "pig.noSplitCombination", "true");

            pigServer.registerQuery("F = foreach B generate x / D.count, y + E.max;");
            Iterator<Tuple> iter = pigServer.openIterator("F");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }

            assertEquals(2, PigStats.get().getJobGraph().size());
        }

        assertEquals(dbfrj.size(), dbshj.size());
        assertEquals(true, TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testConcatenateJobForFRJoin() throws Exception {
        logFile = Util.resetLog(MRCompiler.class, logFile);
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_DIR +  "/{part-00*}" + "' as (x:int,y:int);");

        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.getPigContext().getProperties().setProperty(
                    MRCompiler.FILE_CONCATENATION_THRESHOLD, String.valueOf(MIN_FILE_MERGE_THRESHOLD));

            pigServer.registerQuery("C = join A by y, B by y using 'repl';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }

            assertEquals(3, PigStats.get().getJobGraph().size());
            Util.checkLogFileMessage(logFile,
                    new String[] {"number of input files: 0", "failed to get number of input files"},
                    false
            );

        }
        {
            pigServer.getPigContext().getProperties().setProperty(
                    "pig.noSplitCombination", "true");

            pigServer.registerQuery("C = join A by y, B by y using 'repl';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }

            assertEquals(2, PigStats.get().getJobGraph().size());
        }

        assertEquals(dbfrj.size(), dbshj.size());
        assertEquals(true, TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testTooManyReducers() throws Exception {
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

        pigServer.registerQuery("A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("B = group A by x parallel " + FILE_MERGE_THRESHOLD + ";");
        pigServer.registerQuery("C = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.getPigContext().getProperties().setProperty(
                    MRCompiler.FILE_CONCATENATION_THRESHOLD, String.valueOf(FILE_MERGE_THRESHOLD));
            pigServer.registerQuery("D = join C by $0, B by $0 using 'repl';");
            Iterator<Tuple> iter = pigServer.openIterator("D");

            while(iter.hasNext()) {
                Tuple t = iter.next();
                dbfrj.add(t);
            }

            JobGraph jGraph = PigStats.get().getJobGraph();
            assertEquals(3, jGraph.size());
            // find added map-only concatenate job
            MRJobStats js = (MRJobStats)jGraph.getSuccessors(jGraph.getSources().get(0)).get(0);
            assertEquals(1, js.getNumberMaps());
            assertEquals(0, js.getNumberReduces());
        }
        {
            pigServer.getPigContext().getProperties().setProperty(
                    "pig.noSplitCombination", "true");
            pigServer.registerQuery("D = join C by $0, B by $0 using 'repl';");
            Iterator<Tuple> iter = pigServer.openIterator("D");

            while(iter.hasNext()) {
                Tuple t = iter.next();
                dbshj.add(t);
            }
            assertEquals(2, PigStats.get().getJobGraph().size());
        }
        assertEquals(dbfrj.size(), dbshj.size());
        assertEquals(true, TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testUnknownNumMaps() throws Exception {
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

        pigServer.registerQuery("A = LOAD '" + concatINPUT_DIR + "' as (x:int,y:int);");
        pigServer.registerQuery("B = Filter A by x < 50;");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.getPigContext().getProperties().setProperty(
                    MRCompiler.FILE_CONCATENATION_THRESHOLD, String.valueOf(MIN_FILE_MERGE_THRESHOLD));
            pigServer.registerQuery("C = join A by $0, B by $0 using 'repl';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }

            JobGraph jGraph = PigStats.get().getJobGraph();
            assertEquals(3, jGraph.size());
        }
        {
            pigServer.getPigContext().getProperties().setProperty(
                    "pig.noSplitCombination", "true");
            pigServer.registerQuery("C = join A by $0, B by $0 using 'repl';");
            Iterator<Tuple> iter = pigServer.openIterator("C");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
            assertEquals(2, PigStats.get().getJobGraph().size());
        }
        assertEquals(dbfrj.size(), dbshj.size());
        assertEquals(true, TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testUnknownNumMaps2() throws Exception {
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

        pigServer.registerQuery("A = LOAD '" + INPUT_DIR + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("C = join A by x, B by x using 'repl';");
        DataBag dbfrj = BagFactory.getInstance().newDefaultBag(), dbshj = BagFactory.getInstance().newDefaultBag();
        {
            pigServer.getPigContext().getProperties().setProperty(
                    MRCompiler.FILE_CONCATENATION_THRESHOLD, String.valueOf(MIN_FILE_MERGE_THRESHOLD));
            pigServer.registerQuery("D = join B by $0, C by $0 using 'repl';");
            Iterator<Tuple> iter = pigServer.openIterator("D");

            while(iter.hasNext()) {
                dbfrj.add(iter.next());
            }

            JobGraph jGraph = PigStats.get().getJobGraph();
            assertEquals(5, jGraph.size());
        }
        {
            pigServer.getPigContext().getProperties().setProperty(
                    "pig.noSplitCombination", "true");
            pigServer.registerQuery("D = join B by $0, C by $0 using 'repl';");
            Iterator<Tuple> iter = pigServer.openIterator("D");

            while(iter.hasNext()) {
                dbshj.add(iter.next());
            }
            assertEquals(3, PigStats.get().getJobGraph().size());
        }
        assertEquals(dbfrj.size(), dbshj.size());
        assertEquals(true, TestHelper.compareBags(dbfrj, dbshj));
    }

    @Test
    public void testTooBigReplicatedFile() throws Exception {
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

        pigServer.registerQuery("A = LOAD '" + INPUT_DIR + "' as (x:int,y:int);");
        pigServer.registerQuery("B = LOAD '" + INPUT_FILE + "' as (x:int,y:int);");
        pigServer.registerQuery("C = group B all parallel 5;");
        pigServer.registerQuery("C = foreach C generate MAX(B.x) as x;");
        pigServer.registerQuery("D = join A by x, B by x, C by x using 'repl';");
        {
            // When the replicated input sizes=(12 + 5) is bigger than
            // pig.join.replicated.max.bytes=16, we throw exception
            try {
                pigServer.getPigContext().getProperties().setProperty(
                        PigConfiguration.PIG_JOIN_REPLICATED_MAX_BYTES,
                        String.valueOf(16));
                pigServer.openIterator("D");
                Assert.fail();
            } catch (FrontendException e) {
                assertEquals("Internal error. Distributed cache could" +
                        " not be set up for the replicated files",
                        e.getCause().getCause().getCause().getMessage());
            }

            // If we increase the size to 17, it should work
            pigServer.getPigContext().getProperties().setProperty(
                        PigConfiguration.PIG_JOIN_REPLICATED_MAX_BYTES,
                        String.valueOf(17));
            pigServer.openIterator("D");
        }
    }

    // pig-3975 test scalar alias with file concatenation referenced
    // by multiple mapreduce jobs
    @Test
    public void testSoftLinkDependencyWithMultipleScalarReferences()
                  throws Exception {
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE,
                                      cluster.getProperties());

        pigServer.setBatchOn();
        pigServer.getPigContext().getProperties().setProperty(
                  MRCompiler.FILE_CONCATENATION_THRESHOLD, String.valueOf(FILE_MERGE_THRESHOLD));
        pigServer.getPigContext().getProperties().setProperty("pig.noSplitCombination", "false");
        String query = "A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);"
                       + "B = group A by x parallel " + FILE_MERGE_THRESHOLD + ";"
                       + "C = LOAD '" + INPUT_FILE + "' as (x:int,y:int);"
                       + "D = FOREACH C generate B.$0;"
                       + "STORE D into '/tmp/output1';"
                       + "E = LOAD '" + INPUT_FILE + "' as (x:int,y:int);"
                       + "F = FOREACH E generate B.$0;"
                       + "STORE F into '/tmp/output2';";
        MROperPlan mrplan = Util.buildMRPlanWithOptimizer(Util.buildPp(pigServer, query),pigServer.getPigContext());
        assertEquals("Unexpected number of mapreduce job. Missing concat job?",
                     4, mrplan.size() );

        // look for concat job
        MapReduceOper concatMRop = null;
        for(MapReduceOper mrOp: mrplan) {
            //concatjob == map-plan load-store && reudce-plan empty
            if( mrOp.mapPlan.size() == 2 && mrOp.reducePlan.isEmpty() ) {
                concatMRop = mrOp;
                break;
            }
        }

        if( concatMRop == null ) {
            fail("Cannot find concat job.");
        }
        // 2 mr job reads from the concat job result [B.$0] so there
        // should be 2 mr jobs as successors of the concat job
        assertEquals("Missing dependency for concatjob",
                     2, mrplan.getSuccessors(concatMRop).size());

    }

    // Extra scalar reference should not cause concat job to be created
    @Test
    public void testSoftLinkDoesNotCreateUnnecessaryConcatJob()
                  throws Exception {
        PigServer pigServer = new PigServer(ExecType.MAPREDUCE,
                                      cluster.getProperties());

        pigServer.setBatchOn();
        pigServer.getPigContext().getProperties().setProperty(
                  MRCompiler.FILE_CONCATENATION_THRESHOLD, String.valueOf(FILE_MERGE_THRESHOLD));
        pigServer.getPigContext().getProperties().setProperty("pig.noSplitCombination", "false");
        String query = "A = LOAD '" + INPUT_FILE + "' as (x:int,y:int);"
                       + "B = group A all;"
                       + "C = LOAD '" + INPUT_FILE + "' as (x:int,y:int);"
                       + "D = group C by x;"
                       + "E = group D all;"
                       + "F = FOREACH E generate B.$0;"
                       + "Z = LOAD '" + INPUT_FILE + "' as (x:int,y:int);"
                       + "Y = FOREACH E generate F.$0;"
                       + "STORE Y into '/tmp/output2';";
        MROperPlan mrplan = Util.buildMRPlanWithOptimizer(Util.buildPp(pigServer, query),pigServer.getPigContext());

        // look for concat job
        for(MapReduceOper mrOp: mrplan) {
            //concatjob == map-plan load-store && reudce-plan empty
            if( mrOp.mapPlan.size() == 2 && mrOp.reducePlan.isEmpty() ) {
                fail("Somehow concatjob was created even though there is no large or multiple inputs.");
            }
        }
    }
}
