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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigImplConstants;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.newplan.logical.visitor.ScalarVariableValidator;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLimitVariable {
    private static String[] data = { "1\t11", "2\t3", "3\t10", "4\t11", "5\t10", "6\t15" };
    private static File inputFile;
    private static MiniGenericCluster cluster;
    private static PigServer pigServer;

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        inputFile = Util.createFile(data);
        cluster = MiniGenericCluster.buildCluster();
        Util.copyFromLocalToCluster(cluster, inputFile.getAbsolutePath(), inputFile.getName());
        inputFile.delete();
    }

    @Before
    public void setUp() throws ExecException {
        pigServer = new PigServer(cluster.getExecType(), cluster.getProperties());
    }

    @AfterClass
    public static void oneTimeTearDown() throws IOException {
        Util.deleteFile(cluster, inputFile.getName());
        cluster.shutDown();
    }

    @Test
    public void testLimitVariable1() throws IOException {
        pigServer.getPigContext().getProperties().setProperty(PigConfiguration.PIG_EXEC_MAP_PARTAGG, "" + true);
        String query =
            "a = load '" + inputFile.getName() + "' as (f1:int, f2:int);" +
            "b = group a all;" +
            "c = foreach b generate COUNT(a) as sum;" +
            "d = order a by $0 DESC;" +
            "e = limit d c.sum/2;" + // return top half of the tuples
            "f = group e all;" +
            "g = foreach f generate AVG(e.$0), SUM(e.$1);"
            ;

        Util.registerMultiLineQuery(pigServer, query);
        Iterator<Tuple> it = pigServer.openIterator("g");

        List<Tuple> expectedRes = Util.getTuplesFromConstantTupleStrings(new String[] {
                "(5.0,36L)"});
        Util.checkQueryOutputsAfterSort(it, expectedRes);
        pigServer.getPigContext().getProperties().remove(PigConfiguration.PIG_EXEC_MAP_PARTAGG);
    }

    @Test
    public void testLimitVariable2() throws IOException {
        //add field type here to use  Util.checkQueryOutputsAfterSort comparing the expected and actual
        //results
        String query =
            "a = load '" + inputFile.getName() + "' as (id:int, num:int);" +
            "b = filter a by id == 2;" + // only 1 tuple returned (2,3)
            "c = order a by id ASC;" +
            "d = limit c b.num;" + // test bytearray to long implicit cast
            "e = limit c b.num * 2;" // return all (6) tuples
            ;

        Util.registerMultiLineQuery(pigServer, query);
        Iterator<Tuple> itD = pigServer.openIterator("d");
        List<Tuple> expectedResD = Util.getTuplesFromConstantTupleStrings(new String[] {
                "(1,11)", "(2,3)", "(3,10)" });
        Util.checkQueryOutputsAfterSort(itD, expectedResD);

        Iterator<Tuple> itE = pigServer.openIterator("e");
        List<Tuple> expectedResE = Util.getTuplesFromConstantTupleStrings(new String[] {
                "(1,11)", "(2,3)", "(3,10)", "(4,11)", "(5,10)", "(6,15)" });
        Util.checkQueryOutputsAfterSort(itE, expectedResE);
    }

    @Test
    public void testLimitVariable3() throws IOException {
        //add field type here to use  Util.checkQueryOutputsAfterSort comparing the expected and actual
        //results
        String query =
            "a = load '" + inputFile.getName() + "' as (id:int, num:int);" +
            "b = group a all;" +
            "c = foreach b generate COUNT(a) as sum;" +
            "d = order a by $0 ASC;" +
            "e = limit d 1 * c.sum;" // return all the tuples, test for PIG-2156
            ;

        Util.registerMultiLineQuery(pigServer, query);
        Iterator<Tuple> itE = pigServer.openIterator("e");
        List<Tuple> expectedResE = Util.getTuplesFromConstantTupleStrings(new String[] {
                "(1,11)", "(2,3)", "(3,10)", "(4,11)", "(5,10)", "(6,15)" });
        Util.checkQueryOutputsAfterSort(itE, expectedResE);
    }

    @Test
    public void testLimitVariable4() throws IOException {
        String query =
            "a = load '" + inputFile.getName() + "' as (x:int, y:int);" +
            "b = group a all;" +
            "c = foreach b generate COUNT(a) as sum;" +
            "d = order a by $0 DESC;" +
            "e = filter d by $0 != 4;" +
            "f = limit e c.sum/2;" // return top half of the tuples
            ;

        try {
            HashSet<String> disabledOptimizerRules = new HashSet<String>();
            disabledOptimizerRules.add("PushUpFilter");
            pigServer.getPigContext().getProperties().setProperty(PigImplConstants.PIG_OPTIMIZER_RULES_KEY,
                    ObjectSerializer.serialize(disabledOptimizerRules));
            Util.registerMultiLineQuery(pigServer, query);
            Iterator<Tuple> it = pigServer.openIterator("f");

            // Even if push up filter is disabled order should be retained
            List<Tuple> expectedRes = Util.getTuplesFromConstantTupleStrings(new String[] {
                    "(6,15)", "(5,10)", "(3,10)" });
            Util.checkQueryOutputs(it, expectedRes);
        } finally {
            pigServer.getPigContext().getProperties().remove(PigImplConstants.PIG_OPTIMIZER_RULES_KEY);
        }
    }

    @Test(expected=FrontendException.class)
    public void testLimitVariableException1() throws Throwable {
        String query =
            "a = load '" + inputFile.getName() + "';" +
            "b = group a all;" +
            "c = foreach b generate COUNT(a) as sum;" +
            "d = order a by $0 DESC;" +
            "e = limit d $0;" // reference to non scalar context is not allowed
            ;

        Util.registerMultiLineQuery(pigServer, query);
        try {
            pigServer.openIterator("e");
        } catch (FrontendException fe) {
            Util.checkMessageInException(fe, ScalarVariableValidator.ERR_MSG_SCALAR);
            throw fe;
        }
    }

    @Test(expected=FrontendException.class)
    public void testLimitVariableException2() throws Throwable {
        String query =
            // num is a chararray
            "a = load '" + inputFile.getName() + "' as (id:int, num:chararray);" +
            "b = filter a by num == '3';" +
            "c = foreach b generate num as falsenum;" +
            "d = order a by id DESC;" +
            "e = limit d c.falsenum;" // expression must be Long or Integer
            ;

        Util.registerMultiLineQuery(pigServer, query);
        try {
            pigServer.openIterator("e");
        } catch (FrontendException fe) {
            Util.checkMessageInException(fe,
                    "Limit's expression must evaluate to Long or Integer");
            throw fe;
        }
    }

    @Test
    public void testNestedLimitVariable1() throws Throwable {
        String query =
            // does not work without schema because Util returns typed tuples
            "a = load '" + inputFile.getName() + "' as (id:int, num:int);" +
            "b = group a by num;" +
            "c = foreach b generate COUNT(a) as ntup;" +
            "d = group c all;" +
            "e = foreach d generate MIN(c.ntup) AS min;" +
            "f = foreach b {" +
            " g = order a by id ASC;" +
            " h = limit g e.min;" +
            " generate FLATTEN(h);" +
            "}"
            ;

        Util.registerMultiLineQuery(pigServer, query);
        Iterator<Tuple> it = pigServer.openIterator("f");

        List<Tuple> expectedRes = Util.getTuplesFromConstantTupleStrings(new String[] {
                "(1,11)", "(2,3)", "(3,10)", "(6,15)" });
        Util.checkQueryOutputsAfterSort(it, expectedRes);
    }

    @Test
    public void testZeroLimitVariable() throws Throwable {
        String query =
            "a = load '" + inputFile.getName() + "';" +
            "b = group a all;" +
            "c = foreach b generate COUNT(a) as sum;" +
            "d = limit a c.sum - c.sum; "
            ;

        Util.registerMultiLineQuery(pigServer, query);
        Iterator<Tuple> it = pigServer.openIterator("d");

        List<Tuple> emptyresult = new ArrayList<Tuple>(0);
        Util.checkQueryOutputsAfterSort(it, emptyresult);
    }
}
