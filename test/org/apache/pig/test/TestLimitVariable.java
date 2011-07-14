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
import java.util.List;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.logical.visitor.ScalarVariableValidator;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLimitVariable {
    private static String[] data = { "1\t11", "2\t3", "3\t10", "4\t11", "5\t10", "6\t15" };
    private static File inputFile;
    private static MiniCluster cluster;
    private static PigServer pigServer;

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        inputFile = Util.createFile(data);
        cluster = MiniCluster.buildCluster();
        Util.copyFromLocalToCluster(cluster, inputFile.getAbsolutePath(), inputFile.getName());
        inputFile.delete();
    }

    @Before
    public void setUp() throws ExecException {
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }

    @AfterClass
    public static void oneTimeTearDown() throws IOException {
        Util.deleteFile(cluster, inputFile.getName());
        cluster.shutDown();
    }

    @Test
    public void testLimitVariable1() throws IOException {
        String query = 
            "a = load '" + inputFile.getName() + "';" + 
            "b = group a all;" + 
            "c = foreach b generate COUNT(a) as sum;" + 
            "d = order a by $0 DESC;" + 
            "e = limit d c.sum/2;" // return top half of the tuples
            ;

        Util.registerMultiLineQuery(pigServer, query);
        Iterator<Tuple> it = pigServer.openIterator("e");

        List<Tuple> expectedRes = Util.getTuplesFromConstantTupleStrings(new String[] {
                "(6,15)", "(5,10)", "(4,11)" });
        Util.checkQueryOutputs(it, expectedRes);
    }
    
    @Test
    public void testLimitVariable2() throws IOException {
        String query = 
            "a = load '" + inputFile.getName() + "' as (id, num);" +
            "b = filter a by id == 2;" + // only 1 tuple returned (2,3)
            "c = order a by id ASC;" +
            "d = limit c b.num;" + // test bytearray to long implicit cast
            "e = limit c b.num * 2;" // return all (6) tuples
            ;
        
        Util.registerMultiLineQuery(pigServer, query);
        Iterator<Tuple> itD = pigServer.openIterator("d");
        List<Tuple> expectedResD = Util.getTuplesFromConstantTupleStrings(new String[] {
                "(1,11)", "(2,3)", "(3,10)" });
        Util.checkQueryOutputs(itD, expectedResD);

        Iterator<Tuple> itE = pigServer.openIterator("e");
        List<Tuple> expectedResE = Util.getTuplesFromConstantTupleStrings(new String[] {
                "(1,11)", "(2,3)", "(3,10)", "(4,11)", "(5,10)", "(6,15)" });
        Util.checkQueryOutputs(itE, expectedResE);
    }
    
    @Test
    public void testLimitVariable3() throws IOException {
        String query =
            "a = load '" + inputFile.getName() + "' ;" +
            "b = group a all;" + 
            "c = foreach b generate COUNT(a) as sum;" + 
            "d = order a by $0 ASC;" + 
            "e = limit d 1 * c.sum;" // return all the tuples, test for PIG-2156
            ;

        Util.registerMultiLineQuery(pigServer, query);
        Iterator<Tuple> itE = pigServer.openIterator("e");
        List<Tuple> expectedResE = Util.getTuplesFromConstantTupleStrings(new String[] {
                "(1,11)", "(2,3)", "(3,10)", "(4,11)", "(5,10)", "(6,15)" });
        Util.checkQueryOutputs(itE, expectedResE);
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
}
