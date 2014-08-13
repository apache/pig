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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;

import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestAlgebraicInstantiation {

    Boolean[] nullFlags = new Boolean[]{ false, true};
    static MiniCluster cluster = MiniCluster.buildCluster();
    private PigServer pig;
    private File tmpFile;
    
    public TestAlgebraicInstantiation() throws ExecException {
        pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
    }

    @Before
    public void setUp() throws Exception {
        tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        ps.println("1\t2");
        ps.close();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        cluster.shutDown();
    }

    @Test
    public void testAlgebraicInstantiation() throws IOException {
        pig.registerQuery("a = group (load '" 
                    + Util.generateURI(tmpFile.toString(), pig.getPigContext()) + "') by ($0);");
        pig.registerQuery("b = foreach a generate org.apache.pig.test.TestAlgebraicInstantiation$AlgInstUDF($1.$1);");
        Iterator<Tuple> tupIter = pig.openIterator("b");
        assertEquals("no-args", tupIter.next().toDelimitedString(","));
        pig.registerQuery("DEFINE instantiated org.apache.pig.test.TestAlgebraicInstantiation$AlgInstUDF('args');");
        pig.registerQuery("b = foreach a generate instantiated($1.$1);");
        tupIter = pig.openIterator("b");
        assertEquals("args", tupIter.next().toDelimitedString(","));
    }

    @Test
    public void testRegularInstantiation() throws IOException {
        pig.registerQuery("a = group (load '" 
                    + Util.generateURI(tmpFile.toString(), pig.getPigContext()) + "') by ($0);");
        pig.registerQuery("b = foreach a generate org.apache.pig.test.TestAlgebraicInstantiation$ParamUDF($1.$1);");
        Iterator<Tuple> tupIter = pig.openIterator("b");
        assertEquals("no-args", tupIter.next().toDelimitedString(","));
        pig.registerQuery("DEFINE instantiated org.apache.pig.test.TestAlgebraicInstantiation$ParamUDF('args');");
        pig.registerQuery("b = foreach a generate instantiated($1.$1);");
        tupIter = pig.openIterator("b");
        assertEquals("args", tupIter.next().toDelimitedString(","));
    }
    
    public static class ParamUDF extends EvalFunc<String> {
        private String initType;

        public ParamUDF() {
            initType = "no-args";
        }
        public ParamUDF(String s) {
            initType = s;
        }
        
        @Override
        public String exec(Tuple input) {
            return initType;
        }
    }
    
    public static class AlgInstUDF extends EvalFunc<String> implements Algebraic {

        private String initType;

        public AlgInstUDF() {
            initType = "no-args";
        }

        public AlgInstUDF(String s) {
            initType = s;
        }

        @Override
        public String exec(Tuple input) throws IOException {
            // TODO Auto-generated method stub
            return initType;
        }

        @Override
        public String getFinal() {
            return Final.class.getName();
        }

        @Override
        public String getInitial() {
            return Initial.class.getName();
        }

        @Override
        public String getIntermed() {
            return Intermed.class.getName();
        }

        public static class Initial extends EvalFunc<Tuple> {

            private String initType;

            public Initial() {
                initType = "no-args";
            }

            public Initial(String s) {
                initType = s;
            }

            @Override
            public Tuple exec(Tuple input) {
                return TupleFactory.getInstance().newTuple(initType);
            }
        }

        public static class Intermed extends EvalFunc<Tuple> {

            private String initType;

            public Intermed() {
                initType = "no-args";
            }

            public Intermed(String s) {
                initType = s;
            }

            @Override
            public Tuple exec(Tuple input) {
                return TupleFactory.getInstance().newTuple(initType);
            }
        }

        public static class Final extends EvalFunc<String> {

            private String initType;

            public Final() {
                initType = "no-args";
            }

            public Final(String s) {
                initType = s;
            }

            @Override
            public String exec(Tuple input) {
                return initType;
            }
        }
    }
}
