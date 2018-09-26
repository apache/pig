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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.test.utils.GenPhyOp;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestCredentials {
    private static PigServer pigServer;
    private static PigContext pc;
    private static MiniGenericCluster cluster;
    private static String INPUT_FILE = "input.txt";
    private static String OUTPUT_DIR = "output";
    private static final Text ALIAS = new Text ("testKey");
    private static final String SECRET = "dummySecret";

    public static class CredentialsEvalFunc extends EvalFunc<String>{
        @Override
        public String exec(Tuple input) throws IOException {
            String val = new String(UserGroupInformation.getCurrentUser().getCredentials().getSecretKey(ALIAS));
            if(!SECRET.equals(val)) {
                throw new IOException("Invalid secret");
            }
            return val;
        }

        @Override
        public void addCredentials(Credentials credentials, Configuration conf) {
            Credentials creds = new Credentials();
            creds.addSecretKey(ALIAS, SECRET.getBytes());
            credentials.addAll(creds);
        }
    }

    public static class CredPigStorage extends PigStorage {
        @Override
        public void addCredentials(Credentials credentials, Configuration conf) {
            Credentials creds = new Credentials();
            creds.addSecretKey(ALIAS, SECRET.getBytes());
            credentials.addAll(creds);
        }

        @Override
        public Tuple getNext() throws IOException {
            Tuple ret = super.getNext();
            if(ret == null) {
                return ret;
            }
            byte[] b = UserGroupInformation.getCurrentUser().getCredentials().getSecretKey(ALIAS);
            if(b != null) {
                ret.append(new String(b));
            }
            return ret;
        }

        @Override
        public void putNext(Tuple tuple) throws IOException {
            if(tuple == null) {
                return;
            }
            byte[] b = UserGroupInformation.getCurrentUser().getCredentials().getSecretKey(ALIAS);
            if(b != null) {
                tuple.append(new String(b));
            }
            super.putNext(tuple);
        }
    }

    @BeforeClass
    public static void setup() throws IOException {
        cluster = MiniGenericCluster.buildCluster();
        pc = new PigContext(cluster.getExecType(), cluster.getProperties());
        pc.connect();
        pigServer = new PigServer(cluster.getExecType(), cluster.getProperties());
        GenPhyOp.setPc(pc);
        createInput();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        deleteInput();
        if(pigServer!=null) {
            pigServer.shutdown();
        }
        cluster.shutDown();
    }

    @Test
    public void testCredentialsEvalFunc() throws IOException {
        Tuple expectedResult = (Tuple)Util.getPigConstant("('" + SECRET + "')");
        pigServer.registerQuery("a = load '"+ INPUT_FILE +"' as (i:chararray);");
        pigServer.registerQuery("d = foreach a generate " + CredentialsEvalFunc.class.getName() + "(i);");
        Iterator<Tuple> it = pigServer.openIterator("d");
        assertTrue(it.hasNext());
        assertEquals(expectedResult, it.next());
        assertFalse(it.hasNext());
    }

    @Test
    public void testCredentialsLoadFunc() throws Exception {
        Tuple expectedResult = (Tuple)Util.getPigConstant("('" + SECRET + "')");
        pigServer.registerQuery("a = load '" + INPUT_FILE + "' using " + CredPigStorage.class.getName()
                + "() as (text:chararray, secstr:chararray);");
        pigServer.registerQuery("d = foreach a generate secstr;");
        Iterator<Tuple> it = pigServer.openIterator("d");
        assertTrue(it.hasNext());
        assertEquals(expectedResult, it.next());
        assertFalse(it.hasNext());
    }

    @Test
    public void testCredentialsStoreFunc() throws Exception {
        Tuple expectedResult = (Tuple)Util.getPigConstant("('" + SECRET + "')");
        pigServer.registerQuery("a = load '" + INPUT_FILE + "' using PigStorage() as (text:chararray);");
        pigServer.registerQuery("store a into '" + OUTPUT_DIR +"' using " + CredPigStorage.class.getName() + "();");
        pigServer.registerQuery("c = load '" + OUTPUT_DIR + "' using PigStorage() as (text:chararray, secstr:chararray);");
        pigServer.registerQuery("d = foreach c generate secstr;");
        Iterator<Tuple> it = pigServer.openIterator("d");
        assertTrue(it.hasNext());
        assertEquals(expectedResult, it.next());
        assertFalse(it.hasNext());
    }

    private static void createInput() throws IOException {
        PrintWriter w = new PrintWriter(new FileWriter(INPUT_FILE));
        w.println("dumb");
        w.close();
        Util.copyFromLocalToCluster(cluster, INPUT_FILE, INPUT_FILE);
    }

    private static void deleteInput() throws IOException {
        new File(INPUT_FILE).delete();
        FileUtils.deleteDirectory(new File(OUTPUT_DIR));
        Util.deleteFile(cluster, INPUT_FILE);
        Util.deleteFile(cluster, OUTPUT_DIR);
    }

}
