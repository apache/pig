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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.*;
import org.apache.pig.tools.grunt.Grunt;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;

import junit.framework.TestCase;
@RunWith(JUnit4.class)
public class TestUTF8 extends TestCase {
    private PigServer pigServer;

    @Before
    @Override
    public void setUp() throws Exception{
        pigServer = new PigServer(ExecType.LOCAL, new Properties());
    }

    @Test
    public void testPigStorage() throws Exception{

        File f1 = File.createTempFile("tmp", "");
        PrintWriter pw = new PrintWriter(f1, "UTF-8");
        pw.println("中文");
        pw.println("にほんご");
        pw.println("한국어");
        pw.println("ภาษาไทย");
        pw.close();

        pigServer.registerQuery("a = load '"
                + Util.generateURI(f1.toString(), pigServer.getPigContext())
                + "' using " + PigStorage.class.getName() + "();");
        Iterator<Tuple> iter  = pigServer.openIterator("a");

        assertEquals(DataType.toString(iter.next().get(0)), "中文");
        assertEquals(DataType.toString(iter.next().get(0)), "にほんご");
        assertEquals(DataType.toString(iter.next().get(0)), "한국어");
        assertEquals(DataType.toString(iter.next().get(0)), "ภาษาไทย");

        f1.delete();
    }

    @Test
    public void testScriptParser() throws Throwable {

        String strCmd = "--中文\n";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes("UTF-8"));
        InputStreamReader reader = new InputStreamReader(cmd);

        Grunt grunt = new Grunt(new BufferedReader(reader), pigServer.getPigContext());

        grunt.exec();
    }

    @Test
    public void testQueryParser() throws Exception{
    	File f1 = File.createTempFile("tmp", "");
        PrintWriter pw = new PrintWriter(f1, "UTF-8");
        pw.println("中文");
        pw.close();

        pigServer.registerQuery("a = load '"
                + Util.generateURI(f1.toString(), pigServer.getPigContext())
                + "' using " + PigStorage.class.getName() + "();");
        pigServer.registerQuery("b =  filter a by $0 == '中文';");
        Iterator<Tuple> iter  = pigServer.openIterator("a");

        assertEquals(DataType.toString(iter.next().get(0)), "中文");

        f1.delete();
    }

    @Test
    public void testParamSubstitution() throws Exception{
    	File queryFile = File.createTempFile("query", "");
        PrintWriter ps = new PrintWriter(queryFile);
        ps.println("b = filter a by $0 == '$querystring';");
        ps.close();

        String[] arg = {"querystring='中文'"};

    	ParameterSubstitutionPreprocessor psp = new ParameterSubstitutionPreprocessor(50);
        BufferedReader pigIStream = new BufferedReader(new FileReader(queryFile.toString()));
        StringWriter pigOStream = new StringWriter();

        psp.genSubstitutedFile(pigIStream , pigOStream , arg, null);

        assertTrue(pigOStream.toString().contains("中文"));

        queryFile.delete();
    }
}
