/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.pig.piggybank.test.storage;

import static org.apache.pig.ExecType.LOCAL;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.apache.pig.PigServer;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.piggybank.storage.RegExLoader;
import org.apache.pig.test.Util;
import org.junit.Test;

public class TestRegExLoader extends TestCase {
    private static String patternString = "(\\w+),(\\w+);(\\w+)";
    private final static Pattern pattern = Pattern.compile(patternString);
    private static String patternString2 = "(3),(three);(iii)";
    private final static Pattern pattern2 = Pattern.compile(patternString);

    public static class DummyRegExLoader extends RegExLoader {
        public DummyRegExLoader() {}
        
        @Override
        public Pattern getPattern() {
            return Pattern.compile(patternString);
        }
    }

    public static class DummyRegExLoader2 extends RegExLoader {
        public DummyRegExLoader2() {}
        
        @Override
        public Pattern getPattern() {
            return Pattern.compile(patternString2);
        }
    }

    public static ArrayList<String[]> data = new ArrayList<String[]>();
    static {
        data.add(new String[] { "1,one;i" });
        data.add(new String[] { "2,two;ii" });
        data.add(new String[] { "3,three;iii" });
    }

    @Test
    public void testLoadFromBindTo() throws Exception {       
        PigServer pigServer = new PigServer(LOCAL);
        
        String filename = TestHelper.createTempFile(data, "");
        ArrayList<DataByteArray[]> expected = TestHelper.getExpected(data, pattern);
        
        pigServer.registerQuery("A = LOAD 'file:" + Util.encodeEscape(filename) + 
                "' USING " + DummyRegExLoader.class.getName() + "() AS (key, val);");
        Iterator<?> it = pigServer.openIterator("A");
        int tupleCount = 0;
        while (it.hasNext()) {
            Tuple tuple = (Tuple) it.next();
            if (tuple == null)
              break;
            else {
              TestHelper.examineTuple(expected, tuple, tupleCount);
              tupleCount++;
            }
          }
        assertEquals(data.size(), tupleCount);
    }
        
    @Test
    public void testOnlyLastMatch() throws Exception {       
        PigServer pigServer = new PigServer(LOCAL);
        
        String filename = TestHelper.createTempFile(data, "");

    	ArrayList<String[]> dataE = new ArrayList<String[]>();
        dataE.add(new String[] { "3,three;iii" });
       	ArrayList<DataByteArray[]> expected = TestHelper.getExpected(dataE, pattern2);
        
        pigServer.registerQuery("A = LOAD 'file:" + Util.encodeEscape(filename) + 
                "' USING " + DummyRegExLoader2.class.getName() + "() AS (key, val);");
        Iterator<?> it = pigServer.openIterator("A");
        int tupleCount = 0;
        while (it.hasNext()) {
            Tuple tuple = (Tuple) it.next();
            if (tuple == null)
              break;
            else {
              TestHelper.examineTuple(expected, tuple, tupleCount);
              tupleCount++;
            }
          }
        assertEquals(1, tupleCount);
    }
        
}
