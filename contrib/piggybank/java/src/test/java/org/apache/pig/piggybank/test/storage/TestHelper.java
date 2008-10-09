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

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.apache.pig.data.DataAtom;
import org.apache.pig.data.Tuple;
import org.junit.Test;

public class TestHelper extends TestCase {
    @Test
    public void testTest() {
        assertTrue(true);
    }


    public static ArrayList<String[]> getExpected(ArrayList<String[]> data, Pattern pattern) {
        ArrayList<String[]> expected = new ArrayList<String[]>();
        for (int i = 0; i < data.size(); i++) {
            String string = data.get(i)[0];
            Matcher matcher = pattern.matcher(string);
            matcher.groupCount();
            matcher.find();
            String[] toAdd = new String[] { matcher.group(1), matcher.group(2), matcher.group(3) };
            expected.add(toAdd);
        }

        return expected;
    }

    private static String join(String delimiter, String[] strings) {
        String string = strings[0];
        for (int i = 1; i < strings.length; i++) {
            string += delimiter + strings[i];
        }
        return string;
    }

    public static void examineTuple(ArrayList<String[]> expectedData, Tuple tuple, int tupleCount) {
        for (int i = 0; i < tuple.arity(); i++) {
            DataAtom dataAtom = tuple.getAtomField(i);
            String expected = expectedData.get(tupleCount)[i];
            String actual = dataAtom.toString();
            assertEquals(expected, actual);
        }
    }

    public static String createTempFile(ArrayList<String[]> myData, String delimiter) throws Exception {
        File tmpFile = File.createTempFile("test", ".txt");
        if (tmpFile.exists()) {
            tmpFile.delete();
        }
        PrintWriter pw = new PrintWriter(tmpFile);
        for (int i = 0; i < myData.size(); i++) {
            pw.println(join(delimiter, myData.get(i)));
        }
        pw.close();
        tmpFile.deleteOnExit();
        return tmpFile.getAbsolutePath();
    }
}
