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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.apache.pig.PigServer;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import static org.apache.pig.ExecType.LOCAL;

public class TestMyRegExLoader extends TestCase {
  private static String patternString = "(\\d+)!+(\\w+)~+(\\w+)";
  private final static Pattern pattern = Pattern.compile(patternString);
  public static ArrayList<String[]> data = new ArrayList<String[]>();
  static {
    data.add(new String[] { "1!!!one~i" });
    data.add(new String[] { "2!!two~~ii" });
    data.add(new String[] { "3!three~~~iii" });
  }

  public void testLoadMyRegExFromPigServer() throws Exception {
    ArrayList<DataByteArray[]> expected = TestHelper.getExpected(data, pattern);
    String filename = TestHelper.createTempFile(data, "");
    PigServer pig = new PigServer(LOCAL);
    filename = filename.replace("\\", "\\\\");
    patternString = patternString.replace("\\", "\\\\");
    String query = "A = LOAD '" + filename + "' USING org.apache.pig.piggybank.storage.MyRegExLoader('" + patternString + "');";
    pig.registerQuery(query);
    Iterator<?> it = pig.openIterator("A");
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
}
