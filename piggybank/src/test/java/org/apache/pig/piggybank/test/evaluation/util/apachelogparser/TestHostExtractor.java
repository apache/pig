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

package org.apache.pig.piggybank.test.evaluation.util.apachelogparser;

import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.piggybank.evaluation.util.apachelogparser.HostExtractor;
import org.junit.Test;

public class TestHostExtractor extends TestCase {
  private static HashMap<String, String> tests = new HashMap<String, String>();
  static {
    tests.put("http://sports.espn.go.com/mlb/recap?gameId=281009122", "sports.espn.go.com");
    tests.put("http://www.google.com/search?hl=en&safe=active&rls=GGLG,GGLG:2005-24,GGLG:en&q=purpose+of+life&btnG=Search", "www.google.com");
    tests.put("http://search.msn.com/results.aspx?q=a+simple+test&geovar=56&FORM=REDIR", "search.msn.com");
    tests.put("http://www.altavista.com/web/results?itag=ody&q=a+simple+test&kgs=1&kls=0", "www.altavista.com");
    tests.put("dud", null);
  }

  @Test
  public void testInstantiation() {
    assertNotNull(new HostExtractor());
  }

  @Test
  public void testTests() throws Exception {
    HostExtractor hostExtractor = new HostExtractor();
    int testCount = 0;
    Tuple input=DefaultTupleFactory.getInstance().newTuple(1);
    for (String key : tests.keySet()) {
      input.set(0,key);
      String expected = tests.get(key);
      String output = hostExtractor.exec(input);
      assertEquals(expected, output);
      testCount++;
    }
    assertEquals(tests.size(), testCount);
  }
}
