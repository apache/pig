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
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.scripting.ScriptEngine;
import org.apache.pig.tools.pigstats.PigStats;
import org.junit.Test;

public class TestScriptingLanguagePython {

  @Test
  public void varargTest() throws Exception {
    System.setProperty("python.cachedir", System.getProperty("java.io.tmpdir"));
    PigServer pigServer = new PigServer(ExecType.LOCAL);
    String[] script = {
        "#!/usr/bin/python",

        "from org.apache.pig.scripting import *",

        "@outputSchema(\"s:chararray\")",
        "def firstNonempty(*args):",
        "\tfor v in args:",
        "\t\tif len(v) != 0:",
        "\t\t\treturn v",
        "\treturn ''",

        "if __name__ == \"__main__\":",
        "\tPig.compile(\"\"\"",
        "data = load 'simple_table' AS (string1:chararray, string2:chararray);",
        "data = foreach data generate firstNonempty(string1, string2) as id, string1, string2;",
        "store data into 'simple_out';",
        "\"\"\").bind().runSingle()"
    };
    String[] input = {
        "1\t3",
        "2\t4",
        "3\t5"
    };

    Util.deleteFile(pigServer.getPigContext(), "simple_table");
    Util.deleteFile(pigServer.getPigContext(), "simple_out");
    Util.createInputFile(pigServer.getPigContext(), "simple_table", input);
    Util.createLocalInputFile( "testScript.py", script);

    ScriptEngine scriptEngine = ScriptEngine.getInstance("jython");
    Map<String, List<PigStats>> statsMap = scriptEngine.run(pigServer.getPigContext(), "testScript.py");
    assertEquals(1, statsMap.size());
    Iterator<List<PigStats>> it = statsMap.values().iterator();
    PigStats stats = it.next().get(0);
    assertTrue(stats.isSuccessful());

    String[] output = Util.readOutput(pigServer.getPigContext(), "simple_out");
    assertEquals(3, output.length);
    assertEquals(output[0], "1\t1\t3");
    assertEquals(output[1], "2\t2\t4");
    assertEquals(output[2], "3\t3\t5");
  }

}
