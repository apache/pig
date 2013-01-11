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

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.junit.AfterClass;
import org.junit.Test;

public class TestPigProgressReporting {

    static MiniCluster cluster = MiniCluster.buildCluster();

    @Test
    public void testProgressReportingWithStatusMessage() throws Exception {

        // Get the default time out value
        String taskTimeout = cluster.getProperties().getProperty("mapred.task.timeout");

        try{
            // Override the timeout as 10 secs
            cluster.setProperty("mapred.task.timeout", "10000");

            Util.createInputFile(cluster, "a.txt", new String[] { "dummy"});

            PigServer pig = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());

            String filename = prepareTempFile();
            filename = filename.replace("\\", "\\\\");


            pig.registerQuery("A = load 'a.txt' as (f1:chararray);");
            pig.registerQuery("B = foreach A generate org.apache.pig.test.utils.ReportingUDF();");

            assertEquals("(100)",  pig.openIterator("B").next().toString());
        }finally{
            // Set back the orginal value
            if (taskTimeout!=null)
                cluster.setProperty("mapred.task.timeout", taskTimeout);
        }

    }

    private String prepareTempFile() throws IOException {
        File inputFile = File.createTempFile("test", "txt");
        inputFile.deleteOnExit() ;
        PrintStream ps = new PrintStream(new FileOutputStream(inputFile));
        ps.println("bbb") ;
        ps.close();
        return inputFile.getPath() ;
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        cluster.shutDown();
    }
}
