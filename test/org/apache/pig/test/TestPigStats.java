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

import junit.framework.TestCase;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.tools.pigstats.PigStats;

public class TestPigStats extends TestCase {

    public void testBytesWritten_JIRA_1027() {

        File outputFile = null;
        try {
            outputFile = File.createTempFile("JIAR_1027", ".out");
            String filePath = outputFile.getAbsolutePath();
            outputFile.delete();
            PigServer pig = new PigServer(ExecType.LOCAL);
            pig
                    .registerQuery("A = load 'test/org/apache/pig/test/data/passwd';");
            PigStats stats = pig.store("A", filePath)
                    .getStatistics();
            File dataFile = new File( outputFile.getAbsoluteFile() + File.separator + "part-00000" );
            assertEquals(dataFile.length(), stats.getBytesWritten());
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println( e.getMessage() );
            fail("IOException happened");
        } finally {
            if (outputFile != null) {
                // Hadoop Local mode creates a directory
                // Hence we need to delete a directory recursively
                deleteDirectory(outputFile);
            }
        }

    }
    
    private void deleteDirectory( File dir ) {
        File[] files = dir.listFiles();
        for( File file : files ) {
            if( file.isDirectory() ) {
                deleteDirectory(file);
            } else {
                file.delete();
            }
        }
        dir.delete();
    }
}
