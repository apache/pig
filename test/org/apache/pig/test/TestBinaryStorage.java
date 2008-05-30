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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import org.apache.hadoop.io.WritableComparator;
import org.apache.pig.PigServer;
import org.apache.pig.PigServer.ExecType;
import org.apache.pig.impl.io.FileLocalizer;
import org.junit.Test;

import junit.framework.TestCase;

public class TestBinaryStorage extends PigExecTestCase {
    private static final String simpleEchoStreamingCommand = 
        "perl -ne 'print \"$_\"'";

    private static final int MAX_DATA_SIZE = 1024;
    
    @Test
    public void testBinaryStorageWithAsciiData() throws Exception {
        // Create input file with ascii data
        File input = Util.createInputFile("tmp", "", 
                new String[] {"A,1", "B,2", "C,3", "D,2",
                              "A,5", "B,5", "C,8", "A,8",
                              "D,8", "A,9"});
        
        // Test if data is handled correctly by BinaryStorage
        testBinaryStorage(input);
    }
    
    @Test
    public void testBinaryStorageWithBinaryData() throws Exception {
        // Create input file and fill it with random binary data
        File input = File.createTempFile("tmp", "dat");
        byte[] data = new byte[MAX_DATA_SIZE];
        randomizeBytes(data, 0, data.length);
        
        FileOutputStream os = new FileOutputStream(input);
        os.write(data, 0, data.length);
        os.close();
        
        // Test if data is handled correctly by BinaryStorage
        testBinaryStorage(input);
    }
    
    private void testBinaryStorage(File input) 
    throws Exception {

        // Get input data 
        byte[] inputData = new byte[MAX_DATA_SIZE];
        int inputLen = 
            readFileDataIntoBuffer(new FileInputStream(input), inputData);
        
        // Pig query to run
        pigServer.registerQuery("DEFINE CMD `" + 
                                simpleEchoStreamingCommand + "` " +
                                "input(stdin using BinaryStorage()) " +
                                "output(stdout using BinaryStorage());");
        pigServer.registerQuery("IP = load 'file:" + Util.encodeEscape(input.toString()) + "' using " + 
                                "BinaryStorage() split by 'file';");
        pigServer.registerQuery("OP = stream IP through CMD;");

        // Save the output using BinaryStorage
        String output = "./pig.BinaryStorage.out";
        pigServer.store("OP", output, "BinaryStorage()");
        
        // Get output data 
        InputStream out = FileLocalizer.open(output, pigServer.getPigContext());
        byte[] outputData = new byte[MAX_DATA_SIZE];
        int outputLen = readFileDataIntoBuffer(out, outputData);

        // Check if the data is the same ...
        assertEquals(true, 
                WritableComparator.compareBytes(inputData, 0, inputLen, 
                                                outputData, 0, outputLen) == 0);
        
        // Cleanup
        out.close();
        pigServer.deleteFile(output);
    }

    private static void randomizeBytes(byte[] data, int offset, int length) {
        Random random = new Random();
        for(int i=offset + length - 1; i >= offset; --i) {
            data[i] = (byte) random.nextInt(256);
        }
    }

    private static int readFileDataIntoBuffer(InputStream is, byte[] buffer) 
    throws IOException {
        int n = 0;
        int off = 0, len = buffer.length;
        while (len > 0 && (n = is.read(buffer, off, len)) != -1) {
            off += n;
            len -= n;
        }
        return off;
    }
}
