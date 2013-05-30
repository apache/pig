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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.test.Util;

public class TestMultiStorageCompression extends TestCase {
   
   private static String patternString = "(\\d+)!+(\\w+)~+(\\w+)";
   public static ArrayList<String[]> data = new ArrayList<String[]>();
   static {
      data.add(new String[] { "f1,a,123" });
      data.add(new String[] { "f2,b,234" });
      data.add(new String[] { "f3,c,345" });
      data.add(new String[] { "f4,d,567" });
   }

   public void testMultiStorageShouldSupportBz2() throws Exception {

      String type = "bz2";
      List<String> filesToDelete = new ArrayList<String>();

      String tmpDir = System.getProperty("java.io.tmpdir");
      String outputPath = tmpDir + File.separator + "output001." + type;

      filesToDelete.add(outputPath);

      try {
         runQuery(outputPath, type);
         verifyResults(type, filesToDelete, outputPath);
      } finally {
         cleanUpDirs(filesToDelete);
      }
   }

   public void testMultiStorageShouldSupportGz() throws Exception {

      String type = "gz";
      List<String> filesToDelete = new ArrayList<String>();

      String tmpDir = System.getProperty("java.io.tmpdir");
      String outputPath = tmpDir + File.separator + "output001." + type;

      filesToDelete.add(outputPath);

      try {
         runQuery(outputPath, type);
         verifyResults(type, filesToDelete, outputPath);
      } finally {
         cleanUpDirs(filesToDelete);
      }
   }

   private void cleanUpDirs(List<String> filesToDelete) {
      // Delete files recursively
      Collections.reverse(filesToDelete);
      for (String string : filesToDelete)
         new File(string).delete();
   }


   private void verifyResults(String type, List<String> filesToDelete,
         String outputPath) throws IOException, FileNotFoundException {
      // Verify the output
      File outputDir = new File(outputPath);
      List<String> indexFolders = Arrays.asList(outputDir.list());

      // Assert whether all keys are present
      assertTrue(indexFolders.contains("f1." + type));
      assertTrue(indexFolders.contains("f2." + type));
      assertTrue(indexFolders.contains("f3." + type));
      assertTrue(indexFolders.contains("f4." + type));

      // Sort so that assertions are easy
      Collections.sort(indexFolders);

      for (int i = 0; i < indexFolders.size(); i++) {

         String indexFolder = indexFolders.get(i);
         if (indexFolder.startsWith("._SUCCESS")||indexFolder.startsWith("_SUCCESS"))
             continue;
         String topFolder = outputPath + File.separator + indexFolder;
         File indexFolderFile = new File(topFolder);
         filesToDelete.add(topFolder);
         String[] list = indexFolderFile.list();
         for (String outputFile : list) {

            String file = topFolder + File.separator + outputFile;
            filesToDelete.add(file);

            // Skip off any file starting with .
            if (outputFile.startsWith("."))
               continue;

            // Try to read the records using the codec
            CompressionCodec codec = null;

            
            // Use the codec according to the test case
            if (type.equals("bz2"))
               codec = new BZip2Codec();
            else if (type.equals("gz")) {
               codec = new GzipCodec();
               ((GzipCodec)codec).setConf(new Configuration());
            }

            CompressionInputStream createInputStream = codec
                  .createInputStream(new FileInputStream(file));
            int b;
            StringBuffer sb = new StringBuffer();
            while ((b = createInputStream.read()) != -1) {
               sb.append((char) b);
            }
            createInputStream.close();

            // Assert for the number of fields and keys.
            String[] fields = sb.toString().split("\\t");
            assertEquals(3, fields.length);
            String id = indexFolder.substring(1,2);
            assertEquals("f" + id, fields[0]);

         }

      }
   }

   private void runQuery(String outputPath, String compressionType)
         throws Exception, ExecException, IOException, FrontendException {
      
      // create a data file
      String filename = TestHelper.createTempFile(data, "");
      PigServer pig = new PigServer(LOCAL);
      filename = filename.replace("\\", "\\\\");
      patternString = patternString.replace("\\", "\\\\");
      String query = "A = LOAD '" + Util.encodeEscape(filename)
            + "' USING PigStorage(',') as (a,b,c);";

      String query2 = "STORE A INTO '" + Util.encodeEscape(outputPath)
            + "' USING org.apache.pig.piggybank.storage.MultiStorage" + "('"
            + Util.encodeEscape(outputPath) + "','0', '" + compressionType + "', '\\t');";

      // Run Pig
      pig.setBatchOn();
      pig.registerQuery(query);
      pig.registerQuery(query2);

      pig.executeBatch();
   }
  

}
