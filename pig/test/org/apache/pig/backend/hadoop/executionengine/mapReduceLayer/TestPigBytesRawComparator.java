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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import static junit.framework.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.data.BinInterSedes;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.impl.io.NullableBytesWritable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestPigBytesRawComparator {
  private static PigBytesRawComparator bytesRawComparator ;
  private static ByteArrayOutputStream bas;


  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    bytesRawComparator =  new PigBytesRawComparator();
    bytesRawComparator.setConf(new JobConf());

    //this bytesarray is used for holding serialized NullableBytesWritable
    bas = new ByteArrayOutputStream(BinInterSedes.UNSIGNED_SHORT_MAX + 10);
  }
  @AfterClass
  public static void setupAfterClass() throws Exception {
    bytesRawComparator =  null;
    bas = null;
  }

  @Test
  public void testBoolean() throws Exception {
    assertTrue("boolean True and False considered equal",
        compareTwoObjectsAsNullableBytesWritables(
                                new Boolean(true), new Boolean(false)) !=  0);
  }


  @Test
  public void testSingleInt0and1() throws Exception {
    assertTrue("Integer 0 and Integer 1 considered equal",
        compareTwoObjectsAsNullableBytesWritables(
                                new Integer(0), new Integer(1)) !=  0);
  }

  @Test
  public void testSingleInt() throws Exception {
    assertTrue("Integer 5 and Integer 7 considered equal",
        compareTwoObjectsAsNullableBytesWritables(
                                new Integer(5), new Integer(7)) !=  0);
  }

  @Test
  public void testSingleLong0and1() throws Exception {
     assertTrue("Long 0 and Long 1 considered equal",
        compareTwoObjectsAsNullableBytesWritables(
                                new Long(0), new Long(1)) != 0);
  }

  @Test
  public void testSingleLong() throws Exception {
    assertTrue("Long 5 and Long 7 considered equal",
        compareTwoObjectsAsNullableBytesWritables(
                                new Long(5), new Long(7)) !=  0);
  }

  @Test
  public void testSingleByte() throws Exception {
    assertTrue("Byte 5 and Byte 7 considered equal",
        compareTwoObjectsAsNullableBytesWritables(
                                new Byte((byte)5), new Byte((byte)7)) !=  0);
  }

  @Test
  public void testSingleByteDiffByteArray() throws Exception {
     assertTrue("'ab' and 'ac' considered equal",
        compareTwoObjectsAsNullableBytesWritables(
                           new DataByteArray(new byte[] {'a','b'}),
                           new DataByteArray(new byte[] {'a','c'})) != 0);
     assertTrue("'ab' and 'abc' considered equal",
        compareTwoObjectsAsNullableBytesWritables(
                           new DataByteArray(new byte[] {'a','b'}),
                           new DataByteArray(new byte[] {'a','b','c'})) != 0);
     assertTrue("'ab' and 'cb' considered equal",
        compareTwoObjectsAsNullableBytesWritables(
                           new DataByteArray(new byte[] {'a','b'}),
                           new DataByteArray(new byte[] {'c','b'})) != 0);
     assertTrue("'a' and 'b' considered equal",
        compareTwoObjectsAsNullableBytesWritables(
                           new DataByteArray(new byte[] {'a'}),
                           new DataByteArray(new byte[] {'b'})) != 0);
  }

  @Test
  public void testDifferentType() throws Exception {
     assertTrue("Integer 9999 and Long 9999 considered equal",
        compareTwoObjectsAsNullableBytesWritables(new Integer(9999), new Long(9999)) != 0 );
  }

  @Test
  public void testByteArrayAlphabeticalOrderingByLength() throws Exception {
    // making sure we order as
    // '1'
    // '2'
    // '22'
    // '222' and repeats...
    // If compare includes the header, order suddenly changes when header
    // changes from INTEGER_INSHORT to INTEGER.
    // Here, we're making sure that doesn't happen.
    //
    for(int i = BinInterSedes.UNSIGNED_BYTE_MAX - 3 ;
            i <  BinInterSedes.UNSIGNED_BYTE_MAX + 3; i++){
      DataByteArray dba1 = createRepeatedByteArray((byte)'2', i);
      DataByteArray dba2 = createRepeatedByteArray((byte)'2', i + 1);
      assertTrue("ByteArray order changed at UNSIGNED_BYTE_MAX boundary",
                 compareTwoObjectsAsNullableBytesWritables(dba1, dba2) < 0);
    }
    for(int i = BinInterSedes.UNSIGNED_SHORT_MAX -3 ;
            i <  BinInterSedes.UNSIGNED_SHORT_MAX + 3; i++){
      DataByteArray dba1 = createRepeatedByteArray((byte)'2', i);
      DataByteArray dba2 = createRepeatedByteArray((byte)'2', i + 1);
      assertTrue("ByteArray order changed at UNSIGNED_SHORT_MAX boundary",
                 compareTwoObjectsAsNullableBytesWritables(dba1, dba2) < 0);
    }
  }

  @Test
  public void testLongByteArrays() throws Exception {
    for(int length: new int [] {BinInterSedes.UNSIGNED_BYTE_MAX + 50,
                                BinInterSedes.UNSIGNED_SHORT_MAX + 50} ) {
      // To make sure that offset & length are set correctly for
      // BinInterSedes.SMALLBYTEARRAY and BinInterSedes.BYTEARRAY,
      // create a long bytearray and compare with first or last byte changed

      byte [] ba1 = new byte[length];
      byte [] ba2 = new byte[length];
      Arrays.fill(ba1, 0, length, (byte)'a');
      Arrays.fill(ba2, 0, length, (byte)'a');
      //changing only the last byte
      ba2[length-1] = 'b';
      assertTrue("ByteArray with length: " + length
                 + " compare failed with the last byte",
                 compareTwoObjectsAsNullableBytesWritables(
                      new DataByteArray(ba1), new DataByteArray(ba2)) != 0);
      //setting back
      ba2[length-1] = 'a';

      //now changing the first byte
      ba2[0] = 'b';
      assertTrue("ByteArray with length: " + length
                 + " compare failed with the first byte",
                 compareTwoObjectsAsNullableBytesWritables(
                      new DataByteArray(ba1), new DataByteArray(ba2)) != 0);
    }
  }

  private DataByteArray createRepeatedByteArray(byte c, int num) {
    byte [] ba = new byte[num];
    Arrays.fill(ba, 0, num, c);
    return new DataByteArray(ba);
  }

  // Wrap the passed object with NullableBytesWritable and return the serialized
  // form.
  private byte [] serializeAsNullableBytesWritable(Object obj) throws Exception {
    NullableBytesWritable nbw = new NullableBytesWritable(obj);
    bas.reset();
    DataOutput dout = new DataOutputStream(bas);
    nbw.write(dout);
    return bas.toByteArray();
  }

  // Take two objects wrapped by NullableBytesWritable and comare using
  // PigBytesRawComparator
  private int compareTwoObjectsAsNullableBytesWritables(Object obj1, Object obj2) throws Exception {
    byte [] ba1 = serializeAsNullableBytesWritable(obj1);
    byte [] ba2 = serializeAsNullableBytesWritable(obj2);
    return bytesRawComparator.compare(ba1, 0, ba1.length, ba2, 0, ba2.length);
  }
}
