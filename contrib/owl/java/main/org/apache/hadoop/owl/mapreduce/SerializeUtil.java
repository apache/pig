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
package org.apache.hadoop.owl.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;

/**
 * The class for converting Java objects to String and converting from String to Java Object. Uses same implementation as Pig.
 */
class SerializeUtil {

    /**
     * Serialize the given object.
     * @param obj the object
     * @return the string
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static String serialize(Serializable obj) throws OwlException {
        if (obj == null) return "";
        try {
            ByteArrayOutputStream serialObj = new ByteArrayOutputStream();
            ObjectOutputStream objStream = new ObjectOutputStream(serialObj);
            objStream.writeObject(obj);
            objStream.close();
            return encodeBytes(serialObj.toByteArray());
        } catch (Exception e) {
            throw new OwlException(ErrorType.ERROR_SERIALIZING_DATA, e);
        }
    }

    /**
     * Deserialize the given string to an Object.
     * @param str the string
     * @return the object
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public static Object deserialize(String str) throws OwlException {
        if (str == null || str.length() == 0) return null;
        try {
            ByteArrayInputStream serialObj = new ByteArrayInputStream(decodeBytes(str));
            ObjectInputStream objStream = new ObjectInputStream(serialObj);
            return objStream.readObject();
        } catch (Exception e) {
            throw new OwlException(ErrorType.ERROR_DESERIALIZING_DATA, e);
        }
    }

    /**
     * Encode bytes to a String representation.
     * @param bytes the bytes to encode
     * @return the encoded String 
     */
    private static String encodeBytes(byte[] bytes) {
        StringBuffer strBuf = new StringBuffer();

        for (int i = 0; i < bytes.length; i++) {
            strBuf.append((char) (((bytes[i] >> 4) & 0xF) + 'a'));
            strBuf.append((char) (((bytes[i]) & 0xF) + 'a'));
        }

        return strBuf.toString();
    }

    /**
     * Decode String into a byte array.
     * @param str the string
     * @return the decoded byte array
     */
    private static byte[] decodeBytes(String str) {
        byte[] bytes = new byte[str.length() / 2];
        for (int i = 0; i < str.length(); i+=2) {
            char c = str.charAt(i);
            bytes[i/2] = (byte) ((c - 'a') << 4);
            c = str.charAt(i+1);
            bytes[i/2] += (c - 'a');
        }
        return bytes;
    }
}


