/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.impl.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;

public class ObjectSerializer {

    public static String serialize(Serializable obj) throws IOException {
        if (obj == null)
            return "";
        try {
            ByteArrayOutputStream serialObj = new ByteArrayOutputStream();
            Deflater def = new Deflater(Deflater.BEST_COMPRESSION);
            ObjectOutputStream objStream = new ObjectOutputStream(new DeflaterOutputStream(
                    serialObj, def));
            objStream.writeObject(obj);
            objStream.close();
            return encodeBytes(serialObj.toByteArray());
        } catch (Exception e) {
            throw new IOException("Serialization error: " + e.getMessage(), e);
        }
    }

    public static Object deserialize(String str) throws IOException {
        if (str == null || str.length() == 0)
            return null;
        ObjectInputStream objStream = null;
        try {
            ByteArrayInputStream serialObj = new ByteArrayInputStream(decodeBytes(str));
            objStream = new ClassLoaderObjectInputStream(Thread.currentThread().getContextClassLoader(), new InflaterInputStream(serialObj));
            return objStream.readObject();
        } catch (Exception e) {
            throw new IOException("Deserialization error: " + e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(objStream);
        }
    }

    public static String encodeBytes(byte[] bytes) throws UnsupportedEncodingException {
        return bytes == null ? null : new String(Base64.encodeBase64(bytes), Charset.forName("UTF-8"));
    }

    public static byte[] decodeBytes(String str) throws UnsupportedEncodingException {
        return Base64.decodeBase64(str.getBytes(Charset.forName("UTF-8")));
    }
}
