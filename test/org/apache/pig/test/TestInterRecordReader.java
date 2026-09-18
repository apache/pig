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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;

import org.apache.pig.PigConfiguration;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.InterRecordReader;
import org.junit.Test;

/**
 * Tests for {@link InterRecordReader} sync-marker scanning.
 * PIG-5475 replaced commons-collections CircularFifoBuffer with an
 * equivalent in-line sliding window; these cases cover that path without
 * depending on commons-collections 3 at runtime.
 */
public class TestInterRecordReader {

    private static void setField(Object target, String name, Object value)
            throws Exception {
        Field field = InterRecordReader.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static boolean skipUntil(byte[] syncMarker, byte[] data, long end)
            throws Exception {
        ByteArrayInputStream bi = new ByteArrayInputStream(data);
        BufferedPositionedInputStream bpi = new BufferedPositionedInputStream(bi);
        InterRecordReader reader = new InterRecordReader(syncMarker.length,
                PigConfiguration.PIG_INTERSTORAGE_SYNCMARKER_INTERVAL_DEFAULT);
        setField(reader, "syncMarker", syncMarker);
        setField(reader, "end", end);
        setField(reader, "in", bpi);
        try {
            return reader.skipUntilMarkerOrSplitEndOrEOF();
        } finally {
            bpi.close();
        }
    }

    @Test
    public void testSkipUntilMarkerAtStart() throws Exception {
        byte[] marker = new byte[] { 1, 2, 3 };
        byte[] data = new byte[] { 1, 2, 3, 9, 9 };
        assertTrue(skipUntil(marker, data, data.length));
    }

    @Test
    public void testSkipUntilMarkerAfterPrefix() throws Exception {
        byte[] marker = new byte[] { 1, 2, 3 };
        byte[] data = new byte[] { 9, 8, 7, 1, 2, 3, 4 };
        assertTrue(skipUntil(marker, data, data.length));
    }

    @Test
    public void testSkipUntilMarkerWithOverlap() throws Exception {
        // Marker shares a prefix with itself; data has a near-miss then a hit.
        byte[] marker = new byte[] { 1, 1, 2 };
        byte[] data = new byte[] { 1, 1, 1, 2, 9 };
        assertTrue(skipUntil(marker, data, data.length));
    }

    @Test
    public void testSkipUntilMarkerSignedBytes() throws Exception {
        // Values above 127 used to be stored as Integer then narrowed with
        // byteValue(); the sliding window must match that signed-byte behavior.
        byte[] marker = new byte[] { (byte) 0xFF, (byte) 0x80, 0x04 };
        byte[] data = new byte[] { 0x7F, (byte) 0xFF, (byte) 0x80, 0x04, 0x01 };
        assertTrue(skipUntil(marker, data, data.length));
    }

    @Test
    public void testSkipUntilMarkerMissingReturnsFalse() throws Exception {
        byte[] marker = new byte[] { 1, 2, 3 };
        byte[] data = new byte[] { 4, 5, 6, 7, 8 };
        assertFalse(skipUntil(marker, data, data.length));
    }

    @Test
    public void testSkipUntilMarkerStopsAtSplitEnd() throws Exception {
        byte[] marker = new byte[] { 1, 2, 3 };
        // Marker sits past the split end; reader must not consume the next split.
        byte[] data = new byte[] { 9, 9, 9, 1, 2, 3 };
        assertFalse(skipUntil(marker, data, 3L));
    }
}
