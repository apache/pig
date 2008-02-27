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

package org.apache.pig.backend.datastorage;

import java.io.InputStream;
import java.io.IOException;

/**
 * Unix-like API for an input stream that supports random access.
 *
 */
public abstract class SeekableInputStream extends InputStream {
    
    public enum FLAGS {
        SEEK_SET,
        SEEK_CUR,
        SEEK_END,
    }
    
    /**
     * Seeks to a given offset as specified by whence flags.
     * If whence is SEEK_SET, offset is added to beginning of stream
     * If whence is SEEK_CUR, offset is added to current position inside stream
     * If whence is SEEK_END, offset is added to end of file position
     * 
     * @param offset
     * @param whence
     * @throws IOException
     */
    public abstract void seek(long offset, FLAGS whence) throws IOException;
    
    /**
     * Returns current offset
     * 
     * @return offset
     * @throws IOException
     */
    public abstract long tell() throws IOException;
}
