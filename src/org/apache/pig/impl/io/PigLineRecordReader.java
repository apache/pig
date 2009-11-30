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

package org.apache.pig.impl.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.tools.bzip2r.CBZip2InputStream;

/**
 * This class is for Reading data line by Line in Pig
 * It uses org.apache.hadoop.mapred.LineRecordReader for reading simple Text
 * For BZip it uses a different class which does not do buffering.
 *
 */
public class PigLineRecordReader {

    protected Reader lineReader;
    
    public PigLineRecordReader(BufferedPositionedInputStream in, long offset, long end ) {
        
        if( in.in instanceof CBZip2InputStream ) {
            // Here we ignore the maxLineLength as the Reader can go out of Heap space
            // if the value of maxLineLength is too high
            lineReader = new LineReader(in, offset, end );
        } else {
            lineReader = new BufferingLineReader(in, offset, end, Integer.MAX_VALUE );
        }
    }
    
    /**
     * Wrapper around the original LineReader
     * @param value Text into which line value is written
     * @return true if more data is available, else false
     * @throws IOException
     */
    public boolean next( Text value ) throws IOException {
        return lineReader.getNext( value );
    }
    
    /**
     * Wrapper around the LineReader to provide position
     */
    public long getPosition() throws IOException {
        return lineReader.getPosition();
    }
    
    /**
     * Abstract class that is used to handle reading of
     * values
     */
    public static abstract class Reader {
        
        /**
         * Variable maintaining OS Type used for finding EOL in UNIX and WINDOWS
         */
        protected int os;
        protected static final int OS_UNIX = 0;
        protected static final int OS_WINDOWS = 1;
        InputStream in;
        Reader( InputStream in ) {
            this.in = in;
            os = OS_UNIX;
            if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS"))
                os = OS_WINDOWS;
        }
        /**
         * Provides next line read from InputStream 
         * @param value Text the the line is supposed to be returned in
         * @return true if more data is available, else false
         * @throws IOException
         */
        abstract public boolean getNext( Text value ) throws IOException;
        /**
         * Returns the position of current Buffer
         * @return long value of position of current stream
         */
        abstract public long getPosition() throws IOException;
    }
    
    /**
     * This is a simple implementation of LineReader without buffering
     *
     */
    public static class LineReader extends Reader {

        /**
         * Starting offset of the buffer to be read
         */
        protected long start;
        /**
         * Ending offset until which the buffer can be read. This is a soft boundary.
         */
        protected long end;
        /**
         * Maximum line length expected in the input file
         */
        protected int maxLineLength = 4096;
        LineReader(InputStream in, long start, long end ) {
            super(in);
            this.start = start;
            this.end = end;
        }

        @Override
        public boolean getNext(Text value) throws IOException {
            ByteArrayOutputStream mBuf = new ByteArrayOutputStream(maxLineLength);
            mBuf.reset();
            value.clear();
            while (true) {
                // BufferedPositionedInputStream is buffered, so I don't need
                // to buffer.
                int b = in.read();
                
                if (b == '\n' ) {
                    byte[] array = mBuf.toByteArray();
                    if (array.length != 0 && array[array.length-1]=='\r' 
                        && os==OS_WINDOWS) {
                        // Here we dont copy the last '\r' in the Text Value
                        value.append(array, 0, array.length - 1 );
                    } else {
                        value.append(mBuf.toByteArray(), 0, mBuf.size());
                    }
                    mBuf = null;
                    return true;
                } else if( b == -1 ) {
                    value.append(mBuf.toByteArray(), 0, mBuf.size());
                    mBuf = null;
                    return false;
                } else {
                    mBuf.write(b);
                    // This is the case when length of line is more than maxLineLength
                    if( mBuf.size() == maxLineLength ) {
                        value.append(mBuf.toByteArray(), 0, mBuf.size() );
                        mBuf.reset();
                    }
                }
            }
        }

        @Override
        public long getPosition() throws IOException {
            return ((BufferedPositionedInputStream)in).getPosition();
        }
    }
    
      /**
       * A buffering LineReader. This class uses <code>org.apache.mapred.LineRecordReader</code>
       * for reading line using buffering. 
       */
      public static class BufferingLineReader extends Reader {
        LineRecordReader reader;

        /**
         * Create a line reader that reads from the given stream using the 
         * given buffer-size.
         * @param in InputStream from which BufferingLineReader reads data
         * @throws IOException
         */
        BufferingLineReader(InputStream in, long start, long end, int maxLineLength) {
          super( in );
          this.reader = new LineRecordReader(in, start, end, maxLineLength);
        }
        
        public boolean getNext( Text value ) throws IOException {
            LongWritable key = new LongWritable();
            return this.reader.next( key, value );
        }

        public long getPosition() throws IOException{
            return this.reader.getPos();
        }
      }
}
