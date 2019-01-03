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

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BinInterSedes;
import org.apache.pig.data.InterSedes;
import org.apache.pig.data.InterSedesFactory;
import org.apache.pig.data.Tuple;

/**
 * A record reader used to read data written using {@link InterRecordWriter}
 * It uses the default InterSedes object for deserialization.
 */
public class InterRecordReader extends RecordReader<Text, Tuple> {

  private long start;
  private long lastDataPos;
  private long end;
  private BufferedPositionedInputStream in;
  private Tuple value = null;
  private DataInputStream inData = null;
  private static InterSedes sedes = InterSedesFactory.getInterSedesInstance();

  private byte[] syncMarker;
  private long lastSyncPos = -1;
  private long syncMarkerInterval;
  private long dataBytesSeen = 0;

  public InterRecordReader(int syncMarkerLength, long syncMarkerInterval) {
      this.syncMarker = new byte[syncMarkerLength];
      this.syncMarkerInterval = syncMarkerInterval;
  }

  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();

    // open the file
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());

    // read the magic byte sequence serving as record marker but only if the file is not empty
    if (!(start == 0 && end == 0)) {
        fileIn.readFully(0, syncMarker, 0, syncMarker.length);
    }

    //seek to the start of the split
    fileIn.seek(start);

    in = new BufferedPositionedInputStream(fileIn, start);
    inData = new DataInputStream(in);
  }


    /**
     * Skips to next sync marker
     * @return true if marker was observed, false if EOF or EndOfSplit was reached
     * @throws IOException
     */
  public boolean skipUntilMarkerOrSplitEndOrEOF() throws IOException {
      int b = Integer.MIN_VALUE;
      CircularFifoQueue<Integer> queue = new CircularFifoQueue(syncMarker.length);
      outer:while (b != -1) {
          //There may be a case where we read through a whole split without a marker, then we shouldn't proceed
          // because the records are from the next split which another reader would pick up too
          //One exception of reading past split end is if at least the first byte of the marker was seen before split
          // end.
          if (in.getPosition() >= (end+syncMarker.length-1)) {
              return false;
          }
          b = in.read();

          //EOF reached
          if (b == -1) return false;

          queue.add(b);
          if (queue.size() != queue.maxSize()) {
              //Not enough bytes read yet
              continue outer;
          }
          int i = 0;
          for (Integer seenByte : queue){
              if (syncMarker[i++] != seenByte.byteValue()) {
                  continue outer;
              }
          }
          //Found marker: queue content equals sync marker
          lastSyncPos = in.getPosition();
          return true;
      }
      return false;
  }

    /**
     * Reads a sync marker
     * @return true if sync marker was read, false if EOF reached
     * @throws IOException thrown if neither EOF nor proper sync was found
     */
  private boolean readSyncFullyOrEOF() throws IOException {
      int b = in.read();
      if (b == -1) {
          //EOF reached
          return false;
      }
      if ((byte) b != syncMarker[0]) {
          throw new IOException("Corrupt data file, expected sync marker at position " + in.getPosition());
      }
      int i = 1;
      while (i < syncMarker.length) {
          b = in.read();
          if ((byte) b != syncMarker[i]) {
              throw new IOException("Corrupt data file, expected sync marker at position " + in.getPosition());
          }
          ++i;
      }
      lastSyncPos = in.getPosition();
      return true;

  }

  private boolean readDataOrEOF() throws IOException {
      long preDataPos = in.getPosition();
      int b = in.read();
      if(!BinInterSedes.isTupleByte((byte) b) ) {
          if (b == -1) {
              //EOF reached
              return false;
          } else {
              throw new IOException("Corrupt data file, expected tuple type byte, but seen " + b);
          }
      }
      try {
          value =  (Tuple)sedes.readDatum(inData, (byte)b);
          lastDataPos = in.getPosition();
          dataBytesSeen += (lastDataPos-preDataPos);
          return true;
      } catch (ExecException ee) {
          throw ee;
      }
  }

  public boolean nextKeyValue() throws IOException {

      //No marker has been seen, look for next marker
      if (lastSyncPos == -1) {
          if (!skipUntilMarkerOrSplitEndOrEOF()) {
              return false;
          }
      }

      //If we've read more or equal amount of data than the sync interval, we expect a sync marker or EOF
      if (dataBytesSeen >= syncMarkerInterval) {
          boolean isEOF = !readSyncFullyOrEOF();
          if (isEOF) {
              return false;
          }
          dataBytesSeen = 0;
          //If we've just seen a (non-first) sync marker which was completely in the next split then we need to stop
          if (in.getPosition()-syncMarker.length >= end) {
              return false;
          }
      }

      //Sync marker has been seen, expect data
      return readDataOrEOF();
  }

  @Override
  public Text getCurrentKey() {
      // the key is always null since we don't really have a key for each
      // input record
      return null;
  }

  @Override
  public Tuple getCurrentValue() {
    return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (lastDataPos - start) / (float)(end - start));
    }
  }
  
  public synchronized void close() throws IOException {
    if (in != null) {
      in.close(); 
    }
  }
}
