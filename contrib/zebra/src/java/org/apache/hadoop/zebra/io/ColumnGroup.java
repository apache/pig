/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.zebra.io;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.io.file.tfile.Utils;
import org.apache.hadoop.io.file.tfile.ByteArray;
import org.apache.hadoop.io.file.tfile.RawComparable;
import org.apache.hadoop.zebra.types.CGSchema;
import org.apache.hadoop.zebra.types.ParseException;
import org.apache.hadoop.zebra.types.Partition;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.hadoop.zebra.types.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.hadoop.zebra.types.TypesUtils.TupleReader;
import org.apache.hadoop.zebra.types.TypesUtils.TupleWriter;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * ColumnGroup is the basic unit of a persistent table. The following
 * Configuration parameters can customize the behavior of ColumnGroup.
 * <ul>
 * <li><b>table.output.tfile.minBlock.size</b> (int) Minimum compression block
 * size for underlying TFile (default to 1024*1024).
 * <li><b>table.output.tfile.compression</b> (String) Compression method (one
 * of "none", "lzo", "gz") (default to "lzo").
 * 
 * @see {@link TFile#getSupportedCompressionAlgorithms()}
 *      <li><b>table.input.split.minSize</b> (int) Minimum split size (default
 *      to 64*1024).
 *      </ul>
 */
class ColumnGroup {
  private final static String CONF_COMPRESS = "table.output.tfile.compression";
  private final static String DEFAULT_COMPRESS = "gz";
  private final static String CONF_MIN_BLOCK_SIZE = "table.tfile.minblock.size";
  private final static int DEFAULT_MIN_BLOCK_SIZE = 1024 * 1024;

  private final static String CONF_MIN_SPLIT_SIZE = "table.input.split.minSize";
  private final static int DEFAULT_MIN_SPLIT_SIZE = 64 * 1024;

  private static final double SPLIT_SLOP = 1.1; // 10% slop

  // excluding files start with the following prefix, may change to regex
  private final static String CONF_NON_DATAFILE_PREFIX =
      "table.cg.nondatafile.prefix";
  private final static String SPECIAL_FILE_PREFIX = ".";

  // tmp schema file name, used as a flag of unfinished CG
  private final static String SCHEMA_FILE = ".schema";
  // meta data TFile for entire CG, used as a flag of closed CG
  final static String META_FILE = ".meta";

  static final String BLOCK_NAME_INDEX = "ColumnGroup.index";

  static Path makeMetaFilePath(Path parent) {
    return new Path(parent, META_FILE);
  }

  static String getCompression(Configuration conf) {
    return conf.get(CONF_COMPRESS, DEFAULT_COMPRESS);
  }

  static int getMinBlockSize(Configuration conf) {
    return conf.getInt(CONF_MIN_BLOCK_SIZE, DEFAULT_MIN_BLOCK_SIZE);
  }

  static String getNonDataFilePrefix(Configuration conf) {
    return conf.get(CONF_NON_DATAFILE_PREFIX, SPECIAL_FILE_PREFIX);
  }

  static int getMinSplitSize(Configuration conf) {
    return conf.getInt(CONF_MIN_SPLIT_SIZE, DEFAULT_MIN_SPLIT_SIZE);
  }

  /**
   * Drop a Column Group, maps to deleting all the files relating to this Column
   * Group on the FileSystem.
   * 
   * @param path
   *          the path to the ColumnGroup.
   * @param conf
   *          The configuration object.
   */
  public static void drop(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    fs.delete(path, true);
    // TODO:
    // fs.close();
  }

  /**
   * Scan the file system, looking for TFiles, and build an in-memory index of a
   * column group.
   * 
   * @param fs
   *          The file system
   * @param path
   *          The base path of the column group.
   * @param dirty
   *          Whether to build dirty index or not. Dirty index is built by only
   *          looking at file-level status and not opening up individual TFiles.
   *          The flag may only be set for unsorted ColumnGroups.
   * @param conf
   *          The configuration object.
   * @return The in-memory index object.
   * @throws IOException
   */
  static CGIndex buildIndex(FileSystem fs, Path path, boolean dirty,
      Configuration conf) throws IOException {
    CGIndex ret = new CGIndex();
    FileStatus[] files = fs.listStatus(path, new CGPathFilter(conf));
    Comparator<RawComparable> comparator = null;
    for (FileStatus f : files) {
      if (dirty) {
        ret.add(f.getLen(), f.getPath().getName());
      }
      else {
        FSDataInputStream dis = null;
        TFile.Reader tr = null;
        try {
          dis = fs.open(f.getPath());
          tr = new TFile.Reader(dis, f.getLen(), conf);
          if (comparator == null) {
            comparator = tr.getComparator();
          }
          if (tr.getEntryCount() > 0) {
            CGIndexEntry range =
                new CGIndexEntry(f.getPath().getName(), tr.getEntryCount(), tr
                    .getFirstKey(), tr.getLastKey());
            ret.add(f.getLen(), tr.getEntryCount(), range);
          }
        }
        catch (IOException e) {
          // TODO: log the error, ignore incorrect TFiles.
          e.printStackTrace(System.err);
        }
        finally {
          if (tr != null) {
            tr.close();
          }
          if (dis != null) {
            dis.close();
          }
        }
      }
    }

    ret.sort(comparator);
    return ret;
  }

  /**
   * ColumnGroup reader.
   */
  public static class Reader implements Closeable {
    Path path;
    Configuration conf;
    FileSystem fs;
    CGSchema cgschema;
    Comparator<RawComparable> comparator;
    Projection projection;
    CGIndex cgindex;
    ArrayList<SplitColumn> exec;
    SplitColumn top; // directly associated with logical schema
    SplitColumn leaf; // corresponding to projection
    boolean closed;

    /**
     * Get the Column Group physical schema without loading the full CG index.
     * 
     * @param path
     *          The path to the ColumnGroup.
     * @param conf
     *          The configuration object.
     * @return The ColumnGroup schema.
     * @throws IOException
     */

    public static Schema getSchema(Path path, Configuration conf)
        throws IOException, ParseException {
      FileSystem fs = path.getFileSystem(conf);
      CGSchema cgschema = CGSchema.load(fs, path);
      return cgschema.getSchema();
    }

    /**
     * Create a ColumnGroup reader.
     * 
     * @param path
     *          The directory path to the column group.
     * @param conf
     *          Optional configuration parameters.
     * @throws IOException
     */
    public Reader(Path path, Configuration conf) throws IOException,
        ParseException {
      this(path, true, conf);
    }

    Reader(Path path, boolean dirty, Configuration conf) throws IOException,
        ParseException {
      this.path = path;
      this.conf = conf;

      fs = path.getFileSystem(conf);
      // check existence of path
      if (!fs.exists(path)) {
        throw new IOException("Path doesn't exist: " + path);
      }

      if (!fs.getFileStatus(path).isDir()) {
        throw new IOException("Path exists but not a directory: " + path);
      }

      cgschema = CGSchema.load(fs, path);
      if (cgschema.isSorted()) {
        comparator = TFile.makeComparator(cgschema.getComparator());
      }
      projection = new Projection(cgschema.getSchema()); // default projection to CG schema.
      Path metaFilePath = makeMetaFilePath(path);
      if (!fs.exists(metaFilePath)) {
        // special case for unsorted CG that did not create index properly.
        if (cgschema.isSorted()) {
          throw new FileNotFoundException(
              "Missing Meta File for sorted Column Group");
        }
        cgindex = buildIndex(fs, path, dirty, conf);
      }
      else {
        MetaFile.Reader metaFile = MetaFile.createReader(metaFilePath, conf);
        try {
          cgindex = new CGIndex();
          DataInputStream dis = metaFile.getMetaBlock(BLOCK_NAME_INDEX);
          try {
            cgindex.readFields(dis);
          }
          finally {
            dis.close();
          }
        }
        finally {
          metaFile.close();
        }
      }
    }

    /**
     * Set the projection for the reader. This will affect calls to
     * getScanner(), getStatus(), and getColumnNames().
     * 
     * @param projection
     *          The projection on the column group for subsequent read
     *          operations. If we want select all columns, pass
     *          projection==null.
     */
    public synchronized void setProjection(String projection) throws ParseException {
      if (projection == null) {
        this.projection = new Projection(cgschema.getSchema());
      }
      else {
        this.projection = new Projection(cgschema.getSchema(), projection);
      }
    }

    /**
     * Get the schema of columns of the table (possibly through projection).
     * 
     * @return Schema of the columns of the table (possibly through projection).
     */
    public Schema getSchema() throws ParseException {
      return projection.getSchema();
    }
    
    /**
     * Get the projection
     * @return Projection of this Reader
     */
    public Projection getProjection() {
      return projection;
    }

    public String getSerializer() {
      return cgschema.getSerializer();
    }

    public String getCompressor() {
      return cgschema.getCompressor();
    }

    public CGSchema getCGSchema() {
      return cgschema;
    }

    /**
     * Get a scanner that reads all rows whose row keys fall in a specific
     * range.
     * 
     * @param beginKey
     *          The begin key of the scan range.
     * @param endKey
     *          The end key of the scan range.
     * @param closeReader
     *          close the underlying Reader object when we close the scanner.
     *          Should be set to true if we have only one scanner on top of the
     *          reader, so that we should release resources after the scanner is
     *          closed.
     * @return A scanner object.
     * @throws IOException
     */
    public synchronized TableScanner getScanner(BytesWritable beginKey,
        BytesWritable endKey, boolean closeReader) throws IOException,
        ParseException {
      if (closed) {
        throw new EOFException("Reader already closed");
      }
      if (!isSorted()) {
        throw new IOException(
            "Cannot get key-bounded scanner for unsorted table");
      }
      RawComparable begin =
          (beginKey != null) ? new ByteArray(beginKey.get(), 0, beginKey
              .getSize()) : null;
      RawComparable end =
          (endKey != null) ? new ByteArray(endKey.get(), 0, endKey.getSize())
              : null;
      if (begin != null && end != null) {
        if (comparator.compare(begin, end) >= 0) {
          throw new IOException("Zero-key-range split");
        }
      }

      return new CGScanner(begin, end, closeReader);
    }

    /**
     * Get a scanner that reads a consecutive number of rows as defined in the
     * CGRangeSplit object, which should be obtained from previous calls of
     * rangeSplit().
     * 
     * @param split
     *          The split range. If null, get a scanner to read the complete
     *          column group.
     * @param closeReader
     *          close the underlying Reader object when we close the scanner.
     *          Should be set to true if we have only one scanner on top of the
     *          reader, so that we should release resources after the scanner is
     *          closed.
     * @return A scanner object.
     * @throws IOException
     */
    public synchronized TableScanner getScanner(CGRangeSplit split,
        boolean closeReader) throws IOException, ParseException {
      if (closed) {
        throw new EOFException("Reader already closed");
      }

      if (split == null) {
        return getScanner(new CGRangeSplit(0, cgindex.size()), closeReader);
      }
      if (split.len < 0) {
        throw new IllegalArgumentException("Illegal range split");
      }

      if (split.len == 0) {
        throw new IOException("Zero-length range split");
      }

      return new CGScanner(split, closeReader);
    }

    /**
     * Given a split range, calculate how the file data that fall into the range
     * are distributed among hosts.
     * 
     * @param split
     *          The range-based split. If null, return all blocks.
     * @return a map from host name to the amount of data (in bytes) the host
     *         owns that fall roughly into the key range.
     */
    public BlockDistribution getBlockDistribution(CGRangeSplit split)
        throws IOException {
      if (split == null) {
        return getBlockDistribution(new CGRangeSplit(0, cgindex.size()));
      }

      if ((split.start | split.len | (cgindex.size() - split.start - split.len)) < 0) {
        throw new IndexOutOfBoundsException("Bad split");
      }

      BlockDistribution ret = new BlockDistribution();
      for (int i = split.start; i < split.start + split.len; ++i) {
        CGIndexEntry dfkr = cgindex.get(i);
        Path tfilePath = new Path(path, dfkr.getName());
        FileStatus tfileStatus = fs.getFileStatus(tfilePath);
        BlockLocation[] locations =
            fs.getFileBlockLocations(tfileStatus, 0, tfileStatus.getLen());
        for (BlockLocation l : locations) {
          ret.add(l);
        }
      }

      return ret;
    }

    /**
     * Get a sampling of keys and calculate how data are distributed among
     * key-partitioned buckets. The implementation attempts to calculate all
     * information in one shot to avoid reading TFile index multiple times.
     * Special care is also taken that memory requirement is not linear to the
     * size of total data set for the column group.
     * 
     * @param n
     *          Targeted size of the sampling.
     * @return KeyDistribution object.
     * @throws IOException
     */
    public KeyDistribution getKeyDistribution(int n) throws IOException {
      // TODO: any need for similar capability for unsorted for sorted CGs?
      if (!isSorted()) {
        throw new IOException("Cannot get key distribution for unsorted table");
      }
      KeyDistribution ret = new KeyDistribution(comparator);
      Path[] paths = new Path[cgindex.size()];
      FileStatus[] tfileStatus = new FileStatus[paths.length];
      long totalBytes = 0;
      for (int i = 0; i < paths.length; ++i) {
        paths[i] = cgindex.getPath(i, path);
        tfileStatus[i] = fs.getFileStatus(paths[i]);
        totalBytes += tfileStatus[i].getLen();
      }
      // variable.

      final long EPSILON = (long) (getMinSplitSize(conf) * (SPLIT_SLOP - 1));
      long goalSize = totalBytes / n;
      goalSize = Math.max(getMinSplitSize(conf), goalSize);
      for (int i = 0; i < paths.length; ++i) {
        FileStatus fstatus = tfileStatus[i];
        long blkSize = fstatus.getBlockSize();
        long fileLen = fstatus.getLen();
        long stepSize =
            (goalSize > blkSize) ? goalSize / blkSize * blkSize : blkSize
                / (blkSize / goalSize);
        FSDataInputStream fsdis = null;
        TFile.Reader reader = null;
        long remainLen = fileLen;
        boolean done = false;
        try {
          fsdis = fs.open(paths[i]);
          reader = new TFile.Reader(fsdis, tfileStatus[i].getLen(), conf);
          while ((remainLen > 0) && !done) {
            long splitBytes =
                (remainLen > stepSize * SPLIT_SLOP) ? stepSize : remainLen;
            long offsetBegin = fileLen - remainLen;
            long offsetEnd = offsetBegin + splitBytes;
            BlockLocation[] locations =
                fs.getFileBlockLocations(fstatus, offsetBegin, splitBytes);
            if (locations.length == 0) {
              throw new AssertionError(
                  "getFileBlockLocations returns 0 location");
            }

            Arrays.sort(locations, new Comparator<BlockLocation>() {
              @Override
              public int compare(BlockLocation o1, BlockLocation o2) {
                long diff = o1.getOffset() - o2.getOffset();
                if (diff < 0) return -1;
                if (diff > 0) return 1;
                return 0;
              }
            });
            BlockLocation firstBlock = locations[0];
            BlockLocation lastBlock = locations[locations.length - 1];
            long lastBlockOffsetBegin = lastBlock.getOffset();
            long lastBlockOffsetEnd =
                lastBlockOffsetBegin + lastBlock.getLength();
            if ((firstBlock.getOffset() > offsetBegin)
                || (lastBlockOffsetEnd < offsetEnd)) {
              throw new AssertionError(
                  "Block locations returned by getFileBlockLocations do not cover requested range");
            }

            // Adjust offsets
            if ((offsetEnd > lastBlockOffsetBegin)
                && (offsetEnd - lastBlockOffsetBegin < EPSILON)) {
              // the split includes a bit of the next block, remove it.
              if (offsetEnd != fileLen)
              {
            	// only if this is not the last chunk
                offsetEnd = lastBlockOffsetBegin;
                splitBytes = offsetEnd - offsetBegin;
              }
            }
            else if ((lastBlockOffsetEnd > offsetEnd)
                && (lastBlockOffsetEnd - offsetEnd < EPSILON)) {
              // the split includes almost the whole block, fill it.
              offsetEnd = lastBlockOffsetEnd;
              splitBytes = offsetEnd - offsetBegin;
            }

            RawComparable key = reader.getKeyNear(offsetEnd);
            if (key == null) {
              offsetEnd = fileLen;
              splitBytes = offsetEnd - offsetBegin;
              key = reader.getLastKey();
              done = true; // TFile index too large? Is it necessary now?
            }
            remainLen -= splitBytes;

            BlockDistribution bd = new BlockDistribution();
            for (BlockLocation l : locations) {
              long blkBeginOffset = l.getOffset();
              long blkEndOffset = blkBeginOffset + l.getLength();
              if (blkBeginOffset < offsetBegin) blkBeginOffset = offsetBegin;
              if (blkEndOffset > offsetEnd) blkEndOffset = offsetEnd;
              if (blkEndOffset > blkBeginOffset) {
                bd.add(l, blkEndOffset - blkBeginOffset);
              }
            }
            ret.add(key, bd);
          }
        }
        finally {
          if (reader != null) {
            try {
              reader.close();
            }
            catch (Exception e) {
              // no-op;
            }
          }
          if (fsdis != null) {
            try {
              fsdis.close();
            }
            catch (Exception e) {
              // no-op
            }
          }
        }
      }

      return ret;
    }

    /**
     * Get the status of the ColumnGroup.
     */
    public BasicTableStatus getStatus() {
      return cgindex.status;
    }

    /**
     * Split the ColumnGroup by file orders.
     * 
     * @param n
     *          Targeted number of partitions.
     * @return A list of range-based splits, whose size may be less than or
     *         equal to n.
     */
    public List<CGRangeSplit> rangeSplit(int n) {
      // The output of this method must be only dependent on the cgindex and
      // input parameter n - so that horizontally stitched column groups will
      // get aligned splits.
      int numFiles = cgindex.size();
      if ((numFiles < n) || (n < 0)) {
        return rangeSplit(numFiles);
      }
      List<CGRangeSplit> lst = new ArrayList<CGRangeSplit>();
      int beginIndex = 0;
      for (int i = 0; i < n; ++i) {
        int endIndex = (int) ((long) (i + 1) * numFiles / n);
        lst.add(new CGRangeSplit(beginIndex, endIndex - beginIndex));
        beginIndex = endIndex;
      }

      return lst;
    }

    /**
     * Is the ColumnGroup sorted?
     * 
     * @return Whether the ColumnGroup is sorted.
     */
    public boolean isSorted() {
      return cgschema.isSorted();
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closed = true;
      }
    }

    /**
     * A simple wrapper class over TFile.Reader.Scanner to simplify the creation
     * and resource management.
     */
    static class TFileScanner implements Closeable {
      boolean closed = true;
      FSDataInputStream ins;
      TFile.Reader reader;
      TFile.Reader.Scanner scanner;
      TupleReader tupleReader;

      TFileScanner(FileSystem fs, Path path, RawComparable begin,
          RawComparable end, CGSchema cgschema, Projection projection,
          Configuration conf) throws IOException, ParseException {
        try {
          ins = fs.open(path);
          /*
           * compressor is inside cgschema
           */
          reader = new TFile.Reader(ins, fs.getFileStatus(path).getLen(), conf);
          scanner = reader.createScanner(begin, end);
          /*
           * serializer is inside cgschema: different serializer will require
           * different Reader: for pig, it's TupleReader
           */
          tupleReader = new TupleReader(cgschema.getSchema(), projection);
          closed = false;
        }
        finally {
          if (closed == true) { // failed to instantiate the object.
            if (scanner != null) {
              try {
                scanner.close();
              }
              catch (Exception e) {
                // no-op
              }
            }

            if (reader != null) {
              try {
                reader.close();
              }
              catch (Exception e) {
                // no op
              }
            }

            if (ins != null) {
              try {
                ins.close();
              }
              catch (Exception e) {
                // no op
              }
            }
          }
        }
      }

      void rewind() throws IOException {
        scanner.rewind();
      }

      void getKey(BytesWritable key) throws IOException {
        scanner.entry().getKey(key);
      }

      void getValue(Tuple val) throws IOException, ParseException {
        DataInputStream dis = scanner.entry().getValueStream();
        try {
          tupleReader.get(dis, val);
        }
        finally {
          dis.close();
        }
      }

      boolean seekTo(BytesWritable key) throws IOException {
        return scanner.seekTo(key.get(), 0, key.getSize());
      }

      boolean advance() throws IOException {
        return scanner.advance();
      }

      boolean atEnd() {
        return scanner.atEnd();
      }

      void seekToEnd() throws IOException {
        scanner.seekToEnd();
      }

      @Override
      public void close() throws IOException {
        if (!closed) {
          closed = true;
          try {
            scanner.close();
          }
          catch (Exception e) {
            // no op
          }

          try {
            reader.close();
          }
          catch (Exception e) {
            // no op
          }

          try {
            ins.close();
          }
          catch (Exception e) {
            // no op
          }
        }
      }
    }

    /**
     * ColumnGroup scanner
     */
    class CGScanner implements TableScanner {
      private Projection logicalSchema = null;
      private TFileScanner[] scanners;
      private boolean closeReader;
      private int beginIndex, endIndex;
      private int current; // current scanner
      private boolean scannerClosed = true;

      CGScanner(CGRangeSplit split, boolean closeReader) throws IOException,
          ParseException {
        if (split == null) {
          beginIndex = 0;
          endIndex = cgindex.size();
        }
        else {
          beginIndex = split.start;
          endIndex = split.start + split.len;
        }
        init(null, null, closeReader);
      }

      CGScanner(RawComparable beginKey, RawComparable endKey,
          boolean closeReader) throws IOException, ParseException {
        beginIndex = 0;
        endIndex = cgindex.size();
        if (beginKey != null) {
          beginIndex = cgindex.lowerBound(beginKey, comparator);
        }
        if (endKey != null) {
          endIndex = cgindex.lowerBound(endKey, comparator);
          if (endIndex < cgindex.size()) {
            ++endIndex;
          }
        }
        init(beginKey, endKey, closeReader);
      }

      private void init(RawComparable beginKey, RawComparable endKey,
          boolean doClose) throws IOException, ParseException {
        if (beginIndex > endIndex) {
          throw new IllegalArgumentException("beginIndex > endIndex");
        }
        logicalSchema = ColumnGroup.Reader.this.getProjection();
        List<TFileScanner> tmpScanners =
            new ArrayList<TFileScanner>(endIndex - beginIndex);
        try {
          for (int i = beginIndex; i < endIndex; ++i) {
            RawComparable begin = (i == beginIndex) ? beginKey : null;
            RawComparable end = (i == endIndex - 1) ? endKey : null;
            TFileScanner scanner =
                new TFileScanner(fs, cgindex.getPath(i, path), begin, end,
                    cgschema, logicalSchema, conf);
            // skip empty scanners.
            if (!scanner.atEnd()) {
              tmpScanners.add(scanner);
            }
            else {
              scanner.close();
            }
          }
          scanners = tmpScanners.toArray(new TFileScanner[tmpScanners.size()]);
          this.closeReader = doClose;
          scannerClosed = false;
        }
        finally {
          if (scannerClosed) { // failed to initialize the object.
            for (int i = 0; i < tmpScanners.size(); ++i) {
              try {
                tmpScanners.get(i).close();
              }
              catch (Exception e) {
                // no op
              }
            }
          }
        }
      }

      @Override
      public void getKey(BytesWritable key) throws IOException {
        if (atEnd()) {
          throw new EOFException("No more key-value to read");
        }
        scanners[current].getKey(key);
      }

      @Override
      public void getValue(Tuple row) throws IOException {
        if (atEnd()) {
          throw new EOFException("No more key-value to read");
        }
        try {
          scanners[current].getValue(row);
        } catch (ParseException e) {
          throw new IOException("Invalid Projection: "+e.getMessage());
        }
      }

      @Override
      public String getProjection() {
        return logicalSchema.toString();
      }
      
      public Schema getSchema() {
        return logicalSchema.getSchema();
      }

      @Override
      public boolean advance() throws IOException {
        if (atEnd()) {
          return false;
        }
        scanners[current].advance();
        if (scanners[current].atEnd()) {
          ++current;
          if (!atEnd()) {
            scanners[current].rewind();
          }
        }
        return true;
      }

      @Override
      public boolean atEnd() throws IOException {
        return (current >= scanners.length);
      }

      @Override
      public boolean seekTo(BytesWritable key) throws IOException {
        if (!isSorted()) {
          throw new IOException("Cannot seek in unsorted Column Gruop");
        }
        int index =
            cgindex.lowerBound(new ByteArray(key.get(), 0, key.getSize()),
                comparator);
        if (index > endIndex) {
          seekToEnd();
          return false;
        }

        if ((index < beginIndex)) {
          // move to the beginning
          index = beginIndex;
        }

        current = index - beginIndex;
        return scanners[current].seekTo(key);
      }

      @Override
      public void seekToEnd() throws IOException {
        current = scanners.length;
      }

      @Override
      public void close() throws IOException {
        if (!scannerClosed) {
          scannerClosed = true;
          for (int i = 0; i < scanners.length; ++i) {
            try {
              scanners[i].close();
            }
            catch (Exception e) {
              // no-op
            }
          }
          if (closeReader) {
            Reader.this.close();
          }
        }
      }
    }

    public static class CGRangeSplit implements Writable {
      int start = 0; // starting index in the list
      int len = 0;

      CGRangeSplit(int start, int len) {
        this.start = start;
        this.len = len;
      }

      public CGRangeSplit() {
        // no-op;
      }

      @Override
      public void readFields(DataInput in) throws IOException {
        start = Utils.readVInt(in);
        len = Utils.readVInt(in);
      }

      @Override
      public void write(DataOutput out) throws IOException {
        Utils.writeVInt(out, start);
        Utils.writeVInt(out, len);
      }
    }

    private static class SplitColumn {
      SplitColumn(Partition.SplitType st) {
        this.st = st;
      }

      SplitColumn(int fieldIndex, Partition.SplitType st) {
        this.fieldIndex = fieldIndex;
        this.st = st;
      }

      SplitColumn(int fieldIndex, String key, Partition.SplitType st) {
        this.fieldIndex = fieldIndex;
        this.key = key;
        this.st = st;
      }

      SplitColumn(int fieldIndex, int projIndex, SplitColumn leaf, String key,
          Partition.SplitType st) {
        this(fieldIndex, key, st);
        this.projIndex = projIndex;
      }

      int fieldIndex = -1; // field index to parent
      int projIndex = -1; // index in projection: only used by leaves
      SplitColumn leaf = null;
      String key = null; // MAP key to parent
      ArrayList<SplitColumn> children = null;
      int index = -1; // index in the logical schema
      Object field = null;
      Partition.SplitType st = Partition.SplitType.NONE;

      void dispatch(Object field) {
        this.field = field;
      }

      @SuppressWarnings("unchecked")
      void split() throws ExecException {
        int size = children.size();
        if (st == Partition.SplitType.RECORD) {
          for (int i = 0; i < size; i++) {
            if (children.get(i).projIndex != -1) // a leaf: set projection
                                                  // directly
            ((Tuple) (leaf.field)).set(projIndex, ((Tuple) field).get(children
                .get(i).fieldIndex));
            else children.get(i).field =
                ((Tuple) field).get(children.get(i).fieldIndex);
          }
        }
        else if (st == Partition.SplitType.MAP) {
          for (int i = 0; i < size; i++) {
            if (children.get(i).projIndex != -1) // a leaf: set projection
                                                  // directly
            ((Tuple) (leaf.field)).set(projIndex, ((Map<String, Object>) field)
                .get(children.get(i).key));
            else children.get(i).field =
                ((Map<String, Object>) field).get(children.get(i).key);
          }
        }
      }

      void addChild(SplitColumn child) {
        if (children == null) children = new ArrayList<SplitColumn>();
        children.add(child);
      }
    }
  }

  /**
   * Column Group writer.
   */
  public static class Writer implements Closeable {
    Path path;
    Configuration conf;
    FileSystem fs;
    CGSchema cgschema;
    private boolean finished;

    /**
     * Create a ColumnGroup writer. The semantics are as follows:
     * <ol>
     * <li>If path does not exist:
     * <ul>
     * <li>create the path directory
     * <li>write out the meta data file.
     * </ul>
     * <li>If path exists and the directory is empty: write out the meta data
     * file.
     * <li>If path exists and contains what look like a complete Column Group,
     * ColumnGroupExists exception will be thrown.
     * <li>If path exists and overwrite is true, remove all files under the
     * directory and resume as in Step 2.
     * <li>If path exists directory not empty and overwrite= false,
     * ColumnGroupExists will be thrown.
     * </ol>
     * This constructor never removes a valid/complete ColumnGroup.
     * 
     * @param path
     *          The path to the Column Group, either not existent or must be a
     *          directory.
     * @param schema
     *          The schema of the ColumnGroup. For this version of
     *          implementation, the schema of a table is a comma separated list
     *          of column names, such as "FirstName, LastName, Sex, Department".
     * @param sorted
     *          Whether the column group to be created is sorted or not. If set
     *          to true, we expect the rows inserted by every inserter created
     *          from this Writer must be sorted. Additionally, there exists an
     *          ordering of the inserters Ins-1, Ins-2, ... such that the rows
     *          created by Ins-1, followed by rows created by Ins-2, ... form a
     *          total order.
     * @param overwrite
     *          Should we overwrite the path if it already exists?
     * @param conf
     *          The optional configuration objects.
     * @throws IOException
     */
    public Writer(Path path, String schema, boolean sorted, String serializer,
        String compressor, String owner, String group, short perm,boolean overwrite, Configuration conf)
        throws IOException, ParseException {
      this(path, new Schema(schema), sorted, serializer, compressor, owner, group, perm, overwrite,
          conf);
    }
    
    public Writer(Path path, Schema schema, boolean sorted, String serializer,
        String compressor, String owner, String group, short perm, boolean overwrite, Configuration conf)
        throws IOException, ParseException {
      this.path = path;
      this.conf = conf;

      fs = path.getFileSystem(conf);

      // If meta file already exists, that means the ColumnGroup is complete and
      // valid, we will not proceed.
      checkMetaFile(path);

      // if overwriting, remove everything
      if (overwrite) {
        fs.delete(path, true);
      }

      checkPath(path, true);

      cgschema = new CGSchema(schema, sorted, serializer, compressor, owner, group, perm);
      CGSchema sfNew = CGSchema.load(fs, path);
      if (sfNew != null) {
        // compare input with on-disk schema.
        if (!sfNew.equals(cgschema)) {
          throw new IOException("Schemes are different.");
        }
      }
      else {
        // create the schema file in FS
        cgschema.create(fs, path);
      }
    }

    /**
     * Reopen an already created ColumnGroup for writing. RuntimeException will
     * be thrown if the table is already closed, or if createMetaBlock() is
     * called by some other process.
     */
    public Writer(Path path, Configuration conf) throws IOException,
        ParseException {
      this.path = path;
      this.conf = conf;
      fs = path.getFileSystem(conf);
      checkPath(path, false);
      checkMetaFile(path);
      // read the schema file
      cgschema = CGSchema.load(fs, path);
    }

    /**
     * Release resources used by the object. Unlike close(), finish() does not
     * make the table immutable. However, if a user already adds some meta data
     * into the CG, then finish() would close the column group.
     */
    public void finish() {
      if (!finished) {
        finished = true;
      }
    }

    @Override
    public void close() throws IOException {
      if (!finished) {
        finished = true;
        createIndex();
      }
    }

    public Schema getSchema() {
      return cgschema.getSchema();
    }

    /**
     * Get a inserter with a given name.
     * 
     * @param name
     *          the name of the inserter.
     * @param finishWriter
     *          finish the underlying Writer object upon the close of the
     *          Inserter. Should be set to true if there is only one inserter
     *          operate on the table, so we should call finish() after the
     *          Inserter is closed.
     * 
     * @return A table inserter object.
     * @throws IOException
     */
    public TableInserter getInserter(String name, boolean finishWriter)
        throws IOException {
      if (finished) {
        throw new IOException("ColumnGroup has been closed for insertion.");
      }
      return new CGInserter(name, finishWriter);
    }

    private void createIndex() throws IOException {
      MetaFile.Writer metaFile =
          MetaFile.createWriter(makeMetaFilePath(path), conf);
      CGIndex index = buildIndex(fs, path, false, conf);
      DataOutputStream dos = metaFile.createMetaBlock(BLOCK_NAME_INDEX);
      try {
        index.write(dos);
      }
      finally {
        dos.close();
      }
      metaFile.close();
    }

    private void checkPath(Path p, boolean createNew) throws IOException {
      // check existence of path
      if (!fs.exists(p)) {
        if (createNew) {
          fs.mkdirs(p);
        }
        else {
          throw new IOException("Path doesn't exists for appending: " + p);
        }
      }
      if (!fs.getFileStatus(p).isDir()) {
        throw new IOException("Path exists but not a directory: " + p);
      }
    }

    private void checkMetaFile(Path p) throws IOException {
      Path pathMeta = new Path(p, META_FILE);
      if (fs.exists(pathMeta)) {
        throw new IOException("Index meta file already exists: " + pathMeta);
      }
    }

    /**
     * Inserter for ColumnGroup
     */
    class CGInserter implements TableInserter {
      String name;
      String tmpName;
      boolean finishWriter;
      FSDataOutputStream out;
      TFile.Writer tfileWriter;
      TupleWriter tupleWriter;
      boolean closed = true;
      
      
      private void createTempFile() throws IOException {
        int maxTrial = 10;
        String prefix = ".tmp." + name + ".";
        Random random = new Random();

        while (true) {
          /**
           * Try to set a real random seed by throwing all the runtime
           * ingredients into it.
           */
          random.setSeed(System.nanoTime() * Thread.currentThread().getId()
              * Runtime.getRuntime().freeMemory());
          try {
            tmpName = prefix + String.format("%08X", random.nextInt());
            Path tmpPath = new Path(path, tmpName);
            fs.mkdirs(path);

            if(cgschema.getOwner() != null  || cgschema.getGroup() != null) {
              fs.setOwner(path, cgschema.getOwner(), cgschema.getGroup());
            }  

            FsPermission permission = null;
            if(cgschema.getPerm() != -1) {
                permission = new FsPermission((short) cgschema.getPerm());
            	fs.setPermission(path, permission);
            }  
            
            out = fs.create(tmpPath, false);

            if(cgschema.getOwner() != null || cgschema.getGroup() != null) {
                fs.setOwner(tmpPath, cgschema.getOwner(), cgschema.getGroup());
       	    }  

            if(cgschema.getPerm() != -1) {
        	  fs.setPermission(tmpPath, permission);
            }	
            return;
          }
          catch (IOException e) {
            --maxTrial;
            if (maxTrial == 0) {
              throw e;
            }
            Thread.yield();
          }
        }
      }

      CGInserter(String name, boolean finishWriter) throws IOException {
        this.name = name;
        this.finishWriter = finishWriter;
        this.tupleWriter = new TupleWriter(getSchema());
        try {
          createTempFile();
          tfileWriter =
        	  new TFile.Writer(out, getMinBlockSize(conf), cgschema.getCompressor(), cgschema.getComparator(), conf);
          closed = false;
        }
        finally {
          if (closed) {
            if (tfileWriter != null) {
              try {
                tfileWriter.close();
              }
              catch (Exception e) {
                // no-op
              }
            }
            if (out != null) {
              try {
                out.close();
              }
              catch (Exception e) {
                // no-op
              }
            }
            if (tmpName != null) {
              try {
                fs.delete(new Path(path, tmpName), false);
              }
              catch (Exception e) {
                // no-op
              }
            }
          }
        }
      }

      @Override
      public Schema getSchema() {
        return ColumnGroup.Writer.this.getSchema();
      }

      @Override
      public void insert(BytesWritable key, Tuple row) throws IOException {
        TypesUtils.checkCompatible(row, getSchema());
        DataOutputStream outKey = tfileWriter.prepareAppendKey(key.getSize());
        try {
          outKey.write(key.get(), 0, key.getSize());
        }
        finally {
          outKey.close();
        }

        DataOutputStream outValue = tfileWriter.prepareAppendValue(-1);
        try {
          tupleWriter.put(outValue, row);
        }
        finally {
          outValue.close();
        }
      }

      @Override
      public void close() throws IOException {
        if (closed) {
          return;
        }
        closed = true;

        try {
          // TODO: add schema to each TFile as a meta block?

          tfileWriter.close();
          tfileWriter = null;
          out.close();
          out = null;
          // do renaming only if all the above is successful.
          fs.rename(new Path(path, tmpName), new Path(path, name));
/*
          if(cgschema.getOwner() != null || cgschema.getGroup() != null) {
            fs.setOwner(new Path(path, name), cgschema.getOwner(), cgschema.getGroup());
          }  
          FsPermission permission = null;
          if(cgschema.getPerm() != -1) {
            permission = new FsPermission((short) cgschema.getPerm());
            fs.setPermission(path, permission);
          }
*/                     
          tmpName = null;
          if (finishWriter) {
            finish();
          }
        }
        finally {
          if (tfileWriter != null) {
            try {
              tfileWriter.close();
            }
            catch (Exception e) {
              // no-op
            }
          }
          if (out != null) {
            try {
              out.close();
            }
            catch (Exception e) {
              // no-op
            }
          }
          if (tmpName != null) {
            try {
              fs.delete(new Path(path, tmpName), false);
            }
            catch (Exception e) {
              // no-op
            }
          }
          if (finishWriter) {
            try {
              finish();
            }
            catch (Exception e) {
              // no-op
            }
          }
        }
      }
    }

  }

  /**
   * name, first and last key (inclusive) of a data file
   */
  static class CGIndexEntry implements RawComparable, Writable {
    String name;
    long rows;
    RawComparable firstKey;
    RawComparable lastKey;

    // for reading
    CGIndexEntry() {
      // no-op
    }

    // for writing
    CGIndexEntry(String name, long rows, RawComparable firstKey,
        RawComparable lastKey) {
      this.name = name;
      this.rows = rows;
      this.firstKey = firstKey;
      this.lastKey = lastKey;
    }

    public String getName() {
      return name;
    }

    public long getRows() {
      return rows;
    }

    public RawComparable getFirstKey() {
      return firstKey;
    }

    public RawComparable getLastKey() {
      return lastKey;
    }

    @Override
    public byte[] buffer() {
      return (lastKey != null) ? lastKey.buffer() : null;
    }

    @Override
    public int offset() {
      return (lastKey != null) ? lastKey.offset() : 0;
    }

    @Override
    public int size() {
      return (lastKey != null) ? lastKey.size() : 0;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      name = Utils.readString(in);
      rows = Utils.readVLong(in);
      if (rows == 0) {
        firstKey = null;
        lastKey = null;
      }
      else {
        int firstKeyLen = Utils.readVInt(in);
        byte[] firstKeyBuffer = new byte[firstKeyLen];
        in.readFully(firstKeyBuffer);
        int lastKeyLen = Utils.readVInt(in);
        byte[] lastKeyBuffer = new byte[lastKeyLen];
        in.readFully(lastKeyBuffer);
        firstKey = new ByteArray(firstKeyBuffer);
        lastKey = new ByteArray(lastKeyBuffer);
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      Utils.writeString(out, name);
      Utils.writeVLong(out, rows);
      if (rows > 0) {
        if ((firstKey == null) && (lastKey == null)) {
          throw new IOException("In-memory only entry");
        }
        Utils.writeVInt(out, firstKey.size());
        out.write(firstKey.buffer(), firstKey.offset(), firstKey.size());
        Utils.writeVInt(out, lastKey.size());
        out.write(lastKey.buffer(), lastKey.offset(), lastKey.size());
      }
    }
  }

  static class CGIndex implements Writable {
    boolean dirty = false;
    boolean sorted = true;
    BasicTableStatus status;
    ArrayList<CGIndexEntry> index;

    CGIndex() {
      status = new BasicTableStatus();
      index = new ArrayList<CGIndexEntry>();
    }

    int size() {
      return index.size();
    }

    CGIndexEntry get(int i) {
      return index.get(i);
    }

    List<CGIndexEntry> getIndex() {
      return index;
    }

    Path getPath(int i, Path parent) {
      return new Path(parent, index.get(i).getName());
    }

    void sort(final Comparator<RawComparable> comparator) throws IOException {
      if (dirty && comparator != null) {
        throw new IOException("Cannot sort dirty index");
      }

      if (comparator != null) {
        // sort by keys. For empty TFiles, they are always sorted before
        // non-empty TFiles, and they themselves are sorted by their names.
        Collections.sort(index, new Comparator<CGIndexEntry>() {

          @Override
          public int compare(CGIndexEntry o1, CGIndexEntry o2) {
            if ((o1.getRows() == 0) && (o2.getRows() == 0)) {
              return o1.getName().compareTo(o2.getName());
            }
            if (o1.getRows() == 0) return -1;
            if (o2.getRows() == 0) return 1;
            int cmprv = comparator.compare(o1.lastKey, o2.lastKey);
            if (cmprv == 0) {
              cmprv = comparator.compare(o1.firstKey, o2.firstKey);
              if (cmprv == 0) {
                cmprv = o1.getName().compareTo(o2.getName());
              }
            }
            return cmprv;
          }
        });

        for (int i = 0; i < index.size() - 1; ++i) {
          RawComparable prevLastKey = index.get(i).lastKey;
          RawComparable nextFirstKey = index.get(i + 1).firstKey;
          if (nextFirstKey == null) {
            continue;
          }
          if (comparator.compare(prevLastKey, nextFirstKey) > 0) {
            throw new IOException("Overlapping key ranges");
          }
        }
      }
      else {
        // sort by name
        Collections.sort(index, new Comparator<CGIndexEntry>() {

          @Override
          public int compare(CGIndexEntry o1, CGIndexEntry o2) {
            return o1.name.compareTo(o2.name);
          }
        });
      }

      // update status
      if ((!dirty) && (index.size() > 0)) {
        RawComparable keyFirst = index.get(0).getFirstKey();
        status.beginKey = new BytesWritable();
        status.beginKey.set(keyFirst.buffer(), keyFirst.offset(), keyFirst
            .size());
        RawComparable keyLast = index.get(index.size() - 1).getLastKey();
        status.endKey = new BytesWritable();
        status.endKey.set(keyLast.buffer(), keyLast.offset(), keyLast.size());
      }
      sorted = true;
    }

    // building full index.
    void add(long bytes, long rows, CGIndexEntry range) {
      status.size += bytes;
      status.rows += rows;
      index.add(range);
      sorted = false;
    }

    // building dirty index
    void add(long bytes, String name) {
      dirty = true;
      status.rows = -1; // reset rows to -1.
      status.size += bytes;
      CGIndexEntry next = new CGIndexEntry();
      next.name = name;
      index.add(next);
      sorted = false;
    }

    int lowerBound(RawComparable key, final Comparator<RawComparable> comparator)
        throws IOException {
      if ((key == null) || (comparator == null)) {
        throw new IllegalArgumentException("CGIndex.lowerBound");
      }

      if (!sorted) {
        sort(comparator);
      }

      // Treat null keys as the least key.
      return Utils.lowerBound(index, key, new Comparator<RawComparable>() {
        @Override
        public int compare(RawComparable o1, RawComparable o2) {
          if ((o1.buffer() == null) && (o2.buffer() == null)) {
            return 0;
          }
          if (o1.buffer() == null) return -1;
          if (o2.buffer() == null) return 1;
          return comparator.compare(o1, o2);
        }
      });
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int n = Utils.readVInt(in);
      index.clear();
      index.ensureCapacity(n);
      for (int i = 0; i < n; ++i) {
        CGIndexEntry range = new CGIndexEntry();
        range.readFields(in);
        index.add(range);
      }
      status.readFields(in);
      dirty = false;
      sorted = true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      if (dirty) {
        throw new IOException("Cannot write dirty index");
      }
      if (!sorted) {
        throw new IOException("Please sort index before calling write");
      }
      Utils.writeVInt(out, index.size());
      for (int i = 0; i < index.size(); ++i) {
        index.get(i).write(out);
      }
      status.write(out);
    }
  }

  static class CGPathFilter implements PathFilter {
    private final Configuration conf;

    CGPathFilter(Configuration conf) {
      this.conf = conf;
    }

    public boolean accept(Path p) {
      return p.getName().equals(META_FILE) || p.getName().equals(SCHEMA_FILE)
          || p.getName().startsWith(".tmp.")
          || p.getName().startsWith(getNonDataFilePrefix(conf)) ? false : true;
    }
  }

  /**
   * Dump information about CG.
   * 
   * @param file
   *          Path string of the CG
   * @param out
   *          PrintStream to output the information.
   * @param conf
   *          The configuration object.
   * @throws IOException
   */
  static public void dumpInfo(String file, PrintStream out, Configuration conf)
      throws IOException, Exception {
    // final int maxKeySampleLen = 16;
    dumpInfo(new Path(file), out, conf);
  }

  static public void dumpInfo(Path path, PrintStream out, Configuration conf)
      throws IOException, Exception {
      dumpInfo(path, out, conf, 0);
  }

  static public void dumpInfo(Path path, PrintStream out, Configuration conf, int indent)
      throws IOException, Exception {
    // final int maxKeySampleLen = 16;
    IOutils.indent(out, indent);
    out.println();
    IOutils.indent(out, indent);
    out.println("Column Group : " + path);
    ColumnGroup.Reader reader = new ColumnGroup.Reader(path, false, conf);
    try {
      LinkedHashMap<String, String> properties =
          new LinkedHashMap<String, String>();
      IOutils.indent(out, indent);
      out.println("Serializer: " + reader.getSerializer());
      IOutils.indent(out, indent);
      out.println("Compressor: " + reader.getCompressor());
      properties.put("Schema", reader.getSchema().toString());
      // Now output the properties table.
      int maxKeyLength = 0;
      Set<Map.Entry<String, String>> entrySet = properties.entrySet();
      for (Iterator<Map.Entry<String, String>> it = entrySet.iterator(); it
          .hasNext();) {
        Map.Entry<String, String> e = it.next();
        if (e.getKey().length() > maxKeyLength) {
          maxKeyLength = e.getKey().length();
        }
      }
      for (Iterator<Map.Entry<String, String>> it = entrySet.iterator(); it
          .hasNext();) {
        Map.Entry<String, String> e = it.next();
        IOutils.indent(out, indent);
        out.printf("%s : %s\n", e.getKey(), e.getValue());
      }
      out.println("TFiles within the Column Group :");
      for (CGIndexEntry entry : reader.cgindex.index) {
        IOutils.indent(out, indent);
        out.printf(" *Name : %s\n", entry.name);
        IOutils.indent(out, indent);
        out.printf("  Rows : %d\n", entry.rows);
        if (entry.firstKey != null) {
          IOutils.indent(out, indent);
          out.printf("  First Key : %s\n", headToString(entry.firstKey));
        }
        if (entry.lastKey != null) {
          IOutils.indent(out, indent);
          out.printf("  Larst Key : %s\n", headToString(entry.lastKey));
        }
        // dump TFile info
        // Path pathTFile = new Path(path, entry.name);
        // TFile.dumpInfo(pathTFile.toString(), out, conf);
      }
    }
    finally {
      try {
        reader.close();
      }
      catch (Exception e) {
        // no-op
      }
    }
  }

  private static String headToString(RawComparable raw) {
    return new String(raw.buffer(), raw.offset(), raw.size() > 70 ? 70 : raw
        .size());
  }

  /**
   * Dumping the CG information.
   * 
   * @param args
   *          A list of CG paths.
   */
  public static void main(String[] args) throws Exception {
    System.out.printf("ColumnGroup Dumper\n");
    if (args.length == 0) {
      System.out
          .println("Usage: java ... org.apache.hadoop.zebra.io.ColumnGroup cg-path [cg-path ...]");
      System.exit(0);
    }
    Configuration conf = new Configuration();
    for (String file : args) {
      try {
        dumpInfo(file, System.out, conf);
      }
      catch (IOException e) {
        e.printStackTrace(System.err);
      }
    }
  }
}
