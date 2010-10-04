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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.hadoop.zebra.tfile.TFile;
import org.apache.hadoop.zebra.tfile.Utils;
import org.apache.hadoop.zebra.tfile.ByteArray;
import org.apache.hadoop.zebra.tfile.RawComparable;
import org.apache.hadoop.zebra.types.CGSchema;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.Partition;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.hadoop.zebra.schema.Schema;
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
  static Log LOG = LogFactory.getLog(ColumnGroup.class);
  
  private final static String CONF_COMPRESS = "table.output.tfile.compression";
  private final static String DEFAULT_COMPRESS = "gz";
  private final static String CONF_MIN_BLOCK_SIZE = "table.tfile.minblock.size";
  private final static int DEFAULT_MIN_BLOCK_SIZE = 1024 * 1024;

  private final static String CONF_MIN_SPLIT_SIZE = "table.input.split.minSize";
  private final static int DEFAULT_MIN_SPLIT_SIZE = 64 * 1024;

  static final double SPLIT_SLOP = 1.1; // 10% slop

  // excluding files start with the following prefix, may change to regex
  private final static String CONF_NON_DATAFILE_PREFIX =
      "table.cg.nondatafile.prefix";
  private final static String SPECIAL_FILE_PREFIX = ".";

  // tmp schema file name, used as a flag of unfinished CG
  private final static String SCHEMA_FILE = ".schema";
  // meta data TFile for entire CG, used as a flag of closed CG
  final static String META_FILE = ".meta";

  // sorted table key ranges for default sorted table split generations
  private final static String KEY_RANGE_FOR_DEFAULT_SORTED_SPLIT = ".keyrange";

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
    CGPathFilter cgPathFilter = new CGPathFilter();
    CGPathFilter.setConf(conf);
    FileStatus[] files = fs.listStatus(path, cgPathFilter);
    
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
    
    int idx = 0;
    for (CGIndexEntry e : ret.getIndex()) {
      e.setIndex(idx++);    
    }
    
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
    boolean dirty;

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
      this(path, conf, false);
    }
    
    public Reader(Path path, Configuration conf, boolean mapper) throws IOException,
      ParseException {
      this(path, true, conf, mapper);
    }
    
    Reader(Path path, boolean dirty, Configuration conf) throws IOException,
      ParseException {
      this(path, dirty, conf, false);
    }

    Reader(Path path, boolean dirty, Configuration conf, boolean mapper) throws IOException,
      ParseException {
      this.path = path;
      this.conf = conf;
      this.dirty = dirty;

      fs = path.getFileSystem(conf);
      // check existence of path
      if (!fs.exists(path)) {
        throw new IOException("Path doesn't exist: " + path);
      }

      if (!mapper && !fs.getFileStatus(path).isDir()) {
        throw new IOException("Path exists but not a directory: " + path);
      }

      cgschema = CGSchema.load(fs, path);
      if (cgschema.isSorted()) {
        comparator = TFile.makeComparator(cgschema.getComparator());
      }
      projection = new Projection(cgschema.getSchema()); // default projection to CG schema.
      Path metaFilePath = makeMetaFilePath(path);
      /* If index file is not existing */
      if (!fs.exists(metaFilePath)) {
        throw new FileNotFoundException(
              "Missing Meta File of " + metaFilePath);
      }
      else if (cgschema.isSorted()) {
        MetaFile.Reader metaFile = MetaFile.createReader(metaFilePath, conf);
        try {
          cgindex = new CGIndex();
          DataInputStream dis = metaFile.getMetaBlock(BLOCK_NAME_INDEX);
          try {
            cgindex.readFields(dis);
          } catch (IOException e) {
            throw new IOException("Index file read failure :"+ e.getMessage());
          } finally {
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

    public String getName() {
      return cgschema.getName();
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

    public String getGroup() {
        return cgschema.getGroup();
      }

    public short getPerm() {
        return cgschema.getPerm();
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
    public synchronized CGScanner getScanner(BytesWritable beginKey,
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
          (beginKey != null) ? new ByteArray(beginKey.getBytes(), 0, beginKey
              .getLength()) : null;
      RawComparable end =
          (endKey != null) ? new ByteArray(endKey.getBytes(), 0, endKey.getLength())
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
    public synchronized CGScanner getScanner(CGRangeSplit split,
        boolean closeReader) throws IOException, ParseException {
      if (closed) {
        throw new EOFException("Reader already closed");
      }

      if (split == null) {
        if (cgindex == null)
          cgindex = buildIndex(fs, path, dirty, conf);
        return getScanner(new CGRangeSplit(0, cgindex.size()), closeReader);
      }
      if (split.len < 0) {
        throw new IllegalArgumentException("Illegal range split");
      }

      return new CGScanner(split, closeReader);
    }

    /**
     * Get a scanner that reads the rows defined by rowRange. 
     * 
     * @param closeReader
     *          close the underlying Reader object when we close the scanner.
     *          Should be set to true if we have only one scanner on top of the
     *          reader, so that we should release resources after the scanner is
     *          closed.
     * @param rowSplit specifies part index, start row, and end row.
     * @return A scanner object.
     */
    public synchronized CGScanner getScanner(boolean closeReader, 
                                                CGRowSplit rowSplit)
                        throws IOException, ParseException {
      if (closed) {
        throw new EOFException("Reader already closed");
      }
      
      return new CGScanner(rowSplit, closeReader);
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

      if (cgindex == null)
        cgindex = buildIndex(fs, path, dirty, conf);
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
     * Given a row range, calculate how the file data that fall into the range
     * are distributed among hosts.
     * 
     * @param split
     *          The row-based split. If null, return all blocks.
     * @return a map from host name to the amount of data (in bytes) the host
     *         owns that fall roughly into the key range.
     */
    public BlockDistribution getBlockDistribution(CGRowSplit split)
        throws IOException {
      if (split == null) {
        throw new IOException("Row-based split cannot be null for getBlockDistribution()");
      }

      BlockDistribution ret = new BlockDistribution();
      for (int i = 0; i < split.length; i++)
      {
        FileStatus tfileStatus = fs.getFileStatus(new Path(path, split.names[i]));
        
        BlockLocation[] locations = null;
        long len = 0;
        if (i == 0) {
          if (split.startByteFirst != -1)
          {
            len = split.numBytesFirst;
            locations = fs.getFileBlockLocations(tfileStatus, split.startByteFirst, len);
          }
        } else if (i == split.length - 1) {
           if (split.numBytesLast != -1)
           {
             len = split.numBytesLast;
             locations = fs.getFileBlockLocations(tfileStatus, 0, len);
           }
        }

        if (locations == null)
        {
          len = tfileStatus.getLen();
          locations = fs.getFileBlockLocations(tfileStatus, 0, len);
        }

        for (BlockLocation l : locations) {
          ret.add(l);
        }
      }
      return ret;
    }

  private int getStartBlockIndex(long[] startOffsets, long offset)
  {
    int index = Arrays.binarySearch(startOffsets, offset);
    if (index < 0)
      index = -index - 2;
    return index;
  }
  
  private int getEndBlockIndex(long[] startOffsets, long offset)
  {
    int index = Arrays.binarySearch(startOffsets, offset);
    if (index < 0)
      index = -index - 1;
    return index;
  }

   /**
    * Sets startRow and number of rows in rowSplit based on
    * startOffset and length.
    * 
    * It is assumed that 'startByte' and 'numBytes' in rowSplit itself
    * are not valid.
    */
    void fillRowSplit(CGRowSplit rowSplit, CGRowSplit src) throws IOException {

      if (src.names == null || src.length == 0)
        return;

      boolean noSizeInIndex = false;
      long[] sizes = rowSplit.sizes;
      if (sizes == null)
      {
        /* the on disk table is sorted. Later this will be made unnecessary when
         * CGIndexEntry serializes its bytes field and the meta file versioning is
         * supported.
         */ 
        noSizeInIndex = true;
      }
      rowSplit.names = src.names;
      rowSplit.length = src.length;
      rowSplit.startByteFirst = src.startByteFirst;
      rowSplit.numBytesFirst = src.numBytesFirst;
      rowSplit.numBytesLast = src.numBytesLast;

      Path firstPath = null, lastPath;
      TFile.Reader reader = null;
      
      if (src.startByteFirst != -1)
      {
        firstPath = new Path(path, rowSplit.names[0]);
        long size;
        if (noSizeInIndex)
        {
          FileStatus tfile = fs.getFileStatus(firstPath);
          size = tfile.getLen();
        } else
          size = sizes[0];
        reader = new TFile.Reader(fs.open(firstPath), size, conf);
        try {
          long startRow = reader.getRecordNumNear(src.startByteFirst);
          long endRow = reader.getRecordNumNear(src.startByteFirst + src.numBytesFirst);

          if (endRow < startRow)
            endRow = startRow;
          rowSplit.startRowFirst = startRow;
          rowSplit.numRowsFirst = endRow - startRow;
        } catch (IOException e) {
          reader.close();
          throw e;
        }
      }
      if (src.numBytesLast != -1 && rowSplit.length > 1)
      {
        lastPath = new Path(path, rowSplit.names[rowSplit.length - 1]);
        if (reader == null || !firstPath.equals(lastPath))
        {
          if (reader != null)
            reader.close();
          long size;
          if (noSizeInIndex)
          {
            FileStatus tfile = fs.getFileStatus(lastPath);
            size = tfile.getLen();
          } else
            size = sizes[rowSplit.length - 1];
          reader = new TFile.Reader(fs.open(lastPath), size, conf);
        }
        try {
          long endRow = reader.getRecordNumNear(src.numBytesLast);
          rowSplit.numRowsLast = endRow;
        } catch (IOException e) {
          reader.close();
          throw e;
        }
      }
      if (reader != null)
        reader.close();
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
     * @param nTables
     *          Number of tables in a union
     * @return KeyDistribution object.
     * @throws IOException
     */
    public KeyDistribution getKeyDistribution(int n, int nTables, BlockDistribution lastBd) throws IOException {
      // TODO: any need for similar capability for unsorted for sorted CGs?
      if (!isSorted()) {
        throw new IOException("Cannot get key distribution for unsorted table");
      }
      KeyDistribution ret = new KeyDistribution(comparator);

      if (n < 0)
      {
        /*
        Path keyRangeFile = new Path(path, KEY_RANGE_FOR_DEFAULT_SORTED_SPLIT);
        if (fs.exists(keyRangeFile))
        {
          try {
            FSDataInputStream ins = fs.open(keyRangeFile);
            long minStepSize = ins.readLong();
            int size = ins.readInt();
            for (int i = 0; i < size; i++)
            {
              BytesWritable keyIn = new BytesWritable();
              keyIn.readFields(ins);
              ByteArray key = new ByteArray(keyIn.getBytes());
              ret.add(key);
            }
            ret.setMinStepSize(minStepSize);
            return ret;
          } catch (Exception e) {
            // no-op
          }
        }
        */
        n = 1;
      }

      Path[] paths = new Path[cgindex.size()];
      FileStatus[] tfileStatus = new FileStatus[paths.length];
      long totalBytes = 0;
      for (int i = 0; i < paths.length; ++i) {
        paths[i] = cgindex.getPath(i, path);
        tfileStatus[i] = fs.getFileStatus(paths[i]);
        totalBytes += tfileStatus[i].getLen();
      }

      final long minSize = getMinSplitSize(conf);
      final long EPSILON = (long) (minSize * (SPLIT_SLOP - 1));
      long goalSize = totalBytes / n;
      long batchSize = 0;
      BlockDistribution bd = new BlockDistribution();;
      RawComparable prevKey = null;

      long minStepSize = -1;
      FSDataInputStream nextFsdis = null;
      TFile.Reader nextReader = null;
      for (int i = 0; i < paths.length; ++i) {
        FileStatus fstatus = tfileStatus[i];
        long blkSize = fstatus.getBlockSize();
        long fileLen = fstatus.getLen();
        long stepSize = Math.max(minSize,
            (goalSize < blkSize) ? goalSize : blkSize);
        if (minStepSize== -1 || minStepSize > stepSize)
          minStepSize = stepSize;
        // adjust the block size by the scaling factor
        blkSize /= nTables;
        stepSize = Math.max(minSize,
          (goalSize < blkSize) ? goalSize : blkSize);
        FSDataInputStream fsdis = null;
        TFile.Reader reader = null;
        long remainLen = fileLen;
        try {
          if (nextReader == null)
          {
            fsdis = fs.open(paths[i]);
            reader = new TFile.Reader(fsdis, fileLen, conf);
          } else {
            fsdis = nextFsdis;
            reader = nextReader;
          }
          BlockLocation[] locations =
              fs.getFileBlockLocations(fstatus, 0, fileLen);
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
          
          long[] startOffsets = new long[locations.length];

          for (int ii = 0; ii < locations.length; ii++)
            startOffsets[ii] = locations[ii].getOffset();

          boolean done = false;
          while ((remainLen > 0) && !done) {
            long splitBytes =
                remainLen > stepSize ? stepSize : remainLen;
            long offsetBegin = fileLen - remainLen;
            long offsetEnd = offsetBegin + splitBytes;
            int indexBegin = getStartBlockIndex(startOffsets, offsetBegin);
            int indexEnd = getEndBlockIndex(startOffsets, offsetEnd);
            BlockLocation firstBlock = locations[indexBegin];
            BlockLocation lastBlock = locations[indexEnd-1];
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
                indexEnd--;
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
              if (i < paths.length-1)
              {
                nextFsdis = fs.open(paths[i+1]);
                nextReader = new TFile.Reader(nextFsdis, tfileStatus[i+1].getLen(), conf);
                key = nextReader.getFirstKey();
              }
              done = true; // TFile index too large? Is it necessary now?
            }
            remainLen -= splitBytes;
            batchSize += splitBytes;

            if (key != null && batchSize >= stepSize)
            {
              if (batchSize - splitBytes < EPSILON || splitBytes < EPSILON)
              {
                // the last chunk or this chunk is small enough to create a new range for this key
                setBlockDistribution(bd, reader, locations, fstatus, startOffsets, prevKey, key);
                ret.add(key, bd);
                batchSize = 0;
                bd = new BlockDistribution();
              } else {
                ret.add(prevKey, bd);
                batchSize = splitBytes;
                bd = new BlockDistribution();
                if (batchSize >= stepSize)
                {
                  setBlockDistribution(bd, reader, locations, fstatus, startOffsets, prevKey, key);
                  ret.add(key, bd);
                  batchSize = 0;
                  bd = new BlockDistribution();
                } else {
                  setBlockDistribution(bd, reader, locations, fstatus, startOffsets, prevKey, key);
                }
              }
            } else {
              setBlockDistribution(bd, reader, locations, fstatus, startOffsets, prevKey, key);
            }
            prevKey = key;
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
      if (lastBd != null)
        lastBd.add(bd);
      ret.setMinStepSize(minStepSize);
      
      return ret;
    }

    private void setBlockDistribution(BlockDistribution bd, TFile.Reader reader,
        BlockLocation[] locations, FileStatus fileStatus, long[] startOffsets,
        RawComparable begin, RawComparable end) throws IOException
    {
      long beginOffset, endOffset = -1;
      if (begin == null)
        beginOffset = 0;
      else
        beginOffset = reader.getOffsetForKey(begin);
      if (end != null)
      {
        if (begin == null)
          begin = reader.getFirstKey();
        /* Only if the key range is empty. This is needed because TFile has a 16-byte
         * Magic that causes getOffsetForKey to return 16 (not 0) even on the first key.
         */
        if (comparator.compare(begin, end) != 0)
          endOffset = reader.getOffsetForKey(end);
      }
      int startBlockIndex = (beginOffset == 0 ? 0 : getStartBlockIndex(startOffsets, beginOffset));
      BlockLocation l;
      int endBlockIndex = (end == null ? locations.length : endOffset == -1 ?
          startBlockIndex : getEndBlockIndex(startOffsets, endOffset));
      for (int ii = startBlockIndex; ii < endBlockIndex; ii++) {
        l = locations[ii];
        long blkBeginOffset = l.getOffset();
        long blkEndOffset = blkBeginOffset + l.getLength();
        if (blkEndOffset > blkBeginOffset) {
          bd.add(l, blkEndOffset - blkBeginOffset);
        }
      }
      return;
    }

    /**
     * Get the status of the ColumnGroup.
     */
    public BasicTableStatus getStatus() throws IOException {
      if (cgindex == null)
        cgindex = buildIndex(fs, path, dirty, conf);
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
    public List<CGRangeSplit> rangeSplit(int n) throws IOException {
      // The output of this method must be only dependent on the cgindex and
      // input parameter n - so that horizontally stitched column groups will
      // get aligned splits.
      if (cgindex == null)
        cgindex = buildIndex(fs, path, dirty, conf);
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
     * We already use FileInputFormat to create byte offset-based input splits.
     * Their information is encoded in starts, lengths and paths. This method is 
     * to wrap this information to form CGRowSplit objects at column group level.
     * 
     * @param starts array of starting byte of fileSplits.
     * @param lengths array of length of fileSplits.
     * @param paths array of path of fileSplits.
     * @return A list of CGRowSplit objects. 
     *         
     */
    public List<CGRowSplit> rowSplit(long[] starts, long[] lengths, Path[] paths,
        int[] batches, int numBatches) throws IOException {
      List<CGRowSplit> lst = new ArrayList<CGRowSplit>();
      CGRowSplit cgRowSplit;
      long startFirst, bytesFirst, bytesLast;
      int length;
       
      if (numBatches == 0)
      {
        cgRowSplit = new CGRowSplit(null, null, 0, -1, 0, 0);
        lst.add(cgRowSplit);
        return lst;
      }

      if (cgindex == null)
        cgindex = buildIndex(fs, this.path, dirty, conf);

      if (cgindex.size() == 0)
      {
        cgRowSplit = new CGRowSplit(null, null, 0, -1, 0, 0);
        lst.add(cgRowSplit);
        return lst;
      }

      for (int i=0; i< numBatches; i++) {
        int indexFirst = batches[i];
        int indexLast = batches[i+1] - 1;
        startFirst = starts[indexFirst];
        bytesFirst = lengths[indexFirst];
        bytesLast = lengths[indexLast];
        length = batches[i+1] - batches[i];
        String[] namesInSplit = new String[length];
        long[] sizesInSplit = new long[length];
        for (int j = 0; j < length; j++)
        {
          namesInSplit[j] = paths[indexFirst+j].getName();
          sizesInSplit[j] = cgindex.get(cgindex.getFileIndex(paths[indexFirst+j])).bytes;
        }
        cgRowSplit = new CGRowSplit(namesInSplit, sizesInSplit, length, 
                startFirst, bytesFirst, bytesLast);
        lst.add(cgRowSplit);
      }
      
      return lst;
    }
    
    void rearrangeFileIndices(FileStatus[] fileStatus) throws IOException {
      int size = fileStatus.length;
      FileStatus[] result = new FileStatus[size];
      if (cgindex == null)
        cgindex = buildIndex(fs, path, dirty, conf);
      if (size < cgindex.size())
        throw new AssertionError("Incorrect file list size");
      for (int j, i = 0; i < size; i++)
      {
        j = cgindex.getFileIndex(fileStatus[i].getPath());
        if (j != -1)
          result[j] = fileStatus[i];
      }
      for (int i = 0; i < size; i++)
        fileStatus[i] = result[i];
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

      TFileScanner(FileSystem fs, Path path, CGRowSplit rowRange, 
                    RawComparable begin, RawComparable end, boolean first, boolean last,
                    CGSchema cgschema, Projection projection,
          Configuration conf) throws IOException, ParseException {
        try {
          ins = fs.open(path);
          /*
           * compressor is inside cgschema
           */
          reader = new TFile.Reader(ins, fs.getFileStatus(path).getLen(), conf);
          if (rowRange != null && rowRange.startByteFirst != -1) {
            if (first && rowRange.startByteFirst != -1)
              scanner = reader.createScannerByRecordNum(rowRange.startRowFirst, 
                                              rowRange.startRowFirst + rowRange.numRowsFirst);
            else if (last && rowRange.numBytesLast != -1)
              scanner = reader.createScannerByRecordNum(0, rowRange.numRowsLast);
            else
              scanner = reader.createScanner();
          } else {
            /* TODO: more investigation is needed for the following.
             *  using deprecated API just so that zebra can work with 
             * hadoop jar that does not contain HADOOP-6218 (Record ids for
             * TFile). This is expected to be temporary. Later we should 
             * use the undeprecated API.
             */
            scanner = reader.createScanner(begin, end);
          }
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
        return scanner.seekTo(key.getBytes(), 0, key.getLength());
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
      private TFileScannerInfo[] scanners;
      private boolean closeReader;
      private int beginIndex, endIndex;
      private int current; // current scanner
      private boolean scannerClosed = true;
      private CGRowSplit rowRange;
      private TFileScanner scanner;
      
      private class TFileScannerInfo {
        boolean first, last;
        Path path;
        RawComparable begin, end;
        TFileScannerInfo(boolean first, boolean last, Path path, RawComparable begin, RawComparable end) {
          this.first = first;
          this.last = last;
          this.begin = begin;
          this.end = end;
          this.path = path;
        }
        
        TFileScanner getTFileScanner() throws IOException {
          try {
            return new TFileScanner(fs, path, rowRange, 
                  begin, end, first, last, cgschema, logicalSchema, conf);
          } catch (ParseException e) {
            throw new IOException(e.getMessage());
          }
        }
      }


      CGScanner(CGRangeSplit split, boolean closeReader) throws IOException,
      ParseException {
        if (cgindex== null)
          cgindex = buildIndex(fs, path, dirty, conf);
        if (split == null) {
          beginIndex = 0;
          endIndex = cgindex.size();
        }
        else {
          beginIndex = split.start;
          endIndex = split.start + split.len;
        }
        init(null, null, null, closeReader);
      }
      
      /**
       * Scanner for a range specified by the given row range.
       * 
       * @param rowRange see {@link CGRowSplit}
       * @param closeReader
       */
      CGScanner(CGRowSplit rowRange, boolean closeReader) 
                 throws IOException, ParseException {
        beginIndex = 0;
        endIndex = rowRange.length;
        init(rowRange, null, null, closeReader);
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
        init(null, beginKey, endKey, closeReader);
      }

      private void init(CGRowSplit rowRange, RawComparable beginKey, 
                        RawComparable endKey, boolean doClose) 
             throws IOException, ParseException {
        this.rowRange = rowRange;
        if (beginIndex > endIndex) {
          throw new IllegalArgumentException("beginIndex > endIndex");
        }
        logicalSchema = ColumnGroup.Reader.this.getProjection();
        List<TFileScannerInfo> tmpScanners =
            new ArrayList<TFileScannerInfo>(endIndex - beginIndex);
        try {
          boolean first, last, realFirst = true;
          Path myPath;
          for (int i = beginIndex; i < endIndex; ++i) {
            first = (i == beginIndex);
            last = (i == endIndex -1);
            RawComparable begin = first ? beginKey : null;
            RawComparable end = last ? endKey : null;
            TFileScannerInfo scanner;
            if (rowRange == null)
              myPath = cgindex.getPath(i, path);
            else
              myPath = new Path(path, rowRange.names[i]);
            scanner =
                new TFileScannerInfo(first, last, myPath, begin, end);
            if (realFirst) {
              this.scanner = scanner.getTFileScanner();
              if (this.scanner.atEnd()) {
                this.scanner.close();
                this.scanner = null;
              } else {
                realFirst = false;
                tmpScanners.add(scanner);
              }
            } else {
              TFileScanner myScanner = scanner.getTFileScanner();
              if (!myScanner.atEnd())
                tmpScanners.add(scanner);
              myScanner.close();
            }
          }
          scanners = tmpScanners.toArray(new TFileScannerInfo[tmpScanners.size()]);
          this.closeReader = doClose;
          scannerClosed = false;
        }
        finally {
          if (scannerClosed) { // failed to initialize the object.
            if (scanner != null)
              scanner.close();
          }
        }
      }

      @Override
      public void getKey(BytesWritable key) throws IOException {
          if (atEnd()) {
            throw new EOFException("No more key-value to read");
          }
          scanner.getKey(key);
        }

        @Override
        public void getValue(Tuple row) throws IOException {
          if (atEnd()) {
            throw new EOFException("No more key-value to read");
          }
          try {
            scanner.getValue(row);
          } catch (ParseException e) {
            throw new IOException("Invalid Projection: "+e.getMessage());
          }
        }

      public void getCGKey(BytesWritable key) throws IOException {
        scanner.getKey(key);
      }

      public void getCGValue(Tuple row) throws IOException {
        try {
            scanner.getValue(row);
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
        scanner.advance();
        while (true)
        {
          if (scanner.atEnd()) {
            scanner.close();
            scanner = null;
            ++current;
            if (!atEnd()) {
              scanner = scanners[current].getTFileScanner();
            } else
              return false;
          } else
            return true;
        }
      }

      public boolean advanceCG() throws IOException {
        scanner.advance();
        while (true)
        {
          if (scanner.atEnd()) {
            scanner.close();
            scanner = null;
            ++current;
            if (!atEnd()) {
              scanner = scanners[current].getTFileScanner();
            } else
              return false;
          } else
            return true;
        }
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
        if (atEnd())
        {
          return false;
        }
        int index =
            cgindex.lowerBound(new ByteArray(key.getBytes(), 0, key.getLength()),
                comparator);
        if (index >= endIndex) {
          seekToEnd();
          return false;
        }

        if ((index < beginIndex)) {
          // move to the beginning
          index = beginIndex;
        }

        int prevCurrent = current;
        current = index - beginIndex;
        if (current != prevCurrent)
        {
          if (scanner != null)
          {
            try {
              scanner.close();
            } catch (Exception e) {
              // no-op
            }
          }
          scanner = scanners[current].getTFileScanner();
        }
        return scanner.seekTo(key);
      }

      @Override
      public void seekToEnd() throws IOException {
        if (scanner != null)
        {
          try {
            scanner.close();
          } catch (Exception e) {
            // no-op
          }
        }
        scanner = null;
        current = scanners.length;
      }

      @Override
      public void close() throws IOException {
        if (!scannerClosed) {
          scannerClosed = true;
          if (scanner != null)
          {
            try {
              scanner.close();
              scanner = null;
            } catch (Exception e) {
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
    
    public static class CGRowSplit implements Writable {
      int length; // number of files in the batch
      long startByteFirst = -1;
      long numBytesFirst;
      long startRowFirst = -1;
      long numRowsFirst = -1;
      long numBytesLast = -1;
      long numRowsLast = -1;
      String[] names;
      long[] sizes = null;

      CGRowSplit(String[] names, long[] sizes, int length, long startFirst, long bytesFirst,
          long bytesLast) throws IOException {
        this.names = names;
        this.sizes = sizes;
        this.length = length;

        if (startFirst != -1)
        {
          startByteFirst = startFirst;
          numBytesFirst = bytesFirst;
        }
        if (bytesLast != -1 && this.length > 1)
        {
          numBytesLast = bytesLast;
        }
      }

      public CGRowSplit() {
        // no-op;
      }
      
      @Override
      public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{length = " + length + "}\n");
        for (int i = 0; i < length; i++)
        {
          sb.append("{name = " + names[i] + "}\n");
          sb.append("{size = " + sizes[i] + "}\n");
        }
        sb.append("{startByteFirst = " + startByteFirst + "}\n");
        sb.append("{numBytesFirst = " + numBytesFirst + "}\n");
        sb.append("{startRowFirst = " + startRowFirst + "}\n");
        sb.append("{numRowsFirst = " + numRowsFirst + "}\n");
        sb.append("{numBytesLast = " + numBytesLast + "}\n");
        sb.append("{numRowsLast = " + numRowsLast + "}\n");
        
        return sb.toString();
      }

      @Override
      public void readFields(DataInput in) throws IOException {
        length = Utils.readVInt(in);
        if (length > 0)
        {
          names = new String[length];
          sizes = new long[length];
        }
        for (int i = 0; i < length; i++)
        {
          names[i] = Utils.readString(in);
          sizes[i] = Utils.readVLong(in);
        }
        startByteFirst = Utils.readVLong(in);
        numBytesFirst = Utils.readVLong(in);
        startRowFirst = Utils.readVLong(in);
        numRowsFirst = Utils.readVLong(in);
        numBytesLast = Utils.readVLong(in);
        numRowsLast = Utils.readVLong(in);
      }

      @Override
      public void write(DataOutput out) throws IOException {
        Utils.writeVInt(out, length);
        for (int i = 0; i < length; i++)
        {
          Utils.writeString(out, names[i]);
          Utils.writeVLong(out, sizes[i]);
        }
        Utils.writeVLong(out, startByteFirst);
        Utils.writeVLong(out, numBytesFirst);
        Utils.writeVLong(out, startRowFirst);
        Utils.writeVLong(out, numRowsFirst);
        Utils.writeVLong(out, numBytesLast);
        Utils.writeVLong(out, numRowsLast);
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
    Path finalOutputPath;
    Configuration conf;
    FileSystem fs;
    CGSchema cgschema;
    private boolean finished, closed;
    CGIndex index;

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
    public Writer(Path path, String schema, boolean sorted, String name, String serializer,
        String compressor, String owner, String group, short perm,boolean overwrite, Configuration conf)
        throws IOException, ParseException {
      this(path, new Schema(schema), sorted, null, name, serializer, compressor, owner, group, perm, overwrite,
          conf);
    }
    
    public Writer(Path path, Schema schema, boolean sorted, String name, String serializer,
        String compressor, String owner, String group, short perm,boolean overwrite, Configuration conf)
        throws IOException, ParseException {
      this(path, schema, sorted, null, name, serializer, compressor, owner, group, perm, overwrite,
          conf);
    }

    public Writer(Path path, String schema, boolean sorted, String comparator, String name, String serializer,
        String compressor, String owner, String group, short perm,boolean overwrite, Configuration conf)
        throws IOException, ParseException {
      this(path, new Schema(schema), sorted, comparator, name, serializer, compressor, owner, group, perm, overwrite,
          conf);
    }

    public Writer(Path path, Schema schema, boolean sorted, String comparator, String name, String serializer,
        String compressor, String owner, String group, short perm, boolean overwrite, Configuration conf)
        throws IOException, ParseException {
      this.path = path;
      this.conf = conf;
      this.finalOutputPath = path;

      fs = path.getFileSystem(conf);

      // If meta file already exists, that means the ColumnGroup is complete and
      // valid, we will not proceed.
      checkMetaFile(path);

      // if overwriting, remove everything
      if (overwrite) {
        fs.delete(path, true);
      }

      // create final output path and temporary output path
      checkPath(path, true);
      
      Path parent = path.getParent();
      Path tmpPath1 = new Path(parent, "_temporary");
      Path tmpPath2 = new Path(tmpPath1, name);
      checkPath(tmpPath2, true);
      
      cgschema = new CGSchema(schema, sorted, comparator, name, serializer, compressor, owner, group, perm);
      CGSchema sfNew = CGSchema.load(fs, path);
      if (sfNew != null) {
        // sanity check - compare input with on-disk schema.
        if (!sfNew.equals(cgschema)) {
          throw new IOException("Schema passed in is different from the one on disk");
        }
      } else {
        // create the schema file in FS
        cgschema.create(fs, path);
      }
    }

    /**
     * Reopen an already created ColumnGroup for writing. It accepts
     * a temporary path for column group where cginserter can write.
     * RuntimeException will be thrown if the table is already closed, 
     * or if createMetaBlock() is called by some other process.
     */
    public Writer(Path finalPath, Path workPath, Configuration conf) throws IOException,
        ParseException {
      this.path = workPath;
      finalOutputPath = finalPath;
      this.conf = conf;
      fs = path.getFileSystem(conf);
      checkPath(finalOutputPath, false);
      checkPath(path, true);
      checkMetaFile(finalOutputPath);
      cgschema = CGSchema.load(fs, finalOutputPath);
    }

    /*
     * Reopen an already created ColumnGroup for writing.
     * It takes in a CGSchema to set its own cgschema instead of going
     * to disk to fetch this information. 
     */
    public Writer(Path finalPath, Path workPath, CGSchema cgschema, Configuration conf) throws IOException, ParseException {
      this.path = workPath;
      finalOutputPath = finalPath;      
      this.conf = conf;
      fs = path.getFileSystem(conf);
      this.cgschema = cgschema;
    }

    /**
     * Reopen an already created ColumnGroup for writing. RuntimeException will
     * be thrown if the table is already closed, or if createMetaBlock() is
     * called by some other process.
     */
    public Writer(Path path, Configuration conf) throws IOException,
        ParseException {
      this.path = path;
      finalOutputPath = path;
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
        finish();
      }
      if (!closed) {
        closed = true;
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
      return getInserter(name, finishWriter, true);      
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
     * @param checktype
     *          whether or not do type check.
     * 
     * @return A table inserter object.
     * @throws IOException
     */
    public TableInserter getInserter(String name, boolean finishWriter, boolean checkType)
        throws IOException {
      if (finished) {
        throw new IOException("ColumnGroup has been closed for insertion.");
      }
      return new CGInserter(name, finishWriter, checkType);
    }

    private void createIndex() throws IOException {
      MetaFile.Writer metaFile =
        MetaFile.createWriter(makeMetaFilePath(finalOutputPath), conf);
      index = buildIndex(fs, finalOutputPath, false, conf);
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
      boolean checkType = true;
      
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
      
      CGInserter(String name, boolean finishWriter, boolean checkType) throws IOException {
        this.name = name;
        this.finishWriter = finishWriter;
        this.tupleWriter = new TupleWriter(getSchema());
        this.checkType = checkType;
        
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
        /*
         * If checkType is set to be true, we check for the first row - this is only a sanity check preventing
         * users from messing up output schema;
         * If checkType is set to be false, we do not do any type check. 
         */
        if (checkType == true) {
          TypesUtils.checkCompatible(row, getSchema());
          checkType = false;
        }
        
        DataOutputStream outKey = tfileWriter.prepareAppendKey(key.getLength());
        try {
          outKey.write(key.getBytes(), 0, key.getLength());
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
          fs.rename(new Path(path, tmpName), new Path(finalOutputPath, name));

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
    int index;
    String name;
    long rows, bytes;
    RawComparable firstKey;
    RawComparable lastKey;

    // for reading
    public CGIndexEntry() {
      // no-op
    }

    // for writing
    public CGIndexEntry(String name, long rows, RawComparable firstKey,
        RawComparable lastKey) {
      this.name = name;
      this.rows = rows;
      this.firstKey = firstKey;
      this.lastKey = lastKey;
    }

    public int getIndex() {
      return index;
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
    
    void setIndex (int idx) {
      this.index = idx;
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
    
    int getFileIndex(Path path) throws IOException {
      String filename = path.getName();
      if (index.isEmpty())
        return -1;
      for (CGIndexEntry cgie : index) {
        if (cgie.getName().equals(filename)) {
          return cgie.getIndex(); 
        }
      }
      return -1;
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
      range.bytes = bytes;
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
      next.bytes = bytes;
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
        range.setIndex(i);
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

  public static class CGPathFilter implements PathFilter {
    private static Configuration conf;
   
    public static void setConf(Configuration c) {
      conf = c;
    }

    public boolean accept(Path p) {
      return p.getName().equals(META_FILE) || p.getName().equals(SCHEMA_FILE)
          || p.getName().startsWith(".tmp.")
          || p.getName().startsWith("_")
          || p.getName().startsWith("ttt")
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
      out.println("Name: " + reader.getName());
      IOutils.indent(out, indent);
      out.println("Serializer: " + reader.getSerializer());
      IOutils.indent(out, indent);
      out.println("Compressor: " + reader.getCompressor());      
      IOutils.indent(out, indent);
      out.println("Group: " + reader.getGroup());      
      IOutils.indent(out, indent);
      out.println("Perm: " + reader.getPerm());      

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
      if (reader.cgindex == null)
        reader.cgindex = buildIndex(reader.fs, reader.path, reader.dirty, conf);
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
