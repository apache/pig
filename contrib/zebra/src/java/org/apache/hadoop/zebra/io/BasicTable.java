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
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.io.file.tfile.Utils;
import org.apache.hadoop.io.file.tfile.MetaBlockAlreadyExists;
import org.apache.hadoop.io.file.tfile.MetaBlockDoesNotExist;
import org.apache.hadoop.io.file.tfile.Utils.Version;
import org.apache.hadoop.zebra.io.ColumnGroup.Reader.CGRangeSplit;
import org.apache.hadoop.zebra.types.CGSchema;
import org.apache.hadoop.zebra.types.ParseException;
import org.apache.hadoop.zebra.types.Partition;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.hadoop.zebra.types.Schema;
import org.apache.hadoop.zebra.types.TableSchemaParser;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.data.Tuple;

/**
 * A materialized table that consists of one or more tightly coupled Column
 * Groups.
 * 
 * The following Configuration parameters can customize the behavior of
 * BasicTable.
 * <ul>
 * <li><b>table.output.tfile.minBlock.size</b> (int) Minimum compression block
 * size for underlying TFile (default to 1024*1024).
 * <li><b>table.output.tfile.compression</b> (String) Compression method (one of
 * "none", "lzo", "gz") (default is "gz"). @see
 * {@link TFile#getSupportedCompressionAlgorithms()}
 * <li><b>table.input.split.minSize</b> (int) Minimum split size (default to
 * 64*1024).
 * </ul>
 */
public class BasicTable {
  // name of the BasicTable schema file
  private final static String BT_SCHEMA_FILE = ".btschema";
  // schema version
  private final static Version SCHEMA_VERSION =
      new Version((short) 1, (short) 0);
  // name of the BasicTable meta-data file
  private final static String BT_META_FILE = ".btmeta";
  // column group prefix
  private final static String CGPathPrefix = "CG";
  // default comparator to "memcmp"
  private final static String DEFAULT_COMPARATOR = TFile.COMPARATOR_MEMCMP;

  // no public ctor for instantiating a BasicTable object
  private BasicTable() {
    // no-op
  }

  /**
   * Form a Path for the i-th ColumnGroup.
   * 
   * @param path
   *          The path to the BasicTable
   * @param total
   *          The total number of ColumnGruops, ignored.
   * @param i
   *          The rank of the ColumnGroup to compute the name.
   * @return A Path object.
   */
  static Path makeCGPath(Path path, int total, int i) {
    int digits = 1;
    while (total >= 10) {
      ++digits;
      total /= 10;
    }
    String formatString = "%0" + digits + "d";
    return new Path(path, CGPathPrefix + String.format(formatString, i));
  }

  /**
   * BasicTable reader.
   */
  public static class Reader implements Closeable {
    private Path path;
    private boolean closed = true;
    private SchemaFile schemaFile;
    private Projection projection;
    boolean inferredMapping;
    private MetaFile.Reader metaReader;
    private BasicTableStatus status;
    Partition partition;
    ColumnGroup.Reader[] colGroups;
    Tuple[] cgTuples;

    private synchronized void checkInferredMapping() throws ParseException, IOException {
      if (!inferredMapping) {
        for (int i = 0; i < colGroups.length; ++i) {
          colGroups[i].setProjection(partition.getProjection(i));
          if (partition.isCGNeeded(i))
            cgTuples[i] = TypesUtils.createTuple(colGroups[i].getSchema());
          else
            cgTuples[i] = null;
        }
        partition.setSource(cgTuples);
        inferredMapping = true;
      }
      else {
        // the projection is not changed, so we do not need to recalculate the
        // mapping
      }
    }

    /**
     * Create a BasicTable reader.
     * 
     * @param path
     *          The directory path to the BasicTable.
     * @param conf
     *          Optional configuration parameters.
     * @throws IOException
     */
    public Reader(Path path, Configuration conf) throws IOException {
      try {
        this.path = path;
        schemaFile = new SchemaFile(path, conf);
        metaReader = MetaFile.createReader(new Path(path, BT_META_FILE), conf);
        // create column group readers
        int numCGs = schemaFile.getNumOfPhysicalSchemas();
        Schema schema;
        colGroups = new ColumnGroup.Reader[numCGs];
        cgTuples = new Tuple[numCGs];
        // set default projection that contains everything
        schema = schemaFile.getLogical();
        projection = new Projection(schema);
        String storage = schemaFile.getStorageString();
        partition = new Partition(schema, projection, storage);
        for (int nx = 0; nx < numCGs; nx++) {
          colGroups[nx] =
              new ColumnGroup.Reader(BasicTable.makeCGPath(path, numCGs, nx),
                  conf);
          if (partition.isCGNeeded(nx))
            cgTuples[nx] = TypesUtils.createTuple(colGroups[nx].getSchema());
          else
            cgTuples[nx] = null;
        }
        partition.setSource(cgTuples);
        buildStatus();
        closed = false;
      }
      catch (Exception e) {
        throw new IOException("BasicTable.Reader constructor failed : "
            + e.getMessage());
      }
      finally {
        if (closed) {
          /**
           * Construction fails.
           */
          if (colGroups != null) {
            for (int i = 0; i < colGroups.length; ++i) {
              if (colGroups[i] != null) {
                try {
                  colGroups[i].close();
                }
                catch (Exception e) {
                  // ignore error
                }
              }
            }
          }
          if (metaReader != null) {
            try {
              metaReader.close();
            }
            catch (Exception e) {
              // no-op
            }
          }
        }
      }
    }

    /**
     * Is the Table sorted?
     * 
     * @return Whether the table is sorted.
     */
    public boolean isSorted() {
      return schemaFile.isSorted();
    }

    /**
     * Set the projection for the reader. This will affect calls to
     * {@link #getScanner(RangeSplit, boolean)},
     * {@link #getScanner(BytesWritable, BytesWritable, boolean)},
     * {@link #getStatus()}, {@link #getSchema()}.
     * 
     * @param projection
     *          The projection on the BasicTable for subsequent read operations.
     *          For this version of implementation, the projection is a comma
     *          separated list of column names, such as
     *          "FirstName, LastName, Sex, Department". If we want select all
     *          columns, pass projection==null.
     * @throws IOException
     */
    public synchronized void setProjection(String projection)
        throws ParseException, IOException {
      if (projection == null) {
        this.projection = new Projection(schemaFile.getLogical());
        partition =
            new Partition(schemaFile.getLogical(), this.projection, schemaFile
                .getStorageString());
      }
      else {
        /**
         * the typed schema from projection which is untyped or actually typed
         * as "bytes"
         */
        this.projection =
            new Projection(schemaFile.getLogical(), projection);
        partition =
            new Partition(schemaFile.getLogical(), this.projection, schemaFile
                .getStorageString());
      }
      inferredMapping = false;
    }

    /**
     * Get the status of the BasicTable.
     */
    public BasicTableStatus getStatus() {
      return status;
    }

    /**
     * Given a split range, calculate how the file data that fall into the range
     * are distributed among hosts.
     * 
     * @param split
     *          The range-based split. Can be null to indicate the whole TFile.
     * @return An object that conveys how blocks fall in the split are
     *         distributed across hosts.
     * @see #rangeSplit(int)
     */
    public BlockDistribution getBlockDistribution(RangeSplit split)
        throws IOException {
      BlockDistribution bd = new BlockDistribution();
      for (int nx = 0; nx < colGroups.length; nx++) {
        bd.add(colGroups[nx].getBlockDistribution(split == null ? null : split
            .get(nx)));
      }
      return bd;
    }

    /**
     * Collect some key samples and use them to partition the table. Only
     * applicable to sorted BasicTable. The returned {@link KeyDistribution}
     * object also contains information on how data are distributed for each
     * key-partitioned bucket.
     * 
     * @param n
     *          Targeted size of the sampling.
     * @return KeyDistribution object.
     * @throws IOException
     */
    public KeyDistribution getKeyDistribution(int n) throws IOException {
      KeyDistribution kd =
          new KeyDistribution(TFile.makeComparator(schemaFile.getComparator()));
      for (int nx = 0; nx < colGroups.length; nx++) {
        kd.add(colGroups[nx].getKeyDistribution(n));
      }
      if (kd.size() > (int) (n * 1.5)) {
        kd.resize(n);
      }
      return kd;
    }

    /**
     * Get a scanner that reads all rows whose row keys fall in a specific
     * range. Only applicable to sorted BasicTable.
     * 
     * @param beginKey
     *          The begin key of the scan range. If null, start from the first
     *          row in the table.
     * @param endKey
     *          The end key of the scan range. If null, scan till the last row
     *          in the table.
     * @param closeReader
     *          close the underlying Reader object when we close the scanner.
     *          Should be set to true if we have only one scanner on top of the
     *          reader, so that we should release resources after the scanner is
     *          closed.
     * @return A scanner object.
     * @throws IOException
     */
    public synchronized TableScanner getScanner(BytesWritable beginKey,
        BytesWritable endKey, boolean closeReader) throws IOException {
      try {
        checkInferredMapping();
      }
      catch (Exception e) {
        throw new IOException("getScanner failed : " + e.getMessage());
      }
      return new BTScanner(beginKey, endKey, closeReader, partition);
    }

    /**
     * Get a scanner that reads a consecutive number of rows as defined in the
     * {@link RangeSplit} object, which should be obtained from previous calls
     * of {@link #rangeSplit(int)}.
     * 
     * @param split
     *          The split range. If null, get a scanner to read the complete
     *          table.
     * @param closeReader
     *          close the underlying Reader object when we close the scanner.
     *          Should be set to true if we have only one scanner on top of the
     *          reader, so that we should release resources after the scanner is
     *          closed.
     * @return A scanner object.
     * @throws IOException
     */
    public synchronized TableScanner getScanner(RangeSplit split,
        boolean closeReader) throws IOException, ParseException {
      checkInferredMapping();
      return new BTScanner(split, partition, closeReader);
    }

    /**
     * Get the schema of the table. The schema may be different from
     * {@link BasicTable.Reader#getSchema(Path, Configuration)} if a projection
     * has been set on the table.
     * 
     * @return The schema of the BasicTable.
     */
    public Schema getSchema() {
      return projection.getSchema();
    }

    /**
     * Get the BasicTable schema without loading the full table index.
     * 
     * @param path
     *          The path to the BasicTable.
     * @param conf
     * @return The logical Schema of the table (all columns).
     * @throws IOException
     */
    public static Schema getSchema(Path path, Configuration conf)
        throws IOException {
      SchemaFile schF = new SchemaFile(path, conf);
      return schF.getLogical();
    }

    /**
     * Get the path to the table.
     * 
     * @return The path string to the table.
     */
    public String getPath() {
      return path.toString();
    }

    /**
     * Split the table into at most n parts.
     * 
     * @param n
     *          Maximum number of parts in the output list.
     * @return A list of RangeSplit objects, each of which can be used to
     *         construct TableScanner later.
     */
    @SuppressWarnings("unchecked")
    public List<RangeSplit> rangeSplit(int n) throws IOException {
      // assume all CGs will be split into the same number of horizontal
      // slices
      List<CGRangeSplit>[] cgSplitsAll = new ArrayList[colGroups.length];
      // split each CG
      for (int nx = 0; nx < colGroups.length; nx++) {
        cgSplitsAll[nx] = colGroups[nx].rangeSplit(n);
      }

      // verify all CGs have same number of slices
      int numSlices = -1;
      for (int nx = 0; nx < cgSplitsAll.length; nx++) {
        if (numSlices < 0) {
          numSlices = cgSplitsAll[nx].size();
        }
        else if (cgSplitsAll[nx].size() != numSlices) {
          throw new IOException(
              "BasicTable's column groups were not equally split.");
        }
      }
      // return horizontal slices as RangeSplits
      List<RangeSplit> ret = new ArrayList<RangeSplit>(numSlices);
      for (int slice = 0; slice < numSlices; slice++) {
        CGRangeSplit[] oneSliceSplits = new CGRangeSplit[cgSplitsAll.length];
        for (int cgIndex = 0; cgIndex < cgSplitsAll.length; cgIndex++) {
          oneSliceSplits[cgIndex] = cgSplitsAll[cgIndex].get(slice);
        }
        ret.add(new BasicTable.Reader.RangeSplit(oneSliceSplits));
      }
      return ret;
    }

    /**
     * Close the BasicTable for reading. Resources are released.
     */
    @Override
    public void close() throws IOException {
      if (!closed) {
        try {
          closed = true;
          metaReader.close();
          for (int i = 0; i < colGroups.length; ++i) {
            colGroups[i].close();
          }
        }
        finally {
          try {
            metaReader.close();
          }
          catch (Exception e) {
            // no-op
          }
          for (int i = 0; i < colGroups.length; ++i) {
            try {
              colGroups[i].close();
            }
            catch (Exception e) {
              // no-op
            }
          }
        }
      }
    }

    String getBTSchemaString() {
      return schemaFile.getBTSchemaString();
    }

    String getStorageString() {
      return schemaFile.getStorageString();
    }

    private void buildStatus() {
      status = new BasicTableStatus();
      status.beginKey = colGroups[0].getStatus().getBeginKey();
      status.endKey = colGroups[0].getStatus().getEndKey();
      status.rows = colGroups[0].getStatus().getRows();
      status.size = 0;
      for (int nx = 0; nx < colGroups.length; nx++) {
        status.size += colGroups[nx].getStatus().getSize();
      }
    }

    /**
     * Obtain an input stream for reading a meta block.
     * 
     * @param name
     *          The name of the meta block.
     * @return The input stream for reading the meta block.
     * @throws IOException
     * @throws MetaBlockDoesNotExist
     */
    public DataInputStream getMetaBlock(String name)
        throws MetaBlockDoesNotExist, IOException {
      return metaReader.getMetaBlock(name);
    }

    /**
     * A range-based split on the metaReadertable.The content of the split is
     * implementation-dependent.
     */
    public static class RangeSplit implements Writable {
      CGRangeSplit[] slice;

      RangeSplit(CGRangeSplit[] splits) {
        slice = splits;
      }

      /**
       * Default constructor.
       */
      public RangeSplit() {
        // no-op
      }

      /**
       * @see Writable#readFields(DataInput)
       */
      @Override
      public void readFields(DataInput in) throws IOException {
        int count = Utils.readVInt(in);
        slice = new CGRangeSplit[count];
        for (int nx = 0; nx < count; nx++) {
          CGRangeSplit cgrs = new CGRangeSplit();
          cgrs.readFields(in);
          slice[nx] = cgrs;
        }
      }

      /**
       * @see Writable#write(DataOutput)
       */
      @Override
      public void write(DataOutput out) throws IOException {
        Utils.writeVInt(out, slice.length);
        for (CGRangeSplit split : slice) {
          split.write(out);
        }
      }

      CGRangeSplit get(int index) {
        return slice[index];
      }
    }

    /**
     * BasicTable scanner class
     */
    private class BTScanner implements TableScanner {
      private Projection schema;
      private TableScanner[] cgScanners;
      private int opCount = 0;
      Random random = new Random(System.nanoTime());
      // checking for consistency once every 1000 times.
      private static final int VERIFY_FREQ = 1000;
      private boolean sClosed = false;
      private boolean closeReader;
      private Partition partition;

      private synchronized boolean checkIntegrity() {
        return ((++opCount % VERIFY_FREQ) == 0) && (cgScanners.length > 1);
      }

      public BTScanner(BytesWritable beginKey, BytesWritable endKey,
          boolean closeReader, Partition partition) throws IOException {
        this.partition = partition;
        boolean anyScanner = false;
        try {
          schema = partition.getProjection();
          cgScanners = new TableScanner[colGroups.length];
          for (int i = 0; i < colGroups.length; ++i) {
            // if no CG is needed explicitly by projection but the "countRow" still needs to access some column group
            if (partition.isCGNeeded(i) || (!anyScanner && (i == colGroups.length-1)))
            {
              anyScanner = true;
              cgScanners[i] = colGroups[i].getScanner(beginKey, endKey, false);
            } else
              cgScanners[i] = null;
          }
          this.closeReader = closeReader;
          sClosed = false;
        }
        catch (Exception e) {
          throw new IOException("BTScanner constructor failed : "
              + e.getMessage());
        }
        finally {
          if (sClosed) {
            if (cgScanners != null) {
              for (int i = 0; i < cgScanners.length; ++i) {
                if (cgScanners[i] != null) {
                  try {
                    cgScanners[i].close();
                    cgScanners[i] = null;
                  }
                  catch (Exception e) {
                    // no-op
                  }
                }
              }
            }
          }
        }
      }

      public BTScanner(RangeSplit split, Partition partition,
          boolean closeReader) throws IOException {
        try {
          schema = partition.getProjection();
          cgScanners = new TableScanner[colGroups.length];
          boolean anyScanner = false;
          for (int i = 0; i < colGroups.length; ++i) {
            // if no CG is needed explicitly by projection but the "countRow" still needs to access some column group
            if (partition.isCGNeeded(i) || (!anyScanner && (i == colGroups.length-1)))
            {
              cgScanners[i] =
                  colGroups[i].getScanner(split == null ? null : split.get(i),
                      false);
              anyScanner = true;
            } else
              cgScanners[i] = null;
          }
          this.partition = partition;
          this.closeReader = closeReader;
          sClosed = false;
        }
        catch (Exception e) {
          throw new IOException("BTScanner constructor failed : "
              + e.getMessage());
        }
        finally {
          if (sClosed) {
            if (cgScanners != null) {
              for (int i = 0; i < cgScanners.length; ++i) {
                if (cgScanners[i] != null) {
                  try {
                    cgScanners[i].close();
                    cgScanners[i] = null;
                  }
                  catch (Exception e) {
                    // no-op
                  }
                }
              }
            }
          }
        }
      }

      @Override
      public boolean advance() throws IOException {
        boolean first = false, cur, firstAdvance = true;
        for (int nx = 0; nx < cgScanners.length; nx++) {
          if (cgScanners[nx] != null)
          {
            cur = cgScanners[nx].advance();
            if (!firstAdvance) {
              if (cur != first) {
                throw new IOException(
                    "advance() failed: Column Groups are not evenly positioned.");
              }
            }
            else {
              firstAdvance = false;
              first = cur;
            }
          }
        }
        return first;
      }

      @Override
      public boolean atEnd() throws IOException {
        if (cgScanners.length == 0) {
          return true;
        }
        boolean ret = true;
        int i;
        for (i = 0; i < cgScanners.length; i++)
        {
          if (cgScanners[i] != null)
          {
            ret = cgScanners[i].atEnd();
            break;
          }
        }

        if (i == cgScanners.length)
        {
          return true;
        }
        
        if (!checkIntegrity()) {
          return ret;
        }

        while (true)
        {
          int index = random.nextInt(cgScanners.length - 1) + 1;
          if (cgScanners[index] != null) {
            if (cgScanners[index].atEnd() != ret) {
              throw new IOException(
                  "atEnd() failed: Column Groups are not evenly positioned.");
            }
            break;
          }
        }
        return ret;
      }

      @Override
      public void getKey(BytesWritable key) throws IOException {
        if (cgScanners.length == 0) {
          return;
        }
        
        int i;
        for (i = 0; i < cgScanners.length; i++)
        {
          if (cgScanners[i] != null)
          {
            cgScanners[i].getKey(key);
            break;
          }
        }

        if (i == cgScanners.length)
          return;

        if (!checkIntegrity()) {
          return;
        }

        while (true)
        {
          int index = random.nextInt(cgScanners.length - 1) + 1;
          if (cgScanners[index] != null)
          {
            BytesWritable key2 = new BytesWritable();
            cgScanners[index].getKey(key2);
            if (key.equals(key2)) {
              return;
            }
            break;
          }
        }
        throw new IOException(
            "getKey() failed: Column Groups are not evenly positioned.");
      }

      @Override
      public void getValue(Tuple row) throws IOException {
        if (row.size() < projection.getSchema().getNumColumns()) {
          throw new IOException("Mismatched tuple object");
        }

        for (int i = 0; i < cgScanners.length; ++i)
        {
          if (cgScanners[i] != null)
          {
            if (partition.isCGNeeded(i))
            {
              if (cgTuples[i] == null)
                throw new AssertionError("cgTuples["+i+"] is null");
              cgScanners[i].getValue(cgTuples[i]);
            }
          }
        }

        try {
          partition.read(row);
        }
        catch (Exception e) {
          throw new IOException("getValue() failed: " + e.getMessage());
        }
      }

      @Override
      public boolean seekTo(BytesWritable key) throws IOException {
        boolean first = false, cur;
        for (int nx = 0; nx < cgScanners.length; nx++) {
          if (cgScanners[nx] == null)
            continue;
          cur = cgScanners[nx].seekTo(key);
          if (nx != 0) {
            if (cur != first) {
              throw new IOException(
                  "seekTo() failed: Column Groups are not evenly positioned.");
            }
          }
          else {
            first = cur;
          }
        }
        return first;
      }

      @Override
      public void seekToEnd() throws IOException {
        for (int nx = 0; nx < cgScanners.length; nx++) {
          if (cgScanners[nx] == null)
            continue;
          cgScanners[nx].seekToEnd();
        }
      }

      @Override
      public String getProjection() {
        return schema.toString();
      }
      
      @Override
      public Schema getSchema() {
        return schema.getSchema();
      }

      @Override
      public void close() throws IOException {
        if (sClosed) return;
        sClosed = true;
        try {
          for (int nx = 0; nx < cgScanners.length; nx++) {
            if (cgScanners[nx] == null)
              continue;
            cgScanners[nx].close();
            cgScanners[nx] = null;
          }
          if (closeReader) {
            BasicTable.Reader.this.close();
          }
        }
        finally {
          for (int nx = 0; nx < cgScanners.length; nx++) {
            if (cgScanners[nx] == null)
              continue;
            try {
              cgScanners[nx].close();
              cgScanners[nx] = null;
            }
            catch (Exception e) {
              // no-op
            }
          }
          if (closeReader) {
            try {
              BasicTable.Reader.this.close();
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
   * BasicTable writer.
   */
  public static class Writer implements Closeable {
    private SchemaFile schemaFile;
    private MetaFile.Writer metaWriter;
    private boolean closed = true;
    ColumnGroup.Writer[] colGroups;
    Partition partition;
    Tuple[] cgTuples;

    /**
     * Create a BasicTable writer. The semantics are as follows:
     * <ol>
     * <li>If path does not exist:
     * <ul>
     * <li>create the path directory, and initialize the directory for future
     * row insertion..
     * </ul>
     * <li>If path exists and the directory is empty: initialize the directory
     * for future row insertion.
     * <li>If path exists and contains what look like a complete BasicTable,
     * IOException will be thrown.
     * </ol>
     * This constructor never removes a valid/complete BasicTable.
     * 
     * @param path
     *          The path to the Basic Table, either not existent or must be a
     *          directory.
     * @param btSchemaString
     *          The schema of the Basic Table. For this version of
     *          implementation, the schema of a table is a comma or
     *          semicolon-separated list of column names, such as
     *          "FirstName, LastName; Sex, Department".
     * @param sorted
     *          Whether the table to be created is sorted or not. If set to
     *          true, we expect the rows inserted by every inserter created from
     *          this Writer must be sorted. Additionally, there exists an
     *          ordering of the inserters Ins-1, Ins-2, ... such that the rows
     *          created by Ins-1, followed by rows created by Ins-2, ... form a
     *          total order.
     * @param conf
     *          Optional Configuration objects.
     * 
     * @throws IOException
     * @see Schema
     */
    public Writer(Path path, String btSchemaString, String btStorageString,
        boolean sorted, Configuration conf) throws IOException {
      try {
        schemaFile =
            new SchemaFile(path, btSchemaString, btStorageString,
                DEFAULT_COMPARATOR, sorted, conf);
        partition = schemaFile.getPartition();
        int numCGs = schemaFile.getNumOfPhysicalSchemas();
        colGroups = new ColumnGroup.Writer[numCGs];
        cgTuples = new Tuple[numCGs];
        for (int nx = 0; nx < numCGs; nx++) {
          colGroups[nx] =
              new ColumnGroup.Writer(BasicTable.makeCGPath(path, numCGs, nx),
                  schemaFile.getPhysicalSchema(nx), sorted, schemaFile
                      .getSerializer(nx), schemaFile.getCompressor(nx), false,
                  conf);
          cgTuples[nx] = TypesUtils.createTuple(colGroups[nx].getSchema());
        }
        metaWriter = MetaFile.createWriter(new Path(path, BT_META_FILE), conf);
        partition.setSource(cgTuples);
        closed = false;
      }
      catch (Exception e) {
        throw new IOException("ColumnGroup.Writer constructor failed : "
            + e.getMessage());
      }
      finally {
        ;
        if (!closed) return;
        if (metaWriter != null) {
          try {
            metaWriter.close();
          }
          catch (Exception e) {
            // no-op
          }
        }
        if (colGroups != null) {
          for (int i = 0; i < colGroups.length; ++i) {
            if (colGroups[i] != null) {
              try {
                colGroups[i].close();
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
     * Reopen an already created BasicTable for writing. Excepiton will be
     * thrown if the table is already closed, or is in the process of being
     * closed.
     */
    public Writer(Path path, Configuration conf) throws IOException {
      try {
        schemaFile = new SchemaFile(path, conf);
        int numCGs = schemaFile.getNumOfPhysicalSchemas();
        partition = schemaFile.getPartition();
        colGroups = new ColumnGroup.Writer[numCGs];
        cgTuples = new Tuple[numCGs];
        for (int nx = 0; nx < numCGs; nx++) {
          colGroups[nx] =
              new ColumnGroup.Writer(BasicTable.makeCGPath(path, numCGs, nx),
                  conf);
          cgTuples[nx] = TypesUtils.createTuple(colGroups[nx].getSchema());
        }
        partition.setSource(cgTuples);
        metaWriter = MetaFile.createWriter(new Path(path, BT_META_FILE), conf);
        closed = false;
      }
      catch (Exception e) {
        throw new IOException("ColumnGroup.Writer failed : " + e.getMessage());
      }
      finally {
        if (!closed) return;
        if (metaWriter != null) {
          try {
            metaWriter.close();
          }
          catch (Exception e) {
            // no-op
          }
        }
        if (colGroups != null) {
          for (int i = 0; i < colGroups.length; ++i) {
            if (colGroups[i] != null) {
              try {
                colGroups[i].close();
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
     * Release resources used by the object. Unlike close(), finish() does not
     * make the table immutable.
     */
    public void finish() throws IOException {
      if (closed) return;
      closed = true;
      try {
        for (int nx = 0; nx < colGroups.length; nx++) {
          if (colGroups[nx] != null) {
            colGroups[nx].finish();
          }
        }
        metaWriter.finish();
      }
      finally {
        try {
          metaWriter.finish();
        }
        catch (Exception e) {
          // no-op
        }
        for (int i = 0; i < colGroups.length; ++i) {
          try {
            colGroups[i].finish();
          }
          catch (Exception e) {
            // no-op
          }
        }
      }
    }

    /**
     * Close the BasicTable for writing. No more inserters can be obtained after
     * close().
     */
    @Override
    public void close() throws IOException {
      if (closed) return;
      closed = true;
      try {
        for (int nx = 0; nx < colGroups.length; nx++) {
          if (colGroups[nx] != null) {
            colGroups[nx].close();
          }
        }
        metaWriter.close();
      }
      finally {
        try {
          metaWriter.close();
        }
        catch (Exception e) {
          // no-op
        }
        for (int i = 0; i < colGroups.length; ++i) {
          try {
            colGroups[i].close();
          }
          catch (Exception e) {
            // no-op
          }
        }
      }
    }

    /**
     * Get the schema of the table.
     * 
     * @return the Schema object.
     */
    public Schema getSchema() {
      return schemaFile.getLogical();
    }

    /**
     * Get a inserter with a given name.
     * 
     * @param name
     *          the name of the inserter. If multiple calls to getInserter with
     *          the same name has been called, we expect they are the result of
     *          speculative execution and at most one of them will succeed.
     * @param finishWriter
     *          finish the underlying Writer object upon the close of the
     *          Inserter. Should be set to true if there is only one inserter
     *          operate on the table, so we should call finish() after the
     *          Inserter is closed.
     * 
     * @return A inserter object.
     * @throws IOException
     */
    public TableInserter getInserter(String name, boolean finishWriter)
        throws IOException {
      if (closed) {
        throw new IOException("BasicTable closed");
      }
      return new BTInserter(name, finishWriter, partition);
    }

    /**
     * Obtain an output stream for creating a Meta Block with the specific name.
     * This method can only be called after we insert all rows into the table.
     * All Meta Blocks must be created by a single process prior to closing the
     * table. No more inserter can be created after this call.
     * 
     * @param name
     *          The name of the Meta Block
     * @return The output stream. Close the stream to conclude the writing.
     * @throws IOException
     * @throws MetaBlockAlreadyExists
     */
    public DataOutputStream createMetaBlock(String name)
        throws MetaBlockAlreadyExists, IOException {
      return metaWriter.createMetaBlock(name);
    }

    private class BTInserter implements TableInserter {
      private TableInserter cgInserters[];
      private boolean sClosed = true;
      private boolean finishWriter;
      private Partition partition = null;

      BTInserter(String name, boolean finishWriter, Partition partition)
          throws IOException {
        try {
          cgInserters = new ColumnGroup.Writer.CGInserter[colGroups.length];
          for (int nx = 0; nx < colGroups.length; nx++) {
            cgInserters[nx] = colGroups[nx].getInserter(name, false);
          }
          this.finishWriter = finishWriter;
          this.partition = partition;
          sClosed = false;
        }
        catch (Exception e) {
          throw new IOException("BTInsert constructor failed :"
              + e.getMessage());
        }
        finally {
          if (sClosed) {
            if (cgInserters != null) {
              for (int i = 0; i < cgInserters.length; ++i) {
                if (cgInserters[i] != null) {
                  try {
                    cgInserters[i].close();
                  }
                  catch (Exception e) {
                    // no-op
                  }
                }
              }
            }
          }
        }
      }

      @Override
      public Schema getSchema() {
        return Writer.this.getSchema();
      }

      @Override
      public void insert(BytesWritable key, Tuple row) throws IOException {
        if (sClosed) {
          throw new IOException("Inserter already closed");
        }

        // break the input row into sub-tuples, then insert them into the
        // corresponding CGs
        int curTotal = 0;
        try {
          partition.insert(key, row);
        }
        catch (Exception e) {
          throw new IOException("insert failed : " + e.getMessage());
        }
        for (int nx = 0; nx < colGroups.length; nx++) {
          Tuple subTuple = cgTuples[nx];
          int numCols = subTuple.size();
          cgInserters[nx].insert(key, subTuple);
          curTotal += numCols;
        }
      }

      @Override
      public void close() throws IOException {
        if (sClosed) return;
        sClosed = true;
        try {
          for (TableInserter ins : cgInserters) {
            ins.close();
          }
          if (finishWriter) {
            BasicTable.Writer.this.finish();
          }
        }
        finally {
          for (TableInserter ins : cgInserters) {
            try {
              ins.close();
            }
            catch (Exception e) {
              // no-op
            }
          }
          if (finishWriter) {
            try {
              BasicTable.Writer.this.close();
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
   * Drop a Basic Table, all files consisting of the BasicTable will be removed.
   * 
   * @param path
   *          the path to the Basic Table.
   * @param conf
   *          The configuration object.
   * @throws IOException
   */
  public static void drop(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    fs.delete(path, true);
  }

  static class SchemaFile {
    private Version version;
    String comparator;
    Schema logical;
    Schema[] physical;
    Partition partition;
    boolean sorted;
    String storage;
    CGSchema[] cgschemas;

    // ctor for reading
    public SchemaFile(Path path, Configuration conf) throws IOException {
      readSchemaFile(path, conf);
    }

    public Schema[] getPhysicalSchema() {
      return physical;
    }

    // ctor for writing
    public SchemaFile(Path path, String btSchemaStr, String btStorageStr,
        String btComparator, boolean sorted, Configuration conf)
        throws IOException {
      storage = btStorageStr;
      this.comparator = btComparator;
      try {
        partition = new Partition(btSchemaStr, btStorageStr);
      }
      catch (Exception e) {
        throw new IOException("Partition constructor failed :" + e.getMessage());
      }
      logical = partition.getSchema();
      cgschemas = partition.getCGSchemas();
      physical = new Schema[cgschemas.length];
      for (int nx = 0; nx < cgschemas.length; nx++) {
        physical[nx] = cgschemas[nx].getSchema();
      }
      this.sorted = sorted;
      version = SCHEMA_VERSION;

      // write out the schema
      createSchemaFile(path, conf);
    }

    public String getComparator() {
      return comparator;
    }

    public Partition getPartition() {
      return partition;
    }

    public boolean isSorted() {
      return sorted;
    }

    public Schema getLogical() {
      return logical;
    }

    public int getNumOfPhysicalSchemas() {
      return physical.length;
    }

    public Schema getPhysicalSchema(int nx) {
      return physical[nx];
    }

    public String getSerializer(int nx) {
      return cgschemas[nx].getSerializer();
    }

    public String getCompressor(int nx) {
      return cgschemas[nx].getCompressor();
    }

    /**
     * @return the string representation of the physical schema.
     */
    public String getBTSchemaString() {
      return logical.toString();
    }

    /**
     * @return the string representation of the storage hints
     */
    public String getStorageString() {
      return storage;
    }

    private void createSchemaFile(Path path, Configuration conf)
        throws IOException {
      // TODO: overwrite existing schema file, or need a flag?
      FSDataOutputStream outSchema =
          path.getFileSystem(conf).create(makeSchemaFilePath(path), true);
      version.write(outSchema);
      WritableUtils.writeString(outSchema, comparator);
      WritableUtils.writeString(outSchema, logical.toString());
      WritableUtils.writeString(outSchema, storage);
      WritableUtils.writeVInt(outSchema, physical.length);
      for (int nx = 0; nx < physical.length; nx++) {
        WritableUtils.writeString(outSchema, physical[nx].toString());
      }
      WritableUtils.writeVInt(outSchema, sorted ? 1 : 0);
      outSchema.close();
    }

    private void readSchemaFile(Path path, Configuration conf)
        throws IOException {
      Path pathSchema = makeSchemaFilePath(path);
      if (!path.getFileSystem(conf).exists(pathSchema)) {
        throw new IOException("BT Schema file doesn't exist: " + pathSchema);
      }
      // read schema file
      FSDataInputStream in = path.getFileSystem(conf).open(pathSchema);
      version = new Version(in);
      // verify compatibility against SCHEMA_VERSION
      if (!version.compatibleWith(SCHEMA_VERSION)) {
        new IOException("Incompatible versions, expecting: " + SCHEMA_VERSION
            + "; found in file: " + version);
      }
      comparator = WritableUtils.readString(in);
      String logicalStr = WritableUtils.readString(in);
      try {
        logical = new Schema(logicalStr);
      }
      catch (Exception e) {
        ;
        throw new IOException("Schema build failed :" + e.getMessage());
      }
      storage = WritableUtils.readString(in);
      try {
        partition = new Partition(logicalStr, storage);
      }
      catch (Exception e) {
        throw new IOException("Partition constructor failed :" + e.getMessage());
      }
      int numCGs = WritableUtils.readVInt(in);
      physical = new Schema[numCGs];
      TableSchemaParser parser;
      String cgschemastr;
      try {
        for (int nx = 0; nx < numCGs; nx++) {
          cgschemastr = WritableUtils.readString(in);
          parser = new TableSchemaParser(new StringReader(cgschemastr));
          physical[nx] = parser.RecordSchema(null);
        }
      }
      catch (Exception e) {
        throw new IOException("parser.RecordSchema failed :" + e.getMessage());
      }
      sorted = WritableUtils.readVInt(in) == 1 ? true : false;
      in.close();
    }

    private static Path makeSchemaFilePath(Path parent) {
      return new Path(parent, BT_SCHEMA_FILE);
    }
  }

  static public void dumpInfo(String file, PrintStream out, Configuration conf)
      throws IOException {
      dumpInfo(file, out, conf, 0);
  }

  static public void dumpInfo(String file, PrintStream out, Configuration conf, int indent)
      throws IOException {
    IOutils.indent(out, indent);
    out.println("Basic Table : " + file);
    Path path = new Path(file);
    try {
      BasicTable.Reader reader = new BasicTable.Reader(path, conf);
      IOutils.indent(out, indent);
      out.printf("Schema : %s\n", reader.getBTSchemaString());
      IOutils.indent(out, indent);
      out.printf("Storage Information : %s\n", reader.getStorageString());
      IOutils.indent(out, indent);
      out.println("Column Groups within the Basic Table :");
      for (int nx = 0; nx < reader.colGroups.length; nx++) {
        IOutils.indent(out, indent);
        out.printf("\nColumn Group [%d] :", nx);
        ColumnGroup.dumpInfo(reader.colGroups[nx].path, out, conf, indent);
      }
    }
    catch (Exception e) {
      throw new IOException("BasicTable.Reader failed : " + e.getMessage());
    }
    finally {
      // no-op
    }
  }

  public static void main(String[] args) {
    System.out.printf("BasicTable Dumper\n");
    if (args.length == 0) {
      System.out
          .println("Usage: java ... org.apache.hadoop.zebra.io.BasicTable path [path ...]");
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
