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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.zebra.tfile.TFile;
import org.apache.hadoop.zebra.tfile.Utils;
import org.apache.hadoop.zebra.tfile.MetaBlockAlreadyExists;
import org.apache.hadoop.zebra.tfile.MetaBlockDoesNotExist;
import org.apache.hadoop.zebra.tfile.Utils.Version;
import org.apache.hadoop.zebra.io.ColumnGroup.Reader.CGRangeSplit;
import org.apache.hadoop.zebra.io.ColumnGroup.Reader.CGRowSplit;
import org.apache.hadoop.zebra.io.ColumnGroup.Reader.CGScanner;
import org.apache.hadoop.zebra.types.CGSchema;
import org.apache.hadoop.zebra.mapreduce.BasicTableOutputFormat;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.Partition;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.hadoop.zebra.types.ZebraConf;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.parser.TableSchemaParser;
import org.apache.hadoop.zebra.pig.TableStorer;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.hadoop.zebra.types.SortInfo;
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
  
  static Log LOG = LogFactory.getLog(BasicTable.class);
  
  // name of the BasicTable schema file
  private final static String BT_SCHEMA_FILE = ".btschema";
  // schema version
  private final static Version SCHEMA_VERSION =
      new Version((short) 1, (short) 1);
  // name of the BasicTable meta-data file
  private final static String BT_META_FILE = ".btmeta";

  private final static String DELETED_CG_PREFIX = ".deleted-";
  
  public final static String DELETED_CG_SEPARATOR_PER_TABLE = ",";

  // no public ctor for instantiating a BasicTable object
  private BasicTable() {
    // no-op
  }

  /**
   * Deletes the data for column group specified by cgName.
   * When the readers try to read the fields that were stored in the
   * column group get null since the underlying data is removed.
   * <br> <br>
   * 
   * Effect on the readers that are currently reading from the table while
   * a column group is droped is unspecified. Suggested practice is to 
   * drop column groups when there are no readers or writes for the table.
   * <br> <br>
   * 
   * Column group names are usually specified in the "storage hint" while
   * creating a table. If no name is specified, system assigns a simple name.
   * These names could be obtained through "dumpInfo()" and other methods.
   * <br> <br> 
   *
   * Dropping a column group that has already been removed is a no-op no 
   * exception is thrown.
   * <br> <br> 
   * 
   * Note that this feature is experimental now and subject to changes in the
   * future.
   *
   * @param path path to BasicTable
   * @param conf Configuration determines file system and other parameters.
   * @param cgName name of the column group to drop.
   * @throws IOException IOException could occur for various reasons. E.g.
   *         a user does not have permissions to write to table directory.
   *         
   */
  public static void dropColumnGroup(Path path, Configuration conf,
                                     String cgName) 
                                     throws IOException {
    
    FileSystem fs = FileSystem.get(conf);
    int triedCount = 0;
    int numCGs =  SchemaFile.getNumCGs(path, conf);
    SchemaFile schemaFile = null;
    
    /* Retry up to numCGs times accounting for other CG deleting threads or processes.*/
    while (triedCount ++ < numCGs) {
      try {
        schemaFile = new SchemaFile(path, null, conf);
        break;
      } catch (FileNotFoundException e) {
        LOG.info("Try " + triedCount + " times : " + e.getMessage());
      } catch (Exception e) {
        throw new IOException ("Cannot construct SchemaFile : " + e.getMessage());
      }
    }
    
    if (schemaFile == null) {
      throw new IOException ("Cannot construct SchemaFile");
    }
    
    int cgIdx = schemaFile.getCGByName(cgName);
    if (cgIdx < 0) {
      throw new IOException(path + 
             " : Could not find a column group with the name '" + cgName + "'");
    }
    
    Path cgPath = new Path(path, schemaFile.getName(cgIdx));
        
    //Clean up any previous unfinished attempts to drop column groups?    
    if (schemaFile.isCGDeleted(cgIdx)) {
      // Clean up unfinished delete if it exists. so that clean up can 
      // complete if the previous deletion was interrupted for some reason.
      if (fs.exists(cgPath)) {
        LOG.info(path + " : " + 
                 " clearing unfinished deletion of column group " +
                 cgName + ".");
        fs.delete(cgPath, true);
      }
      LOG.info(path + " : column group " + cgName + " is already deleted.");
      return;
    }
    
    // try to delete the column group:
    
    // first check if the user has enough permissions to list the directory
    fs.listStatus(cgPath);   
    
    //verify if the user has enough permissions by trying to create
    //a temporary file in cg.
    OutputStream out = fs.create(
              new Path(cgPath, ".tmp" + DELETED_CG_PREFIX + cgName), true);
    out.close();
    
    //First try to create a file indicating a column group is deleted.
    try {
      Path deletedCGPath = new Path(path, DELETED_CG_PREFIX + cgName);
      // create without overriding.
      out = fs.create(deletedCGPath, false);
      // should we write anything?
      out.close();
    } catch (IOException e) {
      // one remote possibility is that another user 
      // already deleted CG. 
      SchemaFile tempSchema = new SchemaFile(path, null, conf);
      if (tempSchema.isCGDeleted(cgIdx)) {
        LOG.info(path + " : " + cgName + 
                 " is deleted by someone else. That is ok.");
        return;
      }
      // otherwise, it is some other error.
      throw e;
    }
    
    // At this stage, the CG is marked deleted. Now just try to
    // delete the actual directory:
    if (!fs.delete(cgPath, true)) {
      String msg = path + " : Could not detete column group " +
                   cgName + ". It is marked deleted.";
      LOG.warn(msg);
      throw new IOException(msg);
    }
    
    LOG.info("Dropped " + cgName + " from " + path);
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
    private int firstValidCG = -1; /// First column group that exists.
    private int rowSplitCGIndex = -1;
    Partition partition;
    ColumnGroup.Reader[] colGroups;
    Tuple[] cgTuples;

    private synchronized void checkInferredMapping() throws ParseException, IOException {
      if (!inferredMapping) {
        for (int i = 0; i < colGroups.length; ++i) {
          if (colGroups[i] != null) {
            colGroups[i].setProjection(partition.getProjection(i));
          } 
          if (partition.isCGNeeded(i)) {
            if (isCGDeleted(i)) {
              // this is a deleted column group. Warn about it.
              LOG.warn("Trying to read from deleted column group " + 
                       schemaFile.getName(i) + 
                       ". NULL is returned for corresponding columns. " +
                       "Table at " + path);
            } else {
              cgTuples[i] = TypesUtils.createTuple(colGroups[i].getSchema());
            }
          }
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
     * Returns true if a column group is deleted.
     */
    private boolean isCGDeleted(int nx) {
      return colGroups[nx] == null;
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
      this(path, null, conf);
    }
    public Reader(Path path, String[] deletedCGs, Configuration conf) throws IOException {
      try {
        boolean mapper = (deletedCGs != null);
        this.path = path;
        schemaFile = new SchemaFile(path, deletedCGs, conf);
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
        String comparator = schemaFile.getComparator();
        partition = new Partition(schema, projection, storage, comparator);
        for (int nx = 0; nx < numCGs; nx++) {
          if (!schemaFile.isCGDeleted(nx)) {
            colGroups[nx] =
              new ColumnGroup.Reader(new Path(path, partition.getCGSchema(nx).getName()),
                                     conf, mapper);
            if (firstValidCG < 0) {
              firstValidCG = nx;
            }
          }
          if (colGroups[nx] != null && partition.isCGNeeded(nx))
            cgTuples[nx] = TypesUtils.createTuple(colGroups[nx].getSchema());
          else
            cgTuples[nx] = null;
        }
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
     * @return the list of sorted columns
     */
    public SortInfo getSortInfo()
    {
      return schemaFile.getSortInfo();
    }
    
    /**
     * @return the name of i-th column group 
     */
    public String getName(int i) {
      return schemaFile.getName(i);
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
                .getStorageString(), schemaFile.getComparator());
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
                .getStorageString(), schemaFile.getComparator());
      }
      inferredMapping = false;
    }

    /**
     * Get the status of the BasicTable.
     */
    public BasicTableStatus getStatus() throws IOException {
      if (status == null)
        buildStatus();
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
      if (firstValidCG >= 0)
      {
        for (int nx = 0; nx < colGroups.length; nx++) {
          if (partition.isCGNeeded(nx) && !isCGDeleted(nx)) {
            bd.add(colGroups[nx].getBlockDistribution(split == null ? null : split.getCGRangeSplit()));
          }
        }
      }
      return bd;
    }


    /**
     * Given a row-based split, calculate how the file data that fall into the split
     * are distributed among hosts.
     * 
     * @param split The row-based split. <i>Cannot</i> be null.
     * @return An object that conveys how blocks fall into the split are
     *         distributed across hosts.
     */
    public BlockDistribution getBlockDistribution(RowSplit split)
        throws IOException {
      BlockDistribution bd = new BlockDistribution();
      int cgIdx = split.getCGIndex();      
      bd.add(colGroups[cgIdx].getBlockDistribution(split.getCGRowSplit()));
      
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
     * @param nTables
     *          Number of tables in union
     * @return KeyDistribution object.
     * @throws IOException
     */
    public KeyDistribution getKeyDistribution(int n, int nTables, BlockDistribution lastBd) throws IOException {
      if (firstValidCG >= 0)
      {
        // pick the largest CG as in the row split case
        return colGroups[getRowSplitCGIndex()].getKeyDistribution(n, nTables, lastBd);
      }
      return null;
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
     * Get a scanner that reads a consecutive number of rows as defined in the
     * {@link RowSplit} object.
     * 
     * @param closeReader
     *          close the underlying Reader object when we close the scanner.
     *          Should be set to true if we have only one scanner on top of the
     *          reader, so that we should release resources after the scanner is
     *          closed.
     * @param rowSplit split based on row numbers.
     * 
     * @return A scanner object.
     * @throws IOException
     */
    public synchronized TableScanner getScanner(boolean closeReader,
                                                RowSplit rowSplit) 
      throws IOException, ParseException, ParseException {
      checkInferredMapping();
      return new BTScanner(rowSplit, closeReader, partition);
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
     * @deletedCGs
     *          The deleted column groups from front end; null if unavailable from front end
     * @param conf
     * @return The logical Schema of the table (all columns).
     * @throws IOException
     */
    public static Schema getSchema(Path path, Configuration conf)
        throws IOException {
      // fake an empty deleted cg list as getSchema does not care about deleted cgs
      SchemaFile schF = new SchemaFile(path, new String[0], conf);
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
     * Get the path filter used by the table.
     */
    public PathFilter getPathFilter(Configuration conf) {
      ColumnGroup.CGPathFilter filter = new ColumnGroup.CGPathFilter();
      ColumnGroup.CGPathFilter.setConf(conf);
      return filter;
    }

    /**
     * Split the table into at most n parts.
     * 
     * @param n Maximum number of parts in the output list.
     * @return A list of RangeSplit objects, each of which can be used to
     *         construct TableScanner later.
     */
    public List<RangeSplit> rangeSplit(int n) throws IOException {
      // use the first non-deleted column group to do split, other column groups will be split exactly the same way.
      List<RangeSplit> ret;
      if (firstValidCG >= 0) {
        List<CGRangeSplit> cgSplits = colGroups[firstValidCG].rangeSplit(n);
        int numSlices = cgSplits.size();
        ret = new ArrayList<RangeSplit>(numSlices);
        for (int slice = 0; slice < numSlices; slice++) {
          CGRangeSplit oneSliceSplit = cgSplits.get(slice);
          ret.add(new BasicTable.Reader.RangeSplit(oneSliceSplit));
        }

        return ret;
      } else { // all column groups are dropped.
        ret = new ArrayList<RangeSplit>(1);
        // add a dummy split
        ret.add(new BasicTable.Reader.RangeSplit(new CGRangeSplit(0, 0)));
        return ret;
      }
    }

    /**
     * We already use FileInputFormat to create byte offset-based input splits.
     * Their information is encoded in starts, lengths and paths. This method is 
     * to wrap this information to form RowSplit objects at basic table level.
     * 
     * @param starts array of starting byte of fileSplits.
     * @param lengths array of length of fileSplits.
     * @param paths array of path of fileSplits.
     * @param splitCGIndex index of column group that is used to create fileSplits.
     * @return A list of RowSplit objects, each of which can be used to
     *         construct a TableScanner later. 
     *         
     */
    public List<RowSplit> rowSplit(long[] starts, long[] lengths, Path[] paths,
        int splitCGIndex, int[] batchSizes, int numBatches) throws IOException {
      List<RowSplit> ret;      
      List<CGRowSplit> cgSplits = colGroups[splitCGIndex].rowSplit(starts, lengths, paths, batchSizes, numBatches);
      int numSlices = cgSplits.size();
      ret = new ArrayList<RowSplit>(numSlices);
      for (int slice = 0; slice < numSlices; slice++) {
        CGRowSplit cgRowSplit = cgSplits.get(slice);
        ret.add(new BasicTable.Reader.RowSplit(splitCGIndex, cgRowSplit));
      }
        
      return ret;
    }
    
    /**
     * Rearrange the files according to the column group index ordering
     * 
     * @param filestatus array of FileStatus to be rearraged on 
     */
    public void rearrangeFileIndices(FileStatus[] fileStatus) throws IOException
    {
      colGroups[getRowSplitCGIndex()].rearrangeFileIndices(fileStatus);
    }

    /** 
     * Get index of the column group that will be used for row-based split. 
     * 
     */
    public int getRowSplitCGIndex() throws IOException {
      // Try to find the largest non-deleted and used column group by projection;
      // Try to find the largest non-deleted and used column group by projection;
      if (rowSplitCGIndex == -1)
      {
        int largestCGIndex = -1;
        long largestCGSize = -1;
        for (int i=0; i<colGroups.length; i++) {
          if (!partition.isCGNeeded(i) || isCGDeleted(i)) {
            continue;
          }
          ColumnGroup.Reader reader = colGroups[i];
          BasicTableStatus btStatus = reader.getStatus();
          long size = btStatus.getSize();
          if (size > largestCGSize) {
            largestCGIndex = i;
            largestCGSize = size;
          }
        }
       
        /* We do have a largest non-deleted and used column group,
        and we use it to do split. */
        if (largestCGIndex >= 0) { 
          rowSplitCGIndex = largestCGIndex;
        } else if (firstValidCG >= 0) { /* If all projection columns are either deleted or non-existing,
                                        then we use the first non-deleted column group to do split if it exists. */
          rowSplitCGIndex = firstValidCG;
        } 
      } 
      return rowSplitCGIndex;
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
            if (colGroups[i] != null) {
              colGroups[i].close();
            }
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
    
    public String getDeletedCGs() {
      return schemaFile.getDeletedCGs();
    }

    public static String getDeletedCGs(Path path, Configuration conf)
    throws IOException {
    	SchemaFile schF = new SchemaFile(path, new String[0], conf);
    	return schF.getDeletedCGs();
    }

    private void buildStatus() throws IOException {
      status = new BasicTableStatus();
      if (firstValidCG >= 0) {
        status.beginKey = colGroups[firstValidCG].getStatus().getBeginKey();
        status.endKey = colGroups[firstValidCG].getStatus().getEndKey();
        status.rows = colGroups[firstValidCG].getStatus().getRows();
      } else {
        status.beginKey = new BytesWritable(new byte[0]);
        status.endKey = status.beginKey;
        status.rows = 0;
      }
      status.size = 0;
      for (int nx = 0; nx < colGroups.length; nx++) {
        if (colGroups[nx] != null) {
          status.size += colGroups[nx].getStatus().getSize();
        }
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
      //CGRangeSplit[] slice;
      CGRangeSplit slice;

      RangeSplit(CGRangeSplit split) {
        slice = split;
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
        for (int nx = 0; nx < 1; nx++) {
          CGRangeSplit cgrs = new CGRangeSplit();
          cgrs.readFields(in);
          slice = cgrs;
        }
      }

      /**
       * @see Writable#write(DataOutput)
       */
      @Override
      public void write(DataOutput out) throws IOException {
        //Utils.writeVInt(out, slice.length);
        //for (CGRangeSplit split : slice) {
        //  split.write(out);
        //}
        slice.write(out);
      }

      //CGRangeSplit get(int index) {
       // return slice[index];
      //}
      
      CGRangeSplit getCGRangeSplit() {
        return slice;
      }
    }

    /**
     * A row-based split on the zebra table;
     */
    public static class RowSplit implements Writable {
      int cgIndex;  // column group index where split lies on;
      CGRowSplit slice; 

      RowSplit(int cgidx, CGRowSplit split) {
        this.cgIndex = cgidx;
        this.slice = split;
      }

      /**
       * Default constructor.
       */
      public RowSplit() {
        // no-op
      }
      
      @Override
      public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{cgIndex = " + cgIndex + "}\n");
        sb.append(slice.toString());
        
        return sb.toString();
      }

      /**
       * @see Writable#readFields(DataInput)
       */
      @Override
      public void readFields(DataInput in) throws IOException {
        this.cgIndex = Utils.readVInt(in);
        CGRowSplit cgrs = new CGRowSplit();
        cgrs.readFields(in);
        this.slice = cgrs;
      }

      /**
       * @see Writable#write(DataOutput)
       */
      @Override
      public void write(DataOutput out) throws IOException {
        Utils.writeVInt(out, cgIndex);
        slice.write(out);
      }
      
      int getCGIndex() {
        return cgIndex;
      }

      CGRowSplit getCGRowSplit() {
        return slice;
      }
    }
    
    
    /**
     * BasicTable scanner class
     */
    private class BTScanner implements TableScanner {
      private Projection schema;
      private CGScanner[] cgScanners;
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
        init(null, null, beginKey, endKey, closeReader, partition);
      }
      
      public BTScanner(RangeSplit split, Partition partition,
        boolean closeReader) throws IOException {
        init(null, split, null, null, closeReader, partition);
      }
      
      public BTScanner(RowSplit rowSplit,  boolean closeReader, 
                       Partition partition) throws IOException {
        init(rowSplit, null, null, null, closeReader, partition);
      }      

      /**
       * Creates new CGRowSplit. If the startRow in rowSplit is not set 
       * (i.e. < 0), it sets the startRow and numRows based on 'startByte' 
       * and 'numBytes' from given rowSplit.
       */
      private CGRowSplit makeCGRowSplit(RowSplit rowSplit) throws IOException {
        CGRowSplit inputCGSplit = rowSplit.getCGRowSplit(); 

        int cgIdx = rowSplit.getCGIndex();
        
        CGRowSplit cgSplit = new CGRowSplit();

        // Find the row range :
        if (isCGDeleted(cgIdx)) {
          throw new IOException("CG " + cgIdx + " is deleted.");
        }
        //fill the row numbers.
        colGroups[cgIdx].fillRowSplit(cgSplit, inputCGSplit);
        return cgSplit;
      }
    
      // Helper function for initialization.
      private CGScanner createCGScanner(int cgIndex, CGRowSplit cgRowSplit, 
                                           RangeSplit rangeSplit,
                                           BytesWritable beginKey, 
                                           BytesWritable endKey) 
                      throws IOException, ParseException, 
                             ParseException {        
        if (cgRowSplit != null) {
          return colGroups[cgIndex].getScanner(false, cgRowSplit);
        }      
        if (beginKey != null || endKey != null) {
          return colGroups[cgIndex].getScanner(beginKey, endKey, false);
        }
        return colGroups[cgIndex].getScanner
                ((rangeSplit == null ? null : rangeSplit.getCGRangeSplit()), 
                 false);
      }
      
      /**
       * If rowRange is not null, scanners will be created based on the 
       * row range. <br>
       * If RangeSplit is not null, scaller will be based on the range, <br>
       * otherwise, these are based on keys.
       */
      private void init(RowSplit rowSplit, RangeSplit rangeSplit,
                   BytesWritable beginKey, BytesWritable endKey, 
                   boolean closeReader, Partition partition) throws IOException {
        this.partition = partition;
        boolean anyScanner = false;
        
        CGRowSplit cgRowSplit = null;
        if (rowSplit != null) {
          cgRowSplit = makeCGRowSplit(rowSplit);
        }

        try {
          schema = partition.getProjection();
          cgScanners = new CGScanner[colGroups.length];
          for (int i = 0; i < colGroups.length; ++i) {
            if (!isCGDeleted(i) && partition.isCGNeeded(i)) 
            {
              anyScanner = true;
              cgScanners[i] = createCGScanner(i, cgRowSplit, rangeSplit,
                                              beginKey, endKey);                                                             
            } else
              cgScanners[i] = null;
          }
          if (!anyScanner && firstValidCG >= 0) {
            // if no CG is needed explicitly by projection but the "countRow" still needs to access some column group
            cgScanners[firstValidCG] = createCGScanner(firstValidCG, cgRowSplit, 
                                                       rangeSplit,
                                                       beginKey, endKey);            
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

      @Override
      public boolean advance() throws IOException {
        boolean first = false, cur, firstAdvance = true;
        for (int nx = 0; nx < cgScanners.length; nx++) {
          if (cgScanners[nx] != null)
          {
            cur = cgScanners[nx].advanceCG();
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
          int index = random.nextInt(cgScanners.length);
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
        int i;
        for (i = 0; i < cgScanners.length; i++)
        {
          if (cgScanners[i] != null)
          {
            cgScanners[i].getCGKey(key);
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
          int index = random.nextInt(cgScanners.length);
          if (cgScanners[index] != null)
          {
            BytesWritable key2 = new BytesWritable();
            cgScanners[index].getCGKey(key2);
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
              cgScanners[i].getCGValue(cgTuples[i]);
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
        boolean first = false, cur, firstset = false;
        for (int nx = 0; nx < cgScanners.length; nx++) {
          if (cgScanners[nx] == null)
            continue;
          cur = cgScanners[nx].seekTo(key);
          if (firstset) {
            if (cur != first) {
              throw new IOException(
                  "seekTo() failed: Column Groups are not evenly positioned.");
            }
          }
          else {
            first = cur;
            firstset = true;
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
    boolean sorted;
    private boolean finished;
    Tuple[] cgTuples;
    private Path actualOutputPath;
    private Configuration writerConf;



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
     * @param sortColumns
     *          String of comma-separated sorted columns: null for unsorted tables
     * @param comparator
     *          Name of the comparator used in sorted tables
     * @param conf
     *          Optional Configuration objects.
     * 
     * @throws IOException
     * @see Schema
     */
    public Writer(Path path, String btSchemaString, String btStorageString, String sortColumns,
        String comparator, Configuration conf) throws IOException {
      try {
      	actualOutputPath = path;
    	  writerConf = conf;    	  
        schemaFile =
            new SchemaFile(path, btSchemaString, btStorageString, sortColumns,
                comparator, conf);
        partition = schemaFile.getPartition();
        int numCGs = schemaFile.getNumOfPhysicalSchemas();
        colGroups = new ColumnGroup.Writer[numCGs];
        cgTuples = new Tuple[numCGs];
        sorted = schemaFile.isSorted();
        for (int nx = 0; nx < numCGs; nx++) {
          colGroups[nx] =
              new ColumnGroup.Writer( 
                 new Path(path, schemaFile.getName(nx)),
            		 schemaFile.getPhysicalSchema(nx), 
            		 sorted, 
                 comparator,
            		 schemaFile.getName(nx),
            		 schemaFile.getSerializer(nx), 
            		 schemaFile.getCompressor(nx), 
            		 schemaFile.getOwner(nx), 
            		 schemaFile.getGroup(nx),
            		 schemaFile.getPerm(nx), 
            		 false, 
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
     * a wrapper to support backward compatible constructor
     */
    public Writer(Path path, String btSchemaString, String btStorageString,
        Configuration conf) throws IOException {
      this(path, btSchemaString, btStorageString, null, null, conf);
    }

    /**
     * Reopen an already created BasicTable for writing. Exception will be
     * thrown if the table is already closed, or is in the process of being
     * closed.
     */
    public Writer(Path path, Configuration conf) throws IOException {
      try {
      	actualOutputPath = path;
    	  writerConf = conf;
    	  
    	  if (ZebraConf.getOutputSchema(conf) != null) { 
    	    schemaFile = new SchemaFile(conf);  // Read out schemaFile from conf, instead of from hdfs;
    	  } else { // This is only for io test cases and it cannot happen for m/r and pig cases; 
    	    schemaFile = new SchemaFile(path, new String[0], conf); // fake an empty deleted cg list as no cg should have been deleted now
    	  }
        int numCGs = schemaFile.getNumOfPhysicalSchemas();
        partition = schemaFile.getPartition();
        sorted = schemaFile.isSorted();
        colGroups = new ColumnGroup.Writer[numCGs];
        cgTuples = new Tuple[numCGs];
        Path tmpWorkPath = new Path(path, "_temporary");	
        for (int nx = 0; nx < numCGs; nx++) {
          CGSchema cgschema = new CGSchema(schemaFile.getPhysicalSchema(nx), sorted, 
              schemaFile.getComparator(), schemaFile.getName(nx), schemaFile.getSerializer(nx), schemaFile.getCompressor(nx),
              schemaFile.getOwner(nx), schemaFile.getGroup(nx), schemaFile.getPerm(nx));
          
          colGroups[nx] =
            new ColumnGroup.Writer(
            		new Path(path, partition.getCGSchema(nx).getName()),
            		new Path(tmpWorkPath, partition.getCGSchema(nx).getName()),
                  cgschema,conf);
          
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
      if (finished) return;
      finished = true;
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
      cleanupTempDir();	
      if (closed) return;
      closed = true;
      if (!finished)
        finish();
      try {
        ColumnGroup.CGIndex firstCGIndex = null, cgIndex;
        int first = -1;
        for (int nx = 0; nx < colGroups.length; nx++) {
          if (colGroups[nx] != null) {
            colGroups[nx].close();
            if (first == -1)
            {
              first = nx;
              firstCGIndex = colGroups[nx].index;
            } else {
              cgIndex = colGroups[nx].index;
              if (cgIndex.size() != firstCGIndex.size())
                throw new IOException("Column Group "+colGroups[nx].path.getName()+
                    " has different number of files than in column group " + colGroups[first].path.getName());
              int size = firstCGIndex.size();
              for (int i = 0; i < size; i++)
              {
                if (!cgIndex.get(i).name.equals(firstCGIndex.get(i).name))
                  throw new IOException("File["+i+"] in Column Group "+colGroups[nx].path.getName()+
                      " has a different name: "+cgIndex.get(i).name+" than " + 
                      firstCGIndex.get(i).name + " in column group " + colGroups[first].path.getName());
                if (cgIndex.get(i).rows != firstCGIndex.get(i).rows)
                  throw new IOException("File "+cgIndex.get(i).name+"Column Group "+colGroups[nx].path.getName()+
                      " has a different number of rows, " + cgIndex.get(i).rows + ", than " +
                      firstCGIndex.get(i).rows + " in column group " + colGroups[first].path.getName());
              }
            }
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
     * Removes the temporary directory underneath
     * $path/_temporary used to create intermediate data
     * during recrd writing
     */
    
    private void cleanupTempDir() throws IOException {
    	FileSystem fileSys = actualOutputPath.getFileSystem(writerConf);
        Path pathToRemove = new Path(actualOutputPath, "_temporary");
        if (fileSys.exists(pathToRemove)) {
            if(!fileSys.delete(pathToRemove, true)) {
              LOG.error("Failed to delete the temporary output" + 
                      " directory: " + pathToRemove.toString());            
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
     * @return sortness
     */
    public boolean isSorted() {
    	return sorted;
    }

    /**
     * Get the list of sorted columns.
     * @return the list of sorted columns
     */
    public SortInfo getSortInfo()
    {
      return schemaFile.getSortInfo();
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
      return this.getInserter(name, finishWriter, true);
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
     * @param checktype 
     *          whether or not do type check.
     * 
     * @return A inserter object.
     * @throws IOException
     */
    public TableInserter getInserter(String name, boolean finishWriter, boolean checkType)
        throws IOException {
      if (closed) {
        throw new IOException("BasicTable closed");
      }
      return new BTInserter(name, finishWriter, partition, checkType);
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
        this(name, finishWriter, partition, true);
      }
      
      BTInserter(String name, boolean finishWriter, Partition partition, boolean checkType)
      throws IOException {
        try {
          cgInserters = new ColumnGroup.Writer.CGInserter[colGroups.length];
          for (int nx = 0; nx < colGroups.length; nx++) {
            cgInserters[nx] = colGroups[nx].getInserter(name, false, checkType);
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
              BasicTable.Writer.this.finish();
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
    SortInfo sortInfo = null;
    String storage;
    CGSchema[] cgschemas;
    
    // Array indicating if a physical schema is already dropped
    // It is probably better to create "CGProperties" class and
    // store multiple properties like name there.
    boolean[] cgDeletedFlags;
   
    // ctor for reading
    public SchemaFile(Path path, String[] deletedCGs, Configuration conf) throws IOException {
      readSchemaFile(path, deletedCGs, conf);
    }
    
    // ctor for reading from a job configuration object; we do not need a table path; 
    // all information is held in the job configuration object.
    public SchemaFile(Configuration conf) throws IOException {
      String logicalStr = ZebraConf.getOutputSchema(conf);
      storage = ZebraConf.getOutputStorageHint(conf);
      String sortColumns = ZebraConf.getOutputSortColumns(conf) != null ? ZebraConf.getOutputSortColumns(conf) : "";
      comparator = ZebraConf.getOutputComparator(conf) != null ? ZebraConf.getOutputComparator(conf) : "";      
      
      version = SCHEMA_VERSION;
            
      try {
        logical = new Schema(logicalStr);
      } catch (Exception e) {
        throw new IOException("Schema build failed :" + e.getMessage());
      }
      
      try {
        partition = new Partition(logicalStr, storage, comparator, sortColumns);
       
      } catch (Exception e) {
        throw new IOException("Partition constructor failed :" + e.getMessage());
      }
 
      cgschemas = partition.getCGSchemas();
      physical = new Schema[cgschemas.length];
      //cgDeletedFlags = new boolean[physical.length];
      
      for (int nx = 0; nx < cgschemas.length; nx++) {
        physical[nx] = cgschemas[nx].getSchema();
      }
      
      this.sortInfo = partition.getSortInfo();
      this.sorted = partition.isSorted();
      this.comparator = (this.sortInfo == null ? null : this.sortInfo.getComparator());
      if (this.comparator == null)
        this.comparator = "";
           
      String[] sortColumnStr = sortColumns.split(",");
      if (sortColumnStr.length > 0) {
        sortInfo = SortInfo.parse(SortInfo.toSortString(sortColumnStr), logical, comparator);
      }
    }

    public Schema[] getPhysicalSchema() {
      return physical;
    }

    // ctor for writing
    public SchemaFile(Path path, String btSchemaStr, String btStorageStr, String sortColumns,
        String btComparator, Configuration conf)
        throws IOException {
      storage = btStorageStr;
      try {
        partition = new Partition(btSchemaStr, btStorageStr, btComparator, sortColumns);
      }
      catch (Exception e) {
        throw new IOException("Partition constructor failed :" + e.getMessage());
      }
      this.sortInfo = partition.getSortInfo();
      this.sorted = partition.isSorted();
      this.comparator = (this.sortInfo == null ? null : this.sortInfo.getComparator());
      if (this.comparator == null)
        this.comparator = "";
      logical = partition.getSchema();
      cgschemas = partition.getCGSchemas();
      physical = new Schema[cgschemas.length];
      for (int nx = 0; nx < cgschemas.length; nx++) {
        physical[nx] = cgschemas[nx].getSchema();
      }
      cgDeletedFlags = new boolean[physical.length];

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

    public SortInfo getSortInfo() {
      return sortInfo;
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
    
    public String getName(int nx) {
      return cgschemas[nx].getName();
    }
    
    public String getSerializer(int nx) {
      return cgschemas[nx].getSerializer();
    }

    public String getCompressor(int nx) {
      return cgschemas[nx].getCompressor();
    }

    /**
     * Returns the index for CG with the given name. -1 indicates that there is
     * no CG with the name.
     */
    int getCGByName(String cgName) {
      for(int i=0; i<physical.length; i++) {
        if (cgName.equals(getName(i))) {
          return i;
        }
      }
      return -1;
    }
    
    /** Returns if the CG at the given index is delete */
    boolean isCGDeleted(int idx) {
      return cgDeletedFlags[idx];
    }
    
    public String getOwner(int nx) {
        return cgschemas[nx].getOwner();
      }

    public String getGroup(int nx) {
        return cgschemas[nx].getGroup();
    }

    public short getPerm(int nx) {
        return cgschemas[nx].getPerm();
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
      WritableUtils.writeVInt(outSchema, sortInfo == null ? 0 : sortInfo.size());
      if (sortInfo != null && sortInfo.size() > 0)
      {
        String[] sortedCols = sortInfo.getSortColumnNames();
        for (int i = 0; i < sortInfo.size(); i++)
        {
          WritableUtils.writeString(outSchema, sortedCols[i]);
        }
      }
      outSchema.close();
    }

    private void readSchemaFile(Path path, String[] deletedCGs, Configuration conf)
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
        partition = new Partition(logicalStr, storage, comparator);
      }
      catch (Exception e) {
        throw new IOException("Partition constructor failed :" + e.getMessage());
      }
      cgschemas = partition.getCGSchemas();
      int numCGs = WritableUtils.readVInt(in);
      physical = new Schema[numCGs];
      cgDeletedFlags = new boolean[physical.length];
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
      if (deletedCGs == null)
        setCGDeletedFlags(path, conf);
      else {
        for (String deletedCG : deletedCGs)
        {
          for (int i = 0; i < cgschemas.length; i++)
          {
            if (cgschemas[i].getName().equals(deletedCG))
              cgDeletedFlags[i] = true;
          }
        }
      }
      
      if (version.compareTo(new Version((short)1, (short)0)) > 0)
      {
        int numSortColumns = WritableUtils.readVInt(in);
        if (numSortColumns > 0)
        {
          String[] sortColumnStr = new String[numSortColumns];
          for (int i = 0; i < numSortColumns; i++)
          {
            sortColumnStr[i] = WritableUtils.readString(in);
          }
          sortInfo = SortInfo.parse(SortInfo.toSortString(sortColumnStr), logical, comparator);
        }
      }
      in.close();
    }

    private static int getNumCGs(Path path, Configuration conf) throws IOException {
      Path pathSchema = makeSchemaFilePath(path);
      if (!path.getFileSystem(conf).exists(pathSchema)) {
        throw new IOException("BT Schema file doesn't exist: " + pathSchema);
      }
      // read schema file
      FSDataInputStream in = path.getFileSystem(conf).open(pathSchema);
      Version version = new Version(in);
      // verify compatibility against SCHEMA_VERSION
      if (!version.compatibleWith(SCHEMA_VERSION)) {
        new IOException("Incompatible versions, expecting: " + SCHEMA_VERSION
            + "; found in file: " + version);
      }
      
      // read comparator
      WritableUtils.readString(in);
      // read logicalStr
      WritableUtils.readString(in);
      // read storage
      WritableUtils.readString(in);
      int numCGs = WritableUtils.readVInt(in);
      in.close();

      return numCGs;
    }

    private static Path makeSchemaFilePath(Path parent) {
      return new Path(parent, BT_SCHEMA_FILE);
    }
    
    /**
     * Sets cgDeletedFlags array by checking presense of
     * ".deleted-CGNAME" directory in the table top level
     * directory. 
     */
    void setCGDeletedFlags(Path path, Configuration conf) throws IOException {
      
      Set<String> deletedCGs = new HashSet<String>(); 
      
      for (FileStatus file : path.getFileSystem(conf).listStatus(path)) {
        if (!file.isDir()) {
          String fname =  file.getPath().getName();
          if (fname.startsWith(DELETED_CG_PREFIX)) {
            deletedCGs.add(fname.substring(DELETED_CG_PREFIX.length()));
          }
        }
      }
      
      for(int i=0; i<physical.length; i++) {
        cgDeletedFlags[i] = deletedCGs.contains(getName(i));
      }
    }
    
    String getDeletedCGs() {
      StringBuilder sb = new StringBuilder();
      // comma separated
      boolean first = true;
      for (int i = 0; i < physical.length; i++) {
        if (cgDeletedFlags[i])
        {
          if (first)
            first = false;
          else {
            sb.append(DELETED_CG_SEPARATOR_PER_TABLE);
          }
          sb.append(getName(i));
        }
      }
      return sb.toString();
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
      String schemaStr = reader.getBTSchemaString();
      String storageStr = reader.getStorageString();
      IOutils.indent(out, indent);
      out.printf("Schema : %s\n", schemaStr);
      IOutils.indent(out, indent);
      out.printf("Storage Information : %s\n", storageStr);
      SortInfo sortInfo = reader.getSortInfo();
      if (sortInfo != null && sortInfo.size() > 0)
      {
        IOutils.indent(out, indent);
        String[] sortedCols = sortInfo.getSortColumnNames();
        out.println("Sorted Columns :");
        for (int nx = 0; nx < sortedCols.length; nx++) {
          if (nx > 0)
            out.printf(" , ");
          out.printf("%s", sortedCols[nx]);
        }
        out.printf("\n");
      }
      IOutils.indent(out, indent);
      out.println("Column Groups within the Basic Table :");
      for (int nx = 0; nx < reader.colGroups.length; nx++) {
        IOutils.indent(out, indent);
        out.printf("\nColumn Group [%d] :", nx);
        if (reader.colGroups[nx] != null) {
          ColumnGroup.dumpInfo(reader.colGroups[nx].path, out, conf, indent);
        } else {
          // print basic info for deleted column groups.
          out.printf("\nColum Group : DELETED");
          out.printf("\nName : %s", reader.schemaFile.getName(nx));
          out.printf("\nSchema : %s\n", 
                     reader.schemaFile.cgschemas[nx].getSchema().toString());
        }
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
