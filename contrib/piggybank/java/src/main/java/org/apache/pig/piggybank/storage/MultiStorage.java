/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package org.apache.pig.piggybank.storage;

import java.io.IOException;
import java.io.OutputStream;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.Tuple;
import org.apache.tools.bzip2r.CBZip2OutputStream;

/**
 * The UDF is useful for splitting the output data into a bunch of directories
 * and files dynamically based on user specified key field in the output tuple.
 * 
 * Sample usage: <code>
 * A = LOAD 'mydata' USING PigStorage() as (a, b, c);
 * STORE A INTO '/my/home/output' USING MultiStorage('/my/home/output','0', 'bz2', '\\t');
 * </code> Parameter details:- ========== <b>/my/home/output </b>(Required) :
 * The DFS path where output directories and files will be created. <b> 0
 * </b>(Required) : Index of field whose values should be used to create
 * directories and files( field 'a' in this case). <b>'bz2' </b>(Optional) : The
 * compression type. Default is 'none'. Supported types are:- 'none', 'gz' and
 * 'bz2' <b> '\\t' </b>(Optional) : Output field separator.
 * 
 * Let 'a1', 'a2' be the unique values of field 'a'. Then output may look like
 * this
 * 
 * /my/home/output/a1/a1-0000 /my/home/output/a1/a1-0001
 * /my/home/output/a1/a1-0002 ... /my/home/output/a2/a2-0000
 * /my/home/output/a2/a2-0001 /my/home/output/a2/a2-0002
 * 
 * The prefix '0000*' is the task-id of the mapper/reducer task executing this
 * store. In case user does a GROUP BY on the field followed by MultiStorage(),
 * then its imperative that all tuples for a particular group will go exactly to
 * 1 reducer. So in the above case for e.g. there will be only 1 file each under
 * 'a1' and 'a2' directories.
 */
public class MultiStorage extends Utf8StorageConverter implements StoreFunc {

  // map of all (key-field-values, PigStorage) received by this store
  private Map<String, PigStorage> storeMap;
  private List<OutputStream> outStreamList; // list of all open streams
  private boolean isAbsolute; // Is the user specified output path absolute
  private String partition; // Reduce partition ID executing this store
  private Path outputPath; // User specified output Path
  private Path workOutputPath; // Task specific temporary output path
  private Compression comp; // Compression type of output data.
  private int splitFieldIndex = -1; // Index of the key field
  private String fieldDel; // delimiter of the output record.
  private FileSystem fs; // Output file system

  // filter for removing hidden files in a listing
  public static final PathFilter hiddenPathFilter = new PathFilter() {
    public boolean accept(Path p) {
      String name = p.getName();
      return !name.startsWith("_") && !name.startsWith(".");
    }
  };

  // Compression types supported by this store
  enum Compression {
    none, bz2, bz, gz;
  };

  public MultiStorage(String parentPathStr, String splitFieldIndex) {
    this(parentPathStr, splitFieldIndex, "none");
  }

  public MultiStorage(String parentPathStr, String splitFieldIndex,
      String compression) {
    this(parentPathStr, splitFieldIndex, compression, "\\t");

  }

  /**
   * Constructor
   * 
   * @param parentPathStr
   *          Parent output dir path
   * @param splitFieldIndex
   *          key field index
   * @param compression
   *          'bz2', 'bz', 'gz' or 'none'
   * @param fieldDel
   *          Output record field delimiter.
   */
  public MultiStorage(String parentPathStr, String splitFieldIndex,
      String compression, String fieldDel) {
    this.outputPath = new Path(parentPathStr);
    this.splitFieldIndex = Integer.parseInt(splitFieldIndex);
    this.fieldDel = fieldDel;
    this.storeMap = new HashMap<String, PigStorage>();
    this.outStreamList = new ArrayList<OutputStream>();
    try {
      this.comp = (compression == null) ? Compression.none : Compression
          .valueOf(compression.toLowerCase());
    } catch (IllegalArgumentException e) {
      System.err.println("Exception when converting compression string: "
          + compression + " to enum. No compression will be used");
      this.comp = Compression.none;
    }
  }

  /**
   * Return the work output path suffixed with the parent output dir name.
   * 
   * @param conf
   * @return
   * @throws IOException
   */
  private Path getWorkOutputPath(JobConf conf) throws IOException {
    Path outPath = (conf != null) ? new Path(FileOutputFormat
        .getWorkOutputPath(conf), this.outputPath) : this.outputPath;
    return outPath;
  }

  /**
   * Get the partition number of the reduce task in which it is executing.
   * 
   * @param conf
   * @return
   */
  private String getPartition(JobConf conf) {
    int part = (conf != null) ? conf.getInt("mapred.task.partition", -1) : 0;
    NumberFormat numberFormat = NumberFormat.getInstance();
    numberFormat.setMinimumIntegerDigits(5);
    numberFormat.setGroupingUsed(false);
    return numberFormat.format(part);
  }

  /**
   * hack to get the map/reduce task unique ID in which this is running. Also
   * get the outputPath of the job to be used as base path where field value
   * specific sub-directories will be created.
   * 
   * @throws IOException
   */
  private void initJobSpecificParams() throws IOException {
    this.partition = (this.partition == null) ? getPartition(PigMapReduce.sJobConf)
        : this.partition;
    // workOutputPath = workOutputPath/outputPath. Later we will remove the
    // suffix.
    this.workOutputPath = (this.workOutputPath == null) ? getWorkOutputPath(PigMapReduce.sJobConf)
        : this.workOutputPath;
    if (this.fs == null) {
      this.fs = (PigMapReduce.sJobConf == null) ? FileSystem
          .getLocal(new Configuration()) : FileSystem
          .get(PigMapReduce.sJobConf);
    }
  }

  @Override
  public void bindTo(OutputStream os) throws IOException {
    // Nothing to bind here as we will be writing each tuple into a split
    // based on its schema
  }

  /**
   * Create an appropriate output stream for the fieldValue.
   * 
   * @param fieldValue
   * @return
   * @throws IOException
   */
  private OutputStream createOutputStream(String fieldValue) throws IOException {
    Path path = new Path(fieldValue, fieldValue + '-' + partition);
    Path fieldValueBasedPath = new Path(workOutputPath, path);
    OutputStream os = null;
    switch (comp) {
    case bz:
    case bz2:
      os = fs.create(fieldValueBasedPath.suffix(".bz2"), false);
      os = new CBZip2OutputStream(os);
      break;
    case gz:
      os = fs.create(fieldValueBasedPath.suffix(".gz"), false);
      os = new GZIPOutputStream(os);
      break;
    case none:
      os = fs.create(fieldValueBasedPath, false);
    }
    return os;
  }

  /**
   * Retrieve the pig storage corresponding to the field value.
   * 
   * @param fieldValue
   * @return
   * @throws IOException
   */
  private PigStorage getStore(String fieldValue) throws IOException {
    PigStorage store = storeMap.get(fieldValue);
    if (store == null) {
      store = new PigStorage(fieldDel);
      OutputStream os = createOutputStream(fieldValue);
      store.bindTo(os);
      outStreamList.add(os);
      storeMap.put(fieldValue, store);
    }
    return store;
  }

  @Override
  public void putNext(Tuple tuple) throws IOException {
    initJobSpecificParams();
    if (tuple.size() <= splitFieldIndex) {
      throw new IOException("split field index:" + this.splitFieldIndex
          + " >= tuple size:" + tuple.size());
    }
    Object field = null;
    try {
      field = tuple.get(splitFieldIndex);
    } catch (ExecException exec) {
      throw new IOException(exec);
    }
    PigStorage store = getStore(String.valueOf(field));
    store.putNext(tuple);
  }

  /**
   * Flush the output streams and call pigStorage.finish() for each pigStorage
   * object. Clear the map of pigStorage objects and move the final results to
   * the correct location from the temporary output path since multiquery
   * implementation might ignore our results.
   * 
   * @ throws IOException
   */
  @Override
  public void finish() throws IOException {
    Collection<PigStorage> pigStores = storeMap.values();
    for (PigStorage store : pigStores) {
      store.finish();
    }
    storeMap.clear();
    for (OutputStream os : outStreamList) {
      os.flush();
      os.close();
    }
    outStreamList.clear();
    // move the results here
    if (PigMapReduce.sJobConf != null) {
      Path rem = FileOutputFormat.getWorkOutputPath(PigMapReduce.sJobConf);
      String pathToRemove = rem.toUri().getPath() + (!isAbsolute ? "/" : "");
      moveResults(workOutputPath, pathToRemove);
    }
  }

  /**
   * Moves the files and dir under given path 'p' to the actual path. The API
   * traverses the workOutputPath recursively and renames the files and
   * directories by removing 'rem' from their path names
   * 
   * @param p
   *          The
   * @param rem
   * @throws IOException
   */
  private void moveResults(Path p, String rem) throws IOException {
    for (FileStatus fstat : fs.listStatus(p, hiddenPathFilter)) {
      Path src = fstat.getPath();
      Path dst = new Path(src.toUri().getPath().replace(rem, ""));
      if (fstat.isDir()) {
        fs.mkdirs(dst);
        moveResults(src, rem);
      } else {
        fs.rename(src, dst);
      }
    }
  }

  // @Override
  public Class getStorePreparationClass() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
}
