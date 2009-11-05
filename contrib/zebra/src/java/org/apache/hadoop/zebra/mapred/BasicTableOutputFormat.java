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
package org.apache.hadoop.zebra.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.Partition;
import org.apache.hadoop.zebra.types.SortInfo;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.pig.data.Tuple;
import org.apache.hadoop.zebra.pig.comparator.*;


/**
 * {@link org.apache.hadoop.mapred.OutputFormat} class for creating a
 * BasicTable.
 * 
 * Usage Example:
 * <p>
 * In the main program, add the following code.
 * 
 * <pre>
 * jobConf.setOutputFormat(BasicTableOutputFormat.class);
 * Path outPath = new Path(&quot;path/to/the/BasicTable&quot;);
 * BasicTableOutputFormat.setOutputPath(jobConf, outPath);
 * BasicTableOutputFormat.setSchema(jobConf, &quot;Name, Age, Salary, BonusPct&quot;);
 * </pre>
 * 
 * The above code does the following things:
 * <UL>
 * <LI>Set the output format class to BasicTableOutputFormat.
 * <LI>Set the path to the BasicTable to be created.
 * <LI>Set the schema of the BasicTable to be created. In this case, the
 * to-be-created BasicTable contains three columns with names "Name", "Age",
 * "Salary", "BonusPct".
 * </UL>
 * The user Reducer code (or similarly Mapper code if it is a Map-only job)
 * should look like the following:
 * 
 * <pre>
 * static class MyReduceClass implements Reducer&lt;K, V, BytesWritable, Tuple&gt; {
 *   // keep the tuple object for reuse.
 *   Tuple outRow;
 *   // indices of various fields in the output Tuple.
 *   int idxName, idxAge, idxSalary, idxBonusPct;
 * 
 *   &#064;Override
 *   public void configure(JobConf job) {
 *     Schema outSchema = BasicTableOutputFormat.getSchema(job);
 *     // create a tuple that conforms to the output schema.
 *     outRow = TypesUtils.createTuple(outSchema);
 *     // determine the field indices.
 *     idxName = outSchema.getColumnIndex(&quot;Name&quot;);
 *     idxAge = outSchema.getColumnIndex(&quot;Age&quot;);
 *     idxSalary = outSchema.getColumnIndex(&quot;Salary&quot;);
 *     idxBonusPct = outSchema.getColumnIndex(&quot;BonusPct&quot;);
 *   }
 * 
 *   &#064;Override
 *   public void reduce(K key, Iterator&lt;V&gt; values,
 *       OutputCollector&lt;BytesWritable, Tuple&gt; output, Reporter reporter)
 *       throws IOException {
 *     String name;
 *     int age;
 *     int salary;
 *     double bonusPct;
 *     // ... Determine the value of the individual fields of the row to be inserted.
 *     try {
 *       outTuple.set(idxName, name);
 *       outTuple.set(idxAge, new Integer(age));
 *       outTuple.set(idxSalary, new Integer(salary));
 *       outTuple.set(idxBonusPct, new Double(bonusPct));
 *       output.collect(new BytesWritable(name.getBytes()), outTuple);
 *     }
 *     catch (ExecException e) {
 *       // should never happen
 *     }
 *   }
 * 
 *   &#064;Override
 *   public void close() throws IOException {
 *     // no-op
 *   }
 * 
 * }
 * </pre>
 */
public class BasicTableOutputFormat implements
    OutputFormat<BytesWritable, Tuple> {
  private static final String OUTPUT_PATH = "mapred.lib.table.output.dir";
  private static final String OUTPUT_SCHEMA = "mapred.lib.table.output.schema";
  private static final String OUTPUT_STORAGEHINT =
      "mapred.lib.table.output.storagehint";
  private static final String OUTPUT_SORTCOLUMNS =
      "mapred.lib.table.output.sortcolumns";
  private static final String OUTPUT_COMPARATOR =
      "mapred.lib.table.output.comparator";

  /**
   * Set the output path of the BasicTable in JobConf
   * 
   * @param conf
   *          The JobConf object.
   * @param path
   *          The output path to the table. The path must either not existent,
   *          or must be an empty directory.
   */
  public static void setOutputPath(JobConf conf, Path path) {
    conf.set(OUTPUT_PATH, path.toString());
  }

  /**
   * Get the output path of the BasicTable from JobConf
   * 
   * @param conf
   *          job conf
   * @return The output path.
   */
  public static Path getOutputPath(JobConf conf) {
    String path = conf.get(OUTPUT_PATH);
    return (path == null) ? null : new Path(path);
  }

  /**
   * Set the table schema in JobConf
   * 
   * @param conf
   *          The JobConf object.
   * @param schema
   *          The schema of the BasicTable to be created. For the initial
   *          implementation, the schema string is simply a comma separated list
   *          of column names, such as "Col1, Col2, Col3".
   * 
   * @see Schema#Schema(String)
   */
  public static void setSchema(JobConf conf, String schema)
      throws ParseException {
    conf.set(OUTPUT_SCHEMA, Schema.normalize(schema));
  }

  /**
   * Get the table schema in JobConf.
   * 
   * @param conf
   *          The JobConf object.
   * @return The output schema of the BasicTable. If the schema is not defined
   *         in the conf object at the time of the call, null will be returned.
   */
  public static Schema getSchema(JobConf conf) throws ParseException {
    String schema = conf.get(OUTPUT_SCHEMA);
    if (schema == null) {
      return null;
    }
    schema = schema.replaceAll(";", ",");
    return new Schema(schema);
  }

  private static KeyGenerator makeKeyBuilder(byte[] elems) {
	    ComparatorExpr[] exprs = new ComparatorExpr[elems.length];
	    for (int i = 0; i < elems.length; ++i) {
	      exprs[i] = ExprUtils.primitiveComparator(i, elems[i]);
	    }
	    return new KeyGenerator(ExprUtils.tupleComparator(exprs));
  }

  /**
   * Generates a zebra specific sort key generator which is used to generate BytesWritable key 
   * Sort Key(s) are used to generate this object
   * 
   * @param conf
   *          The JobConf object.
   * @return Object of type zebra.pig.comaprator.KeyGenerator. 
   *         
   */
  public static Object getSortKeyGenerator(JobConf conf) throws IOException, ParseException {

    SortInfo sortInfo = getSortInfo(conf);
    Schema schema     = getSchema(conf);
    String[] sortColNames = sortInfo.getSortColumnNames();

    byte[] types = new byte[sortColNames.length];
    for(int i =0 ; i < sortColNames.length; ++i){
      types[i] = schema.getColumn(sortColNames[i]).getType().pigDataType();
    }
    KeyGenerator builder = makeKeyBuilder(types);
    return builder;

  }


  /**
   * Generates a BytesWritable key for the input key
   * using keygenerate provided. Sort Key(s) are used to generate this object
   *
   * @param builder
   *         Opaque key generator created by getSortKeyGenerator() method
   * @param t
   *         Tuple to create sort key from
   * @return ByteWritable Key 
   *
   */
  public static BytesWritable getSortKey(Object builder, Tuple t) throws Exception {
	  KeyGenerator kg = (KeyGenerator) builder;
	  return kg.generateKey(t);
  }




  /**
   * Set the table storage hint in JobConf, should be called after setSchema is
   * called.
   * 
   * @param conf
   *          The JobConf object.
   * @param storehint
   *          The storage hint of the BasicTable to be created. The format would
   *          be like "[f1, f2.subfld]; [f3, f4]".
   * 
   * @see Schema#Schema(String)
   */
  public static void setStorageHint(JobConf conf, String storehint)
      throws ParseException, IOException {
    String schema = conf.get(OUTPUT_SCHEMA);

    if (schema == null)
      throw new ParseException("Schema has not been set");

    // for sanity check purpose only
    Partition partition = new Partition(schema, storehint, null);

    conf.set(OUTPUT_STORAGEHINT, storehint);
  }

  /**
   * Get the table storage hint in JobConf.
   * 
   * @param conf
   *          The JobConf object.
   * @return The storage hint of the BasicTable. If the storage hint is not
   *         defined in the conf object at the time of the call, an empty string
   *         will be returned.
   */
  public static String getStorageHint(JobConf conf) throws ParseException {
    String storehint = conf.get(OUTPUT_STORAGEHINT);
    return storehint == null ? "" : storehint;
  }

  /**
   * Set the sort info
   *
   * @param conf
   *          The JobConf object.
   *          
   * @param sortColumns
   *          Comma-separated sort column names
   *          
   * @param comparator
   *          comparator class name; null for default
   *
   */
  public static void setSortInfo(JobConf conf, String sortColumns, String comparator) {
    conf.set(OUTPUT_SORTCOLUMNS, sortColumns);
    conf.set(OUTPUT_COMPARATOR, comparator);
  }

  /**
   * Set the sort info
   *
   * @param conf
   *          The JobConf object.
   *          
   * @param sortColumns
   *          Comma-separated sort column names
   */
  public static void setSortInfo(JobConf conf, String sortColumns) {
	  conf.set(OUTPUT_SORTCOLUMNS, sortColumns);
  }
  
  /**
   * Get the SortInfo object 
   *
   * @param conf
   *          The JobConf object.
   * @return SortInfo object; null if the Zebra table is unsorted 
   *
   */
  public static SortInfo getSortInfo(JobConf conf)throws IOException
  {
    String sortColumns = conf.get(OUTPUT_SORTCOLUMNS);
    if (sortColumns == null)
    	return null;
    Schema schema = null;
    try {
      schema = getSchema(conf);
    } catch (ParseException e) {
    	throw new IOException("Schema parsing failure : "+e.getMessage());
    }
    if (schema == null)
    	throw new IOException("Schema not defined");
    String comparator = getComparator(conf);
    return SortInfo.parse(sortColumns, schema, comparator);
  }

  /**
   * Get the  comparator for sort columns
   *
   * @param conf
   *          The JobConf object.
   * @return  comparator String
   *
   */
  private static String getComparator(JobConf conf)
  {
    return conf.get(OUTPUT_COMPARATOR);
  }

  /**
   * Get the output table as specified in JobConf. It is useful for applications
   * to add more meta data after all rows have been added to the table.
   * 
   * @param conf
   *          The JobConf object.
   * @return The output BasicTable.Writer object.
   * @throws IOException
   */
  private static BasicTable.Writer getOutput(JobConf conf) throws IOException {
    String path = conf.get(OUTPUT_PATH);
    if (path == null) {
      throw new IllegalArgumentException("Cannot find output path");
    }
    return new BasicTable.Writer(new Path(path), conf);
  }

  /**
   * Note: we perform the Initialization of the table here. So we expect this to
   * be called before
   * {@link BasicTableOutputFormat#getRecordWriter(FileSystem, JobConf, String, Progressable)}
   * 
   * @see OutputFormat#checkOutputSpecs(FileSystem, JobConf)
   */
  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf conf)
      throws IOException {
    String path = conf.get(OUTPUT_PATH);
    if (path == null) {
      throw new IllegalArgumentException("Cannot find output path");
    }

    String schema = conf.get(OUTPUT_SCHEMA);
    if (schema == null) {
      throw new IllegalArgumentException("Cannot find output schema");
    }
    String storehint, sortColumns, comparator;
    try {
      storehint = getStorageHint(conf);
      sortColumns = (getSortInfo(conf) == null ? null : SortInfo.toSortString(getSortInfo(conf).getSortColumnNames()));
      comparator = getComparator(conf);
    }
    catch (ParseException e) {
      throw new IOException(e);
    }
    BasicTable.Writer writer =
        new BasicTable.Writer(new Path(path), schema, storehint, sortColumns, comparator, conf);

    writer.finish();
  }

  /**
   * @see OutputFormat#getRecordWriter(FileSystem, JobConf, String,
   *      Progressable)
   */
  @Override
  public RecordWriter<BytesWritable, Tuple> getRecordWriter(FileSystem ignored,
      JobConf conf, String name, Progressable progress) throws IOException {
    String path = conf.get(OUTPUT_PATH);
    if (path == null) {
      throw new IllegalArgumentException("Cannot find output path");
    }

    return new TableRecordWriter(path, name, conf, progress);
  }

  /**
   * Close the output BasicTable, No more rows can be added into the table. A
   * BasicTable is not visible for reading until it is "closed".
   * 
   * @param conf
   *          The JobConf object.
   * @throws IOException
   */
  public static void close(JobConf conf) throws IOException {
    BasicTable.Writer table = getOutput(conf);
    table.close();
  }
}

/**
 * Adaptor class for BasicTable RecordWriter.
 */
class TableRecordWriter implements RecordWriter<BytesWritable, Tuple> {
  private final TableInserter inserter;
  private final Progressable progress;

  public TableRecordWriter(String path, String name, JobConf conf,
      Progressable progress) throws IOException {
    BasicTable.Writer writer = new BasicTable.Writer(new Path(path), conf);
    this.inserter = writer.getInserter(name, true);
    this.progress = progress;
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    inserter.close();
    reporter.progress();
  }

  @Override
  public void write(BytesWritable key, Tuple value) throws IOException {
    inserter.insert(key, value);
    progress.progress();
  }
}
