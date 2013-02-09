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

import junit.framework.Assert;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
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
import org.apache.hadoop.zebra.tfile.TFile;
import org.apache.pig.data.Tuple;
import org.apache.hadoop.zebra.pig.comparator.*;
import org.apache.hadoop.util.ReflectionUtils;


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
 * <LI>Set the single path to the BasicTable to be created.
 * <LI>Set the schema of the BasicTable to be created. In this case, the
 * to-be-created BasicTable contains three columns with names "Name", "Age",
 * "Salary", "BonusPct".
 * </UL>
 * 
 * To create multiple output paths. ZebraOutputPartitoner interface needs to be implemented
 * <pre>
 * String multiLocs = &quot;commaSeparatedPaths&quot;    
 * jobConf.setOutputFormat(BasicTableOutputFormat.class);
 * BasicTableOutputFormat.setMultipleOutputPaths(jobConf, multiLocs);
 * jobConf.setOutputFormat(BasicTableOutputFormat.class);
 * BasicTableOutputFormat.setSchema(jobConf, &quot;Name, Age, Salary, BonusPct&quot;);
 * BasicTableOutputFormat.setZebraOutputPartitionClass(
 * 		jobConf, MultipleOutputsTest.OutputPartitionerClass.class);
 * </pre>
 * 
 * 
 * The user ZebraOutputPartitionClass should like this
 * 
 * <pre>
 * 
 *   static class OutputPartitionerClass implements ZebraOutputPartition {
 *   &#064;Override
 *	  public int getOutputPartition(BytesWritable key, Tuple value) {		 
 *
 *        return someIndexInOutputParitionlist0;
 *	  }
 * 
 * </pre>
 * 
 * 
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
 * 
 * @Deprecated Use (@link org.apache.hadoop.zebra.mapreduce.BasicTableOutputFormat) instead
 */
@Deprecated
public class BasicTableOutputFormat implements
    OutputFormat<BytesWritable, Tuple> {
  private static final String OUTPUT_PATH = "mapred.lib.table.output.dir";
  private static final String MULTI_OUTPUT_PATH = "mapred.lib.table.multi.output.dirs";
  private static final String OUTPUT_SCHEMA = "mapred.lib.table.output.schema";
  private static final String OUTPUT_STORAGEHINT =
      "mapred.lib.table.output.storagehint";
  private static final String OUTPUT_SORTCOLUMNS =
      "mapred.lib.table.output.sortcolumns";
  private static final String OUTPUT_COMPARATOR =
      "mapred.lib.table.output.comparator";
  static final String IS_MULTI = "multi";
  private static final String ZEBRA_OUTPUT_PARTITIONER_CLASS = "zebra.output.partitioner.class";
  
  
  /**
   * Set the multiple output paths of the BasicTable in JobConf
   * 
   * @param conf
   *          The JobConf object.
   * @param commaSeparatedLocations
   *          The comma separated output paths to the tables. 
   *          The path must either not existent, or must be an empty directory.
   * @param theClass
   * 	      Zebra output partitoner class
   */
  
  public static void setMultipleOutputs(JobConf conf, String commaSeparatedLocations, Class<? extends ZebraOutputPartition> theClass)
	throws IOException {
	  
	conf.set(MULTI_OUTPUT_PATH, commaSeparatedLocations);

	if(conf.getBoolean(IS_MULTI, true) == false) {
      throw new IllegalArgumentException("Job has been setup as single output path");
	}
	conf.setBoolean(IS_MULTI, true);
	setZebraOutputPartitionClass(conf, theClass);
	  
  }

  /**
   * Set the multiple output paths of the BasicTable in JobConf
   * 
   * @param conf
   *          The JobConf object.
   * @param paths
   *          The list of paths 
   *          The path must either not existent, or must be an empty directory.
   * @param theClass
   * 	      Zebra output partitioner class
   */
  
  public static void setMultipleOutputs(JobConf conf, Class<? extends ZebraOutputPartition> theClass, Path... paths)
	throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path path = paths[0].makeQualified(fs);
    StringBuffer str = new StringBuffer(StringUtils.escapeString(path.toString()));
    for(int i = 1; i < paths.length;i++) {
      str.append(StringUtils.COMMA_STR);
      path = paths[i].makeQualified(fs);
      str.append(StringUtils.escapeString(path.toString()));
    }	  
	conf.set(MULTI_OUTPUT_PATH, str.toString());

	if(conf.getBoolean(IS_MULTI, true) == false) {
      throw new IllegalArgumentException("Job has been setup as single output path");
	}
	conf.setBoolean(IS_MULTI, true);
	setZebraOutputPartitionClass(conf, theClass);
	  
  }
  
  
  /**
   * Set the multiple output paths of the BasicTable in JobConf
   * 
   * @param conf
   *          The JobConf object.
   * @return path
   *          The comma separated output paths to the tables. 
   *          The path must either not existent, or must be an empty directory.
   */
  
  public static Path[] getOutputPaths(JobConf conf)
	throws IOException {

	Path[] result;
	String paths = conf.get(MULTI_OUTPUT_PATH);
	String path = conf.get(OUTPUT_PATH);
	
	if(paths != null && path != null) {
		throw new IllegalArgumentException("Illegal output paths specs. Both multi and single output locs are set");
	}	
	
	if(conf.getBoolean(IS_MULTI, false) == true) {	  
	    if (paths == null || paths.equals("")) {
	    	throw new IllegalArgumentException("Illegal multi output paths");
	    }	    
		String [] list = StringUtils.split(paths);
	    result = new Path[list.length];
	    for (int i = 0; i < list.length; i++) {
	      result[i] = new Path(StringUtils.unEscapeString(list[i]));
	    }
	} else {
	    if (path == null || path.equals("")) {
	      throw new IllegalArgumentException("Cannot find output path");
	    }	    
		result = new Path[1];
		result[0] = new Path(path);
	}

    return result;
	  
  }
  
  
  private static void setZebraOutputPartitionClass(
		  JobConf conf, Class<? extends ZebraOutputPartition> theClass) throws IOException {
	    if (!ZebraOutputPartition.class.isAssignableFrom(theClass))
	        throw new IOException(theClass+" not "+ZebraOutputPartition.class.getName());
	      conf.set(ZEBRA_OUTPUT_PARTITIONER_CLASS, theClass.getName());
  }
  

  public static Class<? extends ZebraOutputPartition> getZebraOutputPartitionClass(JobConf conf) throws IOException {

	Class<?> theClass;	  
    String valueString = conf.get(ZEBRA_OUTPUT_PARTITIONER_CLASS);
    if (valueString == null)
    	throw new IOException("zebra output partitioner class not found");
    try {
    	theClass = conf.getClassByName(valueString);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }	  

    if (theClass != null && !ZebraOutputPartition.class.isAssignableFrom(theClass))
        throw new IOException(theClass+" not "+ZebraOutputPartition.class.getName());
    else if (theClass != null)
      return theClass.asSubclass(ZebraOutputPartition.class);
    else
      return null;
    
  }
  
  
  
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
	if(conf.getBoolean(IS_MULTI, false) == true) {
	      throw new IllegalArgumentException("Job has been setup as multi output paths");
	}
	conf.setBoolean(IS_MULTI, false);

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
   * @deprecated Use {@link #setStorageInfo(JobConf, ZebraSchema, ZebraStorageHint, ZebraSortInfo)} instead.
   */
  public static void setSchema(JobConf conf, String schema) {
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
    //schema = schema.replaceAll(";", ",");
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
   * <br> <br>
   * 
   * Note that the "secure by" feature is experimental now and subject to
   * changes in the future.
   *
   * @param conf
   *          The JobConf object.
   * @param storehint
   *          The storage hint of the BasicTable to be created. The format would
   *          be like "[f1, f2.subfld]; [f3, f4]".
   * 
   * @deprecated Use {@link #setStorageInfo(JobConf, ZebraSchema, ZebraStorageHint, ZebraSortInfo)} instead.
   */
  public static void setStorageHint(JobConf conf, String storehint) throws ParseException, IOException {
    String schema = conf.get(OUTPUT_SCHEMA);

    if (schema == null)
      throw new ParseException("Schema has not been set");

    // for sanity check purpose only
    new Partition(schema, storehint, null);

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
  public static String getStorageHint(JobConf conf) {
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
   * @param comparatorClass
   *          comparator class name; null for default
   *
   * @deprecated Use {@link #setStorageInfo(JobConf, ZebraSchema, ZebraStorageHint, ZebraSortInfo)} instead.
   */
  public static void setSortInfo(JobConf conf, String sortColumns, Class<? extends RawComparator<Object>> comparatorClass) {
    conf.set(OUTPUT_SORTCOLUMNS, sortColumns);
    if (comparatorClass != null)
      conf.set(OUTPUT_COMPARATOR, TFile.COMPARATOR_JCLASS+comparatorClass.getName());
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
   * @deprecated Use {@link #setStorageInfo(JobConf, ZebraSchema, ZebraStorageHint, ZebraSortInfo)} instead.          
   */
  public static void setSortInfo(JobConf conf, String sortColumns) {
	  conf.set(OUTPUT_SORTCOLUMNS, sortColumns);
  }  

  /**
   * Set the table storage info including ZebraSchema, 
   *
   * @param conf
   *          The JobConf object.
   *          
   * @param zSchema The ZebraSchema object containing schema information.
   *  
   * @param zStorageHint The ZebraStorageHint object containing storage hint information.
   * 
   * @param zSortInfo The ZebraSortInfo object containing sorting information.
   *          
   */
  public static void setStorageInfo(JobConf conf, ZebraSchema zSchema, ZebraStorageHint zStorageHint, ZebraSortInfo zSortInfo) 
    throws ParseException, IOException {
    String schemaStr = null;
    String storageHintStr = null;

    /* validity check on schema*/
    if (zSchema == null) {
      throw new IllegalArgumentException("ZebraSchema object cannot be null.");
    } else {
      schemaStr = zSchema.toString();
    }
    
    Schema schema = null;
    try {
      schema = new Schema(schemaStr);
    } catch (ParseException e) {
      throw new ParseException("[" + zSchema + "] " + " is not a valid schema string: " + e.getMessage());
    }

    /* validity check on storage hint*/
    if (zStorageHint == null) {
      storageHintStr = "";
    } else {
      storageHintStr = zStorageHint.toString();
    }
    
    try {
      new Partition(schemaStr, storageHintStr, null);
    } catch (ParseException e) {
      throw new ParseException("[" + zStorageHint + "] " + " is not a valid storage hint string: " + e.getMessage()  ); 
    } catch (IOException e) {
      throw new ParseException("[" + zStorageHint + "] " + " is not a valid storage hint string: " + e.getMessage()  );
    }
    
    conf.set(OUTPUT_SCHEMA, schemaStr);
    conf.set(OUTPUT_STORAGEHINT, storageHintStr);
    
    /* validity check on sort info if user specifies it */
    if (zSortInfo != null) {
      String sortColumnsStr = zSortInfo.getSortColumns();
      String comparatorStr = zSortInfo.getComparator();      

      /* Check existence of comparable class if user specifies it */
      if (comparatorStr != null && comparatorStr != "") {
        try {
          conf.getClassByName(comparatorStr.substring(TFile.COMPARATOR_JCLASS.length()).trim());
        } catch (ClassNotFoundException e) {
          throw new IOException("comparator Class cannot be found : " + e.getMessage());        
        }
      }
      
      try {
        SortInfo.parse(sortColumnsStr, schema, comparatorStr);
      } catch (IOException e) {
        throw new IOException("[" + sortColumnsStr + " + " + comparatorStr + "] " 
            + "is not a valid sort configuration: " + e.getMessage());
      }
      
      if (sortColumnsStr != null)
        conf.set(OUTPUT_SORTCOLUMNS, sortColumnsStr);
      if (comparatorStr != null)
        conf.set(OUTPUT_COMPARATOR, comparatorStr);
    }
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
  private static BasicTable.Writer[] getOutput(JobConf conf) throws IOException {
    Path[] paths = getOutputPaths(conf);
    BasicTable.Writer[] writers = new BasicTable.Writer[paths.length]; 
    for(int i = 0; i < paths.length; i++) {
      writers[i] = new BasicTable.Writer(paths[i], conf);
    }
    
    return writers;
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
    String schema = conf.get(OUTPUT_SCHEMA);
    if (schema == null) {
      throw new IllegalArgumentException("Cannot find output schema");
    }

    String storehint, sortColumns, comparator;
    storehint = getStorageHint(conf);
    sortColumns = (getSortInfo(conf) == null ? null : SortInfo.toSortString(getSortInfo(conf).getSortColumnNames()));
    comparator = getComparator(conf);

    Path [] paths = getOutputPaths(conf);
    for (Path path : paths) {
      BasicTable.Writer writer =
        new BasicTable.Writer(path, schema, storehint, sortColumns, comparator, conf);
      writer.finish();
    }
  }

  /**
   * @see OutputFormat#getRecordWriter(FileSystem, JobConf, String,
   *      Progressable)
   */
  @Override
  public RecordWriter<BytesWritable, Tuple> getRecordWriter(FileSystem ignored,
      JobConf conf, String name, Progressable progress) throws IOException {
    String path = conf.get(OUTPUT_PATH);
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
    BasicTable.Writer tables[] = getOutput(conf);
    for(int i =0; i < tables.length; ++i) {
        tables[i].close();    	
    }
  }
}

/**
 * Adaptor class for BasicTable RecordWriter.
 */
class TableRecordWriter implements RecordWriter<BytesWritable, Tuple> {
  private final TableInserter inserter[];
  private final Progressable progress;
  private org.apache.hadoop.zebra.mapred.ZebraOutputPartition op = null;

  
  public TableRecordWriter(String path, String name, JobConf conf,
      Progressable progress) throws IOException {
	
	if(conf.getBoolean(BasicTableOutputFormat.IS_MULTI, false) == true) {	  
      op = (org.apache.hadoop.zebra.mapred.ZebraOutputPartition) 
    	  ReflectionUtils.newInstance(BasicTableOutputFormat.getZebraOutputPartitionClass(conf), conf);

	}  
    Path [] paths = BasicTableOutputFormat.getOutputPaths(conf);
    inserter = new TableInserter[paths.length];
    for(int i = 0; i < paths.length; ++i) {
    	BasicTable.Writer writer =
    		new BasicTable.Writer(paths[i], conf);
    	this.inserter[i] = writer.getInserter(name, true);
    }	
	  
    this.progress = progress;
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    for(int i = 0; i < this.inserter.length; ++i) {  
      inserter[i].close();
    }  
    reporter.progress();
  }

  @Override
  public void write(BytesWritable key, Tuple value) throws IOException {
	  
	if(op != null ) {	  
		int idx = op.getOutputPartition(key, value);
		if(idx < 0 || (idx >= inserter.length)) {
			throw new IllegalArgumentException("index returned by getOutputPartition is out of range");
		}
		inserter[idx].insert(key, value);
	} else {
		inserter[0].insert(key, value);
	}
	progress.progress();

  }
}
