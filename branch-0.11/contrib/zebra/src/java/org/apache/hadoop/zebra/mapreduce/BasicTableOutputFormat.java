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
package org.apache.hadoop.zebra.mapreduce;

import java.io.IOException;

import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.Partition;
import org.apache.hadoop.zebra.types.SortInfo;
import org.apache.hadoop.zebra.types.ZebraConf;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.tfile.TFile;
import org.apache.pig.data.Tuple;
import org.apache.hadoop.zebra.pig.comparator.*;


/**
 * {@link org.apache.hadoop.mapreduce.OutputFormat} class for creating a
 * BasicTable.
 * 
 * Usage Example:
 * <p>
 * In the main program, add the following code.
 * 
 * <pre>
 * job.setOutputFormatClass(BasicTableOutputFormat.class);
 * Path outPath = new Path(&quot;path/to/the/BasicTable&quot;);
 * BasicTableOutputFormat.setOutputPath(job, outPath);
 * BasicTableOutputFormat.setSchema(job, &quot;Name, Age, Salary, BonusPct&quot;);
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
 * job.setOutputFormatClass(BasicTableOutputFormat.class);
 * BasicTableOutputFormat.setMultipleOutputPaths(job, multiLocs);
 * job.setOutputFormat(BasicTableOutputFormat.class);
 * BasicTableOutputFormat.setSchema(job, &quot;Name, Age, Salary, BonusPct&quot;);
 * BasicTableOutputFormat.setZebraOutputPartitionClass(
 * 		job, MultipleOutputsTest.OutputPartitionerClass.class);
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
 *   public void configure(Job job) {
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
public class BasicTableOutputFormat extends OutputFormat<BytesWritable, Tuple> {
	/**
	 * Set the multiple output paths of the BasicTable in JobContext
	 * 
	 * @param jobContext
	 *          The JobContext object.
	 * @param commaSeparatedLocations
	 *          The comma separated output paths to the tables. 
	 *          The path must either not existent, or must be an empty directory.
	 * @param theClass
	 * 	      Zebra output partitioner class
	 * 
	 * @deprecated Use {@link #setMultipleOutputs(JobContext, class<? extends ZebraOutputPartition>, Path ...)} instead.
	 * 
	 */
	public static void setMultipleOutputs(JobContext jobContext, String commaSeparatedLocations, Class<? extends ZebraOutputPartition> theClass)
	throws IOException {
		Configuration conf = jobContext.getConfiguration();
		ZebraConf.setMultiOutputPath(conf, commaSeparatedLocations);

		if (ZebraConf.getIsMulti(conf, true) == false) {
			throw new IllegalArgumentException("Job has been setup as single output path");
		}

		ZebraConf.setIsMulti(conf, true);
		setZebraOutputPartitionClass(jobContext, theClass);	  
	}

  /**
	 * Set the multiple output paths of the BasicTable in JobContext
	 * 
	 * @param jobContext
	 *          The JobContext object.
   * @param theClass
   *        Zebra output partitioner class          
	 * @param paths
	 *          The list of paths 
	 *          The path must either not existent, or must be an empty directory.
	 */
	public static void setMultipleOutputs(JobContext jobContext, Class<? extends ZebraOutputPartition> theClass, Path... paths)
	throws IOException {
		Configuration conf = jobContext.getConfiguration();
		FileSystem fs = FileSystem.get( conf );
		Path path = paths[0].makeQualified(fs);
		StringBuffer str = new StringBuffer(StringUtils.escapeString(path.toString()));
		for(int i = 1; i < paths.length;i++) {
			str.append(StringUtils.COMMA_STR);
			path = paths[i].makeQualified(fs);
			str.append(StringUtils.escapeString(path.toString()));
		}	  
		ZebraConf.setMultiOutputPath(conf, str.toString());

		if (ZebraConf.getIsMulti(conf, true) == false) {
			throw new IllegalArgumentException("Job has been setup as single output path");
		}

		ZebraConf.setIsMulti(conf, true);
		setZebraOutputPartitionClass(jobContext, theClass);
	}
	
	/**
   * Set the multiple output paths of the BasicTable in JobContext
   * 
   * @param jobContext
   *          The JobContext object.
   * @param theClass
   *          Zebra output partitioner class
   * @param arguments
   *          Arguments string to partitioner class
   * @param paths
   *          The list of paths 
   *          The path must either not existent, or must be an empty directory.
   */
  public static void setMultipleOutputs(JobContext jobContext, Class<? extends ZebraOutputPartition> theClass, String arguments, Path... paths)
  throws IOException {
    setMultipleOutputs(jobContext, theClass, paths);
    if (arguments != null) {
      ZebraConf.setOutputPartitionClassArguments(jobContext.getConfiguration(), arguments);
    }
  }
  
  /**
   * Get the output partition class arguments string from job configuration
   * 
   * @param conf
   *          The job configuration object.
   * @return the output partition class arguments string.
   */
  public static String getOutputPartitionClassArguments(Configuration conf) {
    return ZebraConf.getOutputPartitionClassArguments(conf);
  }  

	/**
	 * Get the multiple output paths of the BasicTable from JobContext
	 * 
	 * @param jobContext
	 *          The JobContext object.
	 * @return path
	 *          The comma separated output paths to the tables. 
	 *          The path must either not existent, or must be an empty directory.
	 */
	public static Path[] getOutputPaths(JobContext jobContext)
	throws IOException {
		Configuration conf = jobContext.getConfiguration();

		Path[] result;
		String paths = ZebraConf.getMultiOutputPath(conf);
		String path = ZebraConf.getOutputPath(conf);

		if(paths != null && path != null) {
			throw new IllegalArgumentException("Illegal output paths specs. Both multi and single output locs are set");
		}	

		if (ZebraConf.getIsMulti(conf, false) == true) {
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
			JobContext jobContext, Class<? extends ZebraOutputPartition> theClass) throws IOException {
		if (!ZebraOutputPartition.class.isAssignableFrom(theClass))
			throw new IOException(theClass+" not "+ZebraOutputPartition.class.getName());
		ZebraConf.setZebraOutputPartitionerClass(jobContext.getConfiguration(), theClass.getName());
	}

	public static Class<? extends ZebraOutputPartition> getZebraOutputPartitionClass(JobContext jobContext) throws IOException {
		Configuration conf = jobContext.getConfiguration();

		Class<?> theClass;	  
		String valueString = ZebraConf.getZebraOutputPartitionerClass(conf);
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
	 * Set the output path of the BasicTable in JobContext
	 * 
	 * @param jobContext
	 *          The JobContext object.
	 * @param path
	 *          The output path to the table. The path must either not existent,
	 *          or must be an empty directory.
	 */
	public static void setOutputPath(JobContext jobContext, Path path) {
		Configuration conf = jobContext.getConfiguration();
		ZebraConf.setOutputPath(conf, path.toString());
		if (ZebraConf.getIsMulti(conf, false) == true) {
			throw new IllegalArgumentException("Job has been setup as multi output paths");
		}
		ZebraConf.setIsMulti(conf, false);

	}

	/**
	 * Get the output path of the BasicTable from JobContext
	 * 
	 * @param jobContext
	 *          jobContext object
	 * @return The output path.
	 */
	public static Path getOutputPath(JobContext jobContext) {
		Configuration conf = jobContext.getConfiguration();
		String path = ZebraConf.getOutputPath(conf);
		return (path == null) ? null : new Path(path);
	}

	/**
	 * Set the table schema in JobContext
	 * 
	 * @param jobContext
	 *          The JobContext object.
	 * @param schema
	 *          The schema of the BasicTable to be created. For the initial
	 *          implementation, the schema string is simply a comma separated list
	 *          of column names, such as "Col1, Col2, Col3".
	 * 
	 * @deprecated Use {@link #setStorageInfo(JobContext, ZebraSchema, ZebraStorageHint, ZebraSortInfo)} instead.
	 */
	public static void setSchema(JobContext jobContext, String schema) {
		Configuration conf = jobContext.getConfiguration();
 		ZebraConf.setOutputSchema(conf, Schema.normalize(schema));
		
    // This is to turn off type check for potential corner cases - for internal use only;
		if (System.getenv("zebra_output_checktype")!= null && System.getenv("zebra_output_checktype").equals("no")) {
		  ZebraConf.setCheckType(conf, false);
    }
	}

	/**
	 * Get the table schema in JobContext.
	 * 
	 * @param jobContext
	 *          The JobContext object.
	 * @return The output schema of the BasicTable. If the schema is not defined
	 *         in the jobContext object at the time of the call, null will be returned.
	 */
	public static Schema getSchema(JobContext jobContext) throws ParseException {
		Configuration conf = jobContext.getConfiguration();
		String schema = ZebraConf.getOutputSchema(conf);
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
	 * @param jobContext
	 *          The JobContext object.
	 * @return Object of type zebra.pig.comaprator.KeyGenerator. 
	 *         
	 */
	public static Object getSortKeyGenerator(JobContext jobContext) throws IOException, ParseException {
		SortInfo sortInfo = getSortInfo( jobContext );
		Schema schema     = getSchema(jobContext);
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
	 * Set the table storage hint in JobContext, should be called after setSchema is
	 * called.
	 * <br> <br>
	 * 
	 * Note that the "secure by" feature is experimental now and subject to
	 * changes in the future.
	 *
	 * @param jobContext
	 *          The JobContext object.
	 * @param storehint
	 *          The storage hint of the BasicTable to be created. The format would
	 *          be like "[f1, f2.subfld]; [f3, f4]".
	 * 
	 * @deprecated Use {@link #setStorageInfo(JobContext, ZebraSchema, ZebraStorageHint, ZebraSortInfo)} instead.
	 */
	public static void setStorageHint(JobContext jobContext, String storehint) throws ParseException, IOException {
		Configuration conf = jobContext.getConfiguration();
		String schema = ZebraConf.getOutputSchema(conf);

		if (schema == null)
			throw new ParseException("Schema has not been set");

		// for sanity check purpose only
		new Partition(schema, storehint, null);

		ZebraConf.setOutputStorageHint(conf, storehint);
	}

	/**
	 * Get the table storage hint in JobContext.
	 * 
	 * @param jobContext
	 *          The JobContext object.
	 * @return The storage hint of the BasicTable. If the storage hint is not
	 *         defined in the jobContext object at the time of the call, an empty string
	 *         will be returned.
	 */
	public static String getStorageHint(JobContext jobContext) {
		Configuration conf = jobContext.getConfiguration();
		String storehint = ZebraConf.getOutputStorageHint(conf);
		return storehint == null ? "" : storehint;
	}

	/**
	 * Set the sort info
	 *
	 * @param jobContext
	 *          The JobContext object.
	 *          
	 * @param sortColumns
	 *          Comma-separated sort column names
	 *          
	 * @param comparatorClass
	 *          comparator class name; null for default
	 *
	 * @deprecated Use {@link #setStorageInfo(JobContext, ZebraSchema, ZebraStorageHint, ZebraSortInfo)} instead.
	 */
	public static void setSortInfo(JobContext jobContext, String sortColumns, Class<? extends RawComparator<Object>> comparatorClass) {
		Configuration conf = jobContext.getConfiguration();
		ZebraConf.setOutputSortColumns(conf, sortColumns);
		if (comparatorClass != null)
		  ZebraConf.setOutputComparator(conf, TFile.COMPARATOR_JCLASS+comparatorClass.getName());
	}

	/**
	 * Set the sort info
	 *
	 * @param jobContext
	 *          The JobContext object.
	 *          
	 * @param sortColumns
	 *          Comma-separated sort column names
	 *          
	 * @deprecated Use {@link #setStorageInfo(JobContext, ZebraSchema, ZebraStorageHint, ZebraSortInfo)} instead.          
	 */
	public static void setSortInfo(JobContext jobContext, String sortColumns) {
	  ZebraConf.setOutputSortColumns(jobContext.getConfiguration(), sortColumns);
	}  

	/**
	 * Set the table storage info including ZebraSchema, 
	 *
	 * @param jobcontext
	 *          The JobContext object.
	 *          
	 * @param zSchema The ZebraSchema object containing schema information.
	 *  
	 * @param zStorageHint The ZebraStorageHint object containing storage hint information.
	 * 
	 * @param zSortInfo The ZebraSortInfo object containing sorting information.
	 *          
	 */
	public static void setStorageInfo(JobContext jobContext, ZebraSchema zSchema, ZebraStorageHint zStorageHint, ZebraSortInfo zSortInfo) 
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

		Configuration conf = jobContext.getConfiguration();
		ZebraConf.setOutputSchema(conf, schemaStr);
		ZebraConf.setOutputStorageHint(conf, storageHintStr);		

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
			  ZebraConf.setOutputSortColumns(conf, sortColumnsStr);
			if (comparatorStr != null)
			  ZebraConf.setOutputComparator(conf, comparatorStr);
		}
	}

	/**
	 * Get the SortInfo object 
	 *
	 * @param jobContext
	 *          The JobContext object.
	 * @return SortInfo object; null if the Zebra table is unsorted 
	 *
	 */
	public static SortInfo getSortInfo(JobContext jobContext)throws IOException
	{
		Configuration conf = jobContext.getConfiguration();
		String sortColumns = ZebraConf.getOutputSortColumns(conf);
		if (sortColumns == null)
			return null;
		Schema schema = null;
		try {
			schema = getSchema(jobContext);
		} catch (ParseException e) {
			throw new IOException("Schema parsing failure : "+e.getMessage());
		}
		if (schema == null)
			throw new IOException("Schema not defined");
		String comparator = getComparator(jobContext);
		return SortInfo.parse(sortColumns, schema, comparator);
	}

	/**
	 * Get the  comparator for sort columns
	 *
	 * @param jobContext
	 *          The JobContext object.
	 * @return  comparator String
	 *
	 */
	private static String getComparator(JobContext jobContext)
	{
	  return ZebraConf.getOutputComparator(jobContext.getConfiguration());
	}

	/**
	 * Get the output table as specified in JobContext. It is useful for applications
	 * to add more meta data after all rows have been added to the table.
	 * 
	 * @param conf
	 *          The JobContext object.
	 * @return The output BasicTable.Writer object.
	 * @throws IOException
	 */
	private static BasicTable.Writer[] getOutput(JobContext jobContext) throws IOException {
		Path[] paths = getOutputPaths(jobContext);
		BasicTable.Writer[] writers = new BasicTable.Writer[paths.length]; 
		for(int i = 0; i < paths.length; i++) {
			writers[i] = new BasicTable.Writer(paths[i], jobContext.getConfiguration());
		}

		return writers;
	}

	/**
	 * Note: we perform the Initialization of the table here. So we expect this to
	 * be called before
	 * {@link BasicTableOutputFormat#getRecordWriter(FileSystem, JobContext, String, Progressable)}
	 * 
	 * @see OutputFormat#checkOutputSpecs(JobContext)
	 */
	@Override
	public void checkOutputSpecs(JobContext jobContext)
	throws IOException {
		Configuration conf = jobContext.getConfiguration();
		String schema = ZebraConf.getOutputSchema(conf);
		if (schema == null) {
			throw new IllegalArgumentException("Cannot find output schema");
		}
		
		String storehint, sortColumns, comparator;
  	storehint = getStorageHint(jobContext);
	  sortColumns = (getSortInfo(jobContext) == null ? null : SortInfo.toSortString(getSortInfo(jobContext).getSortColumnNames()));
		comparator = getComparator( jobContext );
		
		Path[] paths = getOutputPaths(jobContext);

	  for (Path path : paths) {
      BasicTable.Writer writer =
        new BasicTable.Writer(path, schema, storehint, sortColumns, comparator, conf);
      writer.finish();
	  }
	}

	/**
	 * @see OutputFormat#getRecordWriter(TaskAttemptContext)
	 */
	@Override
	public RecordWriter<BytesWritable, Tuple> getRecordWriter(TaskAttemptContext taContext)
	throws IOException {
	  String path = ZebraConf.getOutputPath(taContext.getConfiguration());
		return new TableRecordWriter(path, taContext);
	}

	/**
	 * Close the output BasicTable, No more rows can be added into the table. A
	 * BasicTable is not visible for reading until it is "closed".
	 * 
	 * @param jobContext
	 *          The JobContext object.
	 * @throws IOException
	 */
	public static void close(JobContext jobContext) throws IOException {
		BasicTable.Writer tables[] = getOutput(jobContext);
		for(int i =0; i < tables.length; ++i) {
			tables[i].close();    	
		}
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext taContext)
	throws IOException, InterruptedException {
		return new TableOutputCommitter( taContext ) ;
	}
}

class TableOutputCommitter extends OutputCommitter {
	public TableOutputCommitter(TaskAttemptContext taContext) {

	}

	@Override
	public void abortTask(TaskAttemptContext taContext) throws IOException {
	}

	@Override
	public void cleanupJob(JobContext jobContext) throws IOException {
	}

	@Override
	public void commitTask(TaskAttemptContext taContext) throws IOException {
	}

	@Override
	public boolean needsTaskCommit(TaskAttemptContext taContext)
	throws IOException {
		return false;
	}

	@Override
	public void setupJob(JobContext jobContext) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setupTask(TaskAttemptContext taContext) throws IOException {
		// TODO Auto-generated method stub

	}
}

/**
 * Adaptor class for BasicTable RecordWriter.
 */
class TableRecordWriter extends RecordWriter<BytesWritable, Tuple> {
	private final TableInserter inserter[];
	private org.apache.hadoop.zebra.mapreduce.ZebraOutputPartition op = null;
 	
 	// for Pig's call path;
 	final private BytesWritable KEY0 = new BytesWritable(new byte[0]);
   private int[] sortColIndices = null;
   private KeyGenerator builder = null;
   private Tuple t = null;
   
	public TableRecordWriter(String path, TaskAttemptContext context) throws IOException {	
		Configuration conf = context.getConfiguration();
    if(ZebraConf.getIsMulti(conf, false) == true) {
			op = (org.apache.hadoop.zebra.mapreduce.ZebraOutputPartition) 
			ReflectionUtils.newInstance(BasicTableOutputFormat.getZebraOutputPartitionClass(context), conf);
		}
		
    boolean checkType = ZebraConf.getCheckType(conf, true);
		Path [] paths = BasicTableOutputFormat.getOutputPaths(context);
		inserter = new TableInserter[paths.length];
                String inserterName = "part-" + context.getTaskAttemptID().getTaskID().getId();
		for(int i = 0; i < paths.length; ++i) {
			BasicTable.Writer writer =
				new BasicTable.Writer(paths[i], conf);
			this.inserter[i] = writer.getInserter( inserterName, true, checkType);

			// Set up SortInfo related stuff only once;
			if (i == 0) {
	      if (writer.getSortInfo() != null)
	      {
          sortColIndices = writer.getSortInfo().getSortIndices();
          SortInfo sortInfo =  writer.getSortInfo();
          String[] sortColNames = sortInfo.getSortColumnNames();
          org.apache.hadoop.zebra.schema.Schema schema = writer.getSchema();

          byte[] types = new byte[sortColNames.length];
          
          for(int j =0 ; j < sortColNames.length; ++j){
              types[j] = schema.getColumn(sortColNames[j]).getType().pigDataType();
          }
          t = TypesUtils.createTuple(sortColNames.length);
          builder = makeKeyBuilder(types);
	      }
			}
		}
	}
	
  private KeyGenerator makeKeyBuilder(byte[] elems) {
    ComparatorExpr[] exprs = new ComparatorExpr[elems.length];
    for (int i = 0; i < elems.length; ++i) {
        exprs[i] = ExprUtils.primitiveComparator(i, elems[i]);
    }
    return new KeyGenerator(ExprUtils.tupleComparator(exprs));
  }


	@Override
	public void close(TaskAttemptContext context) throws IOException {
		for(int i = 0; i < this.inserter.length; ++i) {  
			inserter[i].close();
		}  
	}

	@Override
	public void write(BytesWritable key, Tuple value) throws IOException {
    if (key == null) {
      if (sortColIndices != null) { // If this is a sorted table and key is null (Pig's call path);
        for (int i =0; i < sortColIndices.length;++i) {
          t.set(i, value.get(sortColIndices[i]));
        }
        key = builder.generateKey(t);        
      } else { // for unsorted table;
        key = KEY0;
      }
    }
	  	  
		if(op != null ) {	  
			int idx = op.getOutputPartition(key, value);
			if(idx < 0 || (idx >= inserter.length)) {
				throw new IllegalArgumentException("index returned by getOutputPartition is out of range");
			}
			inserter[idx].insert(key, value);
		} else {
			inserter[0].insert(key, value);
		}
	}
}

