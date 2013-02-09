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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.zebra.tfile.RawComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.BasicTableStatus;
import org.apache.hadoop.zebra.io.BlockDistribution;
import org.apache.hadoop.zebra.io.KeyDistribution;
import org.apache.hadoop.zebra.io.BasicTable.Reader;
import org.apache.hadoop.zebra.io.BasicTable.Reader.RangeSplit;
import org.apache.hadoop.zebra.io.BasicTable.Reader.RowSplit;
import org.apache.hadoop.zebra.mapreduce.TableExpr.LeafTableInfo;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.SortInfo;
import org.apache.hadoop.zebra.tfile.TFile;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * {@link org.apache.hadoop.mapreduce.InputFormat} class for reading one or more
 * BasicTables.
 * 
 * Usage Example:
 * <p>
 * In the main program, add the following code.
 * 
 * <pre>
 * job.setInputFormatClass(TableInputFormat.class);
 * TableInputFormat.setInputPaths(jobContext, new Path(&quot;path/to/table1&quot;, new Path(&quot;path/to/table2&quot;);
 * TableInputFormat.setProjection(jobContext, &quot;Name, Salary, BonusPct&quot;);
 * </pre>
 * 
 * The above code does the following things:
 * <UL>
 * <LI>Set the input format class to TableInputFormat.
 * <LI>Set the paths to the BasicTables to be consumed by user's Mapper code.
 * <LI>Set the projection on the input tables. In this case, the Mapper code is
 * only interested in three fields: "Name", "Salary", "BonusPct". "Salary"
 * (perhaps for the purpose of calculating the person's total payout). If no
 * project is specified, then all columns from the input tables will be
 * retrieved. If input tables have different schemas, then the input contains
 * the union of all columns from all the input tables. Absent fields will be
 * left as nul in the input tuple.
 * </UL>
 * The user Mapper code should look like the following:
 * 
 * <pre>
 * static class MyMapClass implements Mapper&lt;BytesWritable, Tuple, K, V&gt; {
 *   // keep the tuple object for reuse.
 *   // indices of various fields in the input Tuple.
 *   int idxName, idxSalary, idxBonusPct;
 * 
 *   &#064;Override
 *   public void configure(Job job) {
 *     Schema projection = TableInputFormat.getProjection(job);
 *     // determine the field indices.
 *     idxName = projection.getColumnIndex(&quot;Name&quot;);
 *     idxSalary = projection.getColumnIndex(&quot;Salary&quot;);
 *     idxBonusPct = projection.getColumnIndex(&quot;BonusPct&quot;);
 *   }
 * 
 *   &#064;Override
 *   public void map(BytesWritable key, Tuple value, OutputCollector&lt;K, V&gt; output,
 *       Reporter reporter) throws IOException {
 *     try {
 *       String name = (String) value.get(idxName);
 *       int salary = (Integer) value.get(idxSalary);
 *       double bonusPct = (Double) value.get(idxBonusPct);
 *       // do something with the input data
 *     } catch (ExecException e) {
 *       e.printStackTrace();
 *     }
 *   }
 * 
 *   &#064;Override
 *   public void close() throws IOException {
 *     // no-op
 *   }
 * }
 * </pre>
 * 
 * A little bit more explanation on the PIG {@link Tuple} objects. A Tuple is an
 * ordered list of PIG datum objects. The permitted PIG datum types can be
 * categorized as Scalar types and Composite types.
 * <p>
 * Supported Scalar types include seven native Java types: Boolean, Byte,
 * Integer, Long, Float, Double, String, as well as one PIG class called
 * {@link DataByteArray} that represents type-less byte array.
 * <p>
 * Supported Composite types include:
 * <UL>
 * <LI>{@link Map} : It is the same as Java Map class, with the additional
 * restriction that the key-type must be one of the scalar types PIG recognizes,
 * and the value-type any of the scaler or composite types PIG understands.
 * <LI>{@link DataBag} : A DataBag is a collection of Tuples.
 * <LI>{@link Tuple} : Yes, Tuple itself can be a datum in another Tuple.
 * </UL>
 */
public class TableInputFormat extends InputFormat<BytesWritable, Tuple> {
  public enum SplitMode {
    UNSORTED, /* Data is not sorted. Default split mode*/
    LOCALLY_SORTED, /* Output by the each mapper is sorted */
    GLOBALLY_SORTED /* Output is locally sorted and the key ranges are not overlapped, not even on boundary */
  };
    
  static Log LOG = LogFactory.getLog(TableInputFormat.class);
  
  private static final String INPUT_EXPR = "mapreduce.lib.table.input.expr";
  private static final String INPUT_PROJ = "mapreduce.lib.table.input.projection";
  private static final String INPUT_SORT = "mapreduce.lib.table.input.sort";
  static final String INPUT_FE = "mapreduce.lib.table.input.fe";
  static final String INPUT_DELETED_CGS = "mapreduce.lib.table.input.deleted_cgs";
  private static final String INPUT_SPLIT_MODE = "mapreduce.lib.table.input.split_mode";
  private static final String UNSORTED = "unsorted";
  private static final String GLOBALLY_SORTED = "globally_sorted";
  private static final String LOCALLY_SORTED = "locally_sorted";
  static final String DELETED_CG_SEPARATOR_PER_UNION = ";";
  
  /**
   * Set the paths to the input table.
   * 
   * @param conf
   *          JobContext object.
   * @param paths
   *          one or more paths to BasicTables. The InputFormat class will
   *          produce splits on the "union" of these BasicTables.
   */
  public static void setInputPaths(JobContext jobContext, Path... paths) {
    if (paths.length < 1) {
      throw new IllegalArgumentException("Requring at least one input path");
    }
    
    Configuration conf = jobContext.getConfiguration();
    
    if (paths.length == 1) {
      setInputExpr(conf, new BasicTableExpr(paths[0]));
    }
    else {
      TableUnionExpr expr = new TableUnionExpr();
      for (Path path : paths) {
        expr.add(new BasicTableExpr(path));
      }
      setInputExpr(conf, expr);
    }
    
    setDeletedCGsInConf( conf, paths );
  }
  
  /**
   * Temporary fix for name node call in each mapper. It sets two flags in the conf so that mapper can skip
   * the work that's done here at frontend.
   * 
   * It needs to check if the flag is already set (because setInputPaths() is also called on the backend
   * thru pig code path (thru TableLoader.setLocation()).
   * 
   * @param conf
   * @param paths
   */
  private static void setDeletedCGsInConf(Configuration conf, Path[] paths) {
      if( !conf.get( INPUT_FE, "false" ).equals( "true" )  ) {
          try {
              StringBuilder sb = new StringBuilder();
              boolean first = true;
              for( Path p : paths ) {
                  if (first)
                      first = false;
                  else
                      sb.append( DELETED_CG_SEPARATOR_PER_UNION );
                  sb.append( BasicTable.Reader.getDeletedCGs( p, conf ) );
              }

              conf.set(INPUT_FE, "true");
              conf.set(INPUT_DELETED_CGS, sb.toString());
          } catch(Exception ex) {
              throw new RuntimeException( "Failed to find deleted column groups" + ex.toString() );
          }
      }
  }

  
  /**
   * Set the input expression in the Configuration object.
   * 
   * @param conf
   *          Configuration object.
   * @param expr
   *          The input table expression.
   */
  static void setInputExpr(Configuration conf, TableExpr expr) {
    StringBuilder out = new StringBuilder();
    expr.encode(out);
    conf.set(INPUT_EXPR, out.toString());
  }

  static TableExpr getInputExpr(JobContext jobContext) throws IOException {
    Configuration conf = jobContext.getConfiguration();
    String expr = conf.get(INPUT_EXPR);
    if (expr == null) {
      // try setting from input path
      Path[] paths = FileInputFormat.getInputPaths( jobContext );
      if (paths != null) {
        setInputPaths(jobContext, paths);
      }
      expr = conf.get(INPUT_EXPR);
    }
      
    if (expr == null) {
      throw new IllegalArgumentException("Input expression not defined.");
    }
    StringReader in = new StringReader(expr);
    return TableExpr.parse(in);
  }
  
  /**
   * Get the schema of a table expr
   * 
   * @param jobContext
   *          JobContext object.
   *                    
   */
  public static Schema getSchema(JobContext jobContext) throws IOException
  {
	  TableExpr expr = getInputExpr( jobContext );
	  return expr.getSchema( jobContext.getConfiguration() );
  }  
  
  /**
   * Set the input projection in the JobContext object.
   * 
   * @param jobContext
   *          JobContext object.
   * @param projection
   *          A common separated list of column names. If we want select all
   *          columns, pass projection==null. The syntax of the projection
   *          conforms to the {@link Schema} string.
   * @deprecated Use {@link #setProjection(JobContext, ZebraProjection)} instead.
   */
  public static void setProjection(JobContext jobContext, String projection) throws ParseException {
	  setProjection( jobContext.getConfiguration(), projection );
  }
  
  /**
   * Set the input projection in the JobContext object.
   * 
   * @param conf
   *          Configuration object.
   * @param projection
   *          A common separated list of column names. If we want select all
   *          columns, pass projection==null. The syntax of the projection
   *          conforms to the {@link Schema} string.
   */
  private static void setProjection(Configuration conf, String projection) throws ParseException {
      conf.set(INPUT_PROJ, Schema.normalize(projection));
  }

  /**
   * Set the input projection in the JobContext object.
   * 
   * @param jobContext
   *          JobContext object.
   * @param projection
   *          A common separated list of column names. If we want select all
   *          columns, pass projection==null. The syntax of the projection
   *          conforms to the {@link Schema} string.
   *
   */
  public static void setProjection(JobContext jobContext, ZebraProjection projection) throws ParseException {
    /* validity check on projection */
    Schema schema = null;
    String normalizedProjectionString = Schema.normalize(projection.toString());
    try {
      schema = getSchema( jobContext );
      new org.apache.hadoop.zebra.types.Projection(schema, normalizedProjectionString);
    } catch (ParseException e) {
      throw new ParseException("[" + projection + "] " + "is not a valid Zebra projection string " + e.getMessage());
    } catch (IOException e) {
      throw new ParseException("[" + projection + "] " + "is not a valid Zebra projection string " + e.getMessage());
    }
    
    Configuration conf = jobContext.getConfiguration();
    conf.set(INPUT_PROJ, normalizedProjectionString);

    // virtual source_table columns require sorted table
    if (Projection.getVirtualColumnIndices(projection.toString()) != null && !getSorted( conf ))
      throw new ParseException("The source_table virtual column is only availabe for sorted table unions.");
  }  

  /**
   * Get the projection from the JobContext
   * 
   * @param jobContext
   *          The JobContext object
   * @return The projection schema. If projection has not been defined, or is
   *         not known at this time, null will be returned. Note that by the time
   *         when this method is called in Mapper code, the projection must
   *         already be known.
   * @throws IOException
   *  
   */
  public static String getProjection(JobContext jobContext) throws IOException, ParseException {
    Configuration conf = jobContext.getConfiguration();
    String strProj = conf.get(INPUT_PROJ);
    // TODO: need to be revisited
    if (strProj != null) return strProj;
    TableExpr expr = getInputExpr( jobContext );
    if (expr != null) {
      return expr.getSchema(conf).toProjectionString();
    }
    return null;
  }
      
  private static boolean globalOrderingRequired(JobContext jobContext)
  {
    Configuration conf = jobContext.getConfiguration();
    String result = conf.get(INPUT_SPLIT_MODE, UNSORTED);
    return result.equalsIgnoreCase(GLOBALLY_SORTED);
  }

  private static void setSorted(JobContext jobContext) {
    Configuration conf = jobContext.getConfiguration();
    conf.setBoolean(INPUT_SORT, true);
  }
  
  private static void setSorted(JobContext jobContext, SplitMode sm)
  {
    setSorted(jobContext);
    Configuration conf = jobContext.getConfiguration();
	  if (sm == SplitMode.GLOBALLY_SORTED)
      conf.set(INPUT_SPLIT_MODE, GLOBALLY_SORTED);
    else if (sm == SplitMode.LOCALLY_SORTED)
      conf.set(INPUT_SPLIT_MODE, LOCALLY_SORTED);
  }
  
  /**
   * Get the SortInfo object regarding a Zebra table
   *
   * @param jobContext
   *          JobContext object
   * @return the zebra tables's SortInfo; null if the table is unsorted.
   */
  public static SortInfo getSortInfo(JobContext jobContext) throws IOException
  {
      Configuration conf = jobContext.getConfiguration();
	  TableExpr expr = getInputExpr(jobContext);
	  SortInfo result = null;
	  int sortSize = 0;
	  if (expr instanceof BasicTableExpr)
    {
      BasicTable.Reader reader = new BasicTable.Reader(((BasicTableExpr) expr).getPath(), conf);
      SortInfo sortInfo = reader.getSortInfo();
      reader.close();
      result = sortInfo;
	  } else {
      List<LeafTableInfo> leaves = expr.getLeafTables(null);
      for (Iterator<LeafTableInfo> it = leaves.iterator(); it.hasNext(); )
      {
        LeafTableInfo leaf = it.next();
        BasicTable.Reader reader = new BasicTable.Reader(leaf.getPath(), conf);
        SortInfo sortInfo = reader.getSortInfo();
        reader.close();
        if (sortSize == 0)
        {
          sortSize = sortInfo.size();
          result = sortInfo;
        } else if (sortSize != sortInfo.size()) {
          throw new IOException("Tables of the table union do not possess the same sort property.");
        }
		  }
	  }
	  return result;
  }

  /**
   * Requires sorted table or table union
   * 
   * @deprecated
   * 
   * @param jobContext
   *          JobContext object.
   * @param sortInfo
   *          ZebraSortInfo object containing sorting information.
   */
  
   public static void requireSortedTable(JobContext jobContext, ZebraSortInfo sortInfo) throws IOException {
     setSplitMode(jobContext, SplitMode.GLOBALLY_SORTED, sortInfo);
   }

  /**
   * 
   * @param conf
   *          JonConf object
   * @param sm
   *          Split mode: unsorted, globally sorted, locally sorted. Default is unsorted
   * @param sortInfo
   *          ZebraSortInfo object containing sorting information. Will be ignored if
   *          the split mode is null or unsorted
   * @throws IOException
   */
  public static void setSplitMode(JobContext jobContext, SplitMode sm, ZebraSortInfo sortInfo) throws IOException {
   if (sm == null || sm == SplitMode.UNSORTED)
    return;
	 TableExpr expr = getInputExpr( jobContext );
     Configuration conf = jobContext.getConfiguration();
	 String comparatorName = null;
 	 String[] sortcolumns = null;
         if (sortInfo != null)
         {
           comparatorName = TFile.COMPARATOR_JCLASS+sortInfo.getComparator();
           String sortColumnNames = sortInfo.getSortColumns();
           if (sortColumnNames != null)
             sortcolumns =  sortColumnNames.trim().split(SortInfo.SORTED_COLUMN_DELIMITER);
           if (sortcolumns == null)
             throw new IllegalArgumentException("No sort columns specified.");
         }

	 if (expr instanceof BasicTableExpr)
	 {
		 BasicTable.Reader reader = new BasicTable.Reader(((BasicTableExpr) expr).getPath(), conf);
		 SortInfo mySortInfo = reader.getSortInfo();

		 reader.close();
		 if (mySortInfo == null)
       throw new IOException("The table is not sorted");
		 if (comparatorName == null)
			 // cheat the equals method's comparator comparison
			 comparatorName = mySortInfo.getComparator();
		 if (sortcolumns != null && !mySortInfo.equals(sortcolumns, comparatorName))
		 {
			 throw new IOException("The table is not properly sorted");
		 }
	 } else {
		 List<LeafTableInfo> leaves = expr.getLeafTables(null);
		 for (Iterator<LeafTableInfo> it = leaves.iterator(); it.hasNext(); )
		 {
			 LeafTableInfo leaf = it.next();
			 BasicTable.Reader reader = new BasicTable.Reader(leaf.getPath(), conf);
			 SortInfo mySortInfo = reader.getSortInfo();
			 reader.close();
			 if (mySortInfo == null)
			   throw new IOException("The table is not sorted");
			 if (comparatorName == null)
				 comparatorName = mySortInfo.getComparator(); // use the first table's comparator as comparison base
			 if (sortcolumns == null)
       {
         sortcolumns = mySortInfo.getSortColumnNames();
         comparatorName = mySortInfo.getComparator();
       } else {
         if (!mySortInfo.equals(sortcolumns, comparatorName))
         {
           throw new IOException("The table is not properly sorted");
         }
       }
		 }
	 }
	 setSorted(jobContext, sm);
  }
  
  /**
   * Get requirement for sorted table
   *
   *@param jobContext JobContext object.
   */
   private static boolean getSorted(Configuration conf) {
    return conf.getBoolean(INPUT_SORT, false);
  }

    /**
     * @see InputFormat#createRecordReader(InputSplit, TaskAttemptContext)
     */
    @Override
    public RecordReader<BytesWritable, Tuple> createRecordReader(InputSplit split,
    		TaskAttemptContext taContext) throws IOException,InterruptedException {
        return createRecordReaderFromJobContext( split, taContext );
    }

    private RecordReader<BytesWritable, Tuple> createRecordReaderFromJobContext(InputSplit split,
                JobContext jobContext) throws IOException,InterruptedException {
        Configuration conf = jobContext.getConfiguration();

    	TableExpr expr = getInputExpr( conf );
    	if (expr == null) {
    		throw new IOException("Table expression not defined");
    	}

    	if ( getSorted( conf ) )
    		expr.setSortedSplit();

    	String strProj = conf.get(INPUT_PROJ);
    	String projection = null;
    	try {
    		if (strProj == null) {
    			projection = expr.getSchema(conf).toProjectionString();
    			ZebraProjection proj = ZebraProjection.createZebraProjection( projection );
    			TableInputFormat.setProjection( jobContext, proj );
    		} else {
    			projection = strProj;
    		}
    	} catch (ParseException e) {
    		throw new IOException("Projection parsing failed : "+e.getMessage());
    	}

    	try {
    		return new TableRecordReader(expr, projection, split, jobContext);
    	} catch (ParseException e) {
    		throw new IOException("Projection parsing faile : "+e.getMessage());
    	}
    }
  
    private static TableExpr getInputExpr(Configuration conf) throws IOException {
    	String expr = conf.get(INPUT_EXPR);
    	if (expr == null) {
    		throw new IllegalArgumentException("Input expression not defined.");
    	}
    	StringReader in = new StringReader(expr);
    	return TableExpr.parse(in);

    }

    /**
     * Get a TableRecordReader on a single split
     * 
     * @param jobContext
     *          JobContext object.
     * @param projection
     *          comma-separated column names in projection. null means all columns in projection
     */
    public static TableRecordReader createTableRecordReader(JobContext jobContext, String projection)
    throws IOException, ParseException, InterruptedException {
    	if (projection != null)
    		setProjection( jobContext, ZebraProjection.createZebraProjection( projection ) );
    	TableInputFormat inputFormat = new TableInputFormat();

    	// a single split is needed
    	List<InputSplit> splits = inputFormat.getSplits( jobContext, true );
    	if( splits.size() != 1 )
    		throw new IOException("Unable to generated one single split for the sorted table (internal error)" );
    	return (TableRecordReader)inputFormat.createRecordReaderFromJobContext( splits.get( 0 ), jobContext );
    }

  private static List<InputSplit> getSortedSplits(Configuration conf, int numSplits,
      TableExpr expr, List<BasicTable.Reader> readers,
      List<BasicTableStatus> status) throws IOException {
    if (expr.sortedSplitRequired() && !expr.sortedSplitCapable()) {
      throw new IOException("Unable to created sorted splits");
    }

    long totalBytes = 0;
    for (Iterator<BasicTableStatus> it = status.iterator(); it.hasNext();) {
      BasicTableStatus s = it.next();
      totalBytes += s.getSize();
    }

    long maxSplits = totalBytes / getMinSplitSize( conf );
    List<InputSplit> splits = new ArrayList<InputSplit>();
    if (maxSplits == 0)                                                                               
        numSplits = 1;                 
    else if (numSplits > maxSplits) {
        numSplits = -1;                
    }    

    for (Iterator<BasicTable.Reader> it = readers.iterator(); it.hasNext();) {
      BasicTable.Reader reader = it.next();
      if (!reader.isSorted()) {
        throw new IOException("Attempting sorted split on unsorted table");
      }
    }

    if (numSplits == 1) {
      BlockDistribution bd = null;
      for (Iterator<BasicTable.Reader> it = readers.iterator(); it.hasNext();) {
        BasicTable.Reader reader = it.next();
        bd = BlockDistribution.sum(bd, reader.getBlockDistribution((RangeSplit) null));
      }
      
      SortedTableSplit split = new SortedTableSplit(0, null, null, bd, conf);
      splits.add(split);
      return splits;
    }
    
    // TODO: Does it make sense to interleave keys for all leaf tables if
    // numSplits <= 0 ?
    int nLeaves = readers.size();
    BlockDistribution lastBd = new BlockDistribution();
    ArrayList<KeyDistribution> btKeyDistributions = new ArrayList<KeyDistribution>();
    for (int i = 0; i < nLeaves; ++i) {
      KeyDistribution btKeyDistri =
          readers.get(i).getKeyDistribution(
              (numSplits <= 0) ? -1 :
              Math.max(numSplits * 5 / nLeaves, numSplits), nLeaves, lastBd);
      btKeyDistributions.add(btKeyDistri);
    }
    
    int btSize = btKeyDistributions.size();
    KeyDistribution[] btKds = new KeyDistribution[btSize];
    Object[] btArray = btKeyDistributions.toArray();
    for (int i = 0; i < btSize; i++)
      btKds[i] = (KeyDistribution) btArray[i];

    KeyDistribution keyDistri = KeyDistribution.merge(btKds);

    if (keyDistri == null) {
      // should never happen.
       return splits;
    }

    keyDistri.resize(lastBd);

    RawComparable[] keys = keyDistri.getKeys();
    for (int i = 0; i <= keys.length; ++i) {
      RawComparable begin = (i == 0) ? null : keys[i - 1];
      RawComparable end = (i == keys.length) ? null : keys[i];
      BlockDistribution bd;
      if (i < keys.length)
        bd = keyDistri.getBlockDistribution(keys[i]);
      else
        bd = lastBd;
      BytesWritable beginB = null, endB = null;
      if (begin != null)
        beginB = new BytesWritable(begin.buffer());
      if (end != null)
        endB = new BytesWritable(end.buffer());
      SortedTableSplit split = new SortedTableSplit(i, beginB, endB, bd, conf);
      splits.add(split);
    }
    LOG.info("getSplits : returning " + splits.size() + " sorted splits.");
    return splits;
  }
  
  static long getMinSplitSize(Configuration conf) {
    return conf.getLong("table.input.split.minSize", 1 * 1024 * 1024L);
  }

  /**
   * Set the minimum split size.
   * 
   * @param jobContext
   *          The job conf object.
   * @param minSize
   *          Minimum size.
   */
  public static void setMinSplitSize(JobContext jobContext, long minSize) {
    jobContext.getConfiguration().setLong("table.input.split.minSize", minSize);
  }
  
  private static class DummyFileInputFormat extends FileInputFormat<BytesWritable, Tuple> {
    /**
      * the next constant and class are copies from FileInputFormat
      */
    private static final PathFilter hiddenFileFilter = new PathFilter(){
        public boolean accept(Path p){
          String name = p.getName(); 
          return !name.startsWith("_") && !name.startsWith("."); 
        }
      }; 
 
    /**
     * Proxy PathFilter that accepts a path only if all filters given in the
     * constructor do. Used by the listPaths() to apply the built-in
     * hiddenFileFilter together with a user provided one (if any).
     */
    private static class MultiPathFilter implements PathFilter {
      private List<PathFilter> filters;
 
      public MultiPathFilter(List<PathFilter> filters) {
        this.filters = filters;
      }
 
      public boolean accept(Path path) {
        for (PathFilter filter : filters) {
          if (!filter.accept(path)) {
            return false;
          }
        }
        return true;
      }
    }
    private Integer[] fileNumbers = null;
 
    private List<BasicTable.Reader> readers;
 
    public Integer[] getFileNumbers() {
      return fileNumbers;
    }
 
    public DummyFileInputFormat(Job job, long minSplitSize, List<BasicTable.Reader> readers) {
     super.setMinInputSplitSize(job, minSplitSize);
     this.readers = readers;
    }
    

    @Override
    public RecordReader<BytesWritable, Tuple> createRecordReader(
    InputSplit arg0, TaskAttemptContext arg1) throws IOException,
        InterruptedException {
       // no-op
      return null;
    }

    @Override
    public long computeSplitSize(long blockSize, long minSize, long maxSize) {
      return super.computeSplitSize(blockSize, minSize, maxSize);
    }

    /**
     * copy from FileInputFormat: add assignment to table file numbers
     */
    @Override
    public List<FileStatus> listStatus(JobContext jobContext) throws IOException {
      Configuration job = jobContext.getConfiguration();
      Path[] dirs = getInputPaths(jobContext);
      if (dirs.length == 0) {
        throw new IOException("No input paths specified in job");
      }

      List<FileStatus> result = new ArrayList<FileStatus>();
      List<IOException> errors = new ArrayList<IOException>();
      
      // creates a MultiPathFilter with the hiddenFileFilter and the
      // user provided one (if any).
      List<PathFilter> filters = new ArrayList<PathFilter>();
      filters.add(hiddenFileFilter);
      PathFilter jobFilter = getInputPathFilter(jobContext);
      if (jobFilter != null) {
        filters.add(jobFilter);
      }
      PathFilter inputFilter = new MultiPathFilter(filters);

      ArrayList<Integer> fileNumberList  = new ArrayList<Integer>();
      int index = 0;
      for (Path p: dirs) {
        FileSystem fs = p.getFileSystem(job); 
        FileStatus[] matches = fs.globStatus(p, inputFilter);
        if (matches == null) {
          errors.add(new IOException("Input path does not exist: " + p));
        } else if (matches.length == 0) {
          errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
        } else {
          for (FileStatus globStat: matches) {
            if (globStat.isDir()) {
              FileStatus[] fileStatuses = fs.listStatus(globStat.getPath(), inputFilter);
              // reorder according to CG index
              BasicTable.Reader reader = readers.get(index);
              if (fileStatuses.length > 1)
                reader.rearrangeFileIndices(fileStatuses);
              for(FileStatus stat: fileStatuses) {
                if (stat != null)
                  result.add(stat);
              }
              fileNumberList.add(fileStatuses.length);
            } else {
              result.add(globStat);
              fileNumberList.add(1);
            }
          }
        }
        index++;
      }
      fileNumbers = new Integer[fileNumberList.size()];
      fileNumberList.toArray(fileNumbers);

      if (!errors.isEmpty()) {
        throw new InvalidInputException(errors);
      }
      LOG.info("Total input paths to process : " + result.size()); 
      return result;
    }
  }
  
  private static List<InputSplit> getRowSplits(Configuration conf,
      TableExpr expr, List<BasicTable.Reader> readers,
      List<BasicTableStatus> status) throws IOException {
    ArrayList<InputSplit> ret = new ArrayList<InputSplit>();
    Job job = new Job(conf);
    long minSplitSize = getMinSplitSize(conf);
  
    long minSize = Math.max(conf.getLong("mapreduce.min.split.size", 1), minSplitSize);
    long totalBytes = 0;
    for (Iterator<BasicTableStatus> it = status.iterator(); it.hasNext(); )
    {
      totalBytes += it.next().getSize();
    }
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    PathFilter filter = null;
    List<BasicTable.Reader> realReaders = new ArrayList<BasicTable.Reader>();
    int[] realReaderIndices = new int[readers.size()];

    for (int i = 0; i < readers.size(); ++i) {
      BasicTable.Reader reader = readers.get(i);
      /* Get the index of the column group that will be used for row-split.*/
      int splitCGIndex = reader.getRowSplitCGIndex();
      
      /* We can create input splits only if there does exist a valid column group for split.
       * Otherwise, we do not create input splits. */
      if (splitCGIndex >= 0) {
        realReaderIndices[realReaders.size()] = i;
        realReaders.add(reader);
         if (first)
         {
           // filter is identical across tables
           filter = reader.getPathFilter(conf);
           first = false;
         } else
           sb.append(",");
         sb.append(reader.getPath().toString() + "/" + reader.getName(splitCGIndex));
       }
     }
     
     DummyFileInputFormat helper = new DummyFileInputFormat(job,minSplitSize, realReaders);
 
     if (!realReaders.isEmpty())
     {
       DummyFileInputFormat.setInputPaths(job, sb.toString());
       DummyFileInputFormat.setInputPathFilter(job, filter.getClass());
       List<InputSplit> inputSplitList = helper.getSplits(job);
       InputSplit[] inputSplits = inputSplitList.toArray(new InputSplit[0]);
 
       /*
        * Potential file batching optimizations include:
        * 1) sort single file inputSplits in the descending order of their sizes so
        *    that the ops of new file opens are spread to a maximum degree;
        * 2) batching the files with maximum block distribution affinities into the same input split
        */
 
       int[] inputSplitBoundaries = new int[realReaders.size()];
       long start, prevStart = Long.MIN_VALUE;
       int tableIndex = 0, fileNumber = 0;
       Integer[] fileNumbers = helper.getFileNumbers();
       if (fileNumbers.length != realReaders.size())
         throw new IOException("Number of tables in input paths of input splits is incorrect.");
       for (int j=0; j<inputSplits.length; j++) {
         FileSplit fileSplit = (FileSplit) inputSplits[j];
         start = fileSplit.getStart();
         if (start <= prevStart)
         {
           fileNumber++;
           if (fileNumber >= fileNumbers[tableIndex])
           {
             inputSplitBoundaries[tableIndex++] = j;
             fileNumber = 0;
           }
         }
         prevStart = start;
       }
       inputSplitBoundaries[tableIndex++] =  inputSplits.length;
       if (tableIndex != realReaders.size())
         throw new IOException("Number of tables in input splits is incorrect.");
       for (tableIndex = 0; tableIndex < realReaders.size(); tableIndex++)
       {
         int startSplitIndex = (tableIndex == 0 ? 0 : inputSplitBoundaries[tableIndex - 1]);
         int splitLen = (tableIndex == 0 ? inputSplitBoundaries[0] :
             inputSplitBoundaries[tableIndex] - inputSplitBoundaries[tableIndex-1]);
         BasicTable.Reader reader = realReaders.get(tableIndex);
         /* Get the index of the column group that will be used for row-split.*/
         int splitCGIndex = reader.getRowSplitCGIndex();
         
         long starts[] = new long[splitLen];
         long lengths[] = new long[splitLen];
         int batches[] = new int[splitLen + 1];
         batches[0] = 0;
         int numBatches = 0;
         Path paths[] = new Path [splitLen];
         long totalLen = 0;
         final double SPLIT_SLOP = 1.1;
         int endSplitIndex = startSplitIndex + splitLen;
         for (int j=startSplitIndex; j< endSplitIndex; j++) {
          FileSplit fileSplit = (FileSplit) inputSplits[j];
          Path p = fileSplit.getPath();
          long blockSize = p.getFileSystem(conf).getBlockSize(p);
          long splitSize = (long) (helper.computeSplitSize(blockSize, minSize, totalBytes) * SPLIT_SLOP);
          start = fileSplit.getStart();
          long length = fileSplit.getLength();
          int index = j - startSplitIndex;
          starts[index] = start;
          lengths[index] = length;
          totalLen += length;
          paths[index] = p;
          if (totalLen >= splitSize)
          {

             for (int ii = batches[numBatches] + 1; ii < index - 1; ii++)
               starts[ii] = -1; // all intermediate files are not split
             batches[++numBatches] = index;
             totalLen = length;
          }
        }
        for (int ii = batches[numBatches] + 1; ii < splitLen - 1; ii++)
          starts[ii] = -1; // all intermediate files are not split
        if (splitLen > 0)
          batches[++numBatches] = splitLen;
        
        List<RowSplit> subSplits = reader.rowSplit(starts, lengths, paths, splitCGIndex,
            batches, numBatches);
        
        int realTableIndex = realReaderIndices[tableIndex];
        for (Iterator<RowSplit> it = subSplits.iterator(); it.hasNext();) {
          RowSplit subSplit = it.next();
          RowTableSplit split = new RowTableSplit(reader, subSplit, realTableIndex, conf);
          ret.add(split);
        }
      }
    }

    LOG.info("getSplits : returning " + ret.size() + " row splits.");
    return ret;
  }

    /**
     * @see InputFormat#getSplits(JobContext)
     */
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
	    return getSplits( jobContext, false );
    }

    List<InputSplit> getSplits(JobContext jobContext, boolean singleSplit) throws IOException {
    	Configuration conf = jobContext.getConfiguration();
    	TableExpr expr = getInputExpr( conf );
    	if( getSorted(conf) ) {
    		expr.setSortedSplit();
    	} else if( singleSplit ) {
    		throw new IOException( "Table must be sorted in order to get a single sorted split" );
    	}

    	if( expr.sortedSplitRequired() && !expr.sortedSplitCapable() ) {
    		throw new IOException( "Unable to created sorted splits" );
    	}

    	String projection;
    	try {
    		projection = getProjection(jobContext);
    	} catch (ParseException e) {
    		throw new IOException("getProjection failed : "+e.getMessage());
    	}
    	List<LeafTableInfo> leaves = expr.getLeafTables(projection);
    	int nLeaves = leaves.size();
    	ArrayList<BasicTable.Reader> readers =
    		new ArrayList<BasicTable.Reader>(nLeaves);
    	ArrayList<BasicTableStatus> status =
    		new ArrayList<BasicTableStatus>(nLeaves);

    	try {
        boolean sorted = expr.sortedSplitRequired();
    		for (Iterator<LeafTableInfo> it = leaves.iterator(); it.hasNext();) {
    			LeafTableInfo leaf = it.next();
    			BasicTable.Reader reader =
    				new BasicTable.Reader(leaf.getPath(), conf );
    			reader.setProjection(leaf.getProjection());
    			BasicTableStatus s = reader.getStatus();
    		    status.add(s);
    			readers.add(reader);
    		}

    		if( readers.isEmpty() ) {
    			// I think we should throw exception here.
    			return new ArrayList<InputSplit>();
    		}

        List<InputSplit> result;
        if (sorted)
        {
          if (singleSplit)
            result = getSortedSplits( conf, 1, expr, readers, status);
          else if (globalOrderingRequired(jobContext))
            result = getSortedSplits(conf, -1, expr, readers, status);
          else
            result = getRowSplits( conf, expr, readers, status);
        } else
          result = getRowSplits( conf, expr, readers, status);

        return result;

    	} catch (ParseException e) {
    		throw new IOException("Projection parsing failed : "+e.getMessage());
    	} finally {
    		for (Iterator<BasicTable.Reader> it = readers.iterator(); it.hasNext();) {
    			try {
    				it.next().close();
    			}
    			catch (Exception e) {
    				e.printStackTrace();
    				// TODO: log the error here.
    			}
    		}
    	}
    }

  @Deprecated
  public synchronized void validateInput(JobContext jobContext) throws IOException {
    // Validating imports by opening all Tables.
    TableExpr expr = getInputExpr(jobContext);
    try {
      String projection = getProjection(jobContext);
      List<LeafTableInfo> leaves = expr.getLeafTables(projection);
      Iterator<LeafTableInfo> iterator = leaves.iterator();
      while (iterator.hasNext()) {
        LeafTableInfo leaf = iterator.next();
        BasicTable.Reader reader =
            new BasicTable.Reader(leaf.getPath(), jobContext.getConfiguration());
        reader.setProjection(projection);
        reader.close();
      }
    } catch (ParseException e) {
    	throw new IOException("Projection parsing failed : "+e.getMessage());
    }
  }

  	/**
  	 * Get a comparable object from the given InputSplit object.
  	 * 
  	 * @param inputSplit An InputSplit instance. It should be type of SortedTableSplit.
  	 * @return a comparable object of type WritableComparable
  	 */
	public static WritableComparable<?> getSortedTableSplitComparable(InputSplit inputSplit) {
        SortedTableSplit split = null;
        if( inputSplit instanceof SortedTableSplit ) {
            split = (SortedTableSplit)inputSplit;
        } else {
            throw new RuntimeException( "LoadFunc expected split of type [" + 
            		SortedTableSplit.class.getCanonicalName() + "]" );
        }
		return new SortedTableSplitComparable( split.getIndex() );
	}

}

/**
 * Adaptor class for sorted InputSplit for table.
 */
class SortedTableSplit extends InputSplit implements Writable {
	// the order of the split in all splits generated.
	private int index;
	
	BytesWritable begin = null, end = null;
  
  String[] hosts;
  long length = 1;

  public SortedTableSplit()
  {
    // no-op for Writable construction
  }
  
  public SortedTableSplit(int index, BytesWritable begin, BytesWritable end,
      BlockDistribution bd, Configuration conf) {
	  this.index = index;
	  
    if (begin != null) {
      this.begin = new BytesWritable();
      this.begin.set(begin.getBytes(), 0, begin.getLength());
    }
    if (end != null) {
      this.end = new BytesWritable();
      this.end.set(end.getBytes(), 0, end.getLength());
    }
    
    if (bd != null) {
      length = bd.getLength();
      hosts =
        bd.getHosts( conf.getInt("mapred.lib.table.input.nlocation", 5) );
    }
  }
  
	public int getIndex() {
		return index;
	}

  @Override
  public long getLength() throws IOException {
    return length;
  }

  @Override
  public String[] getLocations() throws IOException {
    if (hosts == null)
    {
      String[] tmp = new String[1];
      tmp[0] = "";
      return tmp;
    }
    return hosts;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    begin = end = null;
    int bool = WritableUtils.readVInt(in);
    if (bool == 1) {
      begin = new BytesWritable();
      begin.readFields(in);
    }
    bool = WritableUtils.readVInt(in);
    if (bool == 1) {
      end = new BytesWritable();
      end.readFields(in);
    }
    length = WritableUtils.readVLong(in);
    int size = WritableUtils.readVInt(in);
    if (size > 0)
      hosts = new String[size];
    for (int i = 0; i < size; i++)
    	hosts[i] = WritableUtils.readString(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (begin == null) {
      WritableUtils.writeVInt(out, 0);
    }
    else {
      WritableUtils.writeVInt(out, 1);
      begin.write(out);
    }
    if (end == null) {
      WritableUtils.writeVInt(out, 0);
    }
    else {
      WritableUtils.writeVInt(out, 1);
      end.write(out);
    }
    WritableUtils.writeVLong(out, length);
    WritableUtils.writeVInt(out, hosts == null ? 0 : hosts.length);
    for (int i = 0; i < hosts.length; i++)
    {
    	WritableUtils.writeString(out, hosts[i]);
    }
  }
  
  public BytesWritable getBegin() {
    return begin;
  }

  public BytesWritable getEnd() {
    return end;
  }
}

/**
 * Adaptor class for unsorted InputSplit for table.
 */
class RowTableSplit extends InputSplit implements Writable{
  /**
     * 
     */
  String path = null;
  int tableIndex = 0;
  RowSplit split = null;
  String[] hosts = null;
  long length = 1;

  public RowTableSplit(Reader reader, RowSplit split, int tableIndex, Configuration conf)
      throws IOException {
    this.path = reader.getPath();
    this.split = split;
    this.tableIndex = tableIndex;
    BlockDistribution dataDist = reader.getBlockDistribution(split);
    if (dataDist != null) {
      length = dataDist.getLength();
      hosts =
          dataDist.getHosts(conf.getInt("mapred.lib.table.input.nlocation", 5));
    }
  }
  
  public RowTableSplit() {
    // no-op for Writable construction
  }
  
  @Override
  public long getLength() throws IOException {
    return length;
  }

  @Override
  public String[] getLocations() throws IOException {
    return hosts;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    tableIndex = WritableUtils.readVInt(in); 
    path = WritableUtils.readString(in);
    int bool = WritableUtils.readVInt(in);
    if (bool == 1) {
      if (split == null) split = new RowSplit();
      split.readFields(in);
    }
    else {
      split = null;
    }
    hosts = WritableUtils.readStringArray(in);
    length = WritableUtils.readVLong(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, tableIndex);
    WritableUtils.writeString(out, path);
    if (split == null) {
      WritableUtils.writeVInt(out, 0);
    }
    else {
      WritableUtils.writeVInt(out, 1);
      split.write(out);
    }
    WritableUtils.writeStringArray(out, hosts);
    WritableUtils.writeVLong(out, length);
  }

  public String getPath() {
    return path;
  }
  
  public RowSplit getSplit() {
    return split;
  }
  
  public int getTableIndex() {
    return tableIndex;
  }
}
