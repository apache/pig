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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.file.tfile.RawComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.BasicTableStatus;
import org.apache.hadoop.zebra.io.BlockDistribution;
import org.apache.hadoop.zebra.io.KeyDistribution;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.io.BasicTable.Reader;
import org.apache.hadoop.zebra.io.BasicTable.Reader.RangeSplit;
import org.apache.hadoop.zebra.mapred.TableExpr.LeafTableInfo;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.hadoop.zebra.types.SortInfo;
import org.apache.hadoop.zebra.mapred.TableExpr.LeafTableInfo;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;

/**
 * {@link org.apache.hadoop.mapred.InputFormat} class for reading one or more
 * BasicTables.
 * 
 * Usage Example:
 * <p>
 * In the main program, add the following code.
 * 
 * <pre>
 * jobConf.setInputFormat(TableInputFormat.class);
 * TableInputFormat.setInputPaths(jobConf, new Path(&quot;path/to/table1&quot;, new Path(&quot;path/to/table2&quot;);
 * TableInputFormat.setProjection(jobConf, &quot;Name, Salary, BonusPct&quot;);
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
 *   public void configure(JobConf job) {
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
public class TableInputFormat implements InputFormat<BytesWritable, Tuple> {
  private static final String INPUT_EXPR = "mapred.lib.table.input.expr";
  private static final String INPUT_PROJ = "mapred.lib.table.input.projection";
  private static final String INPUT_SORT = "mapred.lib.table.input.sort";

  /**
   * Set the paths to the input table.
   * 
   * @param conf
   *          JobConf object.
   * @param paths
   *          one or more paths to BasicTables. The InputFormat class will
   *          produce splits on the "union" of these BasicTables.
   */
  public static void setInputPaths(JobConf conf, Path... paths) {
    if (paths.length < 1) {
      throw new IllegalArgumentException("Requring at least one input path");
    }
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
  }
  
  //This method escapes commas in the glob pattern of the given paths. 
  private static String[] getPathStrings(String commaSeparatedPaths) {
    int length = commaSeparatedPaths.length();
    int curlyOpen = 0;
    int pathStart = 0;
    boolean globPattern = false;
    List<String> pathStrings = new ArrayList<String>();
    
    for (int i=0; i<length; i++) {
      char ch = commaSeparatedPaths.charAt(i);
      switch(ch) {
        case '{' : {
          curlyOpen++;
          if (!globPattern) {
            globPattern = true;
          }
          break;
        }
        case '}' : {
          curlyOpen--;
          if (curlyOpen == 0 && globPattern) {
            globPattern = false;
          }
          break;
        }
        case ',' : {
          if (!globPattern) {
            pathStrings.add(commaSeparatedPaths.substring(pathStart, i));
            pathStart = i + 1 ;
          }
          break;
        }
      }
    }
    pathStrings.add(commaSeparatedPaths.substring(pathStart, length));
    
    return pathStrings.toArray(new String[0]);
  }

  /**
   * Set the input expression in the JobConf object.
   * 
   * @param conf
   *          JobConf object.
   * @param expr
   *          The input table expression.
   */
  static void setInputExpr(JobConf conf, TableExpr expr) {
    StringBuilder out = new StringBuilder();
    expr.encode(out);
    conf.set(INPUT_EXPR, out.toString());
  }

  static TableExpr getInputExpr(JobConf conf) throws IOException {
    String expr = conf.get(INPUT_EXPR);
    if (expr == null) {
      throw new IllegalArgumentException("Input expression not defined.");
    }
    StringReader in = new StringReader(expr);
    return TableExpr.parse(in);
  }
  
  /**
   * Get the schema of a table expr
   * 
   * @param conf
   *          JobConf object.
   */
  public static Schema getSchema(JobConf conf) throws IOException
  {
	  TableExpr expr = getInputExpr(conf);
	  return expr.getSchema(conf);
  }

  /**
   * Set the input projection in the JobConf object.
   * 
   * @param conf
   *          JobConf object.
   * @param projection
   *          A common separated list of column names. If we want select all
   *          columns, pass projection==null. The syntax of the projection
   *          conforms to the {@link Schema} string.
   * @see Schema#Schema(String)
   */
  public static void setProjection(JobConf conf, String projection) throws ParseException{
    conf.set(INPUT_PROJ, Schema.normalize(projection));
  }

  /**
   * Get the projection from the JobConf
   * 
   * @param conf
   *          The JobConf object
   * @return The projection schema. If projection has not been defined, or is
   *         not known at this time, null will be returned. Note that by the time
   *         when this method is called in Mapper code, the projection must
   *         already be known.
   * @throws IOException
   */
  public static String getProjection(JobConf conf) throws IOException, ParseException {
    String strProj = conf.get(INPUT_PROJ);
    // TODO: need to be revisited
    if (strProj != null) return strProj;
    TableExpr expr = getInputExpr(conf);
    if (expr != null) {
      return expr.getSchema(conf).toProjectionString();
    }
    return null;
  }
  
  /**
   * Set requirement for sorted table
   *
   *@param conf
   *          JobConf object.
   */
  private static void setSorted(JobConf conf) {
    conf.setBoolean(INPUT_SORT, true);
  }
  
  /**
   * Get the SortInfo object regarding a Zebra table
   *
   * @param conf
   *          JobConf object
   * @return the zebra tables's SortInfo; null if the table is unsorted.
   */
  public static SortInfo getSortInfo(JobConf conf) throws IOException
  {
	  TableExpr expr = getInputExpr(conf);
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
   * @param conf
   *          JobConf object.
   * @param sortcolumns
   *          Sort column names.
   * @param comparator
   *          Comparator name. Null means the caller does not care but table union will check
   *          identical comparators regardless as a minimum sanity check
   *        
   */
  public static void requireSortedTable(JobConf conf, String sortcolumns, String comparator) throws IOException {
	 TableExpr expr = getInputExpr(conf);
	 if (expr instanceof BasicTableExpr)
	 {
		 BasicTable.Reader reader = new BasicTable.Reader(((BasicTableExpr) expr).getPath(), conf);
		 SortInfo sortInfo = reader.getSortInfo();
		 reader.close();
		 if (comparator == null && sortInfo != null)
			 // cheat the equals method's comparator comparison
			 comparator = sortInfo.getComparator();
		 if (sortInfo == null || !sortInfo.equals(sortcolumns, comparator))
		 {
			 throw new IOException("The table is not (properly) sorted");
		 }
	 } else {
		 List<LeafTableInfo> leaves = expr.getLeafTables(null);
		 for (Iterator<LeafTableInfo> it = leaves.iterator(); it.hasNext(); )
		 {
			 LeafTableInfo leaf = it.next();
			 BasicTable.Reader reader = new BasicTable.Reader(leaf.getPath(), conf);
			 SortInfo sortInfo = reader.getSortInfo();
			 reader.close();
			 if (comparator == null && sortInfo != null)
				 comparator = sortInfo.getComparator(); // use the first table's comparator as comparison base
			 if (sortInfo == null || !sortInfo.equals(sortcolumns, comparator))
			 {
				 throw new IOException("The table is not (properly) sorted");
			 }
		 }
		 // need key range input splits for sorted table union
		 setSorted(conf);
	 }
  }
  
  /**
   * Requires sorted table or table union. For table union, leading sort columns 
   * of component tables need to be the same
   * 
   * @param conf
   *          JobConf object.
   *        
   */
  public static void requireSortedTable(JobConf conf) throws IOException {
	 TableExpr expr = getInputExpr(conf);
	 if (expr instanceof BasicTableExpr)
	 {
		 BasicTable.Reader reader = new BasicTable.Reader(((BasicTableExpr) expr).getPath(), conf);
		 SortInfo sortInfo = reader.getSortInfo();
		 reader.close();
		 if (sortInfo == null)
		 {
			 throw new IOException("The table is not (properly) sorted");
		 }
	 } else {
		 List<LeafTableInfo> leaves = expr.getLeafTables(null);
		 String sortcolumns = null, comparator = null;
		 for (Iterator<LeafTableInfo> it = leaves.iterator(); it.hasNext(); )
		 {
			 LeafTableInfo leaf = it.next();
			 BasicTable.Reader reader = new BasicTable.Reader(leaf.getPath(), conf);
			 SortInfo sortInfo = reader.getSortInfo();
			 reader.close();
			 if (sortInfo == null)
			 {
				 throw new IOException("The table is not (properly) sorted");
			 }
			 // check the compatible sort info among the member tables of a union
			 if (sortcolumns == null)
			 {
				 sortcolumns = SortInfo.toSortString(sortInfo.getSortColumnNames());
				 comparator = sortInfo.getComparator();
			 } else {
				 if (!sortInfo.equals(sortcolumns, comparator))
				 {
					 throw new IOException("The table is not (properly) sorted");
				 }
			 }
		 }
		 // need key range input splits for sorted table union
		 setSorted(conf);
	 }
  }
  /**
   * Set requirement for sorted table
   *
   *@param conf
   *          JobConf object.
   */
  private static boolean getSorted(JobConf conf) {
    return conf.getBoolean(INPUT_SORT, false);
  }

  /**
   * @see InputFormat#getRecordReader(InputSplit, JobConf, Reporter)
   */
  @Override
  public RecordReader<BytesWritable, Tuple> getRecordReader(InputSplit split,
      JobConf conf, Reporter reporter) throws IOException {
  TableExpr expr = getInputExpr(conf);
    if (expr == null) {
      throw new IOException("Table expression not defined");
    }

    if (getSorted(conf))
      expr.setSortedSplit();

    String strProj = conf.get(INPUT_PROJ);
    String projection = null;
    try {
      if (strProj == null) {
        projection = expr.getSchema(conf).toProjectionString();
        TableInputFormat.setProjection(conf, projection);
      } else {
        projection = strProj;
      }
    } catch (ParseException e) {
    	throw new IOException("Projection parsing failed : "+e.getMessage());
    }

    try {
      return new TableRecordReader(expr, projection, split, conf);
    } catch (ParseException e) {
    	throw new IOException("Projection parsing faile : "+e.getMessage());
    }
  }
  
  /**
   * Get a TableRecordReader on a single split
   * 
   * @param conf
   *          JobConf object.
   * @param projection
   *          comma-separated column names in projection. null means all columns in projection
   */
  
  public TableRecordReader getTableRecordReader(JobConf conf, String projection) throws IOException, ParseException
  {
	// a single split is needed
    if (projection != null)
    	setProjection(conf, projection);
    TableInputFormat inputFormat = new TableInputFormat();
    InputSplit[] splits = inputFormat.getSplits(conf, 1);
    return (TableRecordReader) getRecordReader(splits[0], conf, Reporter.NULL);
  }

  private static InputSplit[] getSortedSplits(JobConf conf, int numSplits,
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

    long maxSplits = totalBytes / getMinSplitSize(conf);

    if (numSplits > maxSplits) {
      numSplits = -1;
    }

    ArrayList<InputSplit> splits = new ArrayList<InputSplit>();

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
        bd = BlockDistribution.sum(bd, reader.getBlockDistribution(null));
      }
      
      if (bd == null) {
        // should never happen.
        return new InputSplit[0];
      }
      
      SortedTableSplit split = new SortedTableSplit(null, null, bd, conf);
      return new InputSplit[] { split };
    }
    
    // TODO: Does it make sense to interleave keys for all leaf tables if
    // numSplits <= 0 ?
    int nLeaves = readers.size();
    KeyDistribution keyDistri = null;
    for (int i = 0; i < nLeaves; ++i) {
      KeyDistribution btKeyDistri =
          readers.get(i).getKeyDistribution(
              (numSplits <= 0) ? -1 :
              Math.max(numSplits * 5 / nLeaves, numSplits));
      keyDistri = KeyDistribution.sum(keyDistri, btKeyDistri);
    }
    
    if (keyDistri == null) {
      // should never happen.
      return new InputSplit[0];
    }
    
    if (numSplits > 0) {
      keyDistri.resize(numSplits);
    }
    
    RawComparable[] rawKeys = keyDistri.getKeys();
    BytesWritable[] keys = new BytesWritable[rawKeys.length];
    for (int i=0; i<keys.length; ++i) {
      RawComparable rawKey = rawKeys[i];
      keys[i] = new BytesWritable();
      keys[i].setSize(rawKey.size());
      System.arraycopy(rawKey.buffer(), rawKey.offset(), keys[i].get(), 0,
          rawKey.size());
    }
    
    // TODO: Should we change to RawComparable to avoid the creation of
    // BytesWritables?
    for (int i = 0; i < keys.length; ++i) {
      BytesWritable begin = (i == 0) ? null : keys[i - 1];
      BytesWritable end = (i == keys.length - 1) ? null : keys[i];
      BlockDistribution bd = keyDistri.getBlockDistribution(keys[i]);
      SortedTableSplit split = new SortedTableSplit(begin, end, bd, conf);
      splits.add(split);
    }
    return splits.toArray(new InputSplit[splits.size()]);
  }
  
  static long getMinSplitSize(JobConf conf) {
    return conf.getLong("table.input.split.minSize", 1 * 1024 * 1024L);
  }

  /**
   * Set the minimum split size.
   * 
   * @param conf
   *          The job conf object.
   * @param minSize
   *          Minimum size.
   */
  public static void setMinSplitSize(JobConf conf, long minSize) {
    conf.setLong("table.input.split.minSize", minSize);
  }
  
  private static InputSplit[] getUnsortedSplits(JobConf conf, int numSplits,
      TableExpr expr, List<BasicTable.Reader> readers,
      List<BasicTableStatus> status) throws IOException {
    long totalBytes = 0;
    for (Iterator<BasicTableStatus> it = status.iterator(); it.hasNext();) {
      BasicTableStatus s = it.next();
      totalBytes += s.getSize();
    }

    long maxSplits = totalBytes / getMinSplitSize(conf);

    if (numSplits > maxSplits) {
      numSplits = -1;
    }

    ArrayList<InputSplit> ret = new ArrayList<InputSplit>();;
    if (numSplits <= 0) {
      for (int i = 0; i < readers.size(); ++i) {
        BasicTable.Reader reader = readers.get(i);
        List<RangeSplit> subSplits = reader.rangeSplit(-1);
        for (Iterator<RangeSplit> it = subSplits.iterator(); it.hasNext();) {
          UnsortedTableSplit split =
              new UnsortedTableSplit(reader, it.next(), conf);
          ret.add(split);
        }
      }
    } else {
      long goalSize = totalBytes / numSplits;

      double SPLIT_SLOP = 1.1;
      for (int i = 0; i < readers.size(); ++i) {
        BasicTable.Reader reader = readers.get(i);
        BasicTableStatus s = status.get(i);
        int nSplits =
            (int) ((s.getSize() + goalSize * (2 - SPLIT_SLOP)) / goalSize);
        if (nSplits > 1) {
          List<RangeSplit> subSplits = reader.rangeSplit(nSplits);
          for (Iterator<RangeSplit> it = subSplits.iterator(); it.hasNext();) {
            UnsortedTableSplit split =
                new UnsortedTableSplit(reader, it.next(), conf);
            ret.add(split);
          }
        } else {
          UnsortedTableSplit split = new UnsortedTableSplit(reader, null, conf);
          ret.add(split);
        }
      }
    }

    return ret.toArray(new InputSplit[ret.size()]);
  }

  /**
   * @see InputFormat#getSplits(JobConf, int)
   */
  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits)
      throws IOException {
    TableExpr expr = getInputExpr(conf);
    if (getSorted(conf))
      expr.setSortedSplit();
    if (expr.sortedSplitRequired() && !expr.sortedSplitCapable()) {
      throw new IOException("Unable to created sorted splits");
    }
    
    String projection;
    try {
      projection = getProjection(conf);
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
      for (Iterator<LeafTableInfo> it = leaves.iterator(); it.hasNext();) {
        LeafTableInfo leaf = it.next();
        BasicTable.Reader reader =
          new BasicTable.Reader(leaf.getPath(), conf);
        reader.setProjection(leaf.getProjection());
        BasicTableStatus s = reader.getStatus();
        if ((s.getRows() != 0) && (s.getSize() > 0)) {
          // skipping empty tables.
          // getRows() may return -1 to indicate unknown row numbers.
          readers.add(reader);
          status.add(s);
        } else {
          reader.close();
        }
      }
      
      if (readers.isEmpty()) {
        return new InputSplit[0];
      }
      
      if (expr.sortedSplitRequired()) {
        return getSortedSplits(conf, numSplits, expr, readers, status);
      }
      return getUnsortedSplits(conf, numSplits, expr, readers, status);
    } catch (ParseException e) {
    	throw new IOException("Projection parsing failed : "+e.getMessage());
    }
    finally {
      for (Iterator<BasicTable.Reader> it = readers.iterator(); it.hasNext();) {
        try {
          it.next().close();
        }
        catch (Exception e) {
          // trap errors.
          // TODO: log the error here.
        }
      }
    }
  }

  @Deprecated
  public synchronized void validateInput(JobConf conf) throws IOException {
    // Validating imports by opening all Tables.
    TableExpr expr = getInputExpr(conf);
    try {
      String projection = getProjection(conf);
      List<LeafTableInfo> leaves = expr.getLeafTables(projection);
      Iterator<LeafTableInfo> iterator = leaves.iterator();
      while (iterator.hasNext()) {
        LeafTableInfo leaf = iterator.next();
        BasicTable.Reader reader =
            new BasicTable.Reader(leaf.getPath(), conf);
        reader.setProjection(projection);
        reader.close();
      }
    } catch (ParseException e) {
    	throw new IOException("Projection parsing failed : "+e.getMessage());
    }
  }
}

/**
 * Adaptor class for sorted InputSplit for table.
 */
class SortedTableSplit implements InputSplit {
  BytesWritable begin = null, end = null;
  
  String[] hosts;
  long length = 1;

  public SortedTableSplit()
  {
    // no-op for Writable construction
  }
  
  public SortedTableSplit(BytesWritable begin, BytesWritable end,
      BlockDistribution bd, JobConf conf) {
    if (begin != null) {
      this.begin = new BytesWritable();
      this.begin.set(begin.get(), 0, begin.getSize());
    }
    if (end != null) {
      this.end = new BytesWritable();
      this.end.set(end.get(), 0, end.getSize());
    }
    
    if (bd != null) {
      length = bd.getLength();
      hosts =
        bd.getHosts(conf.getInt("mapred.lib.table.input.nlocation", 5));
    }
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
    begin = end = null;
    int bool = WritableUtils.readVInt(in);
    if (bool == 1) {
      begin.readFields(in);
    }
    bool = WritableUtils.readVInt(in);
    if (bool == 1) {
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
class UnsortedTableSplit implements InputSplit {
  String path = null;
  RangeSplit split = null;
  String[] hosts = null;
  long length = 1;

  public UnsortedTableSplit(Reader reader, RangeSplit split, JobConf conf)
      throws IOException {
    this.path = reader.getPath();
    this.split = split;
    BlockDistribution dataDist = reader.getBlockDistribution(split);
    if (dataDist != null) {
      length = dataDist.getLength();
      hosts =
          dataDist.getHosts(conf.getInt("mapred.lib.table.input.nlocation", 5));
    }
  }
  
  public UnsortedTableSplit() {
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
    path = WritableUtils.readString(in);
    int bool = WritableUtils.readVInt(in);
    if (bool == 1) {
      if (split == null) split = new RangeSplit();
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
  
  public RangeSplit getSplit() {
    return split;
  }
}
