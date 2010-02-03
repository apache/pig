/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.zebra.pig;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.zebra.pig.comparator.ComparatorExpr;
import org.apache.hadoop.zebra.pig.comparator.ExprUtils;
import org.apache.hadoop.zebra.pig.comparator.KeyGenerator;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.SortInfo;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.StoreConfig;
import org.apache.pig.CommittableStoreFunc;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.SortColInfo.Order;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.Tuple;

/**
 * Pig CommittableStoreFunc for Zebra Table
 */
public class TableStorer implements CommittableStoreFunc {
	private String storageHintString;

	public TableStorer() {	  
	}

	public TableStorer(String storageHintStr) throws ParseException, IOException {
		storageHintString = storageHintStr;
	}
  
	@Override
	public void bindTo(OutputStream os) throws IOException {
		// no op
	}

	@Override
	public void finish() throws IOException {
		// no op
	}

	@Override
	public void putNext(Tuple f) throws IOException {
		// no op
	}

	@Override
	public Class getStorePreparationClass() throws IOException {
		return TableOutputFormat.class;
	}
  
	public String getStorageHintString() {
		return storageHintString;  
	}
  
	private static final Class[] emptyArray = new Class[] {};

	static public void main(String[] args) throws SecurityException, NoSuchMethodException {
		Constructor meth = TableOutputFormat.class.getDeclaredConstructor(emptyArray);
	}

  @Override
  public void commit(Configuration conf) throws IOException {
    try {
      JobConf job = new JobConf(conf);
      StoreConfig storeConfig = MapRedUtil.getStoreConfig(job);
      BasicTable.Writer write = new BasicTable.Writer(new Path(storeConfig.getLocation()), job);
      write.close();
    } catch (IOException ee) {
      throw ee;
    }
  }
}

/**
 * 
 * Table OutputFormat
 * 
 */
class TableOutputFormat implements OutputFormat<BytesWritable, Tuple> {
	@Override
	public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
		StoreConfig storeConfig = MapRedUtil.getStoreConfig(job);
		String location = storeConfig.getLocation(), schemaStr;
    Schema schema = storeConfig.getSchema();
    org.apache.pig.SortInfo pigSortInfo = storeConfig.getSortInfo();

    /* TODO
     * use a home-brewn comparator ??
     */
    String comparator = null;
    String sortColumnNames = null;
    if (pigSortInfo != null)
    {
      List<org.apache.pig.SortColInfo> sortColumns = pigSortInfo.getSortColInfoList();
      StringBuilder sb = new StringBuilder();
      if (sortColumns != null && sortColumns.size() >0)
      {
        org.apache.pig.SortColInfo sortColumn;
        String sortColumnName;
        boolean descending = false;
        for (int i = 0; i < sortColumns.size(); i++)
        {
          sortColumn = sortColumns.get(i);
          sortColumnName = sortColumn.getColName();
          if (sortColumnName == null)
            throw new IOException("Zebra does not support column positional reference yet");
          if (sortColumn.getSortOrder() == Order.DESCENDING)
          {
            Log LOG = LogFactory.getLog(TableLoader.class);
            LOG.warn("Sorting in descending order is not supported by Zebra and the table will be unsorted.");
            descending = true;
            break;
          }
          if (!org.apache.pig.data.DataType.isAtomic(schema.getField(sortColumnName).type))
        	  throw new IOException(schema.getField(sortColumnName).alias+" is not of simple type as required for a sort column now.");
          if (i > 0)
            sb.append(",");
          sb.append(sortColumnName);
        }
        if (!descending)
          sortColumnNames = sb.toString();
      }
    }
    try {
      schemaStr = SchemaConverter.fromPigSchema(schema).toString();
    } catch (ParseException e) {
      throw new IOException("Exception thrown from SchemaConverter: " + e.getMessage());
    }
		TableStorer storeFunc = (TableStorer)MapRedUtil.getStoreFunc(job);   
		BasicTable.Writer writer = new BasicTable.Writer(new Path(location), 
				schemaStr, storeFunc.getStorageHintString(), sortColumnNames, comparator, job);
		writer.finish();
	}
    
	@Override
	public RecordWriter<BytesWritable, Tuple> getRecordWriter(FileSystem ignored,
			JobConf job, String name, Progressable progress) throws IOException {
		return new TableRecordWriter(name, job);
	}
}

/**
 * 
 * Table RecordWriter
 * 
 */
class TableRecordWriter implements RecordWriter<BytesWritable, Tuple> {
	final private BytesWritable KEY0 = new BytesWritable(new byte[0]); 
	private BasicTable.Writer writer;
	private TableInserter inserter;
  private int[] sortColIndices = null;
  KeyGenerator builder;
  Tuple t;

	public TableRecordWriter(String name, JobConf conf) throws IOException {
		StoreConfig storeConfig = MapRedUtil.getStoreConfig(conf);
		String location = storeConfig.getLocation();

		// TODO: how to get? 1) column group splits; 2) flag of sorted-ness,
		// compression, etc.
		writer = new BasicTable.Writer(new Path(location), conf);

    if (writer.getSortInfo() != null)
    {
      sortColIndices = writer.getSortInfo().getSortIndices();
      SortInfo sortInfo =  writer.getSortInfo();
      String[] sortColNames = sortInfo.getSortColumnNames();
      org.apache.hadoop.zebra.schema.Schema schema = writer.getSchema();

      byte[] types = new byte[sortColNames.length];
      for(int i =0 ; i < sortColNames.length; ++i){
        types[i] = schema.getColumn(sortColNames[i]).getType().pigDataType();
      }
      t = TypesUtils.createTuple(sortColNames.length);
      builder = makeKeyBuilder(types);
    }

		inserter = writer.getInserter(name, false);
	}

	@Override
	public void close(Reporter reporter) throws IOException {
		inserter.close();
		writer.finish();
	}

  private KeyGenerator makeKeyBuilder(byte[] elems) {
	    ComparatorExpr[] exprs = new ComparatorExpr[elems.length];
	    for (int i = 0; i < elems.length; ++i) {
	      exprs[i] = ExprUtils.primitiveComparator(i, elems[i]);
	    }
	    return new KeyGenerator(ExprUtils.tupleComparator(exprs));
  }

  @Override
  public void write(BytesWritable key, Tuple value) throws IOException {
    if (sortColIndices != null)
    {
      for(int i =0; i < sortColIndices.length;++i) {
        t.set(i, value.get(sortColIndices[i]));
      }
      key = builder.generateKey(t);
    } else if (key == null) {
      key = KEY0;
    }
    inserter.insert(key, value);
  }
}
