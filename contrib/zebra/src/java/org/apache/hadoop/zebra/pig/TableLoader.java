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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.mapred.TableInputFormat;
import org.apache.hadoop.zebra.mapred.TableRecordReader;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.ColumnType;
import org.apache.hadoop.zebra.schema.Schema.ColumnSchema;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.hadoop.zebra.types.SortInfo;
import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.Slice;
import org.apache.pig.Slicer;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.optimizer.PruneColumns;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;
import org.apache.hadoop.zebra.pig.comparator.*;
import org.apache.pig.IndexableLoadFunc;
import org.apache.hadoop.zebra.io.TableScanner;

/**
 * Pig IndexableLoadFunc and Slicer for Zebra Table
 */
public class TableLoader implements IndexableLoadFunc, Slicer {
	static final Log LOG = LogFactory.getLog(TableLoader.class);
	private TableInputFormat inputFormat;
	private JobConf jobConf;
	private String projectionString;
	private Path[] paths;
	private TableRecordReader indexReader = null;
	private BytesWritable indexKey = null;
    private Tuple tuple;
    private org.apache.hadoop.zebra.schema.Schema schema;  
    private SortInfo sortInfo;
    private boolean sorted = false;
    private org.apache.hadoop.zebra.schema.Schema projectionSchema;
	/**
	 * default constructor
	 */
	public TableLoader() {
		inputFormat = new TableInputFormat();
	}

	/**
	 * @param projectionStr
	 * 		  projection string passed from pig query.
	 */
	public TableLoader(String projectionStr) {
		inputFormat = new TableInputFormat();
		projectionString = projectionStr;	  
	}

  /**
	 * @param projectionStr
	 * 		  projection string passed from pig query.
   * @param sorted
   *      need sorted table(s)?
	 */
	public TableLoader(String projectionStr, String sorted) throws IOException {
      inputFormat = new TableInputFormat();
      if (projectionStr != null && !projectionStr.isEmpty())
        projectionString = projectionStr;	  
      if (sorted.equalsIgnoreCase("sorted"))
        this.sorted = true;
      else
        throw new IOException("Invalid argument to the table loader constructor: "+sorted);
	}

	@Override
	public void initialize(Configuration conf) throws IOException
	{
	  if (conf == null)
	    throw new IOException("Null Configuration passed.");
	  jobConf = new JobConf(conf);
	}
	
	@Override
	public void bindTo(String filePaths, BufferedPositionedInputStream is,
			long offset, long end) throws IOException {

      FileInputFormat.setInputPaths(jobConf, filePaths);
      Path[] paths = FileInputFormat.getInputPaths(jobConf);
			/**
			 * Performing glob pattern matching
			 */
			List<Path> result = new ArrayList<Path>(paths.length);
			for (Path p : paths) {
				FileSystem fs = p.getFileSystem(jobConf);
				FileStatus[] matches = fs.globStatus(p);
				if (matches == null) {
					LOG.warn("Input path does not exist: " + p);
				}
				else if (matches.length == 0) {
					LOG.warn("Input Pattern " + p + " matches 0 files");
				} else {
					for (FileStatus globStat: matches) {
						if (globStat.isDir()) {
							result.add(globStat.getPath());
						} else {
							LOG.warn("Input path " + p + " is not a directory");
						}
					}
				}
			}
			if (result.isEmpty()) {
				throw new IOException("No table specified for input");
			}
			TableInputFormat.setInputPaths(jobConf, result.toArray(new Path[result.size()]));

      TableInputFormat.requireSortedTable(jobConf, null);
      sortInfo = TableInputFormat.getSortInfo(jobConf);
      schema = TableInputFormat.getSchema(jobConf);
      int numcols = schema.getNumColumns();
      tuple = TypesUtils.createTuple(numcols);      
      setProjection();
      /*
       * Use all columns of a table as a projection: not an optimal approach
       * No need to call TableInputFormat.setProjection: by default use all columns
       */
      try {
        indexReader = TableInputFormat.getTableRecordReader(jobConf, null);
      } catch (ParseException e) {
    	  throw new IOException("Exception from TableInputFormat.getTableRecordReader: "+e.getMessage());
      }
      indexKey = new BytesWritable();
    }

  @Override
  public void seekNear(Tuple t) throws IOException
  {
		// SortInfo sortInfo =  inputFormat.getSortInfo(conf, path);
		String[] sortColNames = sortInfo.getSortColumnNames();
		byte[] types = new byte[sortColNames.length];
		for(int i =0 ; i < sortColNames.length; ++i){
			types[i] = schema.getColumn(sortColNames[i]).getType().pigDataType();
		}
		KeyGenerator builder = makeKeyBuilder(types);
		BytesWritable key = builder.generateKey(t);
//	    BytesWritable key = new BytesWritable(((String) t.get(0)).getBytes());
		indexReader.seekTo(key);
  }

  private KeyGenerator makeKeyBuilder(byte[] elems) {
	    ComparatorExpr[] exprs = new ComparatorExpr[elems.length];
	    for (int i = 0; i < elems.length; ++i) {
	      exprs[i] = ExprUtils.primitiveComparator(i, elems[i]);
	    }
	    return new KeyGenerator(ExprUtils.tupleComparator(exprs));
  }  
	/**
	 * @param storage
	 * @param location
	 *        The location format follows the same convention as
	 *        FileInputFormat's comma-separated multiple path format.
	 * @throws IOException
	*/
	private void checkConf(DataStorage storage, String location) throws IOException {
		if (jobConf == null) {
			Configuration conf =
				ConfigurationUtil.toConfiguration(storage.getConfiguration());
			jobConf = new JobConf(conf);
			jobConf.setInputFormat(TableInputFormat.class);			
			
			// TODO: the following code may better be moved to TableInputFormat.
			// Hack: use FileInputFormat to decode comma-separated multiple path
			// format.
			
			FileInputFormat.setInputPaths(jobConf, location);
			paths = FileInputFormat.getInputPaths(jobConf);
			
			/**
			 * Performing glob pattern matching
			 */
			List<Path> result = new ArrayList<Path>(paths.length);
			for (Path p : paths) {
				FileSystem fs = p.getFileSystem(jobConf);
				FileStatus[] matches = fs.globStatus(p);
				if (matches == null) {
					LOG.warn("Input path does not exist: " + p);
				}
				else if (matches.length == 0) {
					LOG.warn("Input Pattern " + p + " matches 0 files");
				} else {
					for (FileStatus globStat: matches) {
						if (globStat.isDir()) {
							result.add(globStat.getPath());
						} else {
							LOG.warn("Input path " + p + " is not a directory");
						}
					}
				}
			}
			if (result.isEmpty()) {
				throw new IOException("No table specified for input");
			}
			
			LOG.info("Total input tables to process : " + result.size()); 
			TableInputFormat.setInputPaths(jobConf, result.toArray(new Path[result.size()]));
			if (sorted)
				TableInputFormat.requireSortedTable(jobConf, null);
		}
	}

	
	private void setProjection() throws IOException {
		try {
			
			String pigLoadSignature = jobConf.get("pig.loader.signature");
			Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
			String prunedProjStr = null;
			if( pigLoadSignature != null)
				prunedProjStr = p.getProperty(pigLoadSignature);
			
			if(prunedProjStr != null ) {
				TableInputFormat.setProjection(jobConf, prunedProjStr);
			} else {
				if (projectionString != null) {    		  
					TableInputFormat.setProjection(jobConf, projectionString);
				}
			}
		} catch (ParseException e) {
			throw new IOException("Schema parsing failed : "+e.getMessage());
		}

		
		
	}
	
	@Override
	public Schema determineSchema(String fileName, ExecType execType,
			DataStorage storage) throws IOException {
		
		checkConf(storage, fileName);
		
		// This is bad but its done for pig. Pig creates one loadfunc object and uses to different
		// signatures. Zebra does not modify jobConf object once created. However, we might have the new
		// signature in this function everytime. 
		String pigLoadSignature = storage.getConfiguration().getProperty("pig.loader.signature");
		if( pigLoadSignature != null) {
			jobConf.set("pig.loader.signature", pigLoadSignature);
		}	
		setProjection();
		
		Projection projection;

		if (!fileName.contains(",")) { // one table;
			org.apache.hadoop.zebra.schema.Schema tschema = BasicTable.Reader.getSchema(new Path(fileName), jobConf);
			try {
				projection = new org.apache.hadoop.zebra.types.Projection(tschema, TableInputFormat.getProjection(jobConf));
				projectionSchema = projection.getProjectionSchema();
			} catch (ParseException e) {
				throw new IOException("Schema parsing failed : "+e.getMessage());
			}
		} else { // table union;
			org.apache.hadoop.zebra.schema.Schema unionSchema = new org.apache.hadoop.zebra.schema.Schema();
			for (Path p : paths) {
				org.apache.hadoop.zebra.schema.Schema schema = BasicTable.Reader.getSchema(p, jobConf);
				try {
					unionSchema.unionSchema(schema);
				} catch (ParseException e) {
					throw new IOException(e.getMessage());
				}
			}
			
			try {
				projection = new org.apache.hadoop.zebra.types.Projection(unionSchema, TableInputFormat.getProjection(jobConf));
				projectionSchema = projection.getProjectionSchema();
			} catch (ParseException e) {
				throw new IOException("Schema parsing failed : "+e.getMessage());
			}
		}		
    
		if (projectionSchema == null) {
			throw new IOException("Cannot determine table projection schema");
		}
    
		try {
			return SchemaConverter.toPigSchema(projectionSchema);
		} catch (FrontendException e) {
			throw new IOException("FrontendException", e);
		}
	}

	@Override
    public RequiredFieldResponse fieldsToRead(RequiredFieldList requiredFieldList) throws FrontendException {

		
		String pigLoadSignature = requiredFieldList.getSignature();
		if(pigLoadSignature == null) {
			throw new FrontendException("Zebra Cannot have null loader signature in fieldsToRead");
		}	
		
		List<RequiredField> rFields = requiredFieldList.getFields();
		if( rFields == null) {
			throw new FrontendException("requiredFieldList.getFields() can not return null in fieldsToRead");
		}	

		Iterator<RequiredField> it= rFields.iterator();
		String projectionStr = "";
		
		while( it.hasNext()) {
			RequiredField rField = (RequiredField) it.next();
			ColumnSchema cs = projectionSchema.getColumn(rField.getIndex());
			
			if(cs == null) {
				throw new FrontendException
				("Null column schema in projection schema in fieldsToRead at index " + rField.getIndex()); 
			}
			
		    if(cs.getType() != ColumnType.MAP && (rField.getSubFields() != null)) {    	
		    	throw new FrontendException
		    	("Zebra cannot have subfields for a non-map column type in fieldsToRead " + 
		    	 "ColumnType:" + cs.getType() + " index in zebra projection schema: " + rField.getIndex()		
		    	);
		    }
		    String name = cs.getName();
	    	projectionStr = projectionStr + name ;
		    if(cs.getType() == ColumnType.MAP) {    	
		    	List<RequiredField> subFields = rField.getSubFields();
		    	
		    	if( subFields != null ) {
		    	
    		    	Iterator<RequiredField> its= subFields.iterator();
	    	    	boolean flag = false;
		        	if(its.hasNext()) {
		        		flag = true;
		    	    	projectionStr += "#" + "{";
		        	}	
		        	String tmp = "";
		        	while(its.hasNext()) {
		        		RequiredField sField = (RequiredField) its.next();	
		        		tmp = tmp + sField.getAlias();
		        		if(its.hasNext()) {
		        			tmp = tmp + "|";
		        		}
		        	}  
		        	if ( flag) {
		        		projectionStr = projectionStr + tmp + "}";
		        	}
		    	}	
		    }
	    	if(it.hasNext()) {
	    		projectionStr = projectionStr + " , ";
	    	}
		}
		Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
		
		if(p == null) {
			throw new FrontendException("Zebra Cannot have null UDFCOntext property");
		}	
		
		if(projectionStr != null && (projectionStr != ""))
			p.setProperty(pigLoadSignature, projectionStr);
				
		RequiredFieldResponse rfr = new RequiredFieldResponse(true);
		
		return rfr;		
	}

	@Override
	public Tuple getNext() throws IOException {
      if (indexReader.atEnd())
        return null;
      indexReader.next(indexKey, tuple);
      return tuple;
	}

  @Override
  public void close() throws IOException {
    if (indexReader != null)
      indexReader.close();
  }

	@Override
	public Slice[] slice(DataStorage store, String location) throws IOException {
		
		checkConf(store, location);
		setProjection();
		// TableInputFormat accepts numSplits < 0 (special case for no-hint)
		InputSplit[] splits = inputFormat.getSplits(jobConf, -1);

		Slice[] slices = new Slice[splits.length];
		for (int nx = 0; nx < slices.length; nx++) {
			slices[nx] = new TableSlice(jobConf, splits[nx], sorted);
		}

		return slices;
	}

	@Override
	public void validate(DataStorage store, String location) throws IOException {
		checkConf(store, location);
	}
  
	static class TableSlice implements Slice {
		private static final long serialVersionUID = 1L;
		private static final Class[] emptyArray = new Class[] {};
    
		private TreeMap<String, String> configMap;
		private InputSplit split;
    
		transient private JobConf conf;
		transient private int numProjCols = 0;
		transient private RecordReader<BytesWritable, Tuple> scanner;
		transient private BytesWritable key;
    transient private boolean sorted = false;

		TableSlice(JobConf conf, InputSplit split, boolean sorted) {
			// hack: expecting JobConf contains nothing but a <string, string>
			// key-value pair store.
			configMap = new TreeMap<String, String>();
			for (Iterator<Map.Entry<String, String>> it = conf.iterator(); it.hasNext();) {
				Map.Entry<String, String> e = it.next();
				configMap.put(e.getKey(), e.getValue());
			}
			
			
			
			this.split = split;
			this.sorted = sorted;
		}

		@Override
		public void close() throws IOException {
			if (scanner == null) {
				throw new IOException("Slice not initialized");
			}
			scanner.close();
		}

		@Override
		public long getLength() {
			try {
				return split.getLength();
			} catch (IOException e) {
				throw new RuntimeException("IOException", e);
			}
		}

		@Override
		public String[] getLocations() {
			try {
				return split.getLocations();
			} catch (IOException e) {
				throw new RuntimeException("IOException", e);
			}
		}

		@Override
		public long getPos() throws IOException {
			if (scanner == null) {
				throw new IOException("Slice not initialized");
			}
			return scanner.getPos();
		}

		@Override
		public float getProgress() throws IOException {
			if (scanner == null) {
				throw new IOException("Slice not initialized");
			}
			return scanner.getProgress();
		}

		@Override
		public long getStart() {
			return 0;
		}

		@Override
		public void init(DataStorage store) throws IOException {
			Configuration localConf = new Configuration();
			for (Iterator<Map.Entry<String, String>> it =
				configMap.entrySet().iterator(); it.hasNext();) {
				Map.Entry<String, String> e = it.next();
				localConf.set(e.getKey(), e.getValue());
			}
			conf = new JobConf(localConf);
			String projection;			
			try
			{
				projection = TableInputFormat.getProjection(conf);
			} catch (ParseException e) {
				throw new IOException("Schema parsing failed :"+e.getMessage());
			}
			numProjCols = Projection.getNumColumns(projection);
			TableInputFormat inputFormat = new TableInputFormat();
			if (sorted)
				TableInputFormat.requireSortedTable(conf, null);
			scanner = inputFormat.getRecordReader(split, conf, Reporter.NULL);
			key = new BytesWritable();
		}

		@Override
		public boolean next(Tuple value) throws IOException {
			
			TypesUtils.formatTuple(value, numProjCols);
			return scanner.next(key, value);
		}
    
		private void writeObject(ObjectOutputStream out) throws IOException {
			out.writeObject(configMap);
			out.writeObject(split.getClass().getName());
			split.write(out);
		} 
    
		@SuppressWarnings("unchecked")
		private void readObject(ObjectInputStream in) throws IOException,
        	ClassNotFoundException {
			configMap = (TreeMap<String, String>) in.readObject();
			String className = (String) in.readObject();
			Class<InputSplit> clazz = (Class<InputSplit>) Class.forName(className);
			try {
				Constructor<InputSplit> meth = clazz.getDeclaredConstructor(emptyArray);
				meth.setAccessible(true);
				split = meth.newInstance();
			} catch (Exception e) {
				throw new ClassNotFoundException("Cannot create instance", e);
			}
			split.readFields(in);
		}
	}

	@Override
	public DataBag bytesToBag(byte[] b) throws IOException {
		throw new IOException("Not implemented");
	}

	@Override
	public String bytesToCharArray(byte[] b) throws IOException {
		throw new IOException("Not implemented");
	}

	@Override
	public Double bytesToDouble(byte[] b) throws IOException {
		throw new IOException("Not implemented");
	}

	@Override
	public Float bytesToFloat(byte[] b) throws IOException {
		throw new IOException("Not implemented");
	}

	@Override
	public Integer bytesToInteger(byte[] b) throws IOException {
		throw new IOException("Not implemented");
	}

	@Override
	public Long bytesToLong(byte[] b) throws IOException {
		throw new IOException("Not implemented");
	}

	public Map<String, Object> bytesToMap(byte[] b) throws IOException {
		throw new IOException("Not implemented");
	}

	@Override
	public Tuple bytesToTuple(byte[] b) throws IOException {
		throw new IOException("Not implemented");
	}
}
