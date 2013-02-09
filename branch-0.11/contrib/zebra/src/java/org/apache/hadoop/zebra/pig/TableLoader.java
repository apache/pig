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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.mapreduce.TableInputFormat;
import org.apache.hadoop.zebra.mapreduce.TableRecordReader;
import org.apache.hadoop.zebra.mapreduce.TableInputFormat.SplitMode;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.ColumnType;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.schema.Schema.ColumnSchema;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.hadoop.zebra.types.SortInfo;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.OrderedLoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.UDFContext;
import org.apache.hadoop.zebra.pig.comparator.*;
import org.apache.pig.IndexableLoadFunc;
import org.apache.pig.CollectableLoadFunc;

/**
 * Pig IndexableLoadFunc and Slicer for Zebra Table
 */
public class TableLoader extends LoadFunc implements LoadMetadata, LoadPushDown,
        IndexableLoadFunc, CollectableLoadFunc, OrderedLoadFunc {
    static final Log LOG = LogFactory.getLog(TableLoader.class);

    private static final String UDFCONTEXT_PROJ_STRING = "zebra.UDFContext.projectionString";
    private static final String UDFCONTEXT_GLOBAL_SORTING = "zebra.UDFContext.globalSorting";
    private static final String UDFCONTEXT_PATHS_STRING = "zebra.UDFContext.pathsString";
    private static final String UDFCONTEXT_INPUT_DELETED_CGS = "zebra.UDFContext.deletedcgs";
    private static final String INPUT_SORT = "mapreduce.lib.table.input.sort";
    private static final String INPUT_SPLIT_MODE = "mapreduce.lib.table.input.split_mode";

    private static final String INPUT_FE = "mapreduce.lib.table.input.fe";
    private static final String INPUT_DELETED_CGS = "mapreduce.lib.table.input.deleted_cgs";
    private static final String GLOBALLY_SORTED = "globally_sorted";
    private static final String LOCALLY_SORTED = "locally_sorted";

    private String projectionString;

    private TableRecordReader tableRecordReader = null;

    private Schema schema;  
    private SortInfo sortInfo;
    private boolean sorted = false;
    private Schema projectionSchema;
    private String udfContextSignature = null;
    
    private Configuration conf = null;

    private KeyGenerator keyGenerator = null;

    /**
     * default constructor
     */
    public TableLoader() {
    }

    /**
     * @param projectionStr
     *           projection string passed from pig query.
     */
    public TableLoader(String projectionStr) {
        if( projectionStr != null && !projectionStr.isEmpty() )
            projectionString = projectionStr;
    }

    /**
     * @param projectionStr
     *           projection string passed from pig query.
     * @param sorted
     *      need sorted table(s)?
     */
    public TableLoader(String projectionStr, String sorted) throws IOException {
        this( projectionStr );
        
        if( sorted.equalsIgnoreCase( "sorted" ) )
            this.sorted = true;
        else
            throw new IOException( "Invalid argument to the table loader constructor: " + sorted );
    }

    @Override
    public void initialize(Configuration conf) throws IOException {
        // Here we do ugly workaround for the problem in pig. the passed in parameter conf contains 
        // value for TableInputFormat.INPUT_PROJ that was set by left table execution in a merge join
        // case. Here, we try to get rid of the side effect and copy everything expect that entry.
        this.conf = new Configuration( false );
        Iterator<Map.Entry<String, String>> it = conf.iterator();
        while( it.hasNext() ) {
            Map.Entry<String, String> entry = it.next();
            String key = entry.getKey();
            if( key.equals( "mapreduce.lib.table.input.projection" ) ) // The string const is defined in TableInputFormat.
                continue;
            this.conf.set( entry.getKey(), entry.getValue() );
        }

        tableRecordReader = createIndexReader();

        String[] sortColNames = sortInfo.getSortColumnNames();
        byte[] types = new byte[sortColNames.length];
        for(int i =0 ; i < sortColNames.length; ++i){
            types[i] = schema.getColumn(sortColNames[i]).getType().pigDataType();
        }
        keyGenerator = makeKeyBuilder( types );
    }

    /**
     * This method is called only once.
     */
    @Override
    public void seekNear(Tuple tuple) throws IOException {
        BytesWritable key = keyGenerator.generateKey( tuple );
        tableRecordReader.seekTo( key );
    }
    
    private TableRecordReader createIndexReader() throws IOException {
        Job job = new Job( conf );

        // Obtain the schema and sort info. for index reader, the table must be sorted.
        schema = TableInputFormat.getSchema( job );
        sorted = true;
        
        setSortOrder( job );
        setProjection( job );

        try {
            return TableInputFormat.createTableRecordReader( job, TableInputFormat.getProjection( job ) );
        } catch(ParseException ex) {
            throw new IOException( "Exception from TableInputFormat.getTableRecordReader: "+ ex.getMessage() );
        } catch(InterruptedException ex){
            throw new IOException( "Exception from TableInputFormat.getTableRecordReader: " + ex.getMessage() );
        }
    }

    
    /**
     * it processes sortedness of table .
     * 
     * @param job
     * @throws IOException
     */
    private void setSortOrder(Job job) throws IOException {
       Properties properties = UDFContext.getUDFContext().getUDFProperties( 
              this.getClass(), new String[]{ udfContextSignature } );
       boolean requireGlobalOrder = "true".equals(properties.getProperty( UDFCONTEXT_GLOBAL_SORTING));
       if (requireGlobalOrder && !sorted)
         throw new IOException("Global sorting can be only asked on table loaded as sorted");
       if( sorted ) {
           SplitMode splitMode = 
             requireGlobalOrder ? SplitMode.GLOBALLY_SORTED : SplitMode.LOCALLY_SORTED;
           TableInputFormat.setSplitMode(job, splitMode, null);
           sortInfo = TableInputFormat.getSortInfo( job );
       }    	
    }

    /**
     * This is a light version of setSortOrder, which is called in getLocation(), which is also called at backend.
     * We need to do this to avoid name node call in mappers. Original setSortOrder() is stilled called (in
     * getSchema()), so it's okay to skip the checks that are performed when setSortOrder() is called.
     * 
     * This will go away once we have a better solution.
     * 
     * @param job
     * @throws IOException
     */
    private void setSortOrderLight(Job job) throws IOException {
        Properties properties = UDFContext.getUDFContext().getUDFProperties( 
               this.getClass(), new String[]{ udfContextSignature } );
        boolean requireGlobalOrder = "true".equals(properties.getProperty( UDFCONTEXT_GLOBAL_SORTING));
        if (requireGlobalOrder && !sorted)
          throw new IOException("Global sorting can be only asked on table loaded as sorted");
        if( sorted ) {
            SplitMode splitMode = 
                requireGlobalOrder ? SplitMode.GLOBALLY_SORTED : SplitMode.LOCALLY_SORTED;

            Configuration conf = job.getConfiguration();
            conf.setBoolean(INPUT_SORT, true);
            if (splitMode == SplitMode.GLOBALLY_SORTED)
                conf.set(INPUT_SPLIT_MODE, GLOBALLY_SORTED);
            else if (splitMode == SplitMode.LOCALLY_SORTED)
                conf.set(INPUT_SPLIT_MODE, LOCALLY_SORTED);
        }        
     }

    /**
     * This method sets projection.
     * 
     * @param job
     * @throws IOException
     */
    private void setProjection(Job job) throws IOException {
      Properties properties = UDFContext.getUDFContext().getUDFProperties( 
          this.getClass(), new String[]{ udfContextSignature } );   
      try {
          String prunedProjStr = properties.getProperty( UDFCONTEXT_PROJ_STRING );
          
          if( prunedProjStr != null ) {
              TableInputFormat.setProjection( job, prunedProjStr );
          } else if( projectionString != null ) {              
              TableInputFormat.setProjection( job, projectionString );
          }
      } catch (ParseException ex) {
          throw new IOException( "Schema parsing failed : " + ex.getMessage() );
      }
    }

    private KeyGenerator makeKeyBuilder(byte[] elems) {
        ComparatorExpr[] exprs = new ComparatorExpr[elems.length];
        for (int i = 0; i < elems.length; ++i) {
            exprs[i] = ExprUtils.primitiveComparator(i, elems[i]);
        }
        return new KeyGenerator(ExprUtils.tupleComparator(exprs));
    }  

    /*
     * Hack: use FileInputFormat to decode comma-separated multiple path format.
     */ 
     private static Path[] getPathsFromLocation(String location, Job j) throws IOException {
         FileInputFormat.setInputPaths( j, location );
         Path[] paths = FileInputFormat.getInputPaths( j );

         /**
          * Performing glob pattern matching
          */
         List<Path> result = new ArrayList<Path>(paths.length);
         Configuration conf = j.getConfiguration();
         for (Path p : paths) {
             FileSystem fs = p.getFileSystem(conf);
             FileStatus[] matches = fs.globStatus(p);
             if( matches == null ) {
                 throw new IOException("Input path does not exist: " + p);
             } else if (matches.length == 0) {
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

         return result.toArray( new Path[result.size()] );
     }

     @Override
     public Tuple getNext() throws IOException {
         if (tableRecordReader.atEnd())
             return null;
         try {
             tableRecordReader.nextKeyValue();
             ArrayList<Object> fields = new ArrayList<Object>(tableRecordReader.getCurrentValue().getAll());
             return DefaultTupleFactory.getInstance().newTuple(fields);
         } catch (InterruptedException ex) {
             throw new IOException( "InterruptedException:" + ex );
         }
     }

     @Override
     public void close() throws IOException {
         if (tableRecordReader != null)
             tableRecordReader.close();
     }

     @SuppressWarnings("unchecked")
     @Override
     public void prepareToRead(org.apache.hadoop.mapreduce.RecordReader reader, PigSplit split)
     throws IOException {
         tableRecordReader = (TableRecordReader)reader;
         if( tableRecordReader == null )
             throw new IOException( "Invalid object type passed to TableLoader" );
     }

     /**
      * This method is called by pig on both frontend and backend.
      */
     @Override
     public void setLocation(String location, Job job) throws IOException {
         Properties properties = UDFContext.getUDFContext().getUDFProperties( 
                 this.getClass(), new String[]{ udfContextSignature } );

         // Retrieve paths from UDFContext to avoid name node call in mapper.
         String pathString = properties.getProperty( UDFCONTEXT_PATHS_STRING );
         Path[]paths = deserializePaths( pathString );
         
         // Retrieve deleted column group information to avoid name node call in mapper.
         String deletedCGs = properties.getProperty( UDFCONTEXT_INPUT_DELETED_CGS );
         job.getConfiguration().set(INPUT_FE, "true");
         job.getConfiguration().set(INPUT_DELETED_CGS, deletedCGs );
         
         TableInputFormat.setInputPaths( job, paths );
         
         // The following obviously goes beyond of set location, but this is the only place that we
         // can do and it's suggested by Pig team.
         setSortOrderLight( job );
         setProjection( job );
     }
     
     private static String serializePaths(Path[] paths) {
         StringBuilder sb = new StringBuilder();
         for( int i = 0; i < paths.length; i++ ) {
             sb.append( paths[i].toString() );
             if( i < paths.length -1 ) {
                 sb.append( ";" );
             }
         }
         return sb.toString();
     }
     
     private static Path[] deserializePaths(String val) {
         String[] paths = val.split( ";" );
         Path[] result = new Path[paths.length];
         for( int i = 0; i < paths.length; i++ ) {
             result[i] = new Path( paths[i] );
         }
         return result;
     }

     @SuppressWarnings("unchecked")
     @Override
     public InputFormat getInputFormat() throws IOException {
         return new TableInputFormat();
     }

     @Override
     public String[] getPartitionKeys(String location, Job job)
     throws IOException {
         return null;
     }

     @Override
     public ResourceSchema getSchema(String location, Job job) throws IOException {
         Properties properties = UDFContext.getUDFContext().getUDFProperties( 
                 this.getClass(), new String[]{ udfContextSignature } );
         
         // Save the paths in UDFContext so that it can be retrieved in setLocation().
         Path[] paths = getPathsFromLocation( location, job );
         properties.setProperty( UDFCONTEXT_PATHS_STRING, serializePaths( paths ) );
         
         TableInputFormat.setInputPaths( job, paths );

         // Save the deleted column group information in UDFContext so that it can be used in setLocation().
         // Property INPUT_DELETED_CGS is set in TableInputFormat.setInputPaths(). We just retrieve it here.
         properties.setProperty( UDFCONTEXT_INPUT_DELETED_CGS,  job.getConfiguration().get(INPUT_DELETED_CGS) );

         Schema tableSchema = null;
         if( paths.length == 1 ) {
             tableSchema = BasicTable.Reader.getSchema( paths[0], job.getConfiguration() );
         } else {
             tableSchema = new Schema();
             for (Path p : paths) {
                 Schema schema = BasicTable.Reader.getSchema( p, job.getConfiguration() );
                 try {
                     tableSchema.unionSchema( schema );
                 } catch (ParseException e) {
                     throw new IOException(e.getMessage());
                 }
             }
         }
         
         // This is needed as it does a check if a unsorted table is loaded as sorted
         // It fails if unosrted table is loaded as sorted
         setSortOrder( job );
         
         /*
         As per pig team any changes to this job object will be thrown away.
         getSchema is needed to return the projectionSchema for the projection
         string specified in TableLoader constructor. So, projectionString is used
         here. However, setLocation() calls setPojection() because that is called after
         projectionPruning and needs to read projection string from UDFCONTEXT
         That also sets/calls TableInputFormat.setProjection(job, $prunedProj). But the 
         job object here in getSchema() is a different copy from setLocation() and hence 
         the changes will not be overridden as per PIG TEAM.  
        */

         projectionSchema = tableSchema;
         try {
        	 if(projectionString != null)
        		 TableInputFormat.setProjection(job, projectionString);        	         	 
             Projection projection = new org.apache.hadoop.zebra.types.Projection( tableSchema, 
                     TableInputFormat.getProjection( job ) );
             projectionSchema = projection.getProjectionSchema();
         } catch (ParseException e) {
             throw new IOException( "Schema parsing failed : "+ e.getMessage() );
         }

         if( projectionSchema == null ) {
             throw new IOException( "Cannot determine table projection schema" );
         }

         return SchemaConverter.convertToResourceSchema( projectionSchema );
     }

     @Override
     public ResourceStatistics getStatistics(String location, Job job)
     throws IOException {
         // Statistics is not supported.
         return null;
     }

     @Override
     public void setPartitionFilter(Expression partitionFilter)
     throws IOException {
         // no-op. It should not be ever called since getPartitionKeys returns null.        
     }

     @Override
     public List<OperatorSet> getFeatures() {
         List<OperatorSet> features = new ArrayList<OperatorSet>(1);
         features.add( LoadPushDown.OperatorSet.PROJECTION );
         return features;
     }

     @Override
     public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList)
     throws FrontendException {
            List<RequiredField> rFields = requiredFieldList.getFields();
            if( rFields == null) {
                throw new FrontendException("requiredFieldList.getFields() can not return null in fieldsToRead");
            }    

            String projectionStr = "";
            
            Iterator<RequiredField> it= rFields.iterator();
            while( it.hasNext() ) {
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
                        if( flag ) {
                            projectionStr = projectionStr + tmp + "}";
                        }
                    }    
                }
                if(it.hasNext()) {
                    projectionStr = projectionStr + " , ";
                }
            }
            
            Properties properties = UDFContext.getUDFContext().getUDFProperties( 
                    this.getClass(), new String[]{ udfContextSignature } );
            if( projectionStr != null && !projectionStr.isEmpty() )
                properties.setProperty( UDFCONTEXT_PROJ_STRING, projectionStr );

            return new RequiredFieldResponse( true );
     }

        @Override
        public void setUDFContextSignature(String signature) {
            udfContextSignature = signature;
        }

		@Override
		public WritableComparable<?> getSplitComparable(InputSplit split)
				throws IOException {
	        return TableInputFormat.getSortedTableSplitComparable( split );
		}
        
		@Override
		public void ensureAllKeyInstancesInSameSplit() throws IOException {
			Properties properties = UDFContext.getUDFContext().getUDFProperties(this.getClass(),
					new String[] { udfContextSignature } );
			properties.setProperty(UDFCONTEXT_GLOBAL_SORTING, "true");
		}
}
