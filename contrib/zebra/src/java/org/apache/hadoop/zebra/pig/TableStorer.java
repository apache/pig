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
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.zebra.mapreduce.BasicTableOutputFormat;
import org.apache.hadoop.zebra.mapreduce.ZebraOutputPartition;
import org.apache.hadoop.zebra.mapreduce.ZebraSchema;
import org.apache.hadoop.zebra.mapreduce.ZebraSortInfo;
import org.apache.hadoop.zebra.mapreduce.ZebraStorageHint;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.ZebraConf;
import org.apache.pig.LoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreMetadata;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

/**
 * Pig LoadFunc implementation for Zebra Table
 */
public class TableStorer extends StoreFunc implements StoreMetadata {
    private static final String UDFCONTEXT_OUTPUT_SCHEMA = "zebra.UDFContext.outputSchema";
    private static final String UDFCONTEXT_SORT_INFO = "zebra.UDFContext.sortInfo";
    private static final String UDFCONTEXT_OUTPUT_CHECKTYPE = "zebra.UDFContext.checkType";

    private String storageHintString = null;
    private String udfContextSignature = null;
    private RecordWriter<BytesWritable, Tuple> tableRecordWriter = null;
    private String partitionClassString = null;
    Class<? extends ZebraOutputPartition> partitionClass = null;
    private String partitionClassArgumentsString = null;

    public TableStorer() {
    }

    public TableStorer(String storageHintString) {
      this.storageHintString = storageHintString;
    }

    public TableStorer(String storageHintString, String partitionClassString) {
      this.storageHintString = storageHintString;
      this.partitionClassString = partitionClassString;
    }

    public TableStorer(String storageHintString, String partitionClassString, String partitionClassArgumentsString) {
      this.storageHintString = storageHintString;
      this.partitionClassString = partitionClassString;
      this.partitionClassArgumentsString = partitionClassArgumentsString;
    }


    @Override
    public void putNext(Tuple tuple) throws IOException {
      try {
        tableRecordWriter.write( null, tuple );
      } catch (InterruptedException e) {
        throw new IOException(e.getMessage());
      }
    }

    @Override
    public void checkSchema(ResourceSchema schema) throws IOException {
      // Get schemaStr and sortColumnNames from the given schema. In the process, we
      // also validate the schema and sorting info.
      ResourceSchema.Order[] orders = schema.getSortKeyOrders();
      boolean descending = false;
      for (ResourceSchema.Order order : orders)
      {
        if (order == ResourceSchema.Order.DESCENDING)
        {
          Log LOG = LogFactory.getLog(TableStorer.class);
          LOG.warn("Sorting in descending order is not supported by Zebra and the table will be unsorted.");
          descending = true;
          break;
        }
      }
      StringBuilder sortColumnNames = new StringBuilder();
      if (!descending) {
        ResourceSchema.ResourceFieldSchema[] fields = schema.getFields();
        int[] index = schema.getSortKeys();
      
        for( int i = 0; i< index.length; i++ ) {
          ResourceFieldSchema field = fields[index[i]];
          String name = field.getName();
          if( name == null )
              throw new IOException("Zebra does not support column positional reference yet");
          if( !org.apache.pig.data.DataType.isAtomic( field.getType() ) )
              throw new IOException( "Field [" + name + "] is not of simple type as required for a sort column now." );
          if( i > 0 )
              sortColumnNames.append( "," );
          sortColumnNames.append( name );
        }
      }

      // Convert resource schema to zebra schema
      org.apache.hadoop.zebra.schema.Schema zebraSchema;
      try {
          zebraSchema = SchemaConverter.convertFromResourceSchema( schema );
      } catch (ParseException ex) {
          throw new IOException("Exception thrown from SchemaConverter: " + ex.getMessage() );
      }

      Properties properties = UDFContext.getUDFContext().getUDFProperties( 
              this.getClass(), new String[]{ udfContextSignature } );
      properties.setProperty( UDFCONTEXT_OUTPUT_SCHEMA, zebraSchema.toString() );
      properties.setProperty( UDFCONTEXT_SORT_INFO, sortColumnNames.toString() );
        
      // This is to turn off type check for potential corner cases - for internal use only;
      if (System.getenv("zebra_output_checktype") != null && System.getenv("zebra_output_checktype").equals("no")) {
        properties.setProperty( UDFCONTEXT_OUTPUT_CHECKTYPE, "no");
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public org.apache.hadoop.mapreduce.OutputFormat getOutputFormat()
    throws IOException {
      return new BasicTableOutputFormat();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepareToWrite(RecordWriter writer)
    throws IOException {
      tableRecordWriter = writer;
      if( tableRecordWriter == null ) {
          throw new IOException( "Invalid type of writer. Expected type: TableRecordWriter." );
      }
    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir)
    throws IOException {
      return LoadFunc.getAbsolutePath( location, curDir );
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
      Configuration conf = job.getConfiguration();
      
      String[] outputs = location.split(",");
      
      if (outputs.length == 1) {
        BasicTableOutputFormat.setOutputPath(job, new Path(location));
      } else if (outputs.length > 1) {
        if (partitionClass == null) {
          try {
            partitionClass = (Class<? extends ZebraOutputPartition>) conf.getClassByName(partitionClassString);
          } catch (ClassNotFoundException e) {
            throw new IOException(e);
          } 
        }
        
        Path[] paths = new Path[outputs.length];
        for (int i=0; i<paths.length; i++) {
          paths[i] = new Path(outputs[i]);
        }

        BasicTableOutputFormat.setMultipleOutputs(job, partitionClass, partitionClassArgumentsString, paths);
      } else {
        throw new IOException( "Invalid location : " + location);
      }

      // Get schema string and sorting info from UDFContext and re-store them to
      // job config.
      Properties properties = UDFContext.getUDFContext().getUDFProperties( 
              this.getClass(), new String[]{ udfContextSignature } );
      ZebraSchema zSchema = ZebraSchema.createZebraSchema(properties.getProperty(UDFCONTEXT_OUTPUT_SCHEMA));
      ZebraSortInfo zSortInfo = ZebraSortInfo.createZebraSortInfo(properties.getProperty(UDFCONTEXT_SORT_INFO), null);
      ZebraStorageHint zStorageHint = ZebraStorageHint.createZebraStorageHint(storageHintString);
      try {
        BasicTableOutputFormat.setStorageInfo(job, zSchema, zStorageHint, zSortInfo);
      } catch (ParseException e) {
        throw new IOException("Invalid storage info: " + e.getMessage());
      }
        
      // Get checktype information from UDFContext and re-store it to job config;
      if (properties.getProperty(UDFCONTEXT_OUTPUT_CHECKTYPE) != null && properties.getProperty(UDFCONTEXT_OUTPUT_CHECKTYPE).equals("no")) {
        ZebraConf.setCheckType(conf, false);
      }
    }

    @Override
    public void storeSchema(ResourceSchema schema, String location, Job job)
    throws IOException {
      //TODO: This is temporary - we will do close at cleanupJob() when OutputCommitter is ready.
      BasicTableOutputFormat.close(job);
    }

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
      udfContextSignature = signature;
    }

    @Override
    public void storeStatistics(ResourceStatistics stats, String location,
            Job job) throws IOException {
      // no-op
    }
}
