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
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.zebra.pig.comparator.ComparatorExpr;
import org.apache.hadoop.zebra.pig.comparator.ExprUtils;
import org.apache.hadoop.zebra.pig.comparator.KeyGenerator;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.SortInfo;
import org.apache.hadoop.zebra.types.TypesUtils;
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

    static final String OUTPUT_STORAGEHINT = "mapreduce.lib.table.output.storagehint";
    static final String OUTPUT_SCHEMA = "mapreduce.lib.table.output.schema";
    static final String OUTPUT_PATH = "mapreduce.lib.table.output.dir";
    static final String SORT_INFO = "mapreduce.lib.table.sort.info";
    static final String OUTPUT_CHECKTYPE = "mapreduce.lib.table.output.checktype";

    private String storageHintString = null;
    private String udfContextSignature = null;
    private TableRecordWriter tableRecordWriter = null;

    public TableStorer() {
    }

    public TableStorer(String storageHintStr) throws ParseException, IOException {
        storageHintString = storageHintStr;
    }

    @Override
    public void putNext(Tuple tuple) throws IOException {
        tableRecordWriter.write( null, tuple );
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
        return new TableOutputFormat();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepareToWrite(RecordWriter writer)
    throws IOException {
        tableRecordWriter = (TableRecordWriter)writer;
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
        conf.set( OUTPUT_STORAGEHINT, storageHintString );
        conf.set( OUTPUT_PATH, location );

        // Get schema string and sorting info from UDFContext and re-store them to
        // job config.
        Properties properties = UDFContext.getUDFContext().getUDFProperties( 
                this.getClass(), new String[]{ udfContextSignature } );
        conf.set( OUTPUT_SCHEMA, properties.getProperty( UDFCONTEXT_OUTPUT_SCHEMA ) );
        conf.set( SORT_INFO, properties.getProperty( UDFCONTEXT_SORT_INFO ) );
        
        // Get checktype information from UDFContext and re-store it to job config;
        if (properties.getProperty(UDFCONTEXT_OUTPUT_CHECKTYPE) != null && properties.getProperty(UDFCONTEXT_OUTPUT_CHECKTYPE).equals("no")) {
          conf.setBoolean(OUTPUT_CHECKTYPE, false);
        }
    }

    @Override
    public void storeSchema(ResourceSchema schema, String location, Job job)
    throws IOException {
    	// no-op. We do close at cleanupJob().
        BasicTable.Writer write = new BasicTable.Writer( new Path( location ), 
                job.getConfiguration());
        write.close();
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

/**
 * 
 * Table OutputFormat
 * 
 */
class TableOutputFormat extends OutputFormat<BytesWritable, Tuple> {
    @Override
    public void checkOutputSpecs(JobContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        String location = conf.get( TableStorer.OUTPUT_PATH );
        String schemaStr = conf.get( TableStorer.OUTPUT_SCHEMA );
        String storageHint = conf.get( TableStorer.OUTPUT_STORAGEHINT );
        String sortColumnNames = conf.get( TableStorer.SORT_INFO );

        BasicTable.Writer writer = new BasicTable.Writer( new Path( location ), 
                schemaStr, storageHint, sortColumnNames, null, conf );
        writer.finish();
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taContext)
    throws IOException, InterruptedException {
        return new TableOutputCommitter() ;
    }

    @Override
    public org.apache.hadoop.mapreduce.RecordWriter<BytesWritable, Tuple> getRecordWriter(
            TaskAttemptContext taContext) throws IOException, InterruptedException {
        return new TableRecordWriter( taContext );
    }

}

// TODO: make corresponding changes for commit and cleanup. Currently, no-ops.
class TableOutputCommitter extends OutputCommitter {
    @Override
    public void abortTask(TaskAttemptContext taContext) throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public void cleanupJob(JobContext jobContext) throws IOException {
//    	Configuration conf = jobContext.getConfiguration();
//        String location = conf.get( TableStorer.OUTPUT_PATH );
//        BasicTable.Writer write = new BasicTable.Writer( new Path( location ), conf );
//        write.close();
    }

    @Override
    public void commitTask(TaskAttemptContext taContext) throws IOException {
    	int i = 0;
    	i++;
        // TODO Auto-generated method stub
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taContext) throws IOException {
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
 * 
 * Table RecordWriter
 * 
 */
class TableRecordWriter extends RecordWriter<BytesWritable, Tuple> {
    final private BytesWritable KEY0 = new BytesWritable(new byte[0]); 
    private BasicTable.Writer writer;
    private TableInserter inserter;
    private int[] sortColIndices = null;
    KeyGenerator builder;
    Tuple t;

    public TableRecordWriter(TaskAttemptContext taContext) throws IOException {
        Configuration conf = taContext.getConfiguration();

        String path = conf.get(TableStorer.OUTPUT_PATH);
        writer = new BasicTable.Writer( new Path( path ), conf );

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

        boolean checkType = conf.getBoolean(TableStorer.OUTPUT_CHECKTYPE, true);
        inserter = writer.getInserter("patition-" + taContext.getTaskAttemptID().getTaskID().getId(), false, checkType);
    }

    @Override
    public void close(TaskAttemptContext taContext) throws IOException {
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
        System.out.println( "Tuple: " + value.toDelimitedString(",") );
        if (sortColIndices != null)    {
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
