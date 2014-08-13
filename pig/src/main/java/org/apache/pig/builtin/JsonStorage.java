/*
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
package org.apache.pig.builtin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.joda.time.DateTime;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreMetadata;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * A JSON Pig store function.  Each Pig tuple is stored on one line (as one
 * value for TextOutputFormat) so that it can be read easily using
 * TextInputFormat.  Pig tuples are mapped to JSON objects.  Pig bags are
 * mapped to JSON arrays.  Pig maps are also mapped to JSON objects.  Maps are
 * assumed to be string to string.  A schema is stored in a side file to deal
 * with mapping between JSON and Pig types. The schema file share the same format
 * as the one we use in PigStorage.
 */
public class JsonStorage extends StoreFunc implements StoreMetadata {

    protected RecordWriter writer = null;
    protected ResourceSchema schema = null;

    private String udfcSignature = null;
    private JsonFactory jsonFactory = null;

    // Default size for the byte buffer, should fit most tuples.
    private static final int BUF_SIZE = 4 * 1024; 
    
    private static final String SCHEMA_SIGNATURE = "pig.jsonstorage.schema";

    /*
     * Methods called on the front end
     */

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        // We will use TextOutputFormat, the default Hadoop output format for
        // text.  The key is unused and the value will be a
        // Text (a string writable type) that we store our JSON data in.
        return new TextOutputFormat<LongWritable, Text>();
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        // FileOutputFormat has a utility method for setting up the output
        // location.  
        FileOutputFormat.setOutputPath(job, new Path(location));
    }

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        // store the signature so we can use it later
        udfcSignature = signature;
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        // We won't really check the schema here, we'll store it in our
        // UDFContext properties object so we have it when we need it on the
        // backend
        
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p =
            udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
        p.setProperty(SCHEMA_SIGNATURE, fixSchema(s).toString());
    }


    /*
     * Methods called on the back end
     */

    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        // Store the record writer reference so we can use it when it's time
        // to write tuples
        this.writer = writer;

        // Get the schema string from the UDFContext object.
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p =
            udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
        String strSchema = p.getProperty(SCHEMA_SIGNATURE);
        if (strSchema == null) {
            throw new IOException("Could not find schema in UDF context");
        }

        // Parse the schema from the string stored in the properties object.
        schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));

        // Build a Json factory
        jsonFactory = new JsonFactory();
    }

    @SuppressWarnings("unchecked")
    public void putNext(Tuple t) throws IOException {
        // Build a ByteArrayOutputStream to write the JSON into
        ByteArrayOutputStream baos = new ByteArrayOutputStream(BUF_SIZE);
        // Build the generator
        JsonGenerator json =
            jsonFactory.createJsonGenerator(baos, JsonEncoding.UTF8);

        // Write the beginning of the top level tuple object
        json.writeStartObject();
        
        ResourceFieldSchema[] fields = schema.getFields();
        for (int i = 0; i < fields.length; i++) {
            int tupleLength = t.size();
            //write col if exists in tuple, null otherwise
            if (i < tupleLength) {
                writeField(json, fields[i], t.get(i));
            } else {
                writeField(json, fields[i], null);
            }
        }
        json.writeEndObject();
        json.close();

        // Hand a null key and our string to Hadoop
        try {
            writer.write(null, new Text(baos.toByteArray()));
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }
    }

    @SuppressWarnings("unchecked")
    private void writeField(JsonGenerator json,
                            ResourceFieldSchema field, 
                            Object d) throws IOException {

        // If the field is missing or the value is null, write a null
        if (d == null) {
            json.writeNullField(field.getName());
            return;
        }

        // Based on the field's type, write it out
        switch (field.getType()) {
        case DataType.BOOLEAN:
            json.writeBooleanField(field.getName(), (Boolean)d);
            return;

        case DataType.INTEGER:
            json.writeNumberField(field.getName(), (Integer)d);
            return;

        case DataType.LONG:
            json.writeNumberField(field.getName(), (Long)d);
            return;

        case DataType.FLOAT:
            json.writeNumberField(field.getName(), (Float)d);
            return;

        case DataType.DOUBLE:
            json.writeNumberField(field.getName(), (Double)d);
            return;

        case DataType.DATETIME:
            json.writeStringField(field.getName(), d.toString());
            return;

        case DataType.BYTEARRAY:
            json.writeStringField(field.getName(), d.toString());
            return;

        case DataType.CHARARRAY:
            json.writeStringField(field.getName(), (String)d);
            return;

        case DataType.BIGINTEGER:
            //Since Jackson doesnt have a writeNumberField for BigInteger we
            //have to do it manually here.
            json.writeFieldName(field.getName());
            json.writeNumber((BigInteger)d);
            return;

        case DataType.BIGDECIMAL:
            json.writeNumberField(field.getName(), (BigDecimal)d);
            return;

        case DataType.MAP:
            json.writeFieldName(field.getName());
            json.writeStartObject();
            for (Map.Entry<String, Object> e : ((Map<String, Object>)d).entrySet()) {
                json.writeStringField(e.getKey(), e.getValue() == null ? null : e.getValue().toString());
            }
            json.writeEndObject();
            return;

        case DataType.TUPLE:
            json.writeFieldName(field.getName());
            json.writeStartObject();

            ResourceSchema s = field.getSchema();
            if (s == null) {
                throw new IOException("Schemas must be fully specified to use "
                    + "this storage function.  No schema found for field " +
                    field.getName());
            }
            ResourceFieldSchema[] fs = s.getFields();

            for (int j = 0; j < fs.length; j++) {
                writeField(json, fs[j], ((Tuple)d).get(j));
            }
            json.writeEndObject();
            return;

        case DataType.BAG:
            json.writeFieldName(field.getName());
            json.writeStartArray();
            s = field.getSchema();
            if (s == null) {
                throw new IOException("Schemas must be fully specified to use "
                    + "this storage function.  No schema found for field " +
                    field.getName());
            }
            fs = s.getFields();
            if (fs.length != 1 || fs[0].getType() != DataType.TUPLE) {
                throw new IOException("Found a bag without a tuple "
                    + "inside!");
            }
            // Drill down the next level to the tuple's schema.
            s = fs[0].getSchema();
            if (s == null) {
                throw new IOException("Schemas must be fully specified to use "
                    + "this storage function.  No schema found for field " +
                    field.getName());
            }
            fs = s.getFields();
            for (Tuple t : (DataBag)d) {
                json.writeStartObject();
                for (int j = 0; j < fs.length; j++) {
                    writeField(json, fs[j], t.get(j));
                }
                json.writeEndObject();
            }
            json.writeEndArray();
            return;
        }
    }

    public void storeStatistics(ResourceStatistics stats,
                                String location,
                                Job job) throws IOException {
        // We don't implement this method
    }

    public void storeSchema(ResourceSchema schema, String location, Job job)
    throws IOException {
        // Store the schema in a side file in the same directory.  MapReduce
        // does not include files starting with "_" when reading data for a job.
        JsonMetadata metadataWriter = new JsonMetadata();
        byte recordDel = '\n';
        byte fieldDel = '\t';
        metadataWriter.setFieldDel(fieldDel);
        metadataWriter.setRecordDel(recordDel);
        metadataWriter.storeSchema(schema, location, job);
    }

    public ResourceSchema fixSchema(ResourceSchema s){
      for (ResourceFieldSchema filed : s.getFields()) {
        if(filed.getType() == DataType.NULL)
          filed.setType(DataType.BYTEARRAY);
      }
      return s;
    }

}
