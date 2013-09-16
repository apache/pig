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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.pig.Expression;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigWarning;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

/**
 * A loader for data stored using {@link JsonStorage}.  This is not a generic
 * JSON loader. It depends on the schema being stored with the data when
 * conceivably you could write a loader that determines the schema from the
 * JSON. 
 */
public class JsonLoader extends LoadFunc implements LoadMetadata {

    protected RecordReader reader = null;
    protected ResourceSchema schema = null;
    
    private String udfcSignature = null;
    private JsonFactory jsonFactory = null;
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private BagFactory bagFactory = BagFactory.getInstance();
    
    private static final String SCHEMA_SIGNATURE = "pig.jsonloader.schema";
    
    public JsonLoader() {
    }
    
    public JsonLoader(String schemaString) throws IOException {
        schema = new ResourceSchema(Utils.parseSchema(schemaString));
    }

    public void setLocation(String location, Job job) throws IOException {
        // Tell our input format where we will be reading from
        FileInputFormat.setInputPaths(job, location);
    }
    
    @SuppressWarnings("unchecked")
    public InputFormat getInputFormat() throws IOException {
        // We will use TextInputFormat, the default Hadoop input format for
        // text.  It has a LongWritable key that we will ignore, and the value
        // is a Text (a string writable) that the JSON data is in.
        return new TextInputFormat();
    }

    public LoadCaster getLoadCaster() throws IOException {
        // We do not expect to do casting of byte arrays, because we will be
        // returning typed data.
        return null;
    }

    @SuppressWarnings("unchecked")
    public void prepareToRead(RecordReader reader, PigSplit split)
    throws IOException {
        this.reader = reader;
        
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

        jsonFactory = new JsonFactory();
    }

    public Tuple getNext() throws IOException {
        Text val = null;
        try {
            // Read the next key value pair from the record reader.  If it's
            // finished, return null
            if (!reader.nextKeyValue()) return null;

            // Get the current value.  We don't use the key.
            val = (Text)reader.getCurrentValue();
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }

        // Create a parser specific for this input line.  This may not be the
        // most efficient approach.
        byte[] newBytes = new byte[val.getLength()];
        System.arraycopy(val.getBytes(), 0, newBytes, 0, val.getLength());
        ByteArrayInputStream bais = new ByteArrayInputStream(newBytes);
        JsonParser p = jsonFactory.createJsonParser(bais);

        // Create the tuple we will be returning.  We create it with the right
        // number of fields, as the Tuple object is optimized for this case.
        ResourceFieldSchema[] fields = schema.getFields();
        Tuple t = tupleFactory.newTuple(fields.length);

        // Read the start object marker.  Throughout this file if the parsing
        // isn't what we expect we return a tuple with null fields rather than
        // throwing an exception.  That way a few mangled lines don't fail the
        // job.
        if (p.nextToken() != JsonToken.START_OBJECT) {
            warn("Bad record, could not find start of record " +
                val.toString(), PigWarning.UDF_WARNING_1);
            return t;
        }

        // Read each field in the record
        for (int i = 0; i < fields.length; i++) {
            t.set(i, readField(p, fields[i], i));
        }

        if (p.nextToken() != JsonToken.END_OBJECT) {
            warn("Bad record, could not find end of record " +
                val.toString(), PigWarning.UDF_WARNING_1);
            return t;
        }
        p.close();
        return t;
    }

    private Object readField(JsonParser p,
                             ResourceFieldSchema field,
                             int fieldnum) throws IOException {
        // Read the next token
        JsonToken tok = p.nextToken();
        if (tok == null) {
            warn("Early termination of record, expected " + schema.getFields().length
                + " fields bug found " + fieldnum, PigWarning.UDF_WARNING_1);
            return null;
        }

        // Check to see if this value was null
        if (tok == JsonToken.VALUE_NULL) return null;

        // Read based on our expected type
        switch (field.getType()) {
        case DataType.BOOLEAN:
            tok = p.nextToken();
            if (tok == JsonToken.VALUE_NULL) return null;
            return p.getBooleanValue();

        case DataType.INTEGER:
            // Read the field name
            tok = p.nextToken();
            if (tok == JsonToken.VALUE_NULL) return null;
            return p.getIntValue();

        case DataType.LONG:
            tok = p.nextToken();
            if (tok == JsonToken.VALUE_NULL) return null;
            return p.getLongValue();

        case DataType.FLOAT:
            tok = p.nextToken();
            if (tok == JsonToken.VALUE_NULL) return null;
            return p.getFloatValue();

        case DataType.DOUBLE:
            tok = p.nextToken();
            if (tok == JsonToken.VALUE_NULL) return null;
            return p.getDoubleValue();

        case DataType.DATETIME:
            tok = p.nextToken();
            if (tok == JsonToken.VALUE_NULL) return null;
            DateTimeFormatter formatter = ISODateTimeFormat.dateTimeParser();
            return formatter.withOffsetParsed().parseDateTime(p.getText());

        case DataType.BYTEARRAY:
            tok = p.nextToken();
            if (tok == JsonToken.VALUE_NULL) return null;
            byte[] b = p.getText().getBytes();
            // Use the DBA constructor that copies the bytes so that we own
            // the memory
            return new DataByteArray(b, 0, b.length);

        case DataType.CHARARRAY:
            tok = p.nextToken();
            if (tok == JsonToken.VALUE_NULL) return null;
            return p.getText();

        case DataType.BIGINTEGER:
            tok = p.nextToken();
            if (tok == JsonToken.VALUE_NULL) return null;
            return p.getBigIntegerValue();

        case DataType.BIGDECIMAL:
            tok = p.nextToken();
            if (tok == JsonToken.VALUE_NULL) return null;
            return p.getDecimalValue();

        case DataType.MAP:
            // Should be a start of the map object
            if (p.nextToken() != JsonToken.START_OBJECT) {
                warn("Bad map field, could not find start of object, field "
                    + fieldnum, PigWarning.UDF_WARNING_1);
                return null;
            }
            Map<String, String> m = new HashMap<String, String>();
            while (p.nextToken() != JsonToken.END_OBJECT) {
                String k = p.getCurrentName();
                String v = p.getCurrentToken() == JsonToken.VALUE_NULL ? null : p.getText();
                m.put(k, v);
            }
            return m;

        case DataType.TUPLE:
            if (p.nextToken() != JsonToken.START_OBJECT) {
                warn("Bad tuple field, could not find start of object, "
                    + "field " + fieldnum, PigWarning.UDF_WARNING_1);
                return null;
            }

            ResourceSchema s = field.getSchema();
            ResourceFieldSchema[] fs = s.getFields();
            Tuple t = tupleFactory.newTuple(fs.length);

            for (int j = 0; j < fs.length; j++) {
                t.set(j, readField(p, fs[j], j));
            }

            if (p.nextToken() != JsonToken.END_OBJECT) {
                warn("Bad tuple field, could not find end of object, "
                    + "field " + fieldnum, PigWarning.UDF_WARNING_1);
                return null;
            }
            return t;

        case DataType.BAG:
            if (p.nextToken() != JsonToken.START_ARRAY) {
                warn("Bad bag field, could not find start of array, "
                    + "field " + fieldnum, PigWarning.UDF_WARNING_1);
                return null;
            }

            s = field.getSchema();
            fs = s.getFields();
            // Drill down the next level to the tuple's schema.
            s = fs[0].getSchema();
            fs = s.getFields();

            DataBag bag = bagFactory.newDefaultBag();

            JsonToken innerTok;
            while ((innerTok = p.nextToken()) != JsonToken.END_ARRAY) {
                if (innerTok != JsonToken.START_OBJECT) {
                    warn("Bad bag tuple field, could not find start of "
                        + "object, field " + fieldnum, PigWarning.UDF_WARNING_1);
                    return null;
                }

                t = tupleFactory.newTuple(fs.length);
                for (int j = 0; j < fs.length; j++) {
                    t.set(j, readField(p, fs[j], j));
                }

                if (p.nextToken() != JsonToken.END_OBJECT) {
                    warn("Bad bag tuple field, could not find end of "
                        + "object, field " + fieldnum, PigWarning.UDF_WARNING_1);
                    return null;
                }
                bag.add(t);
            }
            return bag;
        default:
            throw new IOException("Unknown type in input schema: " +
                field.getType());
        }

    }

    //------------------------------------------------------------------------
    
    public void setUDFContextSignature(String signature) {
        udfcSignature = signature;
    }

    public ResourceSchema getSchema(String location, Job job)
    throws IOException {

        ResourceSchema s;
        if (schema!=null) {
            s = schema;
        } else {
            // Parse the schema
            s = (new JsonMetadata()).getSchema(location, job, true);
    
            if (s == null) {
                throw new IOException("Unable to parse schema found in file in " + location);
            }
        }
    
        // Now that we have determined the schema, store it in our
        // UDFContext properties object so we have it when we need it on the
        // backend
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p =
            udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
        p.setProperty(SCHEMA_SIGNATURE, s.toString());

        return s;
    }

    public ResourceStatistics getStatistics(String location, Job job) 
    throws IOException {
        // We don't implement this one.
        return null;
    }

    public String[] getPartitionKeys(String location, Job job)
    throws IOException {
        // We don't have partitions
        return null;
    }

    public void setPartitionFilter(Expression partitionFilter)
    throws IOException {
        // We don't have partitions
    }
}
