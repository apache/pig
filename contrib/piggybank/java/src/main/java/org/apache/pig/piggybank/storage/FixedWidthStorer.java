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

package org.apache.pig.piggybank.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigWarning;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Stores Pig records in a fixed-width file format. 
 * 
 * Takes a string argument specifying the ranges of each column in a unix 'cut'-like format.
 * Ex: '-5, 10-12, 14, 20-'
 * Ranges are comma-separated, 1-indexed (for ease of use with 1-indexed text editors), and inclusive.
 * A single-column field at position n may be specified as either 'n-n' or simply 'n'.
 *
 * A second optional argument specifies whether to write a header record
 * with the names of each field. 'WRITE_HEADER' writes a header record;
 * 'NO_HEADER' and the default does not write one.
 *
 * All datetimes are stored in UTC.
 *
 * Column spec idea and syntax parser borrowed from Russ Lankenau's FixedWidthLoader implementation
 * at https://github.com/rlankenau/fixed-width-pig-loader 
 */
public class FixedWidthStorer extends StoreFunc {

    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private RecordWriter writer = null;
    
    private ArrayList<FixedWidthLoader.FixedWidthField> columns;

    private ResourceSchema schema = null;
    private ResourceFieldSchema[] fields;

    private boolean writingFirstRecord = true;
    private boolean writeHeader = false;

    private String udfContextSignature = null;
    private static final String SCHEMA_SIGNATURE = "pig.fixedwidthloader.schema";
    private static final Log log = LogFactory.getLog(FixedWidthStorer.class);

    /*
     * Constructors and contructor helper methods
     */

    public FixedWidthStorer() {
        throw new IllegalArgumentException(
            "Usage: org.apache.pig.piggybank.storage.FixedWidthStorer(" +
            "'<column spec>'[, { 'WRITE_HEADER' | 'NO_HEADER' }]" +
            ")"
        );
    }

    public FixedWidthStorer(String columnSpec) {
        columns = FixedWidthLoader.parseColumnSpec(columnSpec);
    }

    public FixedWidthStorer(String columnSpec, String headerStr) {
        this(columnSpec);

        if (headerStr.equalsIgnoreCase("WRITE_HEADER"))
            writeHeader = true;
    }

    /*
     * Methods called on the frontend
     */

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        // Key is unused, Text is where the data is stored in
        return new TextOutputFormat<LongWritable, Text>();
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(location));
    }

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        udfContextSignature = signature;
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        // Not actually checking schema
        // Just storing it to use in the backend
        
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p =
            udfc.getUDFProperties(this.getClass(), new String[]{ udfContextSignature });
        p.setProperty(SCHEMA_SIGNATURE, s.toString());
    }

    /*
     * Methods called on the backend
     */

    @Override
    public void prepareToWrite(RecordWriter writer) throws IOException {
        // Store writer to use in putNext()
        this.writer = writer;

        // Get the schema string from the UDFContext object.
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p = udfc.getUDFProperties(this.getClass(), new String[]{ udfContextSignature });
        String strSchema = p.getProperty(SCHEMA_SIGNATURE);
        if (strSchema == null) {
            throw new IOException("Could not find schema in UDF context");
        }

        schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));
        fields = schema.getFields();
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public void putNext(Tuple t) throws IOException {
        
        // Write header row if this is the first record

        StringBuilder sb = new StringBuilder();
        FixedWidthLoader.FixedWidthField column;
        int offset = 0;

        if (writingFirstRecord && writeHeader) {
            for (int i = 0; i < fields.length; i++) {
                column = columns.get(i);
                sb.append(writeFieldAsString(fields[i], column, offset, fields[i].getName()));
                offset = column.end;
            }

            try {
                writer.write(null, new Text(sb.toString()));
            } catch (InterruptedException ie) {
                throw new IOException(ie);
            }
        }
        writingFirstRecord = false;

        sb = new StringBuilder();
        offset = 0;
        for (int i = 0; i < fields.length; i++) {
            column = columns.get(i);
            sb.append(writeFieldAsString(fields[i], column, offset, t.get(i)));
            offset = column.end;
        }

        try {
            writer.write(null, new Text(sb.toString()));
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }
    }

    @SuppressWarnings("unchecked")
    private String writeFieldAsString(ResourceFieldSchema field,
                                      FixedWidthLoader.FixedWidthField column,
                                      int offset,
                                      Object d) throws IOException {

        StringBuilder sb = new StringBuilder();

        if (offset < column.start) {
            int spaces = column.start - offset;
            for (int i = 0; i < spaces; i++) {
                sb.append(' ');
            }
        }

        int width = column.end - column.start;
        String fieldStr = null;
        if (d != null) {
            if (DataType.findType(d) == DataType.DATETIME)
                fieldStr = ((DateTime) d).toDateTime(DateTimeZone.UTC).toString();
            else
                fieldStr = d.toString();
        }

        // write nulls as spaces
        if (fieldStr == null) {
            for (int i = 0; i < width; i++) {
                sb.append(' ');
            }
            return sb.toString();
        }

        // If the field is too big to fit in column
        if (fieldStr.length() > width) {
            // If it is float or double, try to round it to fit
            byte fieldType = field.getType();
            if (fieldType == DataType.FLOAT || fieldType == DataType.DOUBLE) {
                double doubleVal = ((Number) d).doubleValue();
                int numDigitsLeftOfDecimal = (int) Math.ceil(Math.log10(Math.abs(doubleVal)));

                // Field can be rounded to fit
                if (numDigitsLeftOfDecimal <= width + 2) {
                    int numDigitsRightOfDecimal = width - numDigitsLeftOfDecimal - 1; // should be at least 1
                    String truncated = String.format("%." + numDigitsRightOfDecimal + "f", doubleVal);

                    warn("Cannot fit " + fieldStr + " in field starting at column " + 
                         column.start + " and ending at column " + (column.end - 1) + ". " +
                         "Since the field is a decimal type, truncating it to " + truncated + " " +
                         "to fit in the column.",
                         PigWarning.UDF_WARNING_1);
                    sb.append(truncated);
                } else {
                    // Field is float or double but cannot be rounded to fit
                    warn("Cannot fit " + fieldStr + " in field starting at column " + 
                         column.start + " and ending at column " + (column.end - 1) + ". " +
                         "Writing null (all spaces) instead.",
                         PigWarning.UDF_WARNING_2);
                    for (int i = 0; i < width; i++) {
                        sb.append(' ');
                    }
                }
            } else {
                warn("Cannot fit " + fieldStr + " in field starting at column " + 
                      column.start + " and ending at column " + (column.end - 1) + ". " +
                      "Writing null (all spaces) instead.",
                      PigWarning.UDF_WARNING_2);
                for (int i = 0; i < width; i++) {
                    sb.append(' ');
                }
            }
        } else {
            // Field can fit. Right-justify it.
            int spaces = width - fieldStr.length();
            for (int i = 0; i < spaces; i++) {
                sb.append(' ');
            }
            sb.append(fieldStr);
        }

        return sb.toString();
    }

    public ResourceStatistics getStatistics(String location, Job job)
            throws IOException {
        // Not implemented
        return null;
    }

    public void storeStatistics(ResourceStatistics stats, String location, Job job) 
                                throws IOException {
        // Not implemented
    }

    public String[] getPartitionKeys(String location, Job job)
            throws IOException {
        // Not implemented
        return null;
    }

    public void setPartitionFilter(Expression partitionFilter)
            throws IOException {
        // Not implemented
    }
}