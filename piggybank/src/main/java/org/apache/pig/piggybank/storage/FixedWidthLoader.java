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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigWarning;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * A fixed-width file loader. 
 * 
 * Takes a string argument specifying the ranges of each column in a unix 'cut'-like format.
 * Ex: '-5, 10-12, 14, 20-'
 * Ranges are comma-separated, 1-indexed (for ease of use with 1-indexed text editors), and inclusive.
 * A single-column field at position n may be specified as either 'n-n' or simply 'n'.
 *
 * A second optional argument specifies whether to skip the first row of the input file,
 * assuming it to be a header. As Pig may combine multiple input files each with their own header
 * into a single split, FixedWidthLoader makes sure to skip any duplicate headers as will.
 * 'SKIP_HEADER' skips the row; anything else and the default behavior ('USE_HEADER') is not to skip it.
 *
 * A third optional argument specifies a Pig schema to load the data with. Automatically
 * trims whitespace from numeric fields. Note that if fewer fields are specified in the
 * schema than are specified in the column spec, only the fields in the schema will
 * be used.
 *
 * Warning: fields loaded as char/byte arrays will trim all leading and trailing whitespace
 * from the field value as it is indistiguishable from the spaces that separate different fields.
 *
 * All datetimes are converted to UTC when loaded.
 *
 * Column spec idea and syntax parser borrowed from Russ Lankenau's implementation
 * at https://github.com/rlankenau/fixed-width-pig-loader 
 */
public class FixedWidthLoader extends LoadFunc implements LoadMetadata, LoadPushDown {
    
    public static class FixedWidthField {
        int start, end;

        FixedWidthField(int start, int end) {
            this.start = start;
            this.end = end;
        }
    }

    private TupleFactory tupleFactory = TupleFactory.getInstance();

    private RecordReader reader = null;
    
    private ArrayList<FixedWidthField> columns;

    private ResourceSchema schema = null;
    private ResourceFieldSchema[] fields;

    private boolean loadingFirstRecord = true;
    private boolean skipHeader = false;
    private String header = null;
    private int splitIndex;

    private boolean[] requiredFields = null;
    private int numRequiredFields;

    private String udfContextSignature = null;
    private static final String SCHEMA_SIGNATURE = "pig.fixedwidthloader.schema";
    private static final String REQUIRED_FIELDS_SIGNATURE = "pig.fixedwidthloader.required_fields";
    private static final Log log = LogFactory.getLog(FixedWidthLoader.class);

    /*
     * Constructors and helper methods
     */

    public FixedWidthLoader() {
        throw new IllegalArgumentException(
            "Usage: org.apache.pig.piggybank.storage.FixedWidthLoader(" +
            "'<column spec>'[, { 'USE_HEADER' | 'SKIP_HEADER' }[, '<schema>']]" +
            ")"
        );
    }

    public FixedWidthLoader(String columnSpec) {
        try {
            columns = parseColumnSpec(columnSpec);
            String schemaStr = generateDefaultSchemaString();
            schema = new ResourceSchema(Utils.getSchemaFromString(schemaStr));
            fields = schema.getFields();
        } catch (ParserException e) {
            throw new IllegalArgumentException("Invalid schema format: " + e.getMessage());
        }
    }

    public FixedWidthLoader(String columnSpec, String skipHeaderStr) {
        this(columnSpec);
        if (skipHeaderStr.equalsIgnoreCase("SKIP_HEADER"))
            skipHeader = true;
    }

    public FixedWidthLoader(String columnSpec, String skipHeaderStr, String schemaStr) {
        try {
            columns = parseColumnSpec(columnSpec);
            schemaStr = schemaStr.replaceAll("[\\s\\r\\n]", "");
            schema = new ResourceSchema(Utils.getSchemaFromString(schemaStr));
            fields = schema.getFields();

            for (int i = 0; i < fields.length; i++) {
                byte fieldType = fields[i].getType();
                if (fieldType == DataType.MAP || fieldType == DataType.TUPLE || fieldType == DataType.BAG) {
                    throw new IllegalArgumentException(
                        "Field \"" + fields[i].getName() + "\" is an object type (map, tuple, or bag). " + 
                        "Object types are not supported by FixedWidthLoader."
                    );
                }
            }

            if (fields.length < columns.size())
                warn("More columns specified in column spec than fields specified in schema. Only loading fields specified in schema.",
                     PigWarning.UDF_WARNING_2);
            else if (fields.length > columns.size())
                throw new IllegalArgumentException("More fields specified in schema than columns specified in column spec.");
        } catch (ParserException e) {
            throw new IllegalArgumentException("Invalid schema format: " + e.getMessage());
        }

        if (skipHeaderStr.equalsIgnoreCase("SKIP_HEADER"))
            skipHeader = true;
    }

    public static ArrayList<FixedWidthField> parseColumnSpec(String spec) {
        ArrayList<FixedWidthField> columns = new ArrayList<FixedWidthField>();
        String[] ranges = spec.split(",");

        for (String range : ranges) {

            // Ranges are 1-indexed and inclusive-inclusive [] in spec,
            // but we convert to 0-indexing and inclusive-exclusive [) internally
            
            if (range.indexOf("-") != -1) {
                int start, end;

                String offsets[] = range.split("-", 2);
                offsets[0] = offsets[0].trim();
                offsets[1] = offsets[1].trim();
                
                if (offsets[0].equals(""))
                    start = 0;
                else
                    start = Integer.parseInt(offsets[0]) - 1;

                if (offsets[1].equals(""))
                    end = Integer.MAX_VALUE;
                else
                    end = Integer.parseInt(offsets[1]);

                if (start + 1 < 1)
                    throw new IllegalArgumentException("Illegal column spec '" + range + "': start value must be at least 1");
                if (start + 1 > end)
                    throw new IllegalArgumentException("Illegal column spec '" + range + "': start value must be less than end value"); 

                columns.add(new FixedWidthField(start, end));
            } else {
                int offset = Integer.parseInt(range.trim()) - 1;
                columns.add(new FixedWidthField(offset, offset + 1));
            }
        }

        return columns;
    }

    private String generateDefaultSchemaString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            sb.append((i == 0? "" : ", ") + "f" + i + ": bytearray");
        }
        return sb.toString();
    }

    /*
     * Methods called on the frontend
     */

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new TextInputFormat();
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public void setUDFContextSignature( String signature ) {
        udfContextSignature = signature;
    }

    public ResourceSchema getSchema(String location, Job job)
            throws IOException {

        if (schema != null) {
            // Send schema to backend
            // Schema should have been passed as an argument (-> constructor)
            // or provided in the default constructor

            UDFContext udfc = UDFContext.getUDFContext();
            Properties p = udfc.getUDFProperties(this.getClass(), new String[]{ udfContextSignature });
            p.setProperty(SCHEMA_SIGNATURE, schema.toString());

            return schema;
        } else {
            // Should never get here
            throw new IllegalArgumentException(
                "No schema found: default schema was never created and no user-specified schema was found."
            );
        }
    }

    /*
     * Methods called on the backend
     */

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        // Save reader to use in getNext()
        this.reader = reader;

        splitIndex = split.getSplitIndex();

        // Get schema from front-end
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p = udfc.getUDFProperties(this.getClass(), new String[] { udfContextSignature });

        String strSchema = p.getProperty(SCHEMA_SIGNATURE);
        if (strSchema == null) {
            throw new IOException("Could not find schema in UDF context");
        }
        schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));

        requiredFields = (boolean[]) ObjectSerializer.deserialize(p.getProperty(REQUIRED_FIELDS_SIGNATURE));
        if (requiredFields != null) {
            numRequiredFields = 0;
            for (int i = 0; i < requiredFields.length; i++) {
                if (requiredFields[i])
                    numRequiredFields++;
            }
        }
    }
    
    @Override
    public Tuple getNext() throws IOException {
        if (loadingFirstRecord && skipHeader && (splitIndex == 0 || splitIndex == -1)) {
            try {
                if (!reader.nextKeyValue()) 
                    return null;
                header = ((Text) reader.getCurrentValue()).toString();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        loadingFirstRecord = false;

        String line;
        try {
            if (!reader.nextKeyValue()) return null;
            line = ((Text) reader.getCurrentValue()).toString();

            // if the line is a duplicate header and 'SKIP_HEADER' is set, ignore it
            // (this might happen if multiple files each with a header are combined into a single split)
            if (line.equals(header)) {
                if (!reader.nextKeyValue()) return null;
                line = ((Text) reader.getCurrentValue()).toString();
            }
        } catch (Exception e) {
            throw new IOException(e);
        }

        Tuple t;
        if (requiredFields != null) {
            t = tupleFactory.newTuple(numRequiredFields);
            int count = 0;
            for (int i = 0; i < fields.length; i++) {
                if (requiredFields[i]) {
                    try {
                        t.set(count, readField(line, fields[i], columns.get(i)));
                    } catch (Exception e) {
                        warn("Exception when parsing field \"" + fields[i].getName() + "\" " +
                             "in record " + line.toString() + ": " + e.toString(),
                             PigWarning.UDF_WARNING_1);
                    }
                    count++;
                }
            }
        } else {
            t = tupleFactory.newTuple(fields.length);
            for (int i = 0; i < fields.length; i++) {
                try {
                    t.set(i, readField(line, fields[i], columns.get(i)));
                } catch (Exception e) {
                    warn("Exception when parsing field \"" + fields[i].getName() + "\" " +
                         "in record " + line.toString() + ": " + e.toString(),
                         PigWarning.UDF_WARNING_1);
                }
            }
        }

        return t;
    }

    private Object readField(String line, ResourceFieldSchema field, FixedWidthField column) 
                             throws IOException, IllegalArgumentException {

        int start = column.start;
        int end = Math.min(column.end, line.length());

        if (start > line.length())
            return null;

        if (end <= start)
            return null;

        String s  = line.substring(start, end);
        String sTrim = s.trim();

        switch (field.getType()) {
            case DataType.UNKNOWN:
            case DataType.BYTEARRAY:
            case DataType.CHARARRAY:
                if (s.trim().length() == 0)
                    return null;
                return s.trim();

            case DataType.BOOLEAN:
                return Boolean.parseBoolean(sTrim);

            case DataType.INTEGER:
                return Integer.parseInt(sTrim);

            case DataType.LONG:
                return Long.parseLong(sTrim);

            case DataType.FLOAT:
                return Float.parseFloat(sTrim);
            
            case DataType.DOUBLE:
                return Double.parseDouble(sTrim);

            case DataType.DATETIME:
                return (new DateTime(sTrim)).toDateTime(DateTimeZone.UTC);

            case DataType.MAP:
            case DataType.TUPLE:
            case DataType.BAG:
                throw new IllegalArgumentException("Object types (map, tuple, bag) are not supported by FixedWidthLoader");
            
            default:
                throw new IllegalArgumentException(
                    "Unknown type in input schema: " + field.getType());
        }
    }

    @Override
    public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) throws FrontendException {
        if (requiredFieldList == null)
            return null;

        if (fields != null && requiredFieldList.getFields() != null)
        {
            requiredFields = new boolean[fields.length];

            for (RequiredField f : requiredFieldList.getFields()) {
                requiredFields[f.getIndex()] = true;
            }

            UDFContext udfc = UDFContext.getUDFContext();
            Properties p = udfc.getUDFProperties(this.getClass(), new String[]{ udfContextSignature });
            try {
                p.setProperty(REQUIRED_FIELDS_SIGNATURE, ObjectSerializer.serialize(requiredFields));
            } catch (Exception e) {
                throw new RuntimeException("Cannot serialize requiredFields for pushProjection");
            }
        }

        return new RequiredFieldResponse(true);
    }

    @Override
    public List<OperatorSet> getFeatures() {
        return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
    }

    public ResourceStatistics getStatistics(String location, Job job)
            throws IOException {
        // Not implemented
        return null;
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