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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.log4j.Logger;

import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFuncInterface;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;

import org.apache.pig.builtin.PigStorage;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;

import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.StorageUtil;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;

import org.apache.pig.parser.ParserException;

/**
 * CSV loading and storing with support for multi-line fields, 
 * and escaping of delimiters and double quotes within fields; 
 * uses CSV conventions of Excel 2007.
 * 
 * Arguments allow for control over:
 *
 * Which field delimiter to use (default = ',')
 * Whether line breaks are allowed inside of fields (YES_MULTILINE = yes, NO_MULTILINE = no, default = no)
 * How line breaks are to be written when storing (UNIX = LF, WINDOWS = CRLF, NOCHANGE = system default, default = system default)
 * What to do with header rows (first line of each file):
 *     On load: READ_INPUT_HEADER = read header rows, SKIP_INPUT_HEADER = do not read header rows, default = read header rows
 *     On store: WRITE_OUTPUT_HEADER = write a header row, SKIP_OUTPUT_HEADER = do not write a header row, default = do not write a header row
 *
 * Usage:
 *
 * STORE x INTO '<destFileName>'
 *         USING org.apache.pig.piggybank.storage.CSVExcelStorage(
 *              [DELIMITER[, 
 *                  {YES_MULTILINE | NO_MULTILINE}[, 
 *                      {UNIX | WINDOWS | NOCHANGE}[, 
 *                          {READ_INPUT_HEADER, SKIP_INPUT_HEADER, WRITE_OUTPUT_HEADER, SKIP_OUTPUT_HEADER}]]]]
 *         );
 * 
 * Linebreak settings are only used during store; during load, no conversion is performed.
 *
 * WARNING: A danger with enabling multiline fields during load is that unbalanced
 *          double quotes will cause slurping up of input until a balancing double
 *          quote is found, or until something breaks. If you are not expecting
 *          newlines within fields it is therefore more robust to use NO_MULTILINE,
 *          which is the default for that reason.
 * 
 * This is Adreas Paepcke's <paepcke@cs.stanford.edu> CSVExcelStorage with a few modifications.
 */

public class CSVExcelStorage extends PigStorage implements StoreFuncInterface, LoadPushDown {

    public static enum Linebreaks { UNIX, WINDOWS, NOCHANGE };
    public static enum Multiline { YES, NO };
    public static enum Headers { DEFAULT, READ_INPUT_HEADER, SKIP_INPUT_HEADER, WRITE_OUTPUT_HEADER, SKIP_OUTPUT_HEADER }

    protected final static byte LINEFEED = '\n';
    protected final static byte DOUBLE_QUOTE = '"';
    protected final static byte RECORD_DEL = LINEFEED;

    private static final String FIELD_DELIMITER_DEFAULT_STR = ",";
    private static final String MULTILINE_DEFAULT_STR = "NO_MULTILINE";
    private static final String EOL_DEFAULT_STR = "NOCHANGE";
    private static final String HEADER_DEFAULT_STR = "DEFAULT";
    
    long end = Long.MAX_VALUE;

    private byte fieldDelimiter = ',';
    private Multiline multilineTreatment = Multiline.NO;
    private Linebreaks eolTreatment = Linebreaks.NOCHANGE;
    private Headers headerTreatment = Headers.DEFAULT;

    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private String udfContextSignature;
    private String loadLocation;
    private boolean[] mRequiredColumns = null;
    private boolean mRequiredColumnsInitialized = false;
    
    final Logger logger = Logger.getLogger(getClass().getName());

    @SuppressWarnings("rawtypes")
    protected RecordReader in = null;    

    // For replacing LF with CRLF (Unix --> Windows end-of-line convention):
    Pattern loneLFDetectorPattern = Pattern.compile("([^\r])\n", Pattern.DOTALL | Pattern.MULTILINE);
    Matcher loneLFDetector = loneLFDetectorPattern.matcher("");

    // For removing CR (Windows --> Unix):
    Pattern CRLFDetectorPattern = Pattern.compile("\r\n", Pattern.DOTALL | Pattern.MULTILINE);
    Matcher CRLFDetector = CRLFDetectorPattern.matcher("");


    // Pig Storage with COMMA as delimiter:
    TupleFactory tupleMaker = TupleFactory.getInstance();
    private boolean getNextInQuotedField;
    private int getNextFieldID;
    private boolean nextTupleSkipChar;

    // For handling headers
    private boolean loadingFirstRecord = true;
    private boolean storingFirstRecord = true;
    private String header = null;
    private int splitIndex;

    private static final String SCHEMA_SIGNATURE = "pig.csvexcelstorage.schema";
    protected ResourceSchema schema = null;

    /*-----------------------------------------------------
    | Constructors 
    ------------------------*/

    public CSVExcelStorage() {
        super(FIELD_DELIMITER_DEFAULT_STR);
        initializeInstance(FIELD_DELIMITER_DEFAULT_STR, MULTILINE_DEFAULT_STR, EOL_DEFAULT_STR, HEADER_DEFAULT_STR);
    }
    
    public CSVExcelStorage(String delimiter) {
        super(delimiter);
        initializeInstance(delimiter, MULTILINE_DEFAULT_STR, EOL_DEFAULT_STR, HEADER_DEFAULT_STR);
    }

    public CSVExcelStorage(String delimiter, String multilineTreatmentStr) {
        super(delimiter);
        initializeInstance(delimiter, multilineTreatmentStr, EOL_DEFAULT_STR, HEADER_DEFAULT_STR);
    }

    public CSVExcelStorage(String delimiter, String multilineTreatmentStr, String eolTreatmentStr) {
        super(delimiter);
        initializeInstance(delimiter, multilineTreatmentStr, eolTreatmentStr, HEADER_DEFAULT_STR);
    }

    public CSVExcelStorage(String delimiter, String multilineTreatmentStr, String eolTreatmentStr, String headerTreatmentStr) {
        super(delimiter);
        initializeInstance(delimiter, multilineTreatmentStr, eolTreatmentStr, headerTreatmentStr);
    }
    
    private void initializeInstance(String delimiter, String multilineTreatmentStr, String eolTreatmentStr, String headerTreatmentStr) {
        fieldDelimiter = StorageUtil.parseFieldDel(delimiter);

        multilineTreatment = canonicalizeMultilineTreatmentRequest(multilineTreatmentStr);
        eolTreatment = canonicalizeEOLTreatmentRequest(eolTreatmentStr);
        headerTreatment = canonicalizeHeaderTreatmentRequest(headerTreatmentStr);
    }
    
    private Multiline canonicalizeMultilineTreatmentRequest(String multilineTreatmentStr) {
        if (multilineTreatmentStr.equalsIgnoreCase("YES_MULTILINE"))
            return Multiline.YES;
        else if (multilineTreatmentStr.equalsIgnoreCase("NO_MULTILINE"))
            return Multiline.NO;

        throw new IllegalArgumentException(
                "Unrecognized multiline treatment argument " + multilineTreatmentStr + ". " +
                "Should be either 'YES_MULTILINE' or 'NO_MULTILINE'");
    }
    
    private Linebreaks canonicalizeEOLTreatmentRequest(String eolTreatmentStr) {
        if (eolTreatmentStr.equalsIgnoreCase("UNIX"))
            return Linebreaks.UNIX;
        else if (eolTreatmentStr.equalsIgnoreCase("WINDOWS"))
            return Linebreaks.WINDOWS;
        else if (eolTreatmentStr.equalsIgnoreCase("NOCHANGE"))
            return Linebreaks.NOCHANGE;

        throw new IllegalArgumentException(
                "Unrecognized end-of-line treatment argument " + eolTreatmentStr + ". " +
                "Should be one of 'UNIX', 'WINDOWS', or 'NOCHANGE'");
    }

    private Headers canonicalizeHeaderTreatmentRequest(String headerTreatmentStr) {
        if (headerTreatmentStr.equalsIgnoreCase("DEFAULT"))
            return Headers.DEFAULT;
        else if (headerTreatmentStr.equalsIgnoreCase("READ_INPUT_HEADER"))
            return Headers.READ_INPUT_HEADER;
        else if (headerTreatmentStr.equalsIgnoreCase("SKIP_INPUT_HEADER"))
            return Headers.SKIP_INPUT_HEADER;
        else if (headerTreatmentStr.equalsIgnoreCase("WRITE_OUTPUT_HEADER"))
            return Headers.WRITE_OUTPUT_HEADER;
        else if (headerTreatmentStr.equalsIgnoreCase("SKIP_OUTPUT_HEADER"))
            return Headers.SKIP_OUTPUT_HEADER;

        throw new IllegalArgumentException(
            "Unrecognized header treatment argument " + headerTreatmentStr + ". " +
            "Should be one of 'READ_INPUT_HEADER', 'SKIP_INPUT_HEADER', 'WRITE_OUTPUT_HEADER', 'SKIP_OUTPUT_HEADER'");
    }
    
    // ---------------------------------------- STORAGE -----------------------------
    
    public void checkSchema(ResourceSchema s) throws IOException {
        // Not actually checking schema
        // Actually, just storing it to use in the backend
        
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p =
            udfc.getUDFProperties(this.getClass(), new String[]{ udfContextSignature });
        p.setProperty(SCHEMA_SIGNATURE, s.toString());
    }

    public void prepareToWrite(RecordWriter writer) {
        // Get the schema string from the UDFContext object.
        UDFContext udfc = UDFContext.getUDFContext();
        Properties p =
            udfc.getUDFProperties(this.getClass(), new String[]{ udfContextSignature });

        String strSchema = p.getProperty(SCHEMA_SIGNATURE);
        if (strSchema != null) {
            // Parse the schema from the string stored in the properties object.
            try {
                schema = new ResourceSchema(Utils.getSchemaFromString(strSchema));
            } catch (ParserException pex) {
                logger.warn("Could not parse schema for storing.");
            }
        }

        if (headerTreatment == Headers.DEFAULT) {
            headerTreatment = Headers.SKIP_OUTPUT_HEADER;
        }

        // PigStorage's prepareToWrite()
        super.prepareToWrite(writer);
    }

    /* (non-Javadoc)
     * @see org.apache.pig.builtin.PigStorage#putNext(org.apache.pig.data.Tuple)
     * 
     * Given a tuple that corresponds to one record, write
     * it out as CSV, converting among Unix/Windows line
     * breaks as requested in the instantiation. Also take
     * care of escaping field delimiters, double quotes,
     * and linebreaks embedded within fields,
     * 
     */
    @Override
    public void putNext(Tuple tupleToWrite) throws IOException {
        // If WRITE_OUTPUT_HEADER, store a header record with the names of each field
        if (storingFirstRecord && headerTreatment == Headers.WRITE_OUTPUT_HEADER && schema != null) {
            ArrayList<Object> headerProtoTuple = new ArrayList<Object>();
            ResourceFieldSchema[] fields = schema.getFields();
            for (ResourceFieldSchema field : fields) {
                headerProtoTuple.add(field.getName());
            }
            super.putNext(tupleMaker.newTuple(headerProtoTuple));
        }
        storingFirstRecord = false;

        ArrayList<Object> mProtoTuple = new ArrayList<Object>();
        int embeddedNewlineIndex = -1;
        String fieldStr = null;
        // For good debug messages:
        int fieldCounter = -1;
        
        // Do the escaping:
        for (Object field : tupleToWrite.getAll()) {
            fieldCounter++;

            // Substitute a null value with an empty string. See PIG-2470.
            if (field == null) {
                mProtoTuple.add("");
                continue;
            }
            
            fieldStr = field.toString();
            
            // Embedded double quotes are replaced by two double quotes:
            fieldStr = fieldStr.replaceAll("[\"]", "\"\"");
            
            // If any field delimiters are in the field, or if we did replace
            // any double quotes with a pair of double quotes above,
            // or if the string includes a newline character (LF:\n:0x0A)
            // and we are to allow newlines in fields,
            // then the entire field must be enclosed in double quotes:
            embeddedNewlineIndex =  fieldStr.indexOf(LINEFEED);
            
            if ((fieldStr.indexOf(fieldDelimiter) != -1) || 
                (fieldStr.indexOf(DOUBLE_QUOTE) != -1) ||
                (multilineTreatment == Multiline.YES) && (embeddedNewlineIndex != -1))  {
                fieldStr = "\"" + fieldStr + "\"";
            }
            
            // If requested: replace any Linefeed-only (^J), with LF-Newline (^M^J),
            // This is needed for Excel to recognize a field-internal 
            // new line:

            if ((eolTreatment != Linebreaks.NOCHANGE) && (embeddedNewlineIndex != -1)) {
                if (eolTreatment == Linebreaks.WINDOWS) {
                    loneLFDetector.reset(fieldStr);
                    loneLFDetector.matches();
                    fieldStr = loneLFDetector.replaceAll("$1\r\n");
                } else if (eolTreatment == Linebreaks.UNIX) {
                    CRLFDetector.reset(fieldStr);
                    fieldStr = CRLFDetector.replaceAll("\n");
                }
            }

            mProtoTuple.add(fieldStr);          
        }
        // If Windows line breaks are requested, append 
        // a newline (0x0D a.k.a. ^M) to the last field
        // so that the row termination will end up being
        // \r\n, once the superclass' putNext() method
        // is done below:

        if ((eolTreatment == Linebreaks.WINDOWS) && (fieldStr != null))
            mProtoTuple.set(mProtoTuple.size() - 1, fieldStr + "\r"); 

        Tuple resTuple = tupleMaker.newTuple(mProtoTuple);
        super.putNext(resTuple);
    }

    // ---------------------------------------- LOADING  -----------------------------  

    /* (non-Javadoc)
     * @see org.apache.pig.builtin.PigStorage#getNext()
     */
    @Override
    public Tuple getNext() throws IOException {
        // If SKIP_INPUT_HEADER and this is the first input split, skip header record
        // We store its value as a string though, so we can compare
        // further records to it. If they are the same (this would 
        // happen if multiple small files each with a header were combined
        // into one split), we know to skip the duplicate header record as well.
        if (loadingFirstRecord && headerTreatment == Headers.SKIP_INPUT_HEADER &&
                (splitIndex == 0 || splitIndex == -1)) {
            try {
                if (!in.nextKeyValue())
                    return null;
                header = ((Text) in.getCurrentValue()).toString();
            } catch (InterruptedException e) {
                int errCode = 6018;
                String errMsg = "Error while reading input";
                throw new ExecException(errMsg, errCode, 
                        PigException.REMOTE_ENVIRONMENT, e);
            }
        }
        loadingFirstRecord = false;

        mProtoTuple = new ArrayList<Object>();

        getNextInQuotedField = false;
        boolean evenQuotesSeen = true;
        boolean sawEmbeddedRecordDelimiter = false;
        byte[] buf = null;
        
        if (!mRequiredColumnsInitialized) {
            if (udfContextSignature != null) {
                Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
                mRequiredColumns = (boolean[]) ObjectSerializer.deserialize(p.getProperty(udfContextSignature));
            }
            mRequiredColumnsInitialized = true;
        }
        // Note: we cannot factor out the check for nextKeyValue() being null,
        // because that call overwrites buf with the new line, which is
        // bad if we have a field with a newline.

        try {
            int recordLen = 0;
            getNextFieldID = 0;
            
            while (sawEmbeddedRecordDelimiter || getNextFieldID == 0) {
                Text value = null;
                if (sawEmbeddedRecordDelimiter) {
                    
                    // Deal with pulling more records from the input, because
                    // a double quoted embedded newline was encountered in a field.
                    // Save the length of the record so far, plus one byte for the 
                    // record delimiter (usually newline) that's embedded in the field 
                    // we were working on before falling into this branch:
                    int prevLineLen = recordLen + 1;
                    
                    // Save previous line (the one with the field that has the newline) in a new array.
                    // The last byte will be random; we'll fill in the embedded
                    // record delimiter (usually newline) below:
                    byte[] prevLineSaved = Arrays.copyOf(buf, prevLineLen);
                    prevLineSaved[prevLineLen - 1] = RECORD_DEL;
                    
                    // Read the continuation of the record, unless EOF:
                    if (!in.nextKeyValue()) {
                        return null;
                    }                                                                                           
                    value = (Text) in.getCurrentValue();
                    recordLen = value.getLength();
                    // Grab the continuation's bytes:
                    buf = value.getBytes();
                    
                    // Combine the previous line and the continuation into a new array.
                    // The following copyOf() does half the job: it allocates all the
                    // space, and also copies the previous line into that space:
                    byte[] prevLineAndContinuation = Arrays.copyOf(prevLineSaved, prevLineLen + recordLen);
                    
                    // Now append the continuation. Parms: fromBuf, fromStartPos, toBuf, toStartPos, lengthToCopy:
                    System.arraycopy(buf, 0, prevLineAndContinuation, prevLineLen, recordLen);
                    
                    // We'll work with the combination now:
                    buf = prevLineAndContinuation;
                    
                    // Do the whole record over from the start:
                    mProtoTuple.clear();
                    getNextInQuotedField = false;
                    evenQuotesSeen = true;
                    getNextFieldID = 0;
                    recordLen = prevLineAndContinuation.length;
                    
                } else {
                    // Previous record finished cleanly: start with the next record,
                    // unless EOF:
                    if (!in.nextKeyValue()) {
                        return null;
                    }                                                                                           
                    value = (Text) in.getCurrentValue();

                    // if the line is a duplicate header and 'SKIP_INPUT_HEADER' is set, ignore it
                    // (this might happen if multiple files each with a header are combined into a single split)
                    if (headerTreatment == Headers.SKIP_INPUT_HEADER && value.toString().equals(header)) {
                        if (!in.nextKeyValue())
                            return null;
                        value = (Text) in.getCurrentValue();
                    }

                    buf = value.getBytes();
                    getNextFieldID = 0;
                    recordLen = value.getLength();
                }
                
                nextTupleSkipChar = false;

                ByteBuffer fieldBuffer = ByteBuffer.allocate(recordLen);

                sawEmbeddedRecordDelimiter = processOneInRecord(evenQuotesSeen,
                        buf, recordLen, fieldBuffer);

                // The last field is never delimited by a FIELD_DEL, but by
                // the end of the record. So we need to add that last field.
                // The '!sawEmbeddedRecordDelimiter' handles the case of
                // embedded newlines; we are amidst a field, not at
                // the final record:
                if (!sawEmbeddedRecordDelimiter) 
                    readField(fieldBuffer, getNextFieldID++);
            } // end while

        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode, 
                    PigException.REMOTE_ENVIRONMENT, e);
        }

        Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);
        return t;
    }

    /*
     * Service method for getNext().
     * Looks at char after char in the input record,
     * that was previously pulled in by getNext(),
     * and fills the fieldBuffer with those chars.
     * <p> 
     * If multilineTreatment is Multiline.YES, then
     * the return value indicates whether an embedded
     * newline was found in a field, and that newline
     * was in a field that opened with a double quote
     * that was not closed before the end of the 
     * record was reached. If multilineTreatment
     * is Multine.NO, then the return value is always false.
     * <p> 
     * A return value of true will cause the calling method
     * to continue pulling records from the input stream,
     * until a closing quote is found.
     * <p> 
     * Note that the recordReader that delivers records
     * to out getNext() method above considers record 
     * boundaries to be newlines. We therefore never see an actual
     * newline character embedded in a field. We just
     * run out of record. For Multiline.NO we just take
     * such an end of record at face value; the final 
     * resulting tuple will contain information only up
     * to the first newline that was found. 
     * <p> 
     * For Multiline.YES, when we run out of record 
     * in an open double quote, our return of true from
     * this method will cause the caller getNext() to
     * do its additional readings of records from the
     * stream, until the closing double quote is found.
     *  <p> 
     *  
     * @param evenQuotesSeen
     * @param sawEmbeddedRecordDelimiter
     * @param buf
     * @param recordLen
     * @param fieldBuffer
     * @return
     */
    private boolean processOneInRecord(boolean evenQuotesSeen,
                                       byte[] buf, int recordLen,
                                       ByteBuffer fieldBuffer) {
        for (int i = 0; i < recordLen; i++) {
            if (nextTupleSkipChar) {
                nextTupleSkipChar = false;
                continue;
            }
            byte b = buf[i];
            if (getNextInQuotedField) {
                if (b == DOUBLE_QUOTE) {
                    // Does a double quote immediately follow?
                    if ((i < recordLen-1) && (buf[i+1] == DOUBLE_QUOTE)) {
                        fieldBuffer.put(b);
                        nextTupleSkipChar = true;
                        continue;
                    }
                    evenQuotesSeen = !evenQuotesSeen;

                    // If the quote is ending the last field in a record,
                    // set the genNextInQuotedField flag to false,
                    // so the return statement conditional (see below)
                    // is false, indicating that we're ready for the next record
                    if (!evenQuotesSeen && i == recordLen - 1) {
                        getNextInQuotedField = false;
                    }

                    if (evenQuotesSeen) {
                        fieldBuffer.put(DOUBLE_QUOTE);
                    }
                } else if (!evenQuotesSeen && (b == fieldDelimiter || b == RECORD_DEL)) {
                    getNextInQuotedField = false;
                    readField(fieldBuffer, getNextFieldID++);
                } else {
                    fieldBuffer.put(b);
                }
            } else if (b == DOUBLE_QUOTE) {
                // Does a double quote immediately follow?                  
                if ((i < recordLen-1) && (buf[i+1] == DOUBLE_QUOTE)) {
                    fieldBuffer.put(b);
                    nextTupleSkipChar = true;
                    continue;
                }
                // If we are at the start of a field,
                // that entire field is quoted:
                getNextInQuotedField = true;
                evenQuotesSeen = true;
            } else if (b == fieldDelimiter) {
                readField(fieldBuffer, getNextFieldID++); // end of the field
            } else {
                evenQuotesSeen = true;
                fieldBuffer.put(b);
            }
        } // end for
        return getNextInQuotedField && (multilineTreatment == Multiline.YES);
    }

    private void readField(ByteBuffer buf, int fieldID) {
        if (mRequiredColumns==null || (mRequiredColumns.length>fieldID && mRequiredColumns[fieldID])) {
            byte[] bytes = new byte[buf.position()];
            buf.rewind();
            buf.get(bytes, 0, bytes.length); 
            mProtoTuple.add(new DataByteArray(bytes));
        }
        buf.clear();
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        loadLocation = location;
        FileInputFormat.setInputPaths(job, location);        
    }

    @SuppressWarnings("rawtypes")
    @Override
    public InputFormat getInputFormat() {
        if(loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
            return new Bzip2TextInputFormat();
        } else {
            return new PigTextInputFormat();
        }
    }

    @Override
    public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) {
        in = reader;
        splitIndex = split.getSplitIndex();
        
        if (headerTreatment == Headers.DEFAULT) {
            headerTreatment = Headers.READ_INPUT_HEADER;
        }
    }

    @Override
    public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) throws FrontendException {
        if (requiredFieldList == null)
            return null;
        if (requiredFieldList.getFields() != null)
        {
            int lastColumn = -1;
            for (RequiredField rf: requiredFieldList.getFields())
            {
                if (rf.getIndex()>lastColumn)
                {
                    lastColumn = rf.getIndex();
                }
            }
            mRequiredColumns = new boolean[lastColumn+1];
            for (RequiredField rf: requiredFieldList.getFields())
            {
                if (rf.getIndex()!=-1)
                    mRequiredColumns[rf.getIndex()] = true;
            }
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
            try {
                p.setProperty(udfContextSignature, ObjectSerializer.serialize(mRequiredColumns));
            } catch (Exception e) {
                throw new RuntimeException("Cannot serialize mRequiredColumns");
            }
        }
        return new RequiredFieldResponse(true);
    }

    @Override
    public void setUDFContextSignature(String signature) {
        this.udfContextSignature = signature; 
    }

    @Override
    public List<OperatorSet> getFeatures() {
        return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
    }
}
