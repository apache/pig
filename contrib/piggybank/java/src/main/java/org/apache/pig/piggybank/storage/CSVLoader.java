package org.apache.pig.piggybank.storage;

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Based on earlier version for Pig 0.3 which was Copyright 2009 James Kebinger
 * http://github.com/jkebinger/pig-user-defined-functions 
 * and on built-in PigStorage
 *
 */


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

/**
 * A load function based on PigStorage that implements part of the CSV "standard"
 * This loader properly supports double-quoted fields that contain commas and other
 * double-quotes escaped with backslashes.
 *
 * The following fields are all parsed as one tuple, per each line
 * "the man, he said ""hello"""
 * "one,two,three"
 *
 * This version supports pig 0.7+
 *
 */

public class CSVLoader extends FileInputLoadFunc implements LoadPushDown {

    @SuppressWarnings("rawtypes")
    protected RecordReader in = null;    

    protected static final Log LOG = LogFactory.getLog(CSVLoader.class);
    private static final byte DOUBLE_QUOTE = '"';
    private static final byte FIELD_DEL = ',';
    private static final byte RECORD_DEL = '\n';

    long end = Long.MAX_VALUE;

    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();

    private String signature;
    private String loadLocation;

    private boolean[] mRequiredColumns = null;

    private boolean mRequiredColumnsInitialized = false;

    public CSVLoader() {
    }

    @Override
    public Tuple getNext() throws IOException {
        mProtoTuple = new ArrayList<Object>();

        boolean inField = false;
        boolean inQuotedField = false;
        boolean evenQuotesSeen = true;
        
        if (!mRequiredColumnsInitialized) {
            if (signature != null) {
                Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
                mRequiredColumns = (boolean[])ObjectSerializer.deserialize(p.getProperty(signature));
            }
            mRequiredColumnsInitialized = true;
        }
        try {
            if (!in.nextKeyValue()) {
                return null;
            }                                                                                           
            Text value = (Text) in.getCurrentValue();
            byte[] buf = value.getBytes();
            int len = value.getLength();
            int fieldID = 0;

            ByteBuffer fieldBuffer = ByteBuffer.allocate(len);

            for (int i = 0; i < len; i++) {
                byte b = buf[i];
                inField = true;
                if (inQuotedField) {
                    if (b == DOUBLE_QUOTE) {
                        evenQuotesSeen = !evenQuotesSeen;
                        if (evenQuotesSeen) {
                            fieldBuffer.put(DOUBLE_QUOTE);
                        }
                    } else
                        if (!evenQuotesSeen &&
                                (b == FIELD_DEL || b == RECORD_DEL)) {
                            inQuotedField = false;
                            inField = false;
                            readField(fieldBuffer, fieldID++);
                        } else {
                            fieldBuffer.put(b);
                        }
                } else if (b == DOUBLE_QUOTE) {
                    inQuotedField = true;
                    evenQuotesSeen = true;
                } else if (b == FIELD_DEL) {
                    inField = false;
                    readField(fieldBuffer, fieldID++); // end of the field
                } else {
                    evenQuotesSeen = true;
                    fieldBuffer.put(b);
                }
            }
            if (inField) readField(fieldBuffer, fieldID++);
        } catch (InterruptedException e) {
            int errCode = 6018;
            String errMsg = "Error while reading input";
            throw new ExecException(errMsg, errCode, 
                    PigException.REMOTE_ENVIRONMENT, e);
        }

        Tuple t =  mTupleFactory.newTupleNoCopy(mProtoTuple);
        return t;
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
    public InputFormat getInputFormat() throws IOException {
        if(loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
            return new Bzip2TextInputFormat();
        } else {
            return new PigTextInputFormat();
        }
    }

    @Override
    public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split)
    throws IOException {
        in = reader;
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
                p.setProperty(signature, ObjectSerializer.serialize(mRequiredColumns));
            } catch (Exception e) {
                throw new RuntimeException("Cannot serialize mRequiredColumns");
            }
        }
        return new RequiredFieldResponse(true);
    }

    @Override
    public void setUDFContextSignature(String signature) {
        this.signature = signature; 
    }

    @Override
    public List<OperatorSet> getFeatures() {
        return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
    }
}