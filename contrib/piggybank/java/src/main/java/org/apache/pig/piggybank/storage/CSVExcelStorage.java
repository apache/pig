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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigException;
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

/**
 * CSV loading and storing with support for multi-line fields, 
 * and escaping of delimiters and double quotes within fields; 
 * uses CSV conventions of Excel 2007.
 * 
 * Arguments allow for control over:
 * <ul>
 * <li>   Which delimiter is used (default is ',')
 * <li>   Whether line breaks are allowed inside of fields
 * <li>   Whether line breaks are to be written Unix style, of Windows style
 * </ul>
 * <b>Usage:</b><br> 
 * 		{@code STORE x INTO '<destFileName>'}<br> 
 *      {@code USING CSVExcelStorage(['<delimiter>' [,{'YES_MULTILINE' | 'NO_MULTILINE'} [,{'UNIX' | 'WINDOWS' | 'UNCHANGED'}]]]);}
 * <p>
 *        Defaults are comma, 'NO_MULTILINE', 'UNCHANGED'
 *        The linebreak parameter is only used during store. During load
 *        no conversion is performed.                
 *  <p>               
 * <b>Example:</b><br>
 * 	    {@code STORE res INTO '/tmp/result.csv'}<br> 
 *		{@code USING CSVExcelStorage(',', 'NO_MULTILINE', 'WINDOWS');}
 *<p>
 *			would expect to see comma separated files for load, would
 *			use comma as field separator during store, would treat
 *			every newline as a record terminator, and would use CRLF
 *          as line break characters (0x0d 0x0a: \r\n).
 * <p>          
 * <b>Example:</b><br>
 *      {@code STORE res INTO '/tmp/result.csv'}<br> 
 *		{@code USING CSVExcelStorage(',', 'YES_MULTILINE');}
 * <p>
 *	        would allow newlines inside of fields. During load
 *	 	    such fields are expected to conform to the Excel
 *			requirement that the field is enclosed in double quotes.
 *			On store, the <code>chararray</code> containing the field will accordingly be
 *			enclosed in double quotes.
 * <p>
 * <b>Note:</b><br>
 *       A danger with enabling multiline fields during load is that unbalanced
 * 		 double quotes will cause slurping up of input until a balancing double
 * 		 quote is found, or until something breaks. If you are not expecting
 * 		 newlines within fields it is therefore more robust to use NO_MULTILINE,
 * 	     which is the default for that reason.
 * <p>
 * Excel expects double quotes within fields to be escaped with a second
 * double quote. When such an embedding of double quotes is used, Excel
 * additionally expects the entire fields to be surrounded by double quotes.
 * This package follows that escape mechanism, rather than the use of
 * backslash.
 * <p>
 * <b>Tested with:</b> Pig 0.8.0, Windows Vista, Excel 2007 SP2 MSO(12.0.6545.5004).
 *  <p>
 * <b>Note:</b><br>
 * 		 When a file with newlines embedded in a field is loaded into Excel,
 * 		 the application does not automatically vertically enlarge the respective
 * 		 rows. It is therefore easy to miss when fields consist of multiple lines.
 * 	     To make the multiline rows clear:<br>
 * 		 <ul>
 * 		   <li>Select the column. 
 * 		   <li>On the home ribbon, activate the Format pull-down menu
 *         <li>Select Autofit Row Height.
 *       </ul>
 * <p> 
 * <b>Examples:</b>
 * <br>
 * 		 With multiline turned on:<br>
 *           {@code "Conrad\n}<br>
 *    	     {@code   Emil",Dinger,40}<br>
 *    	      Is read as {@code (Conrad\nEmil,Dinger,40)}
 * <p>   	      
 *         With multiline turned off:<br>   	       
 *           {@code "Conrad\n}<br>
 *  	     {@code  Emil",Dinger,40}<br>
 *    	      is read as<br>
 *    					 {@code (Conrad)}<br>
 *    	      			 {@code (Emil,Dinger,40)}
 * <p>
 *      Always:
 *          <ul>
 *          <li> {@code "Mac ""the knife""",Cohen,30}
 *            is read as {@code (Mac "the knife",Cohen,30)}
 *            
 *     		<li> {@code Jane, "nee, Smith",20}
 *            Is read as {@code (Jane,nee, Smith,20)}
 *          </ul>
 * <p>            
 *     That is, the escape character is the double quote,
 *     not backslash.
 * <p>
 * <b>Known Issues:</b> 
 * <ul>
 * 		<li> When using {@code TAB} {@code{('\t')} as the field delimiter, Excel does not 
 * 	         properly handle newlines embedded in fields. Maybe there is a trick...
 *      <li> Excel will only deal properly with embedded newlines if the file
 *           name does not have a .csv extension. 
 * </ul>
 * 
 * @author "Andreas Paepcke" <paepcke@cs.stanford.edu". 
 * 					The load portion is based on Dmitriy V. Ryaboy's CSVLoader,
 * 					which in turn is loosely based on a version by James Kebinger.
 *
 */

public class CSVExcelStorage extends PigStorage implements StoreFuncInterface, LoadPushDown {

	public static enum Linebreaks {UNIX, WINDOWS, NOCHANGE};
	public static enum Multiline {YES, NO};
	
	protected final static byte LINEFEED = '\n';
	protected final static byte NEWLINE = '\r';
    protected final static byte DOUBLE_QUOTE = '"';
	protected final static byte RECORD_DEL = LINEFEED;
	
	private static byte FIELD_DEL = ',';
	private static String MULTILINE_DEFAULT_STR = "NOMULTILINE";
	private static String LINEBREAKS_DEFAULT_STR = "NOCHANGE";
	private static Multiline MULTILINE_DEFAULT = Multiline.NO;
	private static Linebreaks LINEBREAKS_DEFAULT = Linebreaks.NOCHANGE;
    
	long end = Long.MAX_VALUE;

	Linebreaks eolTreatment = LINEBREAKS_DEFAULT;
	Multiline multilineTreatment = MULTILINE_DEFAULT;
	
    private ArrayList<Object> mProtoTuple = null;
    private TupleFactory mTupleFactory = TupleFactory.getInstance();
    private String signature;
    private String loadLocation;
    private boolean[] mRequiredColumns = null;
    private boolean mRequiredColumnsInitialized = false;
    
	final Logger logger = Logger.getLogger(getClass().getName());
	
	// Counters for in an out records:
	/*   CODE FOR COUNTERS. Also, look for CSVRecords.READ, and CSVRecords.WRITTEN for the other pieces. 
	protected static enum CSVRecords {
		READ,
		WRITTEN
	};
	PigStatusReporter reporter = PigStatusReporter.getInstance();

	*/
	
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
	
	/*-----------------------------------------------------
	| Constructors 
	------------------------*/
		
		
    /**
     * Constructs a CSVExcel load/store that uses {@code comma} as the
     * field delimiter, terminates records on reading a newline
     * within a field (even if the field is enclosed in double quotes),
     * and uses {@code LF} as line terminator. 
     * 
     */
    public CSVExcelStorage() {
    	super(new String(new byte[] {FIELD_DEL}));
    }
    
    /**
     * Constructs a CSVExcel load/store that uses specified string as a field delimiter.
     * 
     * @param delimiter
     *            the single byte character that is used to separate fields.
     *            ("," is the default.)
     */
    public CSVExcelStorage(String delimiter) {
    	super(delimiter);
        initializeInstance(delimiter, MULTILINE_DEFAULT_STR, LINEBREAKS_DEFAULT_STR);
    }
		
    /**
     * Constructs a CSVExcel load/store that uses specified string 
     * as a field delimiter, and allows specification whether to handle
     * line breaks within fields. 
     * <ul>
     * 		<li> For NO_MULTILINE, every line break 
     * 			 will be considered an end-of-record. 
     * 		<li> For YES_MULTILINE, fields may include newlines, 
     * 			 as long as the fields are enclosed in 
     * 			 double quotes.
     * </ul>  
     * <b>Pig example:</b><br>
     * {@code STORE a INTO '/tmp/foo.csv'}<br>
     * {@code USING org.apache.pig.piggybank.storage.CSVExcelStorage(",", "YES_MULTILINE");} 
     * 
     * @param delimiter
     *            the single byte character that is used to separate fields.
     *            ("," is the default.)
     * @param multilineTreatment
     * 			  "YES_MULTILINE" or "NO_MULTILINE"
     *            ("NO_MULTILINE is the default.)
     */
    public CSVExcelStorage(String delimiter, String multilineTreatment) {
    	super(delimiter);
        initializeInstance(delimiter, multilineTreatment, LINEBREAKS_DEFAULT_STR);
    }

    /**
     * Constructs a CSVExcel load/store that uses specified string 
     * as a field delimiter, provides choice whether to manage multiline 
     * fields, and specifies chars used for end of line.
     * <p> 
     * The eofTreatment parameter is only relevant for STORE():
     * <ul>
     * <li>      For "UNIX", newlines will be stored as LF chars
     * <li>      For "WINDOWS", newlines will be stored as CRLF
     * </ul>
     * <b>Pig example:</b><br>
     * {@code STORE a INTO '/tmp/foo.csv'}<br>
     * {@code USING org.apache.pig.piggybank.storage.CSVExcelStorage(",", "NO_MULTILINE", "WINDOWS");} 
     * 
     * @param delimiter
     *            the single byte character that is used to separate fields.
     *            ("," is the default.)
     * @param String 
     * 			  "YES_MULTILINE" or "NO_MULTILINE"
     *            ("NO_MULTILINE is the default.)
     * @param eolTreatment
     * 			  "UNIX", "WINDOWS", or "NOCHANGE" 
     *            ("NOCHANGE" is the default.)
     */
    public CSVExcelStorage(String delimiter, String multilineTreatment,  String eolTreatment) {
    	super(delimiter);
    	initializeInstance(delimiter, multilineTreatment, eolTreatment);
    }

    
    private void initializeInstance(String delimiter, String multilineStr, String theEofTreatment) {
        FIELD_DEL = StorageUtil.parseFieldDel(delimiter);
        multilineTreatment = canonicalizeMultilineTreatmentRequest(multilineStr);
        eolTreatment = canonicalizeEOLTreatmentRequest(theEofTreatment);
    }
    
    private Multiline canonicalizeMultilineTreatmentRequest(String theMultilineStr) {
    	if (theMultilineStr.equalsIgnoreCase("YES_MULTILINE")) {
    		return Multiline.YES;
    	}
    	if (theMultilineStr.equalsIgnoreCase("NO_MULTILINE")) {
    		return Multiline.NO;
    	}
    	return MULTILINE_DEFAULT;
    }
    
    private Linebreaks canonicalizeEOLTreatmentRequest (String theEolTreatmentStr) {
    	if (theEolTreatmentStr.equalsIgnoreCase("Unix"))
    		return Linebreaks.UNIX;
    	if (theEolTreatmentStr.equalsIgnoreCase("Windows"))
    		return Linebreaks.WINDOWS;
    	return LINEBREAKS_DEFAULT;
    }
    
    // ---------------------------------------- STORAGE -----------------------------
    
	/*-----------------------------------------------------
	| putNext()
	------------------------*/
				
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
    	
    	ArrayList<Object> mProtoTuple = new ArrayList<Object>();
    	int embeddedNewlineIndex = -1;
    	String fieldStr = null;
    	// For good debug messages:
    	int fieldCounter = -1;
    	
    	// Do the escaping:
    	for (Object field : tupleToWrite.getAll()) {
    		fieldCounter++;
    		if (field == null) {
    			logger.warn("Field " + fieldCounter + " within tuple '" + tupleToWrite + "' is null.");
    			return;
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
    		
    		if ((fieldStr.indexOf(FIELD_DEL) != -1) || 
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
    	// Checking first for debug enabled is faster:
    	if (logger.isDebugEnabled())
    		logger.debug("Row: " + resTuple);
    	super.putNext(resTuple);
        // Increment number of records written (Couldn't get counters to work. So commented out.
    	//if (reporter != null)
    	//	reporter.getCounter(CSVRecords.WRITTEN).increment(1L);
    }
	
    // ---------------------------------------- LOADING  -----------------------------	
	
    /* (non-Javadoc)
     * @see org.apache.pig.builtin.PigStorage#getNext()
     */
    @Override
    public Tuple getNext() throws IOException {
        mProtoTuple = new ArrayList<Object>();

        getNextInQuotedField = false;
        boolean evenQuotesSeen = true;
        boolean sawEmbeddedRecordDelimiter = false;
        byte[] buf = null;
        
        if (!mRequiredColumnsInitialized) {
            if (signature != null) {
                Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
                mRequiredColumns = (boolean[])ObjectSerializer.deserialize(p.getProperty(signature));
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
        			sawEmbeddedRecordDelimiter = false;
        			getNextFieldID = 0;
        			recordLen = prevLineAndContinuation.length;
        			
        		} else {
        			// Previous record finished cleanly: start with the next record,
        			// unless EOF:
        			if (!in.nextKeyValue()) {
        				return null;
        			}                                                                                           
        			value = (Text) in.getCurrentValue();
        			buf = value.getBytes();
        			getNextFieldID = 0;
        			recordLen = value.getLength();
        		}
        		
        		sawEmbeddedRecordDelimiter = false;

        		nextTupleSkipChar = false;

        		ByteBuffer fieldBuffer = ByteBuffer.allocate(recordLen);

        		sawEmbeddedRecordDelimiter = processOneInRecord(evenQuotesSeen,
						sawEmbeddedRecordDelimiter, buf, recordLen, fieldBuffer);
        		
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
        // Increment number of records read (Couldn't get counters to work. So commented out.
        //if (reporter != null)
        //	reporter.getCounter(CSVRecords.READ).increment(1L);
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
			boolean sawEmbeddedRecordDelimiter, byte[] buf, int recordLen,
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
					if (evenQuotesSeen) {
						fieldBuffer.put(DOUBLE_QUOTE);
					}
				} else if (i == recordLen - 1) {
					// This is the last char we read from the input stream,
					// but we have an open double quote.
					// We either have a run-away quoted field (i.e. a missing
					// closing field in the record), or we have a field with 
					// a record delimiter in it. We assume the latter,
					// and cause the outer while loop to run again, reading
					// more from the stream. Write out the delimiter:
					fieldBuffer.put(b);
					sawEmbeddedRecordDelimiter = true;
					continue;
				} else
					if (!evenQuotesSeen &&
							(b == FIELD_DEL || b == RECORD_DEL)) {
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
			} else if (b == FIELD_DEL) {
				readField(fieldBuffer, getNextFieldID++); // end of the field
			} else {
				evenQuotesSeen = true;
				fieldBuffer.put(b);
			}
		} // end for
		return sawEmbeddedRecordDelimiter && (multilineTreatment == Multiline.YES);
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
