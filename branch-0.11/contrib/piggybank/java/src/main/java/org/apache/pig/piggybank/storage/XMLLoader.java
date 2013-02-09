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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.tools.bzip2r.CBZip2InputStream;




/**
 * A <code>XMLLoaderBufferedPositionedInputStream</code> is the package class and is the 
 * decorator over the BufferedPositionedInputStream which in turn decorate
 * BufferedInputStream. It contains <code>BufferedPositionedInputStream<code>
 * input stream, which it uses as
 * its  basic source of data, possibly reading or providing  additional
 * functionality. The class <code>XMLLoaderBufferedPositionedInputStream</code>
 * itself simply overrides the necessary medthod for reading i.e 
 * <code>read</code> <code>getPosition<code> with versions that
 * pass all requests to the contained  input
 * stream or do some special processing. Subclasses of <code>XMLLoaderBufferedPositionedInputStream</code>
 * may further override some of  these methods
 * and may also provide additional methods
 * and fields.
 * It also provides additional method <code>collectTag<collect> which will give the byte 
 * array between the tag which is a xml record. i.e <tag> .*</tag> will be returned
 *
 * @note we can't use the standard SAX or STAX parser as for a big xml 
 *       the intermittent hadoop block may not be the valid xml and hence those
 *       parser may create pb. 
 *
 * @since   pig 2.0
 */

class XMLLoaderBufferedPositionedInputStream extends BufferedPositionedInputStream { 

    public final static int S_START = 0;
    public final static int S_MATCH_PREFIX = 1;
    public final static int S_MATCH_TAG = 2;

    /**
     * The input streamed to be filtered 
     */
    InputStream wrapperIn;

    /**
     * The field to know if the underlying buffer contains any more bytes
     */
    boolean _isReadable;

    /**
    * The field set the maximum bytes that is readable by this instance of stream.
    */
    private long maxBytesReadable = 0;
    	
    /**
    * The field denote the number of bytes read by this stream. 
    */
    long bytesRead = 0;
    	
    /**
    * Denotes the end of the current split location
    */
    long end = 0;
    
    /**
     * Creates a <code>XMLLoaderBufferedPositionedInputStream</code>
     * by assigning the  argument <code>in</code>
     * to the field <code>this.wrapperIn</code> so as
     * to remember it for later use.
     *
     * @param   in   the underlying input stream,
     */
    public XMLLoaderBufferedPositionedInputStream(InputStream in){
        super(in);
        this.wrapperIn = in;
        setReadable(true);
    }
    
    /**
     * Creates a  split aware <code>XMLLoaderBufferedPositionedInputStream</code>.
     * @param in    the underlying input stream
     * @param start    start location of the split
     * @param end    end location of the split
     */
    public XMLLoaderBufferedPositionedInputStream(InputStream in,long start,long end){
       this(in);
       this.end = end;
       maxBytesReadable = end - start;
    }

    /**
     * Set the stream readable or non readable. This is needed
     * to control the xml parsing.
     * @param flag The boolean flag to be set
     * @see XMLLoaderBufferedPositionedInputStream#isReadable
     */
    private void setReadable(boolean flag) {
       _isReadable = flag;
    }

    /**
     * See if the stream readable or non readable. This is needed
     * to control the xml parsing.
     * @return  true if readable otherwise false
     * @see XMLLoaderBufferedPositionedInputStream#setReadable
     */
    public boolean isReadable() {
       return _isReadable == true;
    }

    /**
     * org.apache.pig.impl.io.BufferedPositionedInputStream.read
     * It is just the wrapper for now.
     * Reads the next byte of data from this input stream. The value
     * byte is returned as an <code>int</code> in the range
     * <code>0</code> to <code>255</code>. If no byte is available
     * because the end of the stream has been reached, the value
     * <code>-1</code> is returned. This method blocks until input data
     * is available, the end of the stream is detected, or an exception
     * is thrown.
     * <p>
     * This method
     * simply performs <code>in.read()</code> and returns the result.
     *
     * @return     the next byte of data, or <code>-1</code> if the end of the
     *             stream is reached.
     * @exception  IOException  if an I/O error occurs.
     * @see        XMLLoaderBufferedPositionedInputStreamInputStream#wrapperIn
     */
    public int read() throws IOException {
       return wrapperIn.read();
    }

    /**
     * This is collect the bytes from current position to the ending tag.
     * This scans for the tags and do the pattern match byte by byte
     * this must be used along with 
     *  XMLLoaderBufferedPositionedInputStream#skipToTag
     *
     * @param tagName the end tag to search for
     *
     * @param limit the end pointer for the block for this mapper
     *
     * @return the byte array containing the documents until the end of tag
     *
     * @see loader.XMLLoaderBufferedPositionedInputStream.collectUntilEndTag
     *
     */
    private byte[] collectUntilEndTag(String tagName, long limit) {

      //@todo use the charset and get the charset encoding from the xml encoding.
      byte[] tmp = tagName.getBytes();
      byte[] tag = new byte[tmp.length + 3];
      tag[0] = (byte)'<';
      tag[1] = (byte)'/';
      for (int i = 0; i < tmp.length; ++i) {
        tag[2+i] = tmp[i];
      }
      tag[tmp.length+2] = (byte)'>';
      
      
      // Create a start tag bytes to handle nested tags
      byte[] startTag = new byte[tmp.length + 1];
      startTag[0] = (byte)'<';
      for (int i = 0; i < tmp.length; ++i) {
         startTag[1+i] = tmp[i];
       }
      //startTag[tmp.length+1] = (byte)'>';
      
      

      ByteArrayOutputStream collectBuf = new ByteArrayOutputStream(1024);
      int idxTagChar = 0;
      int idxStartTagChar = 0;
      boolean startTagMatched = false;
      /*
       * Read till an end tag is found.It need not check for any condition since it 
       * tries to read it till end.One issue that may happen is that if the xml 
       * content is very huge; or if the end tag is not there in a huge file, 
       * then it may blow up the memory. 
       */
      int nestedTags = 0;
      while (true) {
        int b = -1;
        try {
          b = this.read();
          ++bytesRead; // Add one to the bytes read
          if (b == -1) {
            collectBuf.reset();
            this.setReadable(false);
            break;
          }
          collectBuf.write((byte)(b));

          // Check if the start tag has matched except for the last char
          if(startTagMatched )
          {
             startTagMatched = false;
             idxStartTagChar = 0;
             if (b == ' ' || b == '\t' || b == '>')
                ++nestedTags;// increment the nesting count
          }
          
          if (b == startTag[idxStartTagChar]){
             ++idxStartTagChar;
             if(idxStartTagChar == startTag.length)
                startTagMatched = true ; // Set the flag as true if start tag matches
          }else
             idxStartTagChar = 0;
            
          
          
          // start to match the target close tag
          if (b == tag[idxTagChar]) {
            ++idxTagChar;
            if (idxTagChar == tag.length) {
               if(nestedTags==0) // Break the loop if there were no nested tags
                  break;
               else{
                  --nestedTags; // Else decrement the count
                  idxTagChar = 0; // Reset the index
               }
            }
          } else 
            idxTagChar = 0; 
          
        }
        catch (IOException e) {
          this.setReadable(false);
          return null;
        }
      }
      return collectBuf.toByteArray();
    }

    /**
     * This is collect the from the matching tag.
     * This scans for the tags and do the pattern match byte by byte
     * This returns a part doc. it must be used along with 
     * XMLLoaderBufferedPositionedInputStream#collectUntilEndTag
     * 
     * @param tagName the start tag to search for
     *
     * @param limit the end pointer for the block for this mapper
     *
     * @return the byte array containing match of the tag.
     *
     * @see loader.XMLLoaderBufferedPositionedInputStream.collectUntilEndTag
     *
     */
    private byte[] skipToTag(String tagName, long limit) throws IOException {
      
      //@todo use the charset and get the charset encoding from the xml encoding.
      byte[] tmp = tagName.getBytes();
      byte[] tag = new byte[tmp.length + 1];
      tag[0] = (byte)'<';
      for (int i = 0; i < tmp.length; ++i) {
        tag[1+i] = tmp[i];
      }

      ByteArrayOutputStream matchBuf = new ByteArrayOutputStream(512);
      int idxTagChar = 0;
      int state = S_START;
      
      /*
       * Read till the tag is found in this block. If a partial tag block is found
       * then continue on to the next block.matchBuf contains the data that is currently 
       * matched. If the read has reached the end of split and there are matched data 
       * then continue on to the next block.
       */
      while (splitBoundaryCriteria(wrapperIn) ||  (matchBuf.size() > 0 )) {
        int b = -1;
        try {
          b = this.read();
          ++bytesRead; // Increment the bytes read by 1
          if (b == -1) {
            state = S_START;
            matchBuf.reset();
            this.setReadable(false);
            break;
          }
          switch (state) {
            case S_START:
              // start to match the target open tag
              if (b == tag[idxTagChar]) {
                ++idxTagChar;
                matchBuf.write((byte)(b));
                if (idxTagChar == tag.length) {
                  state = S_MATCH_PREFIX;
                }
              } else {  // mismatch
                idxTagChar = 0;
                matchBuf.reset();
              }
              break;
            case S_MATCH_PREFIX:
              // tag match iff next character is whitespaces or close tag mark
              if (b == ' ' || b == '\t' || b == '>') {
                matchBuf.write((byte)(b));
                state = S_MATCH_TAG;
              } else {
                idxTagChar = 0;
                matchBuf.reset();
                state = S_START;
              }
              break;
            case S_MATCH_TAG:
              // keep copy characters until we hit the close tag mark
              matchBuf.write((byte)(b));
              break;
            default:
              throw new IllegalArgumentException("Invalid state: " + state);
          }
          if (state == S_MATCH_TAG && b == '>') {
            break;
          }
          if (state != S_MATCH_TAG && this.getPosition() > limit) {
            // need to break, no record in this block
            break;
          }
        }
        catch (IOException e) {
          this.setReadable(false);
          return null;
        }
      }
      return matchBuf.toByteArray();
    }
    /**
     * Returns whether the split boundary condition has reached or not.
     * For normal files ; the condition is to read till the split end reaches.
     * Gz files will have  maxBytesReadable set to near Long.MAXVALUE, hence
     * this will cause the entire file to be read. For bz2 and bz files, the 
     * condition lies on the position which until which it is read. 
     *  
     * @param wrapperIn2
     * @return true/false depending on whether split boundary has reached or no
     * @throws IOException
     */
    private boolean splitBoundaryCriteria(InputStream wrapperIn2) throws IOException {
       if(wrapperIn2 instanceof CBZip2InputStream)
          return ((CBZip2InputStream)wrapperIn2).getPos() <= end;
       else
          return bytesRead <= maxBytesReadable;
    }

    /**
     * This is collect bytes from start and end tag both inclusive
     * This scans for the tags and do the pattern match byte by byte
     * 
     * @param tagName the start tag to search for
     *
     * @param limit the end pointer for the block for this mapper
     *
     * @return the byte array containing match of the <code><tag>.*</tag><code>.
     *
     * @see loader.XMLLoaderBufferedPositionedInputStream.skipToTag
     *
     * @see loader.XMLLoaderBufferedPositionedInputStream.collectUntilEndTag
     *
     */
    byte[] collectTag(String tagName, long limit) throws IOException {
       ByteArrayOutputStream collectBuf = new ByteArrayOutputStream(1024);
       byte[] beginTag = skipToTag(tagName, limit);

       // No need to search for the end tag if the start tag is not found
       if(beginTag.length > 0 ){ 
          byte[] untilTag = collectUntilEndTag(tagName, limit);
          if (untilTag.length > 0) {
             for (byte b: beginTag) {
             collectBuf.write(b);
           }
           for (byte b: untilTag) {
              collectBuf.write(b);
           }
          }
       }
       return collectBuf.toByteArray();
    }

}


/**
 * The load function to load the XML file
 * This implements the LoadFunc interface which is used to parse records
 * from a dataset. The various helper adaptor function is extended from loader.Utf8StorageConverter
 * which included various functions to cast raw byte data into various datatypes. 
 * other sections of the code can call back to the loader to do the cast.
 * This takes a xmlTag as the arg which it will use to split the inputdataset into
 * multiple records. 
 * <code>
 *    
 * For example if the input xml (input.xml) is like this
 *     <configuration>
 *         <property>
 *            <name> foobar </name>
 *            <value> barfoo </value>
 *         </property>
 *         <ignoreProperty>
 *           <name> foo </name>
 *         </ignoreProperty>
 *         <property>
 *            <name> justname </name>
 *         </property>
 *     </configuration>
 *
 *    And your pig script is like this
 *
 *    --load the jar files
 *    register /homes/aloks/pig/udfLib/loader.jar;
 *    -- load the dataset using XMLLoader
 *    -- A is the bag containing the tuple which contains one atom i.e doc see output
 *    A = load '/user/aloks/pig/input.xml using loader.XMLLoader('property') as (doc:chararray);
 *    --dump the result
 *    dump A;
 *
 *
 *    Then you will get the output
 *
 *    (<property>
 *             <name> foobar </name>
 *             <value> barfoo </value>
 *          </property>)
 *    (<property>
 *             <name> justname </name>
 *          </property>)
 *
 *
 *    Where each () indicate one record 
 *
 * 
 * </code>
 */

public class XMLLoader extends LoadFunc {

    /**
     * logger from pig
     */
    protected final Log mLog = LogFactory.getLog(getClass());

    private XMLFileRecordReader reader = null;


    /**
     * the tuple content which is used while returning
     */
    private ArrayList<Object> mProtoTuple = null;

    /**
     * The record seperated. The default value is 'document'
     */
    public String recordIdentifier = "document";

    private String loadLocation;
    
    public XMLLoader() {

    }

    /**
     * Constructs a Pig loader that uses specified string as the record seperater
     * for example if the recordIdentifier is document. It will consider the record as 
     * <document> .* </document>
     * 
     * @param recordIdentifier the xml tag which is used to pull records
     *
     */
    public XMLLoader(String recordIdentifier) {
        this();
        this.recordIdentifier = recordIdentifier;
    }

    /**
     * Retrieves the next tuple to be processed.
     * @return the next tuple to be processed or null if there are no more tuples
     * to be processed.
     * @throws IOException
     */
    @Override
    public Tuple getNext() throws IOException {
 
        boolean next = false;
        
        try {
            next = reader.nextKeyValue();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        
        if (!next) return null;
        
        Tuple t = null;
     
        try {
            byte[] tagContent = (byte[]) reader.getCurrentValue();
            // No need to create the tuple if there are no contents
            t = (tagContent.length > 0) ? createTuple(tagContent) : null;
        } catch (Exception e) {
            throw new IOException(e);
        }

        return t; 
 
    }
    
    public Tuple createTuple(byte[] content) throws Exception {
        if (mProtoTuple == null) {
            mProtoTuple = new ArrayList<Object>();
        }
        if (content.length > 0) {
            mProtoTuple.add(new DataByteArray(content));
        }
        Tuple t = TupleFactory.getInstance().newTupleNoCopy(mProtoTuple);
        mProtoTuple = null;

        return t;
    }

    /**
     * to check for equality 
     * @param object 
     */
    public boolean equals(Object obj) {
        return equals((XMLLoader)obj);
    }

    /**
     * to check for equality 
     * @param XMLLoader object 
     */
    public boolean equals(XMLLoader other) {
        return this.recordIdentifier.equals(other.recordIdentifier);
    }

    @SuppressWarnings("unchecked")
    @Override
    public InputFormat getInputFormat() throws IOException {
       XMLFileInputFormat inputFormat = new XMLFileInputFormat(recordIdentifier);
       if(loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
          inputFormat.isSplitable = true;
         }
       return inputFormat;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
            throws IOException {
        this.reader = (XMLFileRecordReader) reader;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        loadLocation = location; 
        FileInputFormat.setInputPaths(job, location);
    }
    
    //------------------------------------------------------------------------
    // Implementation of InputFormat
    
    public static class XMLFileInputFormat extends FileInputFormat {

       /**
        * Boolean flag used to identify whether splittable property is explicitly set.
        */
        private boolean isSplitable = false;
        
        private String recordIdentifier;
        
        public XMLFileInputFormat(String recordIdentifier) {
            this.recordIdentifier = recordIdentifier;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public RecordReader createRecordReader(InputSplit split,
                TaskAttemptContext context) throws IOException,
                InterruptedException {
            
            return new XMLFileRecordReader(recordIdentifier);
        }  
        
        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
           CompressionCodec codec = 
              new CompressionCodecFactory(context.getConfiguration()).getCodec(filename);
           return (!(codec == null)) ? isSplitable : true;
        }
    }
    
    //------------------------------------------------------------------------
    // Implementation of RecordReader
    
    public static class XMLFileRecordReader extends RecordReader {

        private long start;
        private long end;
        private String recordIdentifier;
        
        /*
         * xmlloader input stream which has the ability to split the input
         * dataset into records by the specified tag
         */
        private XMLLoaderBufferedPositionedInputStream xmlLoaderBPIS = null;

        public XMLFileRecordReader(String recordIdentifier) {
            this.recordIdentifier = recordIdentifier;
        }
        
        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context)
                throws IOException, InterruptedException {
            FileSplit split = (FileSplit) genericSplit;
            Configuration job = context.getConfiguration();
 
            start = split.getStart();
            end = start + split.getLength();
            final Path file = split.getPath();
 
            // open the file and seek to the start of the split
            FileSystem fs = file.getFileSystem(job);
            FSDataInputStream fileIn = fs.open(split.getPath());
            
            // Seek to the start of the file
            fileIn.seek(start);
        
            if(file.toString().endsWith(".bz2") || file.toString().endsWith(".bz"))
            {
            	// For bzip2 files use CBZip2InputStream to read and supply the upper input stream.
               CBZip2InputStream in = new CBZip2InputStream(fileIn,9, end);
               this.xmlLoaderBPIS = new XMLLoaderBufferedPositionedInputStream(in,start,end);
            }
            else if (file.toString().endsWith(".gz"))
            {
            	CompressionCodecFactory compressionCodecs =  new CompressionCodecFactory(job);
            	final CompressionCodec codec = compressionCodecs.getCodec(file);
            	 if (codec != null) {
            	    end = Long.MAX_VALUE;
            	      CompressionInputStream stream = codec.createInputStream(fileIn);
            	      this.xmlLoaderBPIS = new XMLLoaderBufferedPositionedInputStream(stream,start,end);
            	    }
            }
            
            else
            {
               this.xmlLoaderBPIS = new XMLLoaderBufferedPositionedInputStream(fileIn,start,end);
            }
        }

        
        @Override
        public void close() throws IOException {
            xmlLoaderBPIS.close();
        }

        @Override
        public Object getCurrentKey() throws IOException, InterruptedException {
            return null;
        }

        @Override
        public Object getCurrentValue() throws IOException,
                InterruptedException {            
            return xmlLoaderBPIS.collectTag(recordIdentifier, end);
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
 
            return 0;
        }


        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return xmlLoaderBPIS.isReadable();
        }
        
    }
}
