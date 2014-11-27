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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Parses an XML input file given a specified identifier of tags to be loaded.
 * The output is a bag of XML elements where each element is returned as
 * a chararray containing the text of the matched XML element including the
 * start and tags as well as the data between them. In case of nesting elements
 * of the matching tags, only the top level one is returned.
 *
 */
public class XMLLoader extends LoadFunc {

  /**
   * Use this record reader to read XML tags out of a text file. It matches only
   * the tags identified by an identifier configured through a call to
   * {@link #setXMLIdentifier(String)}. It there are nesting tags of the given
   * identifier, only the top level one is returned which also includes all
   * enclosed tags.
   */
  public static class XMLRecordReader extends RecordReader<LongWritable, Text> {
    protected final RecordReader<LongWritable, Text> wrapped;

    /**Regular expression for XML tag identifier*/
    private static final String XMLTagNameRegExp = "[a-zA-Z\\_][0-9a-zA-Z\\-_]+";
    /**
     * A regular expression that matches key parts in the XML text needed to
     * correctly parse it and find matches of the given identifier
     */
    private Pattern identifiersPattern;

    private LongWritable key;
    private Text value;

    /**Position of the current buffer in the file*/
    private long bufferPos;

    /**Holds parts of the input file that were read but not parsed yet*/
    private String buffer;

    /**Original end of the split to parse*/
    private long originalEnd;

    private boolean terminated;

    public XMLRecordReader(RecordReader<LongWritable, Text> wrapped) {
      this.wrapped = wrapped;
    }

    /**
     * Delegate the initialization method to the wrapped stream after changing
     * the length of the split to be non-ending.
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
	throws IOException, InterruptedException {
      key = new LongWritable();
      value = new Text();
      if (split instanceof FileSplit) {
	FileSplit fsplit = (FileSplit) split;
	originalEnd = fsplit.getStart() + fsplit.getLength();
	Path path = fsplit.getPath();
	long fileEnd = path.getFileSystem(context.getConfiguration()).getFileStatus(path).getLen();
	FileSplit extendedSplit = new FileSplit(path, fsplit.getStart(),
	    Math.min(fsplit.getLength() * 10, fileEnd - fsplit.getStart()), fsplit.getLocations());
	this.wrapped.initialize(extendedSplit, context);
      } else {
	throw new RuntimeException("Cannot override a split of type'"+
	    split.getClass()+"'");
      }
    }

    public void setXMLIdentifier(String identifier) {
      if (!identifier.matches(XMLTagNameRegExp))
	throw new RuntimeException("XML tag identifier '"+identifier+"' does not match the regular expression /"+XMLTagNameRegExp+"/");
      String inlineClosedTagRegExp = "<\\s*"+identifier+"\\s*[^>]*/>";
      String openTagRegExp = "<\\s*"+identifier+"(?:\\s*|\\s+(?:[^/>]*|[^>]*[^>/]))>";
      String closeTagRegExp = "</\\s*"+identifier+"\\s*>";
      identifiersPattern = Pattern.compile("("+inlineClosedTagRegExp+")|("+openTagRegExp+")|("+closeTagRegExp+")");
    }

    /* Delegate all methods to the wrapped stream */
    public void close() throws IOException {
      wrapped.close();
    }

    public boolean equals(Object obj) {
      return wrapped.equals(obj);
    }

    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return key;
    }

    public Text getCurrentValue() throws IOException, InterruptedException {
      return value;
    }

    public float getProgress() throws IOException, InterruptedException {
      return Math.max(1.0f, this.wrapped.getProgress() * 10);
    }

    public int hashCode() {
      return wrapped.hashCode();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (this.terminated)
	return false;
      int depth = 0;
      // In case of an tag matched with an open tag and a closed tag, this buffer
      // is used to accumulate matched element if it is spans multiple lines.
      StringBuffer currentMatch = new StringBuffer();
      try {
      while (true) {
          // The start offset of first matched open tag. This marks the first byte
          // in the range to be copied to output.
          int offsetOfFirstMatchedOpenTag = 0;
    	  
	while (buffer == null || buffer.length() == 0) {
	  if (!wrapped.nextKeyValue())
	    return false; // End of split
	  // if passed the end offset of current split, terminate the matching
	  if (bufferPos >= originalEnd && depth == 0) {
	    this.terminated = true;
	    return false;
	  }

	  bufferPos = wrapped.getCurrentKey().get();
	  buffer = wrapped.getCurrentValue().toString();
	}
	Matcher matcher = identifiersPattern.matcher(buffer);
	while (matcher.find()) {
	  int startOfCurrentMatch = matcher.start();
	  int endOfCurrentMatch = matcher.end();
	  String group;
	  if ((group = matcher.group(1)) != null) {
	    // Matched an inline-closed tag
	    value = new Text(group);
	    this.key.set(bufferPos + matcher.start(1));
	    bufferPos += matcher.end(1);
	    buffer = buffer.substring(endOfCurrentMatch);
	    return true;
	  } else if ((group = matcher.group(2)) != null) {
	    // Matched an open tag
	    // If this is a top-level match (i.e., not enclosed in another matched
	    // tag), all bytes starting from this offset will be copied to output
	    // in one of two cases:
	    // 1- When a matching close tag is found
	    // 2- When an end of line is encountered
	    if (depth == 0) {
	      offsetOfFirstMatchedOpenTag = startOfCurrentMatch;
	      this.key.set(bufferPos + startOfCurrentMatch);
	    }
	    depth++;
	  } else if ((group = matcher.group(3)) != null) {
	    // Matched a closed tag
	    if (depth > 0) {
	      depth--;
	      if (depth == 0) {
		// A full top-level match
		// Copy all bytes to output
		if (currentMatch.length() == 0) {
		  // A full match in one line, return it immediately
		  value = new Text(buffer.substring(offsetOfFirstMatchedOpenTag, endOfCurrentMatch));
		} else {
		  currentMatch.append(buffer, offsetOfFirstMatchedOpenTag, endOfCurrentMatch);
		  value = new Text(currentMatch.toString());
		}
		// Copy remaining non matched part to the buffer for next call
		buffer = buffer.substring(endOfCurrentMatch);
		bufferPos += endOfCurrentMatch;
		return true;
	      }
	    }
	  } else {
	    throw new RuntimeException("Invalid match '"+matcher.group()+"' in string '"+buffer+"'");
	  }
	}
	// No more matches in current line. If we are inside a match (i.e.,
	// an open tag has been matched) copy all parts to the match.
	// Otherwise, just drop it.
	if (depth > 0) {
	  // Inside a match
	  currentMatch.append(buffer, offsetOfFirstMatchedOpenTag, buffer.length());
	}
	buffer = null;
      }
      } catch (InterruptedException e) {
	throw new IOException("Error getting input");
      }

    }

    public String toString() {
      return wrapped.toString();
    }
  }

  /**Location of the file loaded*/
  private String loadLocation;

  /**Underlying record reader*/
  @SuppressWarnings("rawtypes")
  protected RecordReader in = null;

  /**XML tag to parse*/
  private String identifier;

  public XMLLoader(String identifier) {
    this.identifier = identifier;
  }

  @Override
  public void prepareToRead(RecordReader reader, PigSplit split)
      throws IOException {
    in = reader;
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      if (!in.nextKeyValue())
	return null;
      Tuple tuple = createTuple(in.getCurrentValue().toString());
      return tuple;
    } catch (InterruptedException e) {
      e.printStackTrace();
      return null;
    }
  }


  /**
   * Creates a tuple from a matched string
   */
  public Tuple createTuple(String str) {
    return TupleFactory.getInstance().newTuple(new DataByteArray(str));
  }

  @SuppressWarnings("rawtypes")
  @Override
  public InputFormat getInputFormat() throws IOException {
    if(loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
      return new Bzip2TextInputFormat() {
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
	    InputSplit split, TaskAttemptContext context) {
	  try {
	    RecordReader<LongWritable, Text> originalReader =
		super.createRecordReader(split, context);
	    XMLRecordReader reader = new XMLRecordReader(originalReader);
	    reader.setXMLIdentifier(identifier);
	    return reader;
	  } catch (IOException e) {
	    throw new RuntimeException("Cannot create input split", e);
	  } catch (InterruptedException e) {
	    throw new RuntimeException("Cannot create input split", e);
	  }
	}
      };
    } else {
      return new PigTextInputFormat() {
	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
	    InputSplit split, TaskAttemptContext context) {
	  RecordReader<LongWritable, Text> originalReader =
	      super.createRecordReader(split, context);
	  XMLRecordReader reader = new XMLRecordReader(originalReader);
	  reader.setXMLIdentifier(identifier);
	  return reader;
	}
      };
    }
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    loadLocation = location;
    FileInputFormat.setInputPaths(job, location);
  }
}
