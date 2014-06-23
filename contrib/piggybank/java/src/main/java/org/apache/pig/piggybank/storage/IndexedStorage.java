/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pig.piggybank.storage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.IndexableLoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.piggybank.storage.IndexedStorage.IndexedStorageInputFormat.IndexedStorageRecordReader;
import org.apache.pig.piggybank.storage.IndexedStorage.IndexedStorageInputFormat.IndexedStorageRecordReader.IndexedStorageRecordReaderComparator;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataReaderWriter;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.StorageUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;

/**
 * <code>IndexedStorage</code> is a form of <code>PigStorage</code> that supports a
 * per record seek.  <code>IndexedStorage</code> creates a separate (hidden) index file for
 * every data file that is written.  The format of the index file is:
 * <pre>
 * | Header     |
 * | Index Body |
 * | Footer     |
 * </pre>
 * The Header contains the list of record indices (field numbers) that represent index keys.
 * The Index Body contains a <code>Tuple</code> for each record in the data.  
 * The fields of the <code>Tuple</code> are:
 * <ul>
 * <li> The index key(s) <code>Tuple</code> </li>
 * <li> The number of records that share this index key. </li>
 * <li> Offset into the data file to read the first matching record. </li>
 * </ul>
 * The Footer contains sequentially:
 * <ul>
 * <li> The smallest key(s) <code>Tuple</code> in the index. </li>
 * <li> The largest key(s) <code>Tuple</code> in the index. </li>
 * <li> The offset in bytes to the start of the footer </li>
 * </ul>
 * 
 * <code>IndexStorage</code> implements <code>IndexableLoadFunc</code> and
 * can be used as the 'right table' in a PIG 'merge' or 'merge-sparse' join.
 *
 * <code>IndexStorage</code> does not require the data to be globally partitioned & sorted
 * by index keys.  Each partition (separate index) must be locally sorted.
 *
 * Also note IndexStorage is a loader to demonstrate "merge-sparse" join.
 */
public class IndexedStorage extends PigStorage implements IndexableLoadFunc {

	/**
	 * Constructs a Pig Storer that uses specified regex as a field delimiter.
	 * @param delimiter - field delimiter to use
	 * @param offsetsToIndexKeys - list of offset into Tuple for index keys (comma separated)
	 */
	public IndexedStorage(String delimiter, String offsetsToIndexKeys) {
		super(delimiter);

		this.fieldDelimiter = StorageUtil.parseFieldDel(delimiter);

		String[] stroffsetsToIndexKeys = offsetsToIndexKeys.split(",");
		this.offsetsToIndexKeys = new int[stroffsetsToIndexKeys.length];
		for (int i = 0; i < stroffsetsToIndexKeys.length; ++i) {
			this.offsetsToIndexKeys[i] = Integer.parseInt(stroffsetsToIndexKeys[i]);
		}
	}

	@Override
	public OutputFormat getOutputFormat() {
		return new IndexedStorageOutputFormat(fieldDelimiter, offsetsToIndexKeys);
	}

	/**
	 * Assumes this list of readers is already sorted except for the provided element. 
         * This element is bubbled up the array to its appropriate sort location 
         * (faster than doing a Utils sort).
	 */
	private void sortReader(int startIndex) {
		int idx = startIndex;
		while (idx < this.readers.length - 1) {
			IndexedStorageRecordReader reader1 = this.readers[idx];
			IndexedStorageRecordReader reader2 = this.readers[idx+1];
			if (this.readerComparator.compare(reader1, reader2) <= 0) {
				return;
			}
			this.readers[idx] = reader2;
			this.readers[idx+1] = reader1;
			idx++;
		}
	}

	/**
	 * Internal OutputFormat class
	 */
	public static class IndexedStorageOutputFormat extends PigTextOutputFormat {

		public IndexedStorageOutputFormat(byte delimiter, int[] offsetsToIndexKeys) {
			/* Call the base class constructor */
			super(delimiter);

			this.fieldDelimiter = delimiter;
			this.offsetsToIndexKeys = offsetsToIndexKeys;
		}

		@Override
		public RecordWriter<WritableComparable, Tuple> getRecordWriter(
				TaskAttemptContext context) throws IOException,
				InterruptedException {

			Configuration conf = context.getConfiguration();

			FileSystem fs = FileSystem.get(conf);
			Path file = this.getDefaultWorkFile(context, "");    
			FSDataOutputStream fileOut = fs.create(file, false);

			IndexManager indexManager = new IndexManager(offsetsToIndexKeys);
			indexManager.createIndexFile(fs, file);
			return new IndexedStorageRecordWriter(fileOut, this.fieldDelimiter, indexManager);
		}

		/**
		 * Internal class to do the actual record writing and index generation
		 * 
		 */
		public static class IndexedStorageRecordWriter extends PigLineRecordWriter {

			public IndexedStorageRecordWriter(FSDataOutputStream fileOut, byte fieldDel, IndexManager indexManager) throws IOException {
				super(fileOut, fieldDel);

				this.fileOut = fileOut;
				this.indexManager = indexManager;

				/* Write the index header first */
				this.indexManager.WriteIndexHeader();
			}

			@Override
			public void write(WritableComparable key, Tuple value) throws IOException {
				/* Write the data */
				long offset = this.fileOut.getPos();
				super.write(key, value);

				/* Build index */
				this.indexManager.BuildIndex(value, offset);	
			}

			@Override
			public void close(TaskAttemptContext context)
			throws IOException {
				this.indexManager.WriterIndexFooter();
				this.indexManager.Close();
				super.close(context);
			}

			/**
			 * Output stream for data
			 */
			private FSDataOutputStream fileOut;

			/**
			 * Index builder
			 */
			private IndexManager indexManager = null;
		}

		/**
		 * Delimiter to use between fields
		 */
		final private byte fieldDelimiter;

		/**
		 * Offsets to index keys in given tuple
		 */
		final protected int[] offsetsToIndexKeys;
	}


	@Override
	public InputFormat getInputFormat() {
		return new IndexedStorageInputFormat();
	}
	
	@Override
	public Tuple getNext() throws IOException {
		if (this.readers == null) {
			return super.getNext();
		}

		while (currentReaderIndexStart < this.readers.length) {
			IndexedStorageRecordReader r = this.readers[currentReaderIndexStart];

			this.prepareToRead(r, null);
			Tuple tuple = super.getNext();
			if (tuple == null) {
				currentReaderIndexStart++;
				r.close();
				continue; //next Reader
			} 
		
			//if we haven't yet initialized the indexManager (by reading the first index key)	
			if (r.indexManager.lastIndexKeyTuple == null) {

				//initialize the indexManager
				if (r.indexManager.ReadIndex() == null) {
					//There should never be a case where there is a non-null record - but no corresponding index.
					throw new IOException("Missing Index for Tuple: " + tuple);
				}
			}

			r.indexManager.numberOfTuples--;

			if (r.indexManager.numberOfTuples == 0) {
				if (r.indexManager.ReadIndex() == null) {
					r.close();
					currentReaderIndexStart++;
				} else {
					//Since the index of the current reader was increased, we may need to push the
					//current reader back in the sorted list of readers.
					sortReader(currentReaderIndexStart);
				}
			}
			return tuple;
		}

		return null;
	}

	/**
	 * IndexableLoadFunc interface implementation
	 */
	@Override
	public void initialize(Configuration conf) throws IOException {
		try {
			InputFormat inputFormat = this.getInputFormat();
			TaskAttemptID id = HadoopShims.getNewTaskAttemptID();
			
			if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
		                conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
			}
			List<FileSplit> fileSplits = inputFormat.getSplits(HadoopShims.createJobContext(conf, null));
			this.readers = new IndexedStorageRecordReader[fileSplits.size()];
			
			int idx = 0;
			Iterator<FileSplit> it = fileSplits.iterator();
			while (it.hasNext()) {
				FileSplit fileSplit = it.next();
				TaskAttemptContext context = HadoopShims.createTaskAttemptContext(conf, id);
				IndexedStorageRecordReader r = (IndexedStorageRecordReader) inputFormat.createRecordReader(fileSplit, context);
				r.initialize(fileSplit, context);
				this.readers[idx] = r;
				idx++;
			}

			Arrays.sort(this.readers, this.readerComparator);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}		
	}

	@Override
	/* The list of readers is always sorted before and after this call. */
	public void seekNear(Tuple keys) throws IOException {

		/* Keeps track of the last (if any) reader where seekNear was called */
		int lastIndexModified = -1;

		int idx = currentReaderIndexStart;
		while (idx < this.readers.length) {
			IndexedStorageRecordReader r = this.readers[idx];

			/* The key falls within the range of the reader index */
			if (keys.compareTo(r.indexManager.maxIndexKeyTuple) <= 0 && keys.compareTo(r.indexManager.minIndexKeyTuple) >= 0) {
				r.seekNear(keys);
				lastIndexModified = idx;

			/* The key is greater than the current range of the reader index */
			} else if (keys.compareTo(r.indexManager.maxIndexKeyTuple) > 0) {
				currentReaderIndexStart++;
			/* DO NOTHING - The key is less than the current range of the reader index */
			} else {
				break;
			}
			idx++;
		}

		/* 
		 * There is something to sort.  
		 * We can rely on the following invariants that make the following check accurate:
 		 *  - currentReaderIndexStart is always >= 0.
		 *  - lastIndexModified is only positive if seekNear was called.
		 *  - lastIndexModified >= currentReaderIndexStart if lastIndexModifed >= 0.  This is true because the list
 		 * is already sorted.
		 */
		if (lastIndexModified - currentReaderIndexStart >= 0) {

			/*
			 * The following logic is optimized for the (common) case where there are a tiny number of readers that
		         * need to be repositioned relative to the other readers in the much larger sorted list.
			 */

			/* First, just sort the readers that were updated relative to one another. */
			Arrays.sort(this.readers, currentReaderIndexStart, lastIndexModified+1, this.readerComparator);

			/* In descending order, push the updated readers back in the the sorted list. */
			for (idx = lastIndexModified; idx >= currentReaderIndexStart; idx--) {
				sortReader(idx);
			}
		}
	}


	@Override
	public void close() throws IOException {
		for (IndexedStorageRecordReader reader : this.readers) {
			reader.close();
		}
	}

	/**
	 * <code>IndexManager</code> manages the index file (both writing and reading)
	 * It keeps track of the last index read during reading.
	 */
	public static class IndexManager {

		/**
		 * Constructor (called during reading)
		 * @param ifile index file to read
		 */
		public IndexManager(FileStatus ifile) {
			this.indexFile = ifile;
			this.offsetToFooter = -1;
		}

		/**
		 * Constructor (called during writing)
		 * @param offsetsToIndexKeys
		 */
		public IndexManager(int[] offsetsToIndexKeys) {
			this.offsetsToIndexKeys = offsetsToIndexKeys;
			this.offsetToFooter = -1;
		}

		/**
		 * Construct index file path for a given a data file
		 * @param file - Data file
		 * @return - Index file path for given data file
		 */
		private static Path getIndexFileName(Path file) {
			return new Path(file.getParent(), "." + file.getName() + ".index");
		}

		/**
		 * Open the index file for writing for given data file
		 * @param fs
		 * @param file
		 * @throws IOException
		 */
		public void createIndexFile(FileSystem fs, Path file) throws IOException {
			this.indexOut = fs.create(IndexManager.getIndexFileName(file), false);	
		}

		/**
	 	 * Opens the index file.
		 */
		public void openIndexFile(FileSystem fs) throws IOException {
			this.indexIn = fs.open(this.indexFile.getPath());	
		}

		/**
		 * Close the index file
		 * @throws IOException
		 */
		public void Close() throws IOException {
			this.indexOut.close();
		}

		/**
		 * Build index tuple
		 * 
		 * @throws IOException
		 */
		private void BuildIndex(Tuple t, long offset) throws IOException {
			/* Build index key tuple */
			Tuple indexKeyTuple = tupleFactory.newTuple(this.offsetsToIndexKeys.length);
			for (int i = 0; i < this.offsetsToIndexKeys.length; ++i) {
				indexKeyTuple.set(i, t.get(this.offsetsToIndexKeys[i]));
			}

			/* Check if we have already seen Tuple(s) with same index keys */
			if (indexKeyTuple.compareTo(this.lastIndexKeyTuple) == 0) {
				/* We have seen Tuple(s) with given index keys, update the tuple count */
				this.numberOfTuples += 1;
			}
			else {
				if (this.lastIndexKeyTuple != null)
					this.WriteIndex();

				this.lastIndexKeyTuple = indexKeyTuple;
				this.minIndexKeyTuple = ((this.minIndexKeyTuple == null) || (indexKeyTuple.compareTo(this.minIndexKeyTuple) < 0)) ? indexKeyTuple : this.minIndexKeyTuple;
				this.maxIndexKeyTuple = ((this.maxIndexKeyTuple == null) || (indexKeyTuple.compareTo(this.maxIndexKeyTuple) > 0)) ? indexKeyTuple : this.maxIndexKeyTuple;

				/* New index tuple for newly seen index key */
				this.indexTuple = tupleFactory.newTuple(3);

				/* Add index keys to index Tuple */
				this.indexTuple.set(0, indexKeyTuple);

				/* Reset Tuple count for index key */
				this.numberOfTuples = 1;

				/* Remember offset to Tuple with new index keys */
				this.indexTuple.set(2, offset);
			}
		}


		/**
		 * Write index header
		 * @param indexOut - Stream to write to
		 * @param ih - Index header to write
		 * @throws IOException
		 */
		public void WriteIndexHeader() throws IOException {
			/* Number of index keys */
			indexOut.writeInt(this.offsetsToIndexKeys.length);

			/* Offset to index keys */
			for (int i = 0; i < this.offsetsToIndexKeys.length; ++i) {
				indexOut.writeInt(this.offsetsToIndexKeys[i]);
			}
		}

		/**
		 * Read index header
		 * @param indexIn - Stream to read from
		 * @return Index header
		 * @throws IOException
		 */
		public void ReadIndexHeader() throws IOException {
			/* Number of index keys */
			int nkeys = this.indexIn.readInt();

			/* Offset to index keys */
			this.offsetsToIndexKeys = new int[nkeys];
			for (int i = 0; i < nkeys; ++i) {
				offsetsToIndexKeys[i] = this.indexIn.readInt();
			}
		}

		/**
		 * Writes the index footer
		 */
		public void WriterIndexFooter() throws IOException {
			/* Flush indexes for remaining records */
			this.WriteIndex();

			/* record the offset to footer */
			this.offsetToFooter = this.indexOut.getPos();

			/* Write index footer */
			DataReaderWriter.writeDatum(indexOut, this.minIndexKeyTuple);
			DataReaderWriter.writeDatum(indexOut, this.maxIndexKeyTuple);

			/* Offset to footer */
			indexOut.writeLong(this.offsetToFooter);
		}

		/**
		 * Reads the index footer
		 */
		public void ReadIndexFooter() throws IOException {
			long currentOffset = this.indexIn.getPos();

			this.SeekToIndexFooter();
			this.minIndexKeyTuple = (Tuple)DataReaderWriter.readDatum(this.indexIn);
			this.maxIndexKeyTuple = (Tuple)DataReaderWriter.readDatum(this.indexIn);

			this.indexIn.seek(currentOffset);
		}

		/**
		 * Seeks to the index footer
		 */
		public void SeekToIndexFooter() throws IOException {
			if (this.offsetToFooter < 0) {
				/* offset to footer is at last long (8 bytes) in the file */
				this.indexIn.seek(this.indexFile.getLen()-8);
				this.offsetToFooter = this.indexIn.readLong();
			}
			this.indexIn.seek(this.offsetToFooter);
		}

		/**
		 * Writes the current index.
		 */
		public void WriteIndex() throws IOException {
			this.indexTuple.set(1, this.numberOfTuples);
			DataReaderWriter.writeDatum(this.indexOut, this.indexTuple);
		}

		/**
		 * Extracts the index key from the index tuple
		 */
		public Tuple getIndexKeyTuple(Tuple indexTuple) throws IOException {
			if (indexTuple.size() == 3) 
				return (Tuple)indexTuple.get(0);
			else
				throw new IOException("Invalid index record with size " + indexTuple.size());
		}

		/**
		 * Extracts the number of records that share the current key from the index tuple.
		 */
		public long getIndexKeyTupleCount(Tuple indexTuple) throws IOException {
			if (indexTuple.size() == 3) 
				return (Long)indexTuple.get(1);
			else
				throw new IOException("Invalid index record with size " + indexTuple.size());
		}

		/**
		 * Extracts the offset into the data file from the index tuple.
		 */
		public long getOffset(Tuple indexTuple) throws IOException {
			if (indexTuple.size() == 3) 
				return (Long)indexTuple.get(2);
			else
				throw new IOException("Invalid index record with size " + indexTuple.size());
		}
	
		/**
	 	 * Reads the next index from the index file (or null if EOF) and extracts
		 * the index fields.
		 */	
		public Tuple ReadIndex() throws IOException {
			if (this.indexIn.getPos() < this.offsetToFooter) {
				indexTuple = (Tuple)DataReaderWriter.readDatum(this.indexIn);
				if (indexTuple != null) {
					this.lastIndexKeyTuple = this.getIndexKeyTuple(indexTuple);
					this.numberOfTuples = this.getIndexKeyTupleCount(indexTuple);
				}
				return indexTuple;
			}
			return null;
		}

		/**
		 * Scans the index looking for a given key.
		 * @return the matching index tuple OR the last index tuple
		 * greater than the requested key if no match is found.
		 */
		public Tuple ScanIndex(Tuple keys) throws IOException {
			if (lastIndexKeyTuple != null && keys.compareTo(this.lastIndexKeyTuple) <= 0) {
				return indexTuple;
			}

			/* Scan the index looking for given key */
			while ((indexTuple = this.ReadIndex()) != null) {
				if (keys.compareTo(this.lastIndexKeyTuple) > 0)
					continue;
				else
					break;
			}
			
			return indexTuple;
		}

		/**
		 * stores the list of record indices that identify keys.
		 */
		private int[] offsetsToIndexKeys = null;

		/**
		 * offset in bytes to the start of the footer of the index.
		 */
		private long offsetToFooter = -1;

		/**
		 * output stream when writing the index.
		 */
		FSDataOutputStream indexOut;

		/**
		 * input stream when reading the index.
		 */
		FSDataInputStream indexIn;

		/**
		 * Tuple factory to create index tuples
		 */
		private TupleFactory tupleFactory = TupleFactory.getInstance();

		/**
		 * Index key tuple of the form
		 * ((Tuple of index keys), count of tuples with index keys, offset to first tuple with index keys)
		 */
		private Tuple indexTuple = tupleFactory.newTuple(3);

		/**
		 * "Smallest" index key tuple seen
		 */
		private Tuple minIndexKeyTuple = null;

		/**
		 * "Biggest" index key tuple seen
		 */
		private Tuple maxIndexKeyTuple = null;

		/**
		 * Last seen index key tuple 
		 */
		private Tuple lastIndexKeyTuple = null;

		/**
		 * Number of tuples seen for a index key
		 */
		private long numberOfTuples = 0;

		/**
	 	 * The index file.
		 */
		private FileStatus indexFile;
	}

	/** 
	 * Internal InputFormat class
	 */
	public static class IndexedStorageInputFormat extends PigTextInputFormat {

		@Override
		public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
			IndexManager im = null;
			try {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				Path indexFile = IndexManager.getIndexFileName(((FileSplit)split).getPath());
				im = new IndexManager(fs.getFileStatus(indexFile));
				im.openIndexFile(fs);
				im.ReadIndexHeader();
				im.ReadIndexFooter();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return new IndexedStorageRecordReader(im);
		}
		
		@Override
		public boolean isSplitable(JobContext context, Path filename) {
			return false;
		}

		/**
		 * Internal RecordReader class
	 	 */
		public static class IndexedStorageRecordReader extends RecordReader<LongWritable, Text> {
			private long start;
			private long pos;
			private long end;
			private IndexedStorageLineReader in;
			private int maxLineLength;
			private LongWritable key = null;
			private Text value = null;
			private IndexManager indexManager = null;

			@Override
			public String toString() {
				return indexManager.minIndexKeyTuple + "|" + indexManager.lastIndexKeyTuple + "|" + indexManager.maxIndexKeyTuple;
			}

			public IndexedStorageRecordReader(IndexManager im) {
				this.indexManager = im;
			}

			/**
			 * Class to compare record readers using underlying indexes
			 *
			 */
			public static class IndexedStorageRecordReaderComparator implements Comparator<IndexedStorageRecordReader> {
				@Override
				public int compare(IndexedStorageRecordReader o1, IndexedStorageRecordReader o2) {
					Tuple t1 = (o1.indexManager.lastIndexKeyTuple == null) ?  o1.indexManager.minIndexKeyTuple : o1.indexManager.lastIndexKeyTuple;
					Tuple t2 = (o2.indexManager.lastIndexKeyTuple == null) ?  o2.indexManager.minIndexKeyTuple : o2.indexManager.lastIndexKeyTuple;
					return t1.compareTo(t2);
				}
			}

			public static class IndexedStorageLineReader {
				private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
				private int bufferSize = DEFAULT_BUFFER_SIZE;
				private InputStream in;
				private byte[] buffer;
				// the number of bytes of real data in the buffer
				private int bufferLength = 0;
				// the current position in the buffer
				private int bufferPosn = 0;
				private long bufferOffset = 0;

				private static final byte CR = '\r';
				private static final byte LF = '\n';

				/**
				 * Create a line reader that reads from the given stream using the
				 * default buffer-size (64k).
				 * @param in The input stream
				 * @throws IOException
				 */
				public IndexedStorageLineReader(InputStream in) {
					this(in, DEFAULT_BUFFER_SIZE);
				}

				/**
				 * Create a line reader that reads from the given stream using the 
				 * given buffer-size.
				 * @param in The input stream
				 * @param bufferSize Size of the read buffer
				 * @throws IOException
				 */
				public IndexedStorageLineReader(InputStream in, int bufferSize) {
					if( !(in instanceof Seekable) || !(in instanceof PositionedReadable) ) {
					      throw new IllegalArgumentException(
					          "In is not an instance of Seekable or PositionedReadable");
					}
					
					this.in = in;
					this.bufferSize = bufferSize;
					this.buffer = new byte[this.bufferSize];
				}

				/**
				 * Create a line reader that reads from the given stream using the
				 * <code>io.file.buffer.size</code> specified in the given
				 * <code>Configuration</code>.
				 * @param in input stream
				 * @param conf configuration
				 * @throws IOException
				 */
				public IndexedStorageLineReader(InputStream in, Configuration conf) throws IOException {
					this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
				}

				/**
				 * Close the underlying stream.
				 * @throws IOException
				 */
				public void close() throws IOException {
					in.close();
				}

				/**
				 * Read one line from the InputStream into the given Text.  A line
				 * can be terminated by one of the following: '\n' (LF) , '\r' (CR),
				 * or '\r\n' (CR+LF).  EOF also terminates an otherwise unterminated
				 * line.
				 *
				 * @param str the object to store the given line (without newline)
				 * @param maxLineLength the maximum number of bytes to store into str;
				 *  the rest of the line is silently discarded.
				 * @param maxBytesToConsume the maximum number of bytes to consume
				 *  in this call.  This is only a hint, because if the line cross
				 *  this threshold, we allow it to happen.  It can overshoot
				 *  potentially by as much as one buffer length.
				 *
				 * @return the number of bytes read including the (longest) newline
				 * found.
				 *
				 * @throws IOException if the underlying stream throws
				 */
				public int readLine(Text str, int maxLineLength,
						int maxBytesToConsume) throws IOException {
					/* We're reading data from in, but the head of the stream may be
					 * already buffered in buffer, so we have several cases:
					 * 1. No newline characters are in the buffer, so we need to copy
					 *    everything and read another buffer from the stream.
					 * 2. An unambiguously terminated line is in buffer, so we just
					 *    copy to str.
					 * 3. Ambiguously terminated line is in buffer, i.e. buffer ends
					 *    in CR.  In this case we copy everything up to CR to str, but
					 *    we also need to see what follows CR: if it's LF, then we
					 *    need consume LF as well, so next call to readLine will read
					 *    from after that.
					 * We use a flag prevCharCR to signal if previous character was CR
					 * and, if it happens to be at the end of the buffer, delay
					 * consuming it until we have a chance to look at the char that
					 * follows.
					 */
					str.clear();
					int txtLength = 0; //tracks str.getLength(), as an optimization
					int newlineLength = 0; //length of terminating newline
					boolean prevCharCR = false; //true of prev char was CR
					long bytesConsumed = 0;
					do {
						int startPosn = bufferPosn; //starting from where we left off the last time
						if (bufferPosn >= bufferLength) {
							startPosn = bufferPosn = 0;
							if (prevCharCR)
								++bytesConsumed; //account for CR from previous read
							
							bufferOffset = ((Seekable)in).getPos();
							bufferLength = in.read(buffer);
							
							if (bufferLength <= 0)
								break; // EOF
						}
						for (; bufferPosn < bufferLength; ++bufferPosn) { //search for newline
							if (buffer[bufferPosn] == LF) {
								newlineLength = (prevCharCR) ? 2 : 1;
								++bufferPosn; // at next invocation proceed from following byte
								break;
							}
							if (prevCharCR) { //CR + notLF, we are at notLF
								newlineLength = 1;
								break;
							}
							prevCharCR = (buffer[bufferPosn] == CR);
						}
						int readLength = bufferPosn - startPosn;
						if (prevCharCR && newlineLength == 0)
							--readLength; //CR at the end of the buffer
						bytesConsumed += readLength;
						int appendLength = readLength - newlineLength;
						if (appendLength > maxLineLength - txtLength) {
							appendLength = maxLineLength - txtLength;
						}
						if (appendLength > 0) {
							str.append(buffer, startPosn, appendLength);
							txtLength += appendLength;
						}
					} while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);

					if (bytesConsumed > (long)Integer.MAX_VALUE)
						throw new IOException("Too many bytes before newline: " + bytesConsumed);    
					return (int)bytesConsumed;
				}

				/**
				 * Read from the InputStream into the given Text.
				 * @param str the object to store the given line
				 * @param maxLineLength the maximum number of bytes to store into str.
				 * @return the number of bytes read including the newline
				 * @throws IOException if the underlying stream throws
				 */
				public int readLine(Text str, int maxLineLength) throws IOException {
					return readLine(str, maxLineLength, Integer.MAX_VALUE);
				}

				/**
				 * Read from the InputStream into the given Text.
				 * @param str the object to store the given line
				 * @return the number of bytes read including the newline
				 * @throws IOException if the underlying stream throws
				 */
				public int readLine(Text str) throws IOException {
					return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
				}
				
				/**
				 * If given offset is within the buffer, adjust the buffer position to read from
				 * otherwise seek to the given offset from start of the file.
				 * @param offset
				 * @throws IOException
				 */
				public void seek(long offset) throws IOException {
					if ((offset >= bufferOffset) && (offset < (bufferOffset + bufferLength)))
						bufferPosn = (int) (offset - bufferOffset);
					else {
						bufferPosn = bufferLength;
						((Seekable)in).seek(offset);
					}
				}
			}

			@Override
			public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {

				FileSplit split = (FileSplit) genericSplit;
				Configuration job = context.getConfiguration();
				this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
				start = split.getStart();
				end = start + split.getLength();
				final Path file = split.getPath();

				FileSystem fs = file.getFileSystem(job);
				FSDataInputStream fileIn = fs.open(split.getPath());
				boolean skipFirstLine = false;
				if (start != 0) {
					skipFirstLine = true;
					--start;
					fileIn.seek(start);
				}
				in = new IndexedStorageLineReader(fileIn, job);
				if (skipFirstLine) {
					start += in.readLine(new Text(), 0, (int)Math.min((long)Integer.MAX_VALUE, end - start));
				}
				this.pos = start;
			}

			public void seek(long offset) throws IOException {
				in.seek(offset);
				pos = offset;
			}

			/**
			 * Scan the index for given key and seek to appropriate offset in the data 
			 * @param keys to look for
			 * @return true if the given key was found, false otherwise
			 * @throws IOException
			 */
			public boolean seekNear(Tuple keys) throws IOException {
				boolean ret = false;
				Tuple indexTuple = this.indexManager.ScanIndex(keys);
				if (indexTuple != null) {
					long offset = this.indexManager.getOffset(indexTuple) ;
					in.seek(offset);
				
					if (keys.compareTo(this.indexManager.getIndexKeyTuple(indexTuple)) == 0) {
						ret = true;
					}
				}
				
				return ret;
			}

			@Override
			public boolean nextKeyValue() throws IOException,
			InterruptedException {
				if (key == null) {
					key = new LongWritable();
				}
				key.set(pos);
				if (value == null) {
					value = new Text();
				}
				int newSize = 0;
				while (pos < end) {
					newSize = in.readLine(value, maxLineLength,
							Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
									maxLineLength));
					if (newSize == 0) {
						break;
					}
					pos += newSize;
					if (newSize < maxLineLength) {
						break;
					}
				}
				if (newSize == 0) {
					key = null;
					value = null;
					return false;
				} else {
					return true;
				}
			}

			@Override
			public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
				return key;
			}

			@Override
			public Text getCurrentValue() throws IOException,
			InterruptedException {
				return value;
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				if (start == end) {
					return 0.0f;
				} else {
					return Math.min(1.0f, (pos - start) / (float)(end - start));
				}
			}

			@Override
			public void close() throws IOException {
				if (in != null) {
					in.close(); 
				}
			}
		}
	}


	/**
	 * List of record readers.
	 */
	protected IndexedStorageRecordReader[] readers = null;	

	/**
	 * Index into the the list of readers to the current reader.  
 	 * Readers before this index have been fully scanned for keys.
	 */
	protected int currentReaderIndexStart = 0;

	/**
	 * Delimiter to use between fields
	 */
	protected byte fieldDelimiter = '\t';

	/**
	 * Offsets to index keys in tuple
	 */
	final protected int[] offsetsToIndexKeys;

	/**
	 * Comparator used to compare key tuples.
	 */
	protected Comparator<IndexedStorageRecordReader> readerComparator = new IndexedStorageInputFormat.IndexedStorageRecordReader.IndexedStorageRecordReaderComparator();
}
