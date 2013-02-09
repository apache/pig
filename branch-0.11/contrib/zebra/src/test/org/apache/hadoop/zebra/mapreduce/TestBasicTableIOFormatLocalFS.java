/**
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

package org.apache.hadoop.zebra.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.StringTokenizer;
import java.util.TreeMap;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.zebra.tfile.Utils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.mapreduce.BasicTableOutputFormat;
import org.apache.hadoop.zebra.mapreduce.TableInputFormat;
import org.apache.hadoop.zebra.mapreduce.ArticleGenerator.Summary;
import org.apache.hadoop.zebra.mapreduce.TestBasicTableIOFormatLocalFS.FreqWordCache.Item;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.types.Projection;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

/**
 * Testing BasicTableOutputFormat and TableInputFormat using Local FS
 */
public class TestBasicTableIOFormatLocalFS extends TestCase {
	static class Options {
		int taskTrackers = 4;
		int dataNodes = 4;
		int srcFiles = 7;
		int numBatches = 1;
		int srcFileLen = 2 * 1024; // 20KB
		int numMapper = 3;
		int numReducer = 2;
		int numFreqWords = 10;
		long minTableSplitSize = 16 * 1024L;
		boolean localFS = true;
		String compression = "none";
		String rootPath = "TestBasicTableIOFormat";
		String srcPath = "docs";
		String fwdIndexRootPath = "fwdIndex";
		String invIndexTablePath = "invIndex";
		String freqWordTablePath = "freqWord";
	}

	static Log LOG = LogFactory.getLog(TestBasicTableIOFormatLocalFS.class
			.getName());

	Options options;
	Configuration conf;
	MiniDFSCluster dfs;
	FileSystem fileSys;
	Path rootPath;
	Path srcPath;
	Path fwdIndexRootPath;
	Path invIndexTablePath;
	Path freqWordTablePath;
	ArticleGenerator articalGen;
	Map<String, Summary> summary;

	@Override
	protected void setUp() throws IOException {
		if (System.getProperty("hadoop.log.dir") == null) {
			String base = new File(".").getPath(); // getAbsolutePath();
			System
			.setProperty("hadoop.log.dir", new Path(base).toString() + "./logs");
		}

		if (options == null) {
			options = new Options();
		}

		if (conf == null) {
			conf = new Configuration();
		}

		articalGen = new ArticleGenerator(1000, 1, 20, 100);
		summary = new HashMap<String, Summary>();

		if (options.localFS) {
			Path localFSRootPath = new Path(System.getProperty("test.build.data",
			"build/test/data/work-dir"));
			fileSys = localFSRootPath.getFileSystem(conf);
			rootPath = new Path(localFSRootPath, options.rootPath);
		} else {
			dfs = new MiniDFSCluster(conf, options.dataNodes, true, null);
			fileSys = dfs.getFileSystem();
			rootPath = new Path(fileSys.getWorkingDirectory(), options.rootPath);
		}
		conf = getJobConf();
		srcPath = new Path(rootPath, options.srcPath);
		fwdIndexRootPath = new Path(rootPath, options.fwdIndexRootPath);
		invIndexTablePath = new Path(rootPath, options.invIndexTablePath);
		freqWordTablePath = new Path(rootPath, options.freqWordTablePath);
	}

	@Override
	protected void tearDown() throws IOException {
		if (dfs != null) {
			dfs.shutdown();
		}
	}

	Configuration getJobConf() {
		Configuration conf = new Configuration();
		conf.setInt("table.input.split.minSize", 1);
		options.minTableSplitSize = 1; // force more splits
		conf.setInt("dfs.block.size", 1024); // force multiple blocks
		conf.set("table.output.tfile.compression", options.compression);
		conf.setInt("mapred.app.freqWords.count", options.numFreqWords);
		return conf;
	}

	/**
	 * Create a bunch of text files under a sub-directory of the srcPath. The name
	 * of the sub-directory is named after the batch name.
	 * 
	 * @param batchName
	 *          The batch name.
	 * @throws IOException
	 */
	void createSourceFiles(String batchName) throws IOException {
		LOG.info("Creating source data folder: " + batchName);
		Path batchDir = new Path(srcPath, batchName);
		LOG.info("Cleaning directory: " + batchName);
		fileSys.delete(batchDir, true);
		LOG.info("Generating input files: " + batchName);
		articalGen.batchArticalCreation(fileSys, new Path(srcPath, batchName),
				"doc-", options.srcFiles, options.srcFileLen);
		Summary s = articalGen.getSummary();
		// dumpSummary(s);
		long tmp = 0;
		for (Iterator<Long> it = s.wordCntDist.values().iterator(); it.hasNext(); tmp += it
		.next())
			;
		Assert.assertEquals(tmp, s.wordCount);
		summary.put(batchName, s);
		articalGen.resetSummary();
	}

	/**
	 * Generate forward index. Map-only task.
	 * 
	 * <pre>
	 * Map Input:
	 *    K = LongWritable (byte offset)
	 *    V = Text (text line)
	 * Map Output:
	 *    K = word: String.
	 *    V = Tuple of {fileName:String, wordPos:Integer, lineNo:Integer }
	 * </pre>
	 */
	static class ForwardIndexGen {
		static class MapClass extends
		Mapper<LongWritable, Text, BytesWritable, Tuple> {
			private BytesWritable outKey;
			private Tuple outRow;
			// index into the output tuple for fileName, wordPos, lineNo.
			private int idxFileName, idxWordPos, idxLineNo;
			private String filePath;
			// maintain line number and word position.
			private int lineNo = 0;
			private int wordPos = 0;

			@Override
			public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
				if (filePath == null) {
					FileSplit split = (FileSplit) context.getInputSplit();
					filePath = split.getPath().toString();
				}
				String line = value.toString();
				StringTokenizer st = new StringTokenizer(line, " ");
				while (st.hasMoreElements()) {
					byte[] word = st.nextToken().getBytes();
					outKey.set(word, 0, word.length);
					TypesUtils.resetTuple(outRow);
					try {
						outRow.set(idxFileName, filePath);
						outRow.set(idxWordPos, new Integer(wordPos));
						outRow.set(idxLineNo, new Integer(lineNo));
						context.write(outKey, outRow);
					} catch (ExecException e) {
						e.printStackTrace();
					}

					++wordPos;
				}
				++lineNo;

			}

			@Override
			public void setup(Context context) {
				LOG.info("ForwardIndexGen.MapClass.configure");
				outKey = new BytesWritable();
				try {
					Schema outSchema = BasicTableOutputFormat.getSchema(context);
					outRow = TypesUtils.createTuple(outSchema);
					idxFileName = outSchema.getColumnIndex("fileName");
					idxWordPos = outSchema.getColumnIndex("wordPos");
					idxLineNo = outSchema.getColumnIndex("lineNo");
				} catch (IOException e) {
					throw new RuntimeException("Schema parsing failed : "
							+ e.getMessage());
				} catch (ParseException e) {
					throw new RuntimeException("Schema parsing failed : "
							+ e.getMessage());
				}
			}

		}
	}

	/**
	 * Run forward index generation.
	 * 
	 * @param batchName
	 * @throws IOException
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	void runForwardIndexGen(String batchName) throws IOException, ParseException, 
	InterruptedException, ClassNotFoundException {
		LOG.info("Run Map-only job to convert source data to forward index: "
				+ batchName);

		//JobConf jobConf = getJobConf("fwdIndexGen-" + batchName);
		Job job = new Job();
		job.setJobName( "fwdIndexGen-" + batchName );
		// input-related settings
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(ForwardIndexGen.MapClass.class);

		//Path wd = fileSys.getWorkingDirectory();
		FileInputFormat.setInputPaths(job, new Path(srcPath, batchName));

		// output related settings
		Path outPath = new Path(fwdIndexRootPath, batchName);
		fileSys.delete(outPath, true);
		job.setOutputFormatClass(BasicTableOutputFormat.class);
		BasicTableOutputFormat.setOutputPath(job, outPath);
		BasicTableOutputFormat.setSchema(job, "fileName:string, wordPos:int, lineNo:int");

		// set map-only job.
		job.setNumReduceTasks(0);
		job.submit();
		job.waitForCompletion(true);
		BasicTableOutputFormat.close( job );
	}

	/**
	 * Count the # of rows of a BasicTable
	 * 
	 * @param tablePath
	 *          The path to the BasicTable
	 * @return Number of rows.
	 * @throws IOException
	 */
	long countRows(Path tablePath) throws IOException, ParseException {
		BasicTable.Reader reader = new BasicTable.Reader(tablePath, conf);
		//reader.setProjection("");
		long totalRows = 0;
		TableScanner scanner = reader.getScanner(null, true);
		for (; !scanner.atEnd(); scanner.advance()) {
			++totalRows;
		}
		scanner.close();
		return totalRows;
	}

	/**
	 * Given a batch ID, return the batch name.
	 * 
	 * @param i
	 *          batch ID
	 * @return Batch name.
	 */
	static String batchName(int i) {
		return String.format("batch-%03d", i);
	}

	/**
	 * Inverted index for one word.
	 */
	static class InvIndex implements Writable {
		int count = 0;
		// a map from filePath to all occurrences of the word positions.
		Map<String, ArrayList<Integer>> index;

		InvIndex() {
			index = new TreeMap<String, ArrayList<Integer>>();
		}

		InvIndex(String fileName, int pos) {
			this();
			add(fileName, pos);
		}

		void add(String fileName, int pos) {
			++count;
			ArrayList<Integer> list = index.get(fileName);
			if (list == null) {
				list = new ArrayList<Integer>(1);
				index.put(fileName, list);
			}
			list.add(pos);
		}

		void add(String fileName, ArrayList<Integer> positions) {
			ArrayList<Integer> list = index.get(fileName);
			if (list == null) {
				list = new ArrayList<Integer>();
				index.put(fileName, list);
			}
			count += positions.size();
			list.ensureCapacity(list.size() + positions.size());
			list.addAll(positions);
		}

		void reduce(InvIndex other) {
			for (Iterator<Map.Entry<String, ArrayList<Integer>>> it = other.index
					.entrySet().iterator(); it.hasNext();) {
				Map.Entry<String, ArrayList<Integer>> e = it.next();
				add(e.getKey(), e.getValue());
			}
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			count = 0;
			index.clear();
			count = Utils.readVInt(in);
			if (count > 0) {
				int n = Utils.readVInt(in);
				for (int i = 0; i < n; ++i) {
					String fileName = Utils.readString(in);
					int m = Utils.readVInt(in);
					ArrayList<Integer> list = new ArrayList<Integer>(m);
					index.put(fileName, list);
					for (int j = 0; j < m; ++j) {
						list.add(Utils.readVInt(in));
					}
				}
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			Utils.writeVInt(out, count);
			if (count > 0) {
				Utils.writeVInt(out, index.size());
				for (Iterator<Map.Entry<String, ArrayList<Integer>>> it = index
						.entrySet().iterator(); it.hasNext();) {
					Map.Entry<String, ArrayList<Integer>> e = it.next();
					Utils.writeString(out, e.getKey());
					ArrayList<Integer> list = e.getValue();
					Utils.writeVInt(out, list.size());
					for (Iterator<Integer> it2 = list.iterator(); it2.hasNext();) {
						Utils.writeVInt(out, it2.next());
					}
				}
			}
		}
	}

	/**
	 * Generate inverted Index.
	 * 
	 * <pre>
	 * Mapper Input = 
	 *    K: BytesWritable word; 
	 *    V: Tuple { fileName:String, wordPos:Integer };
	 *    
	 * Mapper Output =
	 *    K: BytesWritable word;
	 *    V: InvIndex;
	 *   
	 * Reducer Output =
	 *    K: BytesWritable word; 
	 *    V: Tuple {count:Integer, index: Map of {fileName:String, Bag of {wordPos:Integer}}};
	 * </pre>
	 */
	static class InvertedIndexGen {
		static class MapClass extends
		Mapper<BytesWritable, Tuple, BytesWritable, InvIndex> {
			// index of fileName and wordPos fileds of the input tuple
			int idxFileName, idxWordPos;

			@Override
			public void map(BytesWritable key, Tuple value, Context context)
			throws IOException, InterruptedException {
				try {
					String fileName = (String) value.get(idxFileName);
					int wordPos = (Integer) value.get(idxWordPos);
					context.write(key, new InvIndex(fileName, wordPos));
				} catch (ExecException e) {
					e.printStackTrace();
				}
			}

			@Override
			public void setup(Context context) {
				LOG.info("InvertedIndexGen.MapClass.configure");
				String projection;
				try {
					projection = TableInputFormat.getProjection(context);
				} catch (ParseException e) {
					throw new RuntimeException("Schema parsing failed : "
							+ e.getMessage());
				} catch (IOException e) {
					throw new RuntimeException("TableInputFormat.getProjection", e);
				}
				idxFileName = Projection.getColumnIndex(projection, "fileName");
				idxWordPos = Projection.getColumnIndex(projection, "wordPos");
			}

		}

		static class CombinerClass extends
		Reducer<BytesWritable, InvIndex, BytesWritable, InvIndex> {

			@Override
			public void reduce(BytesWritable key, Iterable<InvIndex> values, Context context)
			throws IOException, InterruptedException {
				InvIndex sum = new InvIndex();
				Iterator<InvIndex> it = values.iterator();
				for (; it.hasNext();) {
					sum.reduce(it.next());
				}
				context.write(key, sum);
			}

		}

		static class ReduceClass extends
		Reducer<BytesWritable, InvIndex, BytesWritable, Tuple> {
			Tuple outRow;
			int idxCount, idxIndex;
			Schema wordPosSchema;
			int idxWordPos;

			@Override
			public void setup(Context context) {
				LOG.info("InvertedIndexGen.ReduceClass.configure");
				try {
					Schema outSchema = BasicTableOutputFormat.getSchema(context);
					outRow = TypesUtils.createTuple(outSchema);
					idxCount = outSchema.getColumnIndex("count");
					idxIndex = outSchema.getColumnIndex("index");
					wordPosSchema = new Schema("wordPos");
					idxWordPos = wordPosSchema.getColumnIndex("wordPos");
				} catch (IOException e) {
					throw new RuntimeException("Schema parsing failed :" + e.getMessage());
				} catch (ParseException e) {
					throw new RuntimeException("Schema parsing failed :" + e.getMessage());
				}
			}

			Map<String, DataBag> convertInvIndex(Map<String, ArrayList<Integer>> index)
			throws IOException {
				Map<String, DataBag> ret = new TreeMap<String, DataBag>();
				for (Iterator<Map.Entry<String, ArrayList<Integer>>> it = index
						.entrySet().iterator(); it.hasNext();) {
					Map.Entry<String, ArrayList<Integer>> e = it.next();
					DataBag bag = TypesUtils.createBag();
					for (Iterator<Integer> it2 = e.getValue().iterator(); it2.hasNext();) {
						Tuple tuple = TypesUtils.createTuple(wordPosSchema);
						tuple.set(idxWordPos, it2.next());
						bag.add(tuple);
					}
					ret.put(e.getKey(), bag);
				}

				return ret;
			}

			@Override
			public void reduce(BytesWritable key, Iterable<InvIndex> values, Context context)
			throws IOException, InterruptedException {
				InvIndex sum = new InvIndex();
				Iterator<InvIndex> it = values.iterator();
				for (; it.hasNext();) {
					sum.reduce(it.next());
				}
				try {
					outRow.set(idxCount, sum.count);
					outRow.set(idxIndex, convertInvIndex(sum.index));
					context.write(key, outRow);
				} catch (ExecException e) {
					e.printStackTrace();
				}
			}
		}
	}

	void runInvertedIndexGen() throws IOException, ParseException,
	InterruptedException, ClassNotFoundException {
		LOG.info("Converting forward index to inverted index");
		//JobConf jobConf = getJobConf("runInvertedIndexGen");

		Job job = new Job();
		job.setJobName( "runInvertedIndexGen" );
		// input-related settings
		job.setInputFormatClass(TableInputFormat.class);
		job.setMapperClass(InvertedIndexGen.MapClass.class);
		Path[] paths = new Path[options.numBatches];
		for (int i = 0; i < options.numBatches; ++i) {
			paths[i] = new Path(fwdIndexRootPath, batchName(i));
		}
		TableInputFormat.setInputPaths(job, paths);
		// TableInputFormat.setProjection(jobConf, "fileName, wordPos");
		TableInputFormat.setMinSplitSize(job, options.minTableSplitSize);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(InvIndex.class);

		// output related settings
		fileSys.delete(invIndexTablePath, true);
		job.setOutputFormatClass(BasicTableOutputFormat.class);
		job.setReducerClass(InvertedIndexGen.ReduceClass.class);
		job.setCombinerClass(InvertedIndexGen.CombinerClass.class);
		BasicTableOutputFormat.setOutputPath(job, invIndexTablePath);
		BasicTableOutputFormat.setSchema(job, "count:int, index:map()");
		job.setNumReduceTasks(options.numReducer);

		job.submit();
		job.waitForCompletion(true);
		BasicTableOutputFormat.close( job );
	}

	void reduce(Summary sum, Summary delta) {
		sum.lineCount += delta.lineCount;
		sum.wordCount += delta.wordCount;
		reduce(sum.wordCntDist, delta.wordCntDist);
	}

	void reduce(Map<String, Long> sum, Map<String, Long> delta) {
		for (Iterator<Map.Entry<String, Long>> it = delta.entrySet().iterator(); it
		.hasNext();) {
			Map.Entry<String, Long> e = it.next();
			String key = e.getKey();
			Long base = sum.get(key);
			sum.put(key, (base == null) ? e.getValue() : base + e.getValue());
		}
	}

	void dumpSummary(Summary s) {
		LOG.info("Dumping Summary");
		LOG.info("Word Count: " + s.wordCount);
		for (Iterator<Map.Entry<String, Long>> it = s.wordCntDist.entrySet()
				.iterator(); it.hasNext();) {
			Map.Entry<String, Long> e = it.next();
			LOG.info(e.getKey() + "->" + e.getValue());
		}
	}

	/**
	 * Verify the word counts from the invIndexTable is the same as collected from
	 * the ArticleGenerator.
	 * 
	 * @throws IOException
	 */
	void verifyWordCount() throws IOException, ParseException {
		Summary expected = new Summary();
		for (Iterator<Summary> it = summary.values().iterator(); it.hasNext();) {
			Summary e = it.next();
			// dumpSummary(e);
			reduce(expected, e);
		}
		// LOG.info("Dumping aggregated Summary");
		// dumpSummary(expected);

		Summary actual = new Summary();
		BasicTable.Reader reader = new BasicTable.Reader(invIndexTablePath, conf);
		reader.setProjection("count");
		TableScanner scanner = reader.getScanner(null, true);
		Tuple tuple = TypesUtils.createTuple(Projection.toSchema(scanner
				.getProjection()));
		BytesWritable key = new BytesWritable();
		for (; !scanner.atEnd(); scanner.advance()) {
			scanner.getKey(key);
			scanner.getValue(tuple);
			int count = 0;
			try {
				count = (Integer) tuple.get(0);
			} catch (ExecException e) {
				e.printStackTrace();
			}
			actual.wordCount += count;
			String word = new String(key.getBytes(), 0, key.getLength());
			actual.wordCntDist.put(word, (long) count);
		}
		scanner.close();
		// LOG.info("Dumping MR calculated Summary");
		// dumpSummary(actual);
		Assert.assertEquals(expected.wordCount, actual.wordCount);
		Assert.assertEquals(expected.wordCntDist.size(), actual.wordCntDist.size());
		for (Iterator<Map.Entry<String, Long>> it = expected.wordCntDist.entrySet()
				.iterator(); it.hasNext();) {
			Map.Entry<String, Long> e = it.next();
			String word = e.getKey();
			Long myCount = actual.wordCntDist.get(word);
			Assert.assertFalse(word, myCount == null);
			Assert.assertEquals(word, e.getValue(), myCount);
		}
	}

	/**
	 * Caching the top K frequent words.
	 */
	static class FreqWordCache {
		static class Item {
			BytesWritable word;
			int count;

			Item(BytesWritable w, int c) {
				word = new BytesWritable();
				word.set(w.getBytes(), 0, w.getLength());
				count = c;
			}
		}

		int k;
		PriorityQueue<Item> words;

		FreqWordCache(int k) {
			if (k <= 0) {
				throw new IllegalArgumentException("Expecting positive int");
			}
			this.k = k;
			words = new PriorityQueue<Item>(k, new Comparator<Item>() {
				@Override
				public int compare(Item o1, Item o2) {
					if (o1.count != o2.count) {
						return o1.count - o2.count;
					}
					return -o1.word.compareTo(o2.word);
				}
			});
		}

		void add(BytesWritable word, int cnt) {
			while ((words.size() >= k) && words.peek().count < cnt) {
				words.poll();
			}
			if ((words.size() < k) || words.peek().count == cnt) {
				words.add(new Item(word, cnt));
			}
		}

		void add(Iterator<BytesWritable> itWords, int cnt) {
			while ((words.size() >= k) && words.peek().count < cnt) {
				words.poll();
			}
			if ((words.size() < k) || words.peek().count == cnt) {
				for (; itWords.hasNext();) {
					words.add(new Item(itWords.next(), cnt));
				}
			}
		}

		Item[] toArray() {
			Item[] ret = new Item[words.size()];
			for (int i = 0; i < ret.length; ++i) {
				ret[i] = words.poll();
			}

			for (int i = 0; i < ret.length / 2; ++i) {
				Item tmp = ret[i];
				ret[i] = ret[ret.length - i - 1];
				ret[ret.length - i - 1] = tmp;
			}

			return ret;
		}
	}

	/**
	 * Get the most frequent words from inverted index. The mapper uses a priority
	 * queue to keep the top frequent words in memory and output the results in
	 * close().
	 * 
	 * <pre>
	 * Mapper Input = 
	 *    K: BytesWritable word; 
	 *    V: Tuple { count:Integer }.
	 *    
	 * Mapper Output =
	 *    K: IntWritabl: count
	 *    V: BytesWritable: word
	 *   
	 * Reducer Output =
	 *    K: BytesWritable word;
	 *    V: Tuple { count:Integer }.
	 * </pre>
	 */
	static class FreqWords {
		static int getFreqWordsCount(Configuration conf) {
			return conf.getInt("mapred.app.freqWords.count", 100);
		}

		static class MapClass extends
		Mapper<BytesWritable, Tuple, IntWritable, BytesWritable> {
			int idxCount;
			FreqWordCache freqWords;
			IntWritable intWritable;

			@Override
			public void map(BytesWritable key, Tuple value, Context context)
			throws IOException {
				try {
					int count = (Integer) value.get(idxCount);
					freqWords.add(key, count);
					context.progress();
				} catch (ExecException e) {
					e.printStackTrace();
				}
			}

			@Override
			public void setup(Context context) {
				LOG.info("FreqWords.MapClass.configure");
				String inSchema;
				try {
					inSchema = TableInputFormat.getProjection(context);
				} catch (ParseException e) {
					throw new RuntimeException("Projection parsing failed : "
							+ e.getMessage());
				} catch (IOException e) {
					throw new RuntimeException("TableInputFormat.getprojection", e);
				}
				idxCount = Projection.getColumnIndex(inSchema, "count");
				intWritable = new IntWritable();
				freqWords = new FreqWordCache(getFreqWordsCount(context.getConfiguration()));
			}

			@Override
			public void cleanup(Context context) throws IOException, InterruptedException {
				Item[] items = freqWords.toArray();
				for (Item i : items) {
					intWritable.set(i.count);
					context.write(intWritable, i.word);
				}
			}
		}

		static class CombinerClass extends
		Reducer<IntWritable, BytesWritable, IntWritable, BytesWritable> {
			FreqWordCache freqWords;
			IntWritable intWritable;

			@Override
			public void setup(Context context) {
				LOG.info("FreqWords.CombinerClass.configure");
				freqWords = new FreqWordCache(getFreqWordsCount(context.getConfiguration()));
				intWritable = new IntWritable();
			}

			@Override
			public void cleanup(Context context) throws IOException, InterruptedException {
				Item[] items = freqWords.toArray();
				for (Item i : items) {
					intWritable.set(i.count);
					context.write(intWritable, i.word);
				}
			}

			@Override
			public void reduce(IntWritable key, Iterable<BytesWritable> values, Context context)
			throws IOException, InterruptedException {
				freqWords.add(values.iterator(), key.get());
				context.progress();
			}
		}

		static class ReduceClass extends
		Reducer<IntWritable, BytesWritable, BytesWritable, Tuple> {
			FreqWordCache freqWords;
			Tuple outRow;
			int idxCount;

			@Override
			public void setup(Context context) {
				LOG.info("FreqWords.ReduceClass.configure");
				freqWords = new FreqWordCache(getFreqWordsCount(context.getConfiguration()));
				try {
					Schema outSchema = BasicTableOutputFormat.getSchema(context);
					outRow = TypesUtils.createTuple(outSchema);
					idxCount = outSchema.getColumnIndex("count");
				} catch (IOException e) {
					throw new RuntimeException("Schema parsing failed : "
							+ e.getMessage());
				} catch (ParseException e) {
					throw new RuntimeException("Schema parsing failed : "
							+ e.getMessage());
				}
			}

			@Override
			public void cleanup(Context context) throws IOException, InterruptedException {
				Item[] items = freqWords.toArray();
				for (Item i : items) {
					try {
						outRow.set(idxCount, new Integer(i.count));
						context.write(i.word, outRow);
					} catch (ExecException e) {
						e.printStackTrace();
					}
				}
			}

			@Override
			public void reduce(IntWritable key, Iterable<BytesWritable> values, Context context)
			throws IOException {
				freqWords.add(values.iterator(), key.get());
				context.progress();
			}
		}
	}

	static class InverseIntRawComparator implements RawComparator<IntWritable> {
		IntWritable.Comparator comparator;

		InverseIntRawComparator() {
			comparator = new IntWritable.Comparator();
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -comparator.compare(b1, s1, l1, b2, s2, l2);
		}

		@Override
		public int compare(IntWritable o1, IntWritable o2) {
			return -comparator.compare(o1, o2);
		}
	}

	void runFreqWords() throws IOException, ParseException, 
	InterruptedException, ClassNotFoundException {
		LOG.info("Find the most frequent words");
		//JobConf jobConf = getJobConf("runFreqWords");
		Job job = new Job();
		job.setJobName( "runFreqWords" );
		// input-related settings
		job.setInputFormatClass(TableInputFormat.class);
		job.setMapperClass(FreqWords.MapClass.class);
		TableInputFormat.setInputPaths(job, invIndexTablePath);
		TableInputFormat.setProjection(job, "count");
		TableInputFormat.setMinSplitSize(job, options.minTableSplitSize);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		// Set customized output comparator.
		job.setSortComparatorClass(InverseIntRawComparator.class);

		// output related settings
		fileSys.delete(freqWordTablePath, true);
		job.setOutputFormatClass(BasicTableOutputFormat.class);
		job.setReducerClass(FreqWords.ReduceClass.class);
		job.setCombinerClass(FreqWords.CombinerClass.class);
		BasicTableOutputFormat.setOutputPath(job, freqWordTablePath);
		BasicTableOutputFormat.setSchema(job, "count:int");
		job.setNumReduceTasks(1);

		job.submit();
		job.waitForCompletion(true);
		BasicTableOutputFormat.close( job );
	}

	void printFreqWords() throws IOException, ParseException {
		LOG.info("Printing the most frequent words");
		BasicTable.Reader reader = new BasicTable.Reader(freqWordTablePath, conf);
		TableScanner scanner = reader.getScanner(null, true);
		BytesWritable key = new BytesWritable();
		Schema schema = Projection.toSchema(scanner.getProjection());
		int idxCount = schema.getColumnIndex("count");
		Tuple value = TypesUtils.createTuple(schema);
		for (; !scanner.atEnd(); scanner.advance()) {
			scanner.getKey(key);
			scanner.getValue(value);
			try {
				String word = new String(key.getBytes(), 0, key.getLength());
				int count = (Integer) value.get(idxCount);
				LOG.info(String.format("%s\t%d", word, count));
			} catch (ExecException e) {
				e.printStackTrace();
			}
		}
		scanner.close();
	}

	/**
	 * Testing BasicTableOutputFormat and TableInputFormat by running a sequence
	 * of MapReduce jobs.
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public void testBasicTable() throws IOException, ParseException, 
	InterruptedException, ClassNotFoundException {
		LOG.info("testBasicTable");
		LOG.info("testing BasicTableOutputFormat in Map-only job");
		for (int i = 0; i < options.numBatches; ++i) {
			String batchName = batchName(i);
			createSourceFiles(batchName);
			runForwardIndexGen(batchName);
			LOG.info("Forward index conversion complete: " + batchName);
			//      System.out.println( "expected number = " + summary.get( batchName).wordCount );
			//      System.out.println( "actual number = "  + countRows( new Path( fwdIndexRootPath, batchName)) );
			Assert.assertEquals(summary.get(batchName).wordCount, countRows(new Path(
					fwdIndexRootPath, batchName)));
		}
		runInvertedIndexGen();
		verifyWordCount();
		runFreqWords();
		printFreqWords();
	}
}
