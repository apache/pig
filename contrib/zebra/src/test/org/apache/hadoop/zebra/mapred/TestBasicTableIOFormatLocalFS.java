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

package org.apache.hadoop.zebra.mapred;

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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.mapred.BasicTableOutputFormat;
import org.apache.hadoop.zebra.mapred.TableInputFormat;
import org.apache.hadoop.zebra.mapred.ArticleGenerator.Summary;
import org.apache.hadoop.zebra.mapred.TestBasicTableIOFormatLocalFS.FreqWordCache.Item;
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
  MiniMRCluster mr;
  ArticleGenerator articalGen;
  Map<String, Summary> summary;

  @Override
  protected void setUp() throws IOException {
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getPath(); // getAbsolutePath();
      Path path = new Path(".");
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
      mr = new MiniMRCluster(options.taskTrackers, "file:///", 3);
    } else {
      dfs = new MiniDFSCluster(conf, options.dataNodes, true, null);
      fileSys = dfs.getFileSystem();
      rootPath = new Path(options.rootPath);
      mr = new MiniMRCluster(options.taskTrackers, fileSys.getUri().toString(),
          1);
    }
    conf = getJobConf("TestBasicTableIOFormat");
    srcPath = new Path(rootPath, options.srcPath);
    fwdIndexRootPath = new Path(rootPath, options.fwdIndexRootPath);
    invIndexTablePath = new Path(rootPath, options.invIndexTablePath);
    freqWordTablePath = new Path(rootPath, options.freqWordTablePath);
  }

  @Override
  protected void tearDown() throws IOException {
    if (mr != null) {
      mr.shutdown();
    }
    if (dfs != null) {
      dfs.shutdown();
    }
  }

  JobConf getJobConf(String name) {
    JobConf jobConf = mr.createJobConf();
    jobConf.setJobName(name);
    jobConf.setInt("table.input.split.minSize", 1);
    options.minTableSplitSize = 1; // force more splits
    jobConf.setInt("dfs.block.size", 1024); // force multiple blocks
    jobConf.set("table.output.tfile.compression", options.compression);
    jobConf.setInt("mapred.app.freqWords.count", options.numFreqWords);
    return jobConf;
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
    static class MapClass implements
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
      public void map(LongWritable key, Text value,
          OutputCollector<BytesWritable, Tuple> output, Reporter reporter)
          throws IOException {
        if (filePath == null) {
          FileSplit split = (FileSplit) reporter.getInputSplit();
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
            output.collect(outKey, outRow);
          } catch (ExecException e) {
            e.printStackTrace();
          }

          ++wordPos;
        }
        ++lineNo;

      }

      @Override
      public void configure(JobConf job) {
        LOG.info("ForwardIndexGen.MapClass.configure");
        outKey = new BytesWritable();
        try {
          Schema outSchema = BasicTableOutputFormat.getSchema(job);
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

      @Override
      public void close() throws IOException {
        // no-op
      }
    }
  }

  /**
   * Run forward index generation.
   * 
   * @param batchName
   * @throws IOException
   */
  void runForwardIndexGen(String batchName) throws IOException, ParseException {
    LOG.info("Run Map-only job to convert source data to forward index: "
        + batchName);

    JobConf jobConf = getJobConf("fwdIndexGen-" + batchName);

    // input-related settings
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setMapperClass(ForwardIndexGen.MapClass.class);
    FileInputFormat.setInputPaths(jobConf, new Path(srcPath, batchName));
    jobConf.setNumMapTasks(options.numMapper);

    // output related settings
    Path outPath = new Path(fwdIndexRootPath, batchName);
    fileSys.delete(outPath, true);
    jobConf.setOutputFormat(BasicTableOutputFormat.class);
    BasicTableOutputFormat.setOutputPath(jobConf, outPath);
    BasicTableOutputFormat.setSchema(jobConf, "fileName:string, wordPos:int, lineNo:int");

    // set map-only job.
    jobConf.setNumReduceTasks(0);
    JobClient.runJob(jobConf);
    BasicTableOutputFormat.close(jobConf);
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
    reader.setProjection("");
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
    static class MapClass implements
        Mapper<BytesWritable, Tuple, BytesWritable, InvIndex> {
      // index of fileName and wordPos fileds of the input tuple
      int idxFileName, idxWordPos;

      @Override
      public void map(BytesWritable key, Tuple value,
          OutputCollector<BytesWritable, InvIndex> output, Reporter reporter)
          throws IOException {
        try {
          String fileName = (String) value.get(idxFileName);
          int wordPos = (Integer) value.get(idxWordPos);
          output.collect(key, new InvIndex(fileName, wordPos));
        } catch (ExecException e) {
          e.printStackTrace();
        }
      }

      @Override
      public void configure(JobConf job) {
        LOG.info("InvertedIndexGen.MapClass.configure");
        String projection;
        try {
          projection = TableInputFormat.getProjection(job);
        } catch (ParseException e) {
          throw new RuntimeException("Schema parsing failed : "
              + e.getMessage());
        } catch (IOException e) {
          throw new RuntimeException("TableInputFormat.getProjection", e);
        }
        idxFileName = Projection.getColumnIndex(projection, "fileName");
        idxWordPos = Projection.getColumnIndex(projection, "wordPos");
      }

      @Override
      public void close() throws IOException {
        // no-op
      }
    }

    static class CombinerClass implements
        Reducer<BytesWritable, InvIndex, BytesWritable, InvIndex> {

      @Override
      public void reduce(BytesWritable key, Iterator<InvIndex> values,
          OutputCollector<BytesWritable, InvIndex> output, Reporter reporter)
          throws IOException {
        InvIndex sum = new InvIndex();
        for (; values.hasNext();) {
          sum.reduce(values.next());
        }
        output.collect(key, sum);
      }

      @Override
      public void configure(JobConf job) {
        LOG.info("InvertedIndexGen.CombinerClass.configure");
      }

      @Override
      public void close() throws IOException {
        // no-op
      }
    }

    static class ReduceClass implements
        Reducer<BytesWritable, InvIndex, BytesWritable, Tuple> {
      Tuple outRow;
      int idxCount, idxIndex;
      Schema wordPosSchema;
      int idxWordPos;

      @Override
      public void configure(JobConf job) {
        LOG.info("InvertedIndexGen.ReduceClass.configure");
        try {
          Schema outSchema = BasicTableOutputFormat.getSchema(job);
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

      @Override
      public void close() throws IOException {
        // no-op
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
      public void reduce(BytesWritable key, Iterator<InvIndex> values,
          OutputCollector<BytesWritable, Tuple> output, Reporter reporter)
          throws IOException {
        InvIndex sum = new InvIndex();
        for (; values.hasNext();) {
          sum.reduce(values.next());
        }
        try {
          outRow.set(idxCount, sum.count);
          outRow.set(idxIndex, convertInvIndex(sum.index));
          output.collect(key, outRow);
        } catch (ExecException e) {
          e.printStackTrace();
        }
      }
    }
  }

  void runInvertedIndexGen() throws IOException, ParseException {
    LOG.info("Converting forward index to inverted index");
    JobConf jobConf = getJobConf("runInvertedIndexGen");

    // input-related settings
    jobConf.setInputFormat(TableInputFormat.class);
    jobConf.setMapperClass(InvertedIndexGen.MapClass.class);
    Path[] paths = new Path[options.numBatches];
    for (int i = 0; i < options.numBatches; ++i) {
      paths[i] = new Path(fwdIndexRootPath, batchName(i));
    }
    TableInputFormat.setInputPaths(jobConf, paths);
    // TableInputFormat.setProjection(jobConf, "fileName, wordPos");
    TableInputFormat.setMinSplitSize(jobConf, options.minTableSplitSize);
    jobConf.setNumMapTasks(options.numMapper);
    jobConf.setMapOutputKeyClass(BytesWritable.class);
    jobConf.setMapOutputValueClass(InvIndex.class);

    // output related settings
    fileSys.delete(invIndexTablePath, true);
    jobConf.setOutputFormat(BasicTableOutputFormat.class);
    jobConf.setReducerClass(InvertedIndexGen.ReduceClass.class);
    jobConf.setCombinerClass(InvertedIndexGen.CombinerClass.class);
    BasicTableOutputFormat.setOutputPath(jobConf, invIndexTablePath);
    BasicTableOutputFormat.setSchema(jobConf, "count:int, index:map()");
    jobConf.setNumReduceTasks(options.numReducer);

    JobClient.runJob(jobConf);
    BasicTableOutputFormat.close(jobConf);
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
      String word = new String(key.get(), 0, key.getSize());
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
        word.set(w.get(), 0, w.getSize());
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

    static class MapClass implements
        Mapper<BytesWritable, Tuple, IntWritable, BytesWritable> {
      int idxCount;
      FreqWordCache freqWords;
      IntWritable intWritable;
      OutputCollector<IntWritable, BytesWritable> out;

      @Override
      public void map(BytesWritable key, Tuple value,
          OutputCollector<IntWritable, BytesWritable> output, Reporter reporter)
          throws IOException {
        if (out == null)
          out = output;
        try {
          int count = (Integer) value.get(idxCount);
          freqWords.add(key, count);
          reporter.progress();
        } catch (ExecException e) {
          e.printStackTrace();
        }
      }

      @Override
      public void configure(JobConf job) {
        LOG.info("FreqWords.MapClass.configure");
        String inSchema;
        try {
          inSchema = TableInputFormat.getProjection(job);
        } catch (ParseException e) {
          throw new RuntimeException("Projection parsing failed : "
              + e.getMessage());
        } catch (IOException e) {
          throw new RuntimeException("TableInputFormat.getprojection", e);
        }
        idxCount = Projection.getColumnIndex(inSchema, "count");
        intWritable = new IntWritable();
        freqWords = new FreqWordCache(getFreqWordsCount(job));
      }

      @Override
      public void close() throws IOException {
        Item[] items = freqWords.toArray();
        for (Item i : items) {
          intWritable.set(i.count);
          out.collect(intWritable, i.word);
        }
      }
    }

    static class CombinerClass implements
        Reducer<IntWritable, BytesWritable, IntWritable, BytesWritable> {
      FreqWordCache freqWords;
      OutputCollector<IntWritable, BytesWritable> out;
      IntWritable intWritable;

      @Override
      public void configure(JobConf job) {
        LOG.info("FreqWords.CombinerClass.configure");
        freqWords = new FreqWordCache(getFreqWordsCount(job));
        intWritable = new IntWritable();
      }

      @Override
      public void close() throws IOException {
        Item[] items = freqWords.toArray();
        for (Item i : items) {
          intWritable.set(i.count);
          out.collect(intWritable, i.word);
        }
      }

      @Override
      public void reduce(IntWritable key, Iterator<BytesWritable> values,
          OutputCollector<IntWritable, BytesWritable> output, Reporter reporter)
          throws IOException {
        if (out == null) {
          out = output;
        }
        freqWords.add(values, key.get());
        reporter.progress();
      }
    }

    static class ReduceClass implements
        Reducer<IntWritable, BytesWritable, BytesWritable, Tuple> {
      FreqWordCache freqWords;
      OutputCollector<BytesWritable, Tuple> out;
      Tuple outRow;
      int idxCount;

      @Override
      public void configure(JobConf job) {
        LOG.info("FreqWords.ReduceClass.configure");
        freqWords = new FreqWordCache(getFreqWordsCount(job));
        try {
          Schema outSchema = BasicTableOutputFormat.getSchema(job);
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
      public void close() throws IOException {
        Item[] items = freqWords.toArray();
        for (Item i : items) {
          try {
            outRow.set(idxCount, new Integer(i.count));
            out.collect(i.word, outRow);
          } catch (ExecException e) {
            e.printStackTrace();
          }
        }
      }

      @Override
      public void reduce(IntWritable key, Iterator<BytesWritable> values,
          OutputCollector<BytesWritable, Tuple> output, Reporter reporter)
          throws IOException {
        if (out == null) {
          out = output;
        }
        freqWords.add(values, key.get());
        reporter.progress();
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

  void runFreqWords() throws IOException, ParseException {
    LOG.info("Find the most frequent words");
    JobConf jobConf = getJobConf("runFreqWords");

    // input-related settings
    jobConf.setInputFormat(TableInputFormat.class);
    jobConf.setMapperClass(FreqWords.MapClass.class);
    TableInputFormat.setInputPaths(jobConf, invIndexTablePath);
    TableInputFormat.setProjection(jobConf, "count");
    TableInputFormat.setMinSplitSize(jobConf, options.minTableSplitSize);
    // jobConf.setNumMapTasks(options.numMapper);
    jobConf.setNumMapTasks(-1);
    jobConf.setMapOutputKeyClass(IntWritable.class);
    jobConf.setMapOutputValueClass(BytesWritable.class);
    // Set customized output comparator.
    jobConf.setOutputKeyComparatorClass(InverseIntRawComparator.class);

    // output related settings
    fileSys.delete(freqWordTablePath, true);
    jobConf.setOutputFormat(BasicTableOutputFormat.class);
    jobConf.setReducerClass(FreqWords.ReduceClass.class);
    jobConf.setCombinerClass(FreqWords.CombinerClass.class);
    BasicTableOutputFormat.setOutputPath(jobConf, freqWordTablePath);
    BasicTableOutputFormat.setSchema(jobConf, "count:int");
    jobConf.setNumReduceTasks(1);

    JobClient.runJob(jobConf);
    BasicTableOutputFormat.close(jobConf);
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
        String word = new String(key.get(), 0, key.getSize());
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
   */
  public void testBasicTable() throws IOException, ParseException {
    LOG.info("testBasicTable");
    LOG.info("testing BasicTableOutputFormat in Map-only job");
    for (int i = 0; i < options.numBatches; ++i) {
      String batchName = batchName(i);
      createSourceFiles(batchName);
      runForwardIndexGen(batchName);
      LOG.info("Forward index conversion complete: " + batchName);
      Assert.assertEquals(summary.get(batchName).wordCount, countRows(new Path(
          fwdIndexRootPath, batchName)));
    }
    runInvertedIndexGen();
    verifyWordCount();
    runFreqWords();
    printFreqWords();
  }
}
