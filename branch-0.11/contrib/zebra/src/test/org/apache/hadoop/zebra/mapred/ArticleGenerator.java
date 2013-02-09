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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.zebra.tfile.RandomDistribution.DiscreteRNG;
import org.apache.hadoop.zebra.tfile.RandomDistribution.Flat;

/**
 * Generate some input text files.
 */
class ArticleGenerator {
  Random random;
  Dictionary dict;
  int pageWidth;
  DiscreteRNG lastLineLenGen;
  DiscreteRNG paragraphLineLenGen;
  DiscreteRNG paragraphLenGen;
  long wordCount;
  long lineCount;

  /**
   * Create an article generator.
   * 
   * @param dictWordCnt
   *          Number of words in the dictionary.
   * @param minWordLen
   *          Minimum word length
   * @param maxWordLen
   *          Maximum word length
   * @param lineWidth
   *          Line width.
   */
  ArticleGenerator(int dictWordCnt, int minWordLen, int maxWordLen,
      int pageWidth) {
    random = new Random(System.nanoTime());
    dict = new Dictionary(random, dictWordCnt, minWordLen, maxWordLen, 100);
    this.pageWidth = pageWidth;
    lastLineLenGen = new Flat(random, 1, pageWidth);
    paragraphLineLenGen = new Flat(random, pageWidth * 3 / 4, pageWidth);
    paragraphLenGen = new Flat(random, 1, 40);
  }

  /**
   * Create an article
   * 
   * @param fs
   *          File system.
   * @param path
   *          path of the file
   * @param length
   *          Expected size of the file.
   * @throws IOException
   */
  void createArticle(FileSystem fs, Path path, long length) throws IOException {
    FSDataOutputStream fsdos = fs.create(path, false);
    StringBuilder sb = new StringBuilder();
    int remainLinesInParagraph = paragraphLenGen.nextInt();
    while (fsdos.getPos() < length) {
      if (remainLinesInParagraph == 0) {
        remainLinesInParagraph = paragraphLenGen.nextInt();
        fsdos.write('\n');
      }
      int lineLen = paragraphLineLenGen.nextInt();
      if (--remainLinesInParagraph == 0) {
        lineLen = lastLineLenGen.nextInt();
      }
      sb.setLength(0);
      while (sb.length() < lineLen) {
        if (sb.length() > 0) {
          sb.append(' ');
        }
        sb.append(dict.nextWord());
        ++wordCount;
      }
      sb.append('\n');
      fsdos.write(sb.toString().getBytes());
      ++lineCount;
    }
    fsdos.close();
  }

  /**
   * Create a bunch of files under the same directory.
   * 
   * @param fs
   *          File system
   * @param parent
   *          directory where files should be created
   * @param prefix
   *          prefix name of the files
   * @param n
   *          total number of files
   * @param length
   *          length of each file.
   * @throws IOException
   */
  void batchArticalCreation(FileSystem fs, Path parent, String prefix, int n,
      long length) throws IOException {
    for (int i = 0; i < n; ++i) {
      createArticle(fs, new Path(parent, String.format("%s%06d", prefix, i)),
          length);
    }
  }

  static class Summary {
    long wordCount;
    long lineCount;
    Map<String, Long> wordCntDist;

    Summary() {
      wordCntDist = new HashMap<String, Long>();
    }
  }

  void resetSummary() {
    wordCount = 0;
    lineCount = 0;
    dict.resetWordCnts();
  }

  Summary getSummary() {
    Summary ret = new Summary();
    ret.wordCount = wordCount;
    ret.lineCount = lineCount;
    ret.wordCntDist = dict.getWordCounts();
    return ret;
  }
}
