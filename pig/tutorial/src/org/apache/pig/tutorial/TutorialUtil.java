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
package org.apache.pig.tutorial;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class TutorialUtil {
  
  /**
   * This function splits a search query string into a set 
   * of non-empty words 
   */
  protected static String[] splitToWords(String query) {
    List<String> res = new LinkedList<String>();
    String[] words = query.split("\\W");
    for (String word : words) {
      if (!word.equals("")) {
        res.add(word);
      }
    }
    return res.toArray(new String[res.size()]);
  }

  /**
   *   This is a simple utility function that make word-level
   * ngrams from a set of words
   * @param words
   * @param ngrams
   * @param size
   */
  protected static void makeNGram(String[] words, Set<String> ngrams, int size) {
    int stop = words.length - size + 1;
    for (int i = 0; i < stop; i++) {
      StringBuilder sb = new StringBuilder();
      for (int j = 0; j < size; j++) {
        sb.append(words[i + j]).append(" ");
      }
      sb.deleteCharAt(sb.length() - 1);
      ngrams.add(sb.toString());
    }
    if (size > 1) {
      makeNGram(words, ngrams, size - 1);
    }
  }
  
}
