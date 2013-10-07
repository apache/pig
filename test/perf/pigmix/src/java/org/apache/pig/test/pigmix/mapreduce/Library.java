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
package org.apache.pig.test.pigmix.mapreduce;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

/**
 * A collection of static functions for use by the pigmix map reduce tasks.
 */
public class Library {

    public static List<Text> splitLine(Text line, char delimiter) {
        String s = line.toString();
        List<Text> cols = new ArrayList<Text>();
        int start = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == delimiter) {
                if (start == i) cols.add(new Text()); // null case
                else cols.add(new Text(s.substring(start, i)));
                start = i + 1;
            }
        }
        // Grab the last one.
        if (start != s.length() - 1) cols.add(new Text(s.substring(start)));

        return cols;
    }

    public static Text mapLookup(Text mapCol, Text key) {
        List<Text> kvps = splitLine(mapCol, '');

        for (Text potential : kvps) {
            // Split potential on ^D
            List<Text> kv = splitLine(potential, '');
            if (kv.size() != 2) return null;
            if (kv.get(0).equals(potential)) return kv.get(1);
        }

        return null;
    }

        
                
}
