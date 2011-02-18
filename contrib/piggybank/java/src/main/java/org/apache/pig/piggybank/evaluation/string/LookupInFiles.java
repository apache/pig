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
package org.apache.pig.piggybank.evaluation.string;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
* <dl>
* <dt><b>Syntax:</b></dt>
* <dd><code>int lookupInFiles(String expression,... <comma separated filelist>)</code>.</dd>
* <dt><b>Input:</b></dt>
* <dd><code>files are text files on DFS</code>.</dd>
* <dt><b>Output:</b></dt>
* <dd><code>if any file contains expression, return 1, otherwise, 0</code>.</dd>
* </dl>
*/

public class LookupInFiles extends EvalFunc<Integer> {
    boolean initialized = false;
    ArrayList<String> mFiles = new ArrayList<String>();
    Map<String, Boolean> mKeys = new HashMap<String, Boolean>();
    static Map<ArrayList<String>, Map<String, Boolean>> mTables = new HashMap<ArrayList<String>, Map<String, Boolean>>(); 

    @Override
    public Schema outputSchema(Schema input) {
      try {
          return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.INTEGER));
      } catch (Exception e) {
        return null;
      }
    }
    
    public void init(Tuple tuple) throws IOException {
        for (int count = 1; count < tuple.size(); count++) {
            if (!(tuple.get(count) instanceof String)) {
                String msg = "LookupInFiles : Filename should be a string.";
                throw new IOException(msg);
            }
            mFiles.add((String) tuple.get(count));
        }

        if (mTables.containsKey(mFiles))
        {

            mKeys = mTables.get(mFiles);
        }
        else
        {
            Properties props = ConfigurationUtil.toProperties(PigMapReduce.sJobConfInternal.get());
            for (int i = 0; i < mFiles.size(); ++i) {
                // Files contain only 1 column with the key. No Schema. All keys
                // separated by new line.
    
                BufferedReader reader = null;
    
                InputStream is = null;
                try {
                    is = FileLocalizer.openDFSFile(mFiles.get(i), props);
                } catch (IOException e) {
                    String msg = "LookupInFiles : Cannot open file "+mFiles.get(i);
                    throw new IOException(msg, e);
                }
                try {
                    reader = new BufferedReader(new InputStreamReader(is));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (!mKeys.containsKey(line))
                            mKeys.put(line, true);
                    }
                    is.close();
                } catch (IOException e) {
                    String msg = "LookupInFiles : Cannot read file "+mFiles.get(i);
                    throw new IOException(msg, e);
                }
            }
            mTables.put(mFiles, mKeys);
        }
        initialized=true;
    }

    @Override
    public Integer exec(Tuple input) throws IOException {
        if (!initialized)
            init(input);
        if (input.get(0)==null)
            return null;
        if (mKeys.containsKey(input.get(0).toString()))
            return 1;
        return 0;
    }
}
