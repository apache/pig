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
package org.apache.pig.impl.builtin;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.InterStorage;
import org.apache.pig.impl.io.ReadToEndLoader;
import org.apache.pig.impl.util.UDFContext;

/**
 * ReadScalars reads a line from a file and returns it as its value. The
 * file is only read once, and the same line is returned over and over again.
 * This is useful for incorporating a result from an agregation into another
 * evaluation.
 */
public class ReadScalars extends EvalFunc<Object> {
    private String scalarfilename = null;
  //  private String charset = "UTF-8";
    private Object value = null;

    // in-core input : used by illustrator
    private Map<String, DataBag> inputBuffer = null;

    private boolean valueLoaded = false;

    /**
     * Java level API
     *
     * @param input
     *            expects a single constant that is the name of the file to be
     *            read
     */
    @Override
    public Object exec(Tuple input) throws IOException {
        if (!valueLoaded) {
            if (input == null || input.size() == 0) {
                valueLoaded = true;
                return null;
            }

            int pos;
            if (inputBuffer != null)
            {
                pos = DataType.toInteger(input.get(0));
                scalarfilename = DataType.toString(input.get(1));
                DataBag inputBag = inputBuffer.get(scalarfilename);
                if (inputBag == null || inputBag.size() ==0)
                {
                    log.warn("No scalar field to read, returning null");
                    valueLoaded = true;
                    return null;
                } else if (inputBag.size() > 1) {
                    String msg = "Scalar has more than one row in the output.";
                    throw new ExecException(msg);
                }
                Tuple t1 = inputBag.iterator().next();
                value = t1.get(pos);
                valueLoaded = true;
                return value;
            }

            ReadToEndLoader loader;
            try {
                pos = DataType.toInteger(input.get(0));
                scalarfilename = DataType.toString(input.get(1));

                // Hadoop security need this property to be set
                Configuration conf = UDFContext.getUDFContext().getJobConf();
                if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
                    conf.set("mapreduce.job.credentials.binary",
                            System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
                }
                loader = new ReadToEndLoader(
                        new InterStorage(), conf, scalarfilename, 0);
            } catch (Exception e) {
                throw new ExecException("Failed to open file '" + scalarfilename
                        + "'; error = " + e.getMessage());
            }
            try {
                Tuple t1 = loader.getNext();
                if(t1 == null){
                    log.warn("No scalar field to read, returning null");
                    valueLoaded = true;
                    return null;
                }
                value = t1.get(pos);
                Tuple t2 = loader.getNext();
                if(t2 != null){
                    String msg = "Scalar has more than one row in the output. "
                        + "1st : " + t1 + ", 2nd :" + t2;
                    throw new ExecException(msg);
                }
                valueLoaded = true;

            } catch (Exception e) {
                throw new ExecException(e.getMessage());
            }
        }
        return value;
    }

    public void setOutputBuffer(Map<String, DataBag> inputBuffer) {
        this.inputBuffer = inputBuffer;
        value = null;
        valueLoaded = false;
    }
}
