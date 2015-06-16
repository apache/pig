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
package org.apache.pig.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.LoadFunc;
import org.apache.pig.impl.io.ReadToEndLoader;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;

public class TestReadToEndLoader {
    @Test
    public void testIsReaderForLastSplitClosed() throws Exception {
        final LoadFunc loadFunc = mock(LoadFunc.class);
        final InputFormat inputFormat = mock(InputFormat.class);
        final RecordReader recordReader = mock(RecordReader.class);
        final InputSplit inputSplit = mock(InputSplit.class);
        // Define behavior
        when(loadFunc.getInputFormat()).thenReturn(inputFormat);
        when(inputFormat.createRecordReader(
                any(InputSplit.class), any(TaskAttemptContext.class))).thenReturn(recordReader);
        when(inputFormat.getSplits(any(JobContext.class))).thenReturn(Arrays.asList(inputSplit));
        Configuration conf = new Configuration();
        ReadToEndLoader loader = new ReadToEndLoader(loadFunc, conf, "loc", 0);
        // This will return null since we haven't specified any behavior for this method
        Assert.assertNull(loader.getNext());
        // Verify that RecordReader.close for the last input split is called once
        verify(recordReader, times(1)).close();
    }
}
