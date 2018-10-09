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

package org.apache.pig.backend.hadoop.executionengine.tez.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.pig.impl.util.Pair;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.hadoop.InputSplitInfoMem;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto;

public class TezInputHelper {
    private static final Log LOG = LogFactory.getLog(TezInputHelper.class);

    /**
     * This method creates input splits similar to 
     * {@link org.apache.tez.mapreduce.hadoop.MRInputHelpers#generateInputSplitsToMem}
     * but only does it for mapreduce API and does not do grouping of splits or create
     * {@link org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto}
     * which is an expensive operation.
     *
     * @param conf an instance of Configuration. This
     *        Configuration instance should contain adequate information to
     *        be able to generate splits - like the InputFormat being used and
     *        related configuration.
     * @return an instance of {@link InputSplitInfoMem} which supports a subset
     *         of the APIs defined on {@link InputSplitInfo}
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static InputSplitInfoMem generateInputSplitsToMem(Configuration conf)
            throws IOException, ClassNotFoundException, InterruptedException {

        InputSplitInfoMem splitInfoMem = null;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Generating mapreduce api input splits");
        }
        Job job = Job.getInstance(conf);
        org.apache.hadoop.mapreduce.InputSplit[] splits = generateSplits(job);
        splitInfoMem = new InputSplitInfoMem(splits, createTaskLocationHintsFromSplits(splits), splits.length,
                job.getCredentials(), job.getConfiguration());
        return splitInfoMem;
    }

    private static org.apache.hadoop.mapreduce.InputSplit[] generateSplits(JobContext jobContext)
            throws ClassNotFoundException, IOException, InterruptedException {
        Configuration conf = jobContext.getConfiguration();

        // This is the real input format.
        org.apache.hadoop.mapreduce.InputFormat<?, ?> inputFormat = null;
        try {
            inputFormat = ReflectionUtils.newInstance(jobContext.getInputFormatClass(), conf);
        }
        catch (ClassNotFoundException e) {
            throw new TezUncheckedException(e);
        }

        org.apache.hadoop.mapreduce.InputFormat<?, ?> finalInputFormat = inputFormat;
        List<org.apache.hadoop.mapreduce.InputSplit> array = finalInputFormat.getSplits(jobContext);
        org.apache.hadoop.mapreduce.InputSplit[] splits = (org.apache.hadoop.mapreduce.InputSplit[]) array
                .toArray(new org.apache.hadoop.mapreduce.InputSplit[array.size()]);

        // sort the splits into order based on size, so that the biggest
        // go first
        Arrays.sort(splits, new InputSplitComparator());
        return splits;
    }

    /**
     * Comparator for org.apache.hadoop.mapreduce.InputSplit
     */
    private static class InputSplitComparator implements Comparator<org.apache.hadoop.mapreduce.InputSplit> {
        @Override
        public int compare(org.apache.hadoop.mapreduce.InputSplit o1, org.apache.hadoop.mapreduce.InputSplit o2) {
            try {
                long len1 = o1.getLength();
                long len2 = o2.getLength();
                if (len1 < len2) {
                    return 1;
                }
                else if (len1 == len2) {
                    return 0;
                }
                else {
                    return -1;
                }
            }
            catch (IOException ie) {
                throw new RuntimeException("exception in InputSplit compare", ie);
            }
            catch (InterruptedException ie) {
                throw new RuntimeException("exception in InputSplit compare", ie);
            }
        }
    }

    private static List<TaskLocationHint> createTaskLocationHintsFromSplits(
            org.apache.hadoop.mapreduce.InputSplit[] newFormatSplits) throws IOException, InterruptedException {
        List<TaskLocationHint> listLocationHint = new ArrayList<>(newFormatSplits.length);
        for(org.apache.hadoop.mapreduce.InputSplit input : newFormatSplits) {
            listLocationHint.add(TaskLocationHint.createTaskLocationHint(
                    new HashSet<String>(Arrays.asList(input.getLocations())), null));
        }
        return listLocationHint;
    }

    /**
     * Creates MRSplitsProto from inputSplitInfo and adds into splitsBuilder.
     * @param inputSplitInfo
     * @param conf
     * @param splitsBuilder
     * @param spillThreshold
     * @return Pair containing first element, a long, as serialized size and second element, a boolean, as true if all splits are serialized. Second element
     * will be false, if only some of the splits are serialized because we reached to spillThreshold. 
     */
    public static Pair<Long, Boolean> createSplitsProto(InputSplitInfo inputSplitInfo, Configuration conf, MRSplitsProto.Builder splitsBuilder, long spillThreshold
            ) {
          try {
            return createSplitsProto(inputSplitInfo.getNewFormatSplits(), new SerializationFactory(conf), splitsBuilder, spillThreshold);
          } catch (IOException e) {
            throw new RuntimeException(e);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
    }

    /**
     * Creates MRSplitsProto from given org.apache.hadoop.mapreduce.InputSplit and adds into splitsBuilder.
     * @param inputSplits
     * @param serializationFactory
     * @param splitsBuilder
     * @param spillThreshold
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    private static Pair<Long, Boolean> createSplitsProto(InputSplit[] inputSplits,
            SerializationFactory serializationFactory, MRSplitsProto.Builder splitsBuilder,
            long spillThreshold) throws IOException, InterruptedException {
        MRSplitProto split = null;
        long serializedSize = 0;
        boolean allSerialized = true;
        for (int i=0;i<inputSplits.length;i++) {
          split = MRInputHelpers.createSplitProto(inputSplits[i], serializationFactory);
          serializedSize += split.getSerializedSize();
          splitsBuilder.addSplits(split);
          // check for threshold after adding split, it may cause splitsSerializedSize to become more than spillThreshold,
          // but we don't want to waste already serialized split
          if(serializedSize > spillThreshold && i != (inputSplits.length - 1)) {
              allSerialized = false;
              break;
          }
        }
        return new Pair<Long,Boolean>(serializedSize, allSerialized);
    }
}
