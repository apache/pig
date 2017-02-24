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
package org.apache.pig.tools.pigstats;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.PigContext;

/**
 * EmptyPigStats encapsulates dummy statistics of a fetch task, since during a
 * fetch no MR jobs are executed
 *
 */
public class EmptyPigStats extends PigStats {

    private final List<OutputStats> outputStatsList;
    private final List<InputStats> inputStatsList;
    private final JobGraph emptyJobPlan = new JobGraph();

    public EmptyPigStats() {
        // initialize empty stats
        OutputStats os = new OutputStats(null, -1, -1, true);
        this.outputStatsList = Collections.unmodifiableList(Arrays.asList(os));

        InputStats is = new InputStats(null, -1, -1, true);
        this.inputStatsList = Collections.unmodifiableList(Arrays.asList(is));
    }

    public EmptyPigStats(PigContext pigContext, POStore poStore) {
        super.pigContext = pigContext;
        super.startTime = super.endTime = System.currentTimeMillis();
        super.userId = System.getProperty("user.name");

        Configuration conf = ConfigurationUtil.toConfiguration(pigContext.getProperties());

        // initialize empty stats
        OutputStats os = new OutputStats(null, -1, -1, true);
        os.setConf(conf);
        os.setPOStore(poStore);
        this.outputStatsList = Collections.unmodifiableList(Arrays.asList(os));

        InputStats is = new InputStats(null, -1, -1, true);
        is.setConf(conf);
        this.inputStatsList = Collections.unmodifiableList(Arrays.asList(is));
    }

    @Override
    public JobClient getJobClient() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmbedded() {
        return false;
    }

    @Override
    public Map<String, List<PigStats>> getAllStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getAllErrorMessages() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JobGraph getJobGraph() {
       return emptyJobPlan;
    }

    @Override
    public List<String> getOutputLocations() {
        return Collections.emptyList();
    }

    @Override
    public List<String> getOutputNames() {
        return Collections.emptyList();
    }

    @Override
    public long getNumberBytes(String location) {
        return -1L;
    }

    @Override
    public long getNumberRecords(String location) {
        return -1L;
    }

    @Override
    public String getOutputAlias(String location) {
        return null;
    }

    @Override
    public long getSMMSpillCount() {
        return 0L;
    }

    @Override
    public long getProactiveSpillCountRecords() {
        return 0L;
    }

    @Override
    public long getProactiveSpillCountObjects() {
        return 0L;
    }

    @Override
    public long getBytesWritten() {
        return 0L;
    }

    @Override
    public long getRecordWritten() {
        return 0L;
    }

    @Override
    public int getNumberJobs() {
        return 0;
    }

    @Override
    public List<OutputStats> getOutputStats() {
        return outputStatsList;
    }

    @Override
    public OutputStats result(String alias) {
        return outputStatsList.get(0);
    }

    @Override
    public List<InputStats> getInputStats() {
        return inputStatsList;
    }

    @Override
    public void setBackendException(String jobId, Exception e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void start() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void stop() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumberSuccessfulJobs() {
        return -1;
    }

    @Override
    public int getNumberFailedJobs() {
        return -1;
    }

}
