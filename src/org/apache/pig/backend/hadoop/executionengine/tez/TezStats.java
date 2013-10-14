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
package org.apache.pig.backend.hadoop.executionengine.tez;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.mapred.JobClient;
import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;

public class TezStats extends PigStats {

    @Override
    public JobClient getJobClient() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isEmbedded() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isSuccessful() {
        // TODO Auto-generated method stub
        return true;
    }

    @Override
    public Map<String, List<PigStats>> getAllStats() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> getAllErrorMessages() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Properties getPigProperties() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JobGraph getJobGraph() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> getOutputLocations() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> getOutputNames() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getNumberBytes(String location) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getNumberRecords(String location) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getOutputAlias(String location) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getSMMSpillCount() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getProactiveSpillCountObjects() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getProactiveSpillCountRecords() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getBytesWritten() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getRecordWritten() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getScriptId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getFeatures() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getDuration() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getNumberJobs() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public List<OutputStats> getOutputStats() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public OutputStats result(String alias) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<InputStats> getInputStats() {
        // TODO Auto-generated method stub
        return null;
    }
}
