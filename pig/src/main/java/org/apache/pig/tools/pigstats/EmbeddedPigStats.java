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

import org.apache.hadoop.mapred.JobClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This class encapsulates the statistics collected from an embedded Pig script. 
 * It includes a collection of {@link SimplePigStats} corresponding to the 
 * instances of Pig scripts run by the given embedded script.
 */
final class EmbeddedPigStats extends PigStats {

    private Map<String, List<PigStats>> statsMap;
    
    @Override
    public boolean isSuccessful() {
        boolean isSuccessful = true;
        for (List<PigStats> lst : statsMap.values()) {
            for (PigStats stats : lst) {
                if (!stats.isSuccessful()) {
                    isSuccessful = false;
                    break;
                }
            }
        }        
        return isSuccessful;
    }

    @Override
    public List<String> getAllErrorMessages() {
        ArrayList<String> msgLst = new ArrayList<String>();
        for (List<PigStats> lst : statsMap.values()) {
            for (PigStats stats : lst) {
                if (!stats.isSuccessful()) {
                    msgLst.add(stats.getErrorMessage());
                    break;
                }
            }          
        }
        return msgLst;
    }

    @Override
    public Map<String, List<PigStats>> getAllStats() {        
        return statsMap;
    }

    @Override
    public boolean isEmbedded() {
        return true;
    }

    @Override
    public JobClient getJobClient() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getBytesWritten() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDuration() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getErrorCode() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getFeatures() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<InputStats> getInputStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public JobGraph getJobGraph() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNumberBytes(String location) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumberJobs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNumberRecords(String location) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getOutputAlias(String location) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getOutputLocations() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getOutputNames() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<OutputStats> getOutputStats() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Properties getPigProperties() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getProactiveSpillCountObjects() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getProactiveSpillCountRecords() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getRecordWritten() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getReturnCode() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSMMSpillCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getScriptId() {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public OutputStats result(String alias) {
        throw new UnsupportedOperationException();
    }
   
    //-------------------------------------------------------------------------
    
    EmbeddedPigStats(Map<String, List<PigStats>> statsMap) {
        this.statsMap = statsMap;
    }
 
}
