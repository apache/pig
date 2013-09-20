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

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.backend.BackendException;
import org.apache.pig.backend.hadoop.executionengine.Launcher;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.tools.pigstats.PigStats;

/**
 * Main class that launches pig for Tez
 */
public class TezLauncher extends Launcher {

    @Override
    public PigStats launchPig(PhysicalPlan php, String grpName, PigContext pc)
            throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void explain(PhysicalPlan pp, PigContext pc, PrintStream ps,
            String format, boolean verbose) throws PlanException,
            VisitorException, IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public void kill() throws BackendException {
        // TODO Auto-generated method stub
    }

    @Override
    public void killJob(String jobID, JobConf jobConf) throws BackendException {
        // TODO Auto-generated method stub
    }
}

