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
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.IOException;

import org.apache.pig.data.Tuple;

/**
 * This class is the static Mapper class used by Pig
 * to execute Pig map only jobs. It gets a TargetedTuple
 * as input to the map function. Using the targets in it,
 * attaches the tuple as input to the target operators.
 * 
 * The map plan is then executed. The result is then collected
 * into the output collector provide by Hadoop which is configured
 * to write to the store file.
 * 
 * There is a reporter running in a separate thread that keeps
 * reporting progress to let the job tracker know that we are 
 * alive. The sleep time for the reporter thread can be configured
 * per job via the "pig.reporter.sleep.time". By default it uses
 * a 10 sec sleep time
 */

public class PigMapOnly {

    public static class Map extends PigMapBase {

        @Override
        public void collect(Context oc, Tuple tuple) 
                throws InterruptedException, IOException {            
            oc.write(null, tuple);
        }
    }
}
