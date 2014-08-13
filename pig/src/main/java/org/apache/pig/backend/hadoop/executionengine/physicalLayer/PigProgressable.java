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
package org.apache.pig.backend.hadoop.executionengine.physicalLayer;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;


/**
 * Pig's progress indicator.  An implemenation of this interface is passed to
 * UDFs to allow them to send heartbeats.  By default Hadoop will kill a task
 * if it does not receive a heartbeat every 600 seconds.  Any operation that
 * may take more than this should call progress on a regular basis.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface PigProgressable {

    /**
     * Report progress.
     */
    public void progress();
    
    /**
     * Report progress with a message.
     * @param msg message to send with progress report.
     */
    public void progress(String msg);
}
