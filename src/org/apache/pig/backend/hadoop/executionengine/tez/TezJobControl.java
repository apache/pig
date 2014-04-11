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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop23.PigJobControl;
import org.apache.pig.tools.pigstats.tez.TezStats;

public class TezJobControl extends PigJobControl {

    private static final Log LOG = LogFactory.getLog(TezJobControl.class);
    private TezJobNotifier notifier = null;
    private TezStats stats = null;

    public TezJobControl(String groupName, int timeToSleep) {
        super(groupName, timeToSleep);
    }

    public void setJobNotifier(TezJobNotifier notifier) {
        this.notifier = notifier;
    }

    public void setTezStats(TezStats stats) {
        this.stats = stats;
    }

    @Override
    public void run() {
        try {
            super.run();
            try {
                // Wait for the only jobs finished.
                while (!allFinished()) {
                    try {
                        Thread.sleep(timeToSleep);
                    } catch (InterruptedException e) {
                        // Do nothing
                    }
                }
            } catch (Exception e) {
                throw e;
            } finally {
                stop();
                if (stats!=null) {
                    stats.accumulateStats(this);
                }
                if (notifier != null) {
                    notifier.complete(this);
                    notifier = null;
                }
            }
        } catch (Exception e) {
            // should not happen
            LOG.error("Unexpected error", e);
            throw new RuntimeException(e);
        } finally {
            // Try notify if not notified. Else process will hang.
            if (notifier != null) {
                notifier.complete(this);
                notifier = null;
            }
        }
    }

}
