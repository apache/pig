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

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.util.Progressable;
import org.apache.pig.backend.hadoop.executionengine.TaskContext;
import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class PigStatusReporter extends StatusReporter implements Progressable, PigWarnCounter {

    private static PigStatusReporter reporter = null;

    private TaskContext<?> context = null;

    //@StaticDataCleanup
    public static void staticDataCleanup() {
        reporter = null;
    }

    private PigStatusReporter() {
    }

    /**
     * Get singleton instance of the context
     */
    public static PigStatusReporter getInstance() {
        if (reporter == null) {
            reporter = new PigStatusReporter();
        }
        return reporter;
    }

    public void setContext(TaskContext<?> context) {
        this.context = context;
    }

    /**
     * @deprecated use {@link org.apache.pig.tools.pigstats.PigStatusReporter#incrCounter} instead.
     * This method returns MR counter which is not compatible with Tez mode. Use
     * incrCounter() that is compatible with both MR and Tez mode.
     */
    @Override
    @Deprecated
    public Counter getCounter(Enum<?> name) {
        return (context == null) ? null : context.getCounter(name);
    }

    /**
     * @deprecated use {@link org.apache.pig.tools.pigstats.PigStatusReporter#incrCounter} instead.
     * This method returns MR counter which is not compatible with Tez mode. Use
     * incrCounter() that is compatible with both MR and Tez mode.
     */
    @Override
    @Deprecated
    public Counter getCounter(String group, String name) {
        return context == null ? null : context.getCounter(group, name);
    }

    public boolean incrCounter(Enum<?> name, long incr) {
        return context == null ? false : context.incrCounter(name, incr);
    }

    public boolean incrCounter(String group, String name, long incr) {
        return context == null ? false : context.incrCounter(group, name, incr);
    }

    @Override
    public boolean incrWarnCounter(Enum<?> name, Object incr) {
        return incrCounter(name, (Long)incr);
    }

    @Override
    public boolean incrWarnCounter(String group, String name, Object incr) {
        return incrCounter(group, name, (Long)incr);
    }

    @Override
    public void progress() {
        if (context != null) {
            context.progress();
        }
    }

    @Override
    public void setStatus(String status) {
        if (context != null) {
            context.setStatus(status);
        }
    }

    public float getProgress() {
        return context == null ? 0f : context.getProgress();
    }
}
