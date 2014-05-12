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
package org.apache.pig.backend.hadoop.executionengine.tez;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.pig.backend.hadoop.executionengine.TaskContext;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.mapreduce.hadoop.mapred.MRCounters.MRCounter;
import org.apache.tez.runtime.api.TezProcessorContext;

public class TezTaskContext extends TaskContext<TezProcessorContext> {
    private TezProcessorContext context;

    public TezTaskContext(TezProcessorContext context) {
        this.context = context;
    }

    @Override
    public TezProcessorContext get() {
        return context;
    }

    @Override
    public Counter getCounter(Enum<?> name) {
        if (context == null) {
            return null;
        }
        TezCounter tezCounter = context.getCounters().findCounter(name);
        return new MRCounter(tezCounter);
    }

    @Override
    public Counter getCounter(String group, String name) {
        if (context == null) {
            return null;
        }
        TezCounter tezCounter = context.getCounters().getGroup(group).findCounter(name);
        return new MRCounter(tezCounter);
    }

    @Override
    public boolean incrCounter(Enum<?> name, long delta) {
        if (context == null) {
            return false;
        }
        TezCounter counter = context.getCounters().findCounter(name);
        counter.increment(delta);
        return true;
    }

    @Override
    public boolean incrCounter(String group, String name, long delta) {
        if (context == null) {
            return false;
        }
        TezCounter counter = context.getCounters().getGroup(group).findCounter(name);
        counter.increment(delta);
        return true;
    }
}
