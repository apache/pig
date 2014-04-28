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
package org.apache.pig.backend.hadoop.executionengine.shims;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.pig.backend.hadoop.executionengine.fetch.FetchContext;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.TezProcessorContext;

public class TaskContext<T> {
    private T context;

    public TaskContext(T context) {
        this.context = context;
    }

    public T get() {
        return context;
    }

    public Counter getCounter(Enum<?> name) {
        Counter counter = null;
        if (context == null) {
            return counter;
        }
        if (context instanceof TaskInputOutputContext) {
            TaskInputOutputContext<?,?,?,?> taskContext = (TaskInputOutputContext<?,?,?,?>) context;
            counter = taskContext.getCounter(name);
        } else if (context instanceof FetchContext) {
            FetchContext fetchContext = (FetchContext) context;
            counter = fetchContext.getCounter(name);
        }
        return counter;
    }

    public Counter getCounter(String group, String name) {
        Counter counter = null;
        if (context == null) {
            return counter;
        }
        if (context instanceof TaskInputOutputContext) {
            TaskInputOutputContext<?,?,?,?> taskContext = (TaskInputOutputContext<?,?,?,?>) context;
            counter = taskContext.getCounter(group, name);
        } else if (context instanceof FetchContext) {
            FetchContext fetchContext = (FetchContext) context;
            counter = fetchContext.getCounter(group, name);
        }
        return counter;
    }

    public boolean incrCounter(Enum<?> name, long delta) {
        if (context == null) {
            return false;
        }
        if (context instanceof TaskInputOutputContext) {
            TaskInputOutputContext<?,?,?,?> taskContext = (TaskInputOutputContext<?,?,?,?>) context;
            Counter counter = taskContext.getCounter(name);
            counter.increment(delta);
            return true;
        } else if (context instanceof FetchContext) {
            FetchContext fetchContext = (FetchContext) context;
            Counter counter = fetchContext.getCounter(name);
            counter.increment(delta);
            return true;
        } else if (context instanceof TezProcessorContext) {
            TezProcessorContext tezContext = (TezProcessorContext) context;
            TezCounter counter = tezContext.getCounters().findCounter(name);
            counter.increment(delta);
            return true;
        }
        return false;
    }

    public boolean incrCounter(String group, String name, long delta) {
        if (context == null) {
            return false;
        }
        if (context instanceof TaskInputOutputContext) {
            TaskInputOutputContext<?,?,?,?> taskContext = (TaskInputOutputContext<?,?,?,?>) context;
            Counter counter = taskContext.getCounter(group, name);
            counter.increment(delta);
            return true;
        } else if (context instanceof FetchContext) {
            FetchContext fetchContext = (FetchContext) context;
            Counter counter = fetchContext.getCounter(group, name);
            counter.increment(delta);
            return true;
        } else if (context instanceof TezProcessorContext) {
            TezProcessorContext tezContext = (TezProcessorContext) context;
            TezCounter counter = tezContext.getCounters().getGroup(group).findCounter(name);
            counter.increment(delta);
            return true;
        }
        return false;
    }

    public void progress() {
        if (context == null) {
            return;
        }
        if (context instanceof TaskInputOutputContext) {
            TaskInputOutputContext<?,?,?,?> taskContext = (TaskInputOutputContext<?,?,?,?>) context;
            taskContext.progress();
        }
    }

    public float getProgress() {
        if (context == null) {
            return 0f;
        }
        if (context instanceof TaskInputOutputContext) {
            TaskInputOutputContext<?,?,?,?> taskContext = (TaskInputOutputContext<?,?,?,?>) context;
            return taskContext.getProgress();
        }
        return 0f;
    }

    public void setStatus(String status) {
        if (context == null) {
            return;
        }
        if (context instanceof TaskInputOutputContext) {
            TaskInputOutputContext<?,?,?,?> taskContext = (TaskInputOutputContext<?,?,?,?>) context;
            taskContext.setStatus(status);
        }
    }
}
