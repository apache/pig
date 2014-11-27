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
package org.apache.hadoop.mapred;

import java.util.Iterator;

public class DowngradeHelper {
    // This is required since hadoop 2 TaskReport allows
    // only package level access to this api
    public static Iterator<TaskReport> downgradeTaskReports(
            org.apache.hadoop.mapreduce.TaskReport[] reports) {
        return reports == null ? null : new TaskReportIterator(reports);
    }

    private static class TaskReportIterator implements Iterator<TaskReport> {

        private org.apache.hadoop.mapreduce.TaskReport[] reports;
        private int curIndex = 0;

        public TaskReportIterator(org.apache.hadoop.mapreduce.TaskReport[] reports) {
            this.reports = reports;
        }

        @Override
        public boolean hasNext() {
            return curIndex < this.reports.length ;
        }

        @Override
        public TaskReport next() {
            return TaskReport.downgrade(reports[curIndex++]);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }
}
