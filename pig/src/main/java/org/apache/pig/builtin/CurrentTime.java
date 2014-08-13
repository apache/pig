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
package org.apache.pig.builtin;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.joda.time.DateTime;

public class CurrentTime extends EvalFunc<DateTime> {
    private DateTime dateTime;
    private boolean isInitialized = false;

    /**
     * This is a default constructor for Pig reflection purposes. It should
     * never actually be used.
     */
    public CurrentTime() {}

    @Override
    public DateTime exec(Tuple input) throws IOException {
        if (!isInitialized) {
            String dateTimeValue = UDFContext.getUDFContext().getJobConf().get("pig.job.submitted.timestamp");
            if (dateTimeValue == null) {
                throw new ExecException("pig.job.submitted.timestamp was not set!");
            }
            dateTime = new DateTime(Long.parseLong(dateTimeValue));
            isInitialized  = true;
        }
        return dateTime;
    }
}
