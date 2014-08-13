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
package org.apache.pig;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStatsUtil;

/**
 * A utility to help run PIG scripts within a Java program.
 */

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class PigRunner {

    // return codes
    public static abstract class ReturnCode {
        public final static int UNKNOWN = -1;
        public final static int SUCCESS = 0;      
        public final static int RETRIABLE_EXCEPTION = 1;
        public final static int FAILURE = 2;         
        public final static int PARTIAL_FAILURE = 3; 
        public final static int ILLEGAL_ARGS = 4;
        public final static int IO_EXCEPTION = 5;
        public final static int PIG_EXCEPTION = 6;
        public final static int PARSE_EXCEPTION = 7;
        public final static int THROWABLE_EXCEPTION = 8;
    }
    
    public static PigStats run(String[] args, PigProgressNotificationListener listener) {
        return PigStatsUtil.getPigStats(Main.run(args, listener));
    }
    
}
