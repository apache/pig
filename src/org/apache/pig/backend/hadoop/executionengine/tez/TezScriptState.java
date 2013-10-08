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

import org.apache.pig.tools.pigstats.ScriptState;

/**
 * ScriptStates encapsulates settings for a Pig script that runs on a hadoop
 * cluster. These settings are added to all Tez jobs spawned by the script and
 * in turn are persisted in the hadoop job xml. With the properties already in
 * the job xml, users who want to know the relations between the script and Tez
 * jobs can derive them from the job xmls.
 */
public class TezScriptState extends ScriptState {
    public TezScriptState(String id) {
        super(id);
        // TODO Auto-generated constructor stub
    }
}

