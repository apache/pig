/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.owl.protocol;

/**
 * This class represents the body of a REST message (see OwlRestMessage)
 */
public class OwlRestBody {

    /** The Owl command to be executed */
    private String command;

    /** The results of the command execution */
    private OwlObject[] results;


    /**
     * Instantiates a new owl rest body.
     */
    public OwlRestBody() {
    }

    /**
     * Gets the value of command
     * @return the command
     */
    public String getCommand() {
        return command;
    }

    /**
     * Gets the value of results
     * @return the results
     */
    public OwlObject[] getResults() {
        return results;
    }

    /**
     * Sets the value of command
     * @param command the command to set
     */
    public void setCommand(String command) {
        this.command = command;
    }

    /**
     * Sets the value of results
     * @param results the results to set
     */
    public void setResults(OwlObject[] results) {
        this.results = results;
    }
}
