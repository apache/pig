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
package org.apache.hadoop.owl.protocol;


/**
 * This class represents the base class for a REST message send over the network
 */
public class OwlRestMessage {

    /** The message header */
    private OwlRestHeader h;

    /** The message body */
    private OwlRestBody   b;

    public OwlRestMessage() {
        this.h = new OwlRestHeader();
        this.b = new OwlRestBody();
    }

    /**
     * Gets the header for the message.
     * 
     * @return the h
     */
    public OwlRestHeader getH() {
        return this.h;
    }

    /**
     * Sets the header for the message.
     * 
     * @param h
     *            the new header
     */
    public void setH(OwlRestHeader h) {
        this.h = h;
    }

    /**
     * Sets the body for the message.
     * 
     * @param b
     *            the new body
     */
    public void setB(OwlRestBody b) {
        this.b = b;
    }

    /**
     * Gets the body for the message.
     * 
     * @return the body
     */
    public OwlRestBody getB() {
        return b;
    }
}

// eof
