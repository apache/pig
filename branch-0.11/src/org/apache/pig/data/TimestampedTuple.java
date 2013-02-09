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
package org.apache.pig.data;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TimestampedTuple extends DefaultTuple {

    /**
     * 
     */
    private static final long serialVersionUID = 2L;
    private static final Log log = LogFactory.getLog(TimestampedTuple.class);
    static String defaultDelimiter = "[,\t]";

    protected double timestamp = 0;      // timestamp of this tuple
    protected boolean heartbeat = false;  // true iff this is a heartbeat (i.e. purpose is just to convey new timestamp; carries no data)
    
    public double getTimeStamp() {
        return timestamp;
    }
    public void setTimeStamp(double t) {
        this.timestamp = t;
    }
    public boolean isHeartBeat() {
        return heartbeat;
    }
    public void setHeartBeat(boolean h) {
        this.heartbeat = h;
    }
    public TimestampedTuple(int numFields) {
        super(numFields);
    }
    
    public TimestampedTuple(String textLine, String delimiter, int timestampColumn, 
                            SimpleDateFormat dateFormat){
        if (delimiter == null) {
            delimiter = defaultDelimiter;
        }
        String[] splitString = textLine.split(delimiter, -1);
        mFields = new ArrayList<Object>(splitString.length-1);
        for (int i = 0; i < splitString.length; i++) {
            if (i==timestampColumn){
                try{
                    timestamp = dateFormat.parse(splitString[i]).getTime()/1000.0;
                }catch(ParseException e){
                    log.error("Could not parse timestamp " + splitString[i]);
                }
            }else{
                mFields.add(splitString[i]);
            }
        }
    }
}
