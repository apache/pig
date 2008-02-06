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
package org.apache.pig.impl.eval.window;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.pig.data.Datum;
import org.apache.pig.data.TimestampedTuple;
import org.apache.pig.impl.eval.collector.DataCollector;
import org.apache.pig.impl.eval.EvalSpecVisitor;



public class TimeWindowSpec extends WindowSpec {
    private static final long serialVersionUID = 1L;
    double duration;  // duration in seconds
    transient List<TimestampedTuple> window;
        
    public TimeWindowSpec(windowType type, double duration){
        super(type);
        this.duration = duration;
        window = new LinkedList<TimestampedTuple>();
    }
    
    @Override
    protected DataCollector setupDefaultPipe(DataCollector endOfPipe) {
        return new DataCollector(endOfPipe) {

            @Override
            public void add(Datum d){
                
                boolean changed = false;
                
                if (!(((TimestampedTuple) d).isHeartBeat())) {
                    window.add((TimestampedTuple) d);
                    changed = true;
                }

                double expireTime = ((TimestampedTuple) d).getTimeStamp() - duration;
                while (true) {
                    TimestampedTuple tail = window.get(0);
                    
                    if (tail != null & tail.getTimeStamp()<= expireTime) {
                        window.remove(0);
                        changed = true;
                    } else {
                        break;
                    }
                }
                
                if (changed) {
                    // emit entire window content to output collector
                    for (Iterator<TimestampedTuple> it = window.iterator(); it.hasNext(); ) {
                        successor.add(it.next());
                    }
                }
            }
            
            
        };
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[WINDOW ");
        sb.append(type);
        sb.append(" TIME ");
        sb.append(duration);
        sb.append("]");
        return sb.toString();
    }

    public void visit(EvalSpecVisitor v) {
        v.visitTimeWindow(this);
    }
}
