/********************************************************************************
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
 * 
 * Performance timer factory.
 *
 * $Header:$
 */


package org.apache.pig.tools.timer;

import java.io.PrintStream;
import java.lang.String;
import java.util.HashMap;
import java.util.Collection;
import java.util.Iterator;

import org.apache.pig.tools.timer.PerformanceTimer;


public class PerformanceTimerFactory
{

/**
 * Get the timer factory.
 * @return timer factory. 
 */
public static PerformanceTimerFactory getPerfTimerFactory()
{
    if (self == null) {
        self = new PerformanceTimerFactory();
    }
    return self;
}

/**
 * Get a performance timer.  If the indicated timer does not exist, it will be
 * created.  If a timer of that name already exists, it will be returned.
 * @param name Name of the timer to return.
 * @return the timer.
 */
public PerformanceTimer getTimer(String name)
{
    PerformanceTimer timer = mTimers.get(name);
    if (timer == null) {
        timer = new PerformanceTimer(name);
        mTimers.put(name, timer);
    } 
    return timer;
}

/**
 * Call print on all of the known performance timers.
 * @param out output stream to dump to
 */
public void dumpTimers(PrintStream out)
{
    Collection<PerformanceTimer> c = mTimers.values();
    Iterator<PerformanceTimer> i = c.iterator();
    while (i.hasNext()) {
        i.next().print(out);
    }
}

public void dumpTimers()
{
    dumpTimers(System.out);
}

private static PerformanceTimerFactory self = null;

private PerformanceTimerFactory()
{
    mTimers = new HashMap<String, PerformanceTimer>();
}

HashMap<String, PerformanceTimer> mTimers;

}
