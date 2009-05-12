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
 * Performance timers.
 *
 * $Header:$
 */

package org.apache.pig.tools.timer;

import java.io.PrintStream;
import java.lang.String;

public class PerformanceTimer
{

/**
 * Start the timer.
 */
public void start()
{
    mStartedAt = System.nanoTime();
    mStarts++;
    mState = State.RUNNING;
}

/**
 * Stop the timer.
 */
public void stop()
{
    mState = State.STOPPED;
    mNanosecs += System.nanoTime() - mStartedAt;
}

/**
 * Dump the total time, total number of starts and stops, and average run time of the
 * timer to an output stream.
 * @param out output stream to write info to.
 */
public void print(PrintStream out)
{
    if (mStarts == 0) {
        out.println(mName + " never started.");
        return;
    }
    
    if (mState == State.RUNNING) out.print("WARNING:  timer still running!  ");
    out.print(mName + ": ");
    double t = mNanosecs / 1000000000.0;
    out.print(t);
    out.print(".  Run ");
    out.print(mStarts);
    out.print(" times, average run time ");
    long avg = mNanosecs / mStarts;
    t = avg / 1000000000.0;
    out.print(t);
    out.println(".");
}

/**
 * @param name Name of this timer.
 */
PerformanceTimer(String name)
{
    mNanosecs = 0;
    mStarts = 0;
    mName = name;
    mState = State.STOPPED;
}


private enum State { RUNNING, STOPPED };

private State mState;
private long mNanosecs;
private long mStartedAt;
private int mStarts;
private String mName;

}
