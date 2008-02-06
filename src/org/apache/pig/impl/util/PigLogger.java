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
package org.apache.pig.impl.util;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;

public class PigLogger 
{

private static Logger mLogger = null;
private static boolean mHaveSetAppenders = false;

/**
 * Get an instance of the underlying log4j logger.  This first makes sure
 * the PigLogger is initialized and then returns the underlying logger.
 */ 
public static Logger getLogger()
{
    if (mLogger == null) {
        mLogger = Logger.getLogger("org.apache.pig");
    }
    return mLogger;
}

/**
 * Set up a log appender for the junit tests, this way they cn write out log
 * messages.
 */
public static void setAppenderForJunit()
{
    if (!mHaveSetAppenders) {
        Logger log = getLogger();
        log.setLevel(Level.INFO);
        ConsoleAppender screen = new ConsoleAppender(new PatternLayout());
        screen.setThreshold(Level.INFO);
        screen.setTarget(ConsoleAppender.SYSTEM_ERR);
        log.addAppender(screen);
        mHaveSetAppenders = true;
    }
}


}
