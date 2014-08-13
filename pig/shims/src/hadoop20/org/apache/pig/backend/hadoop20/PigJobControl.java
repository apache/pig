/**
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
package org.apache.pig.backend.hadoop20;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.ExecType;
import org.apache.pig.impl.PigContext;

/**
 * extends the hadoop JobControl to remove the hardcoded sleep(5000)
 * as most of this is private we have to use reflection
 * 
 * See {@link https://svn.apache.org/repos/asf/hadoop/common/branches/branch-0.20/src/mapred/org/apache/hadoop/mapred/jobcontrol/JobControl.java}
 *
 */
public class PigJobControl extends JobControl {
  private static final Log log = LogFactory.getLog(PigJobControl.class);

  private static Field runnerState;

  private static Method checkRunningJobs;
  private static Method checkWaitingJobs;
  private static Method startReadyJobs;

  private static boolean initSuccesful;

  static {
    try {

      runnerState = JobControl.class.getDeclaredField("runnerState");
      runnerState.setAccessible(true);

      checkRunningJobs = JobControl.class.getDeclaredMethod("checkRunningJobs");
      checkRunningJobs.setAccessible(true);
      checkWaitingJobs = JobControl.class.getDeclaredMethod("checkWaitingJobs");
      checkWaitingJobs.setAccessible(true);
      startReadyJobs = JobControl.class.getDeclaredMethod("startReadyJobs");
      startReadyJobs.setAccessible(true);
      initSuccesful = true;
    } catch (Exception e) {
      log.warn("falling back to default JobControl (not using hadoop 0.20 ?)", e);
      initSuccesful = false;
    }
  }

  // The thread can be in one of the following state
  private static final int RUNNING = 0;
  private static final int SUSPENDED = 1;
  private static final int STOPPED = 2;
  private static final int STOPPING = 3;
  private static final int READY = 4;

  private int timeToSleep;

  /**
   * Construct a job control for a group of jobs.
   * @param groupName a name identifying this group
   * @param pigContext
   * @param conf
   */
  public PigJobControl(String groupName, int timeToSleep) {
    super(groupName);
    this.timeToSleep = timeToSleep;
  }

  public int getTimeToSleep() {
    return timeToSleep;
  }

  public void setTimeToSleep(int timeToSleep) {
    this.timeToSleep = timeToSleep;
  }

  private void setRunnerState(int state) {
    try {
      runnerState.set(this, state);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  private int getRunnerState() {
    try {
      return (Integer)runnerState.get(this);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   *  The main loop for the thread.
   *  The loop does the following:
   *    Check the states of the running jobs
   *    Update the states of waiting jobs
   *    Submit the jobs in ready state
   */
  public void run() {
    if (!initSuccesful) {
      super.run();
      return;
    }
    setRunnerState(PigJobControl.RUNNING);
    while (true) {
      while (getRunnerState() == PigJobControl.SUSPENDED) {
        try {
          Thread.sleep(timeToSleep);
        }
        catch (Exception e) {

        }
      }
      mainLoopAction();
      if (getRunnerState() != PigJobControl.RUNNING &&
          getRunnerState() != PigJobControl.SUSPENDED) {
        break;
      }
      try {
        Thread.sleep(timeToSleep);
      }
      catch (Exception e) {

      }
      if (getRunnerState() != PigJobControl.RUNNING &&
          getRunnerState() != PigJobControl.SUSPENDED) {
        break;
      }
    }
    setRunnerState(PigJobControl.STOPPED);
  }

  private void mainLoopAction() {
    try {
      checkRunningJobs.invoke(this);
      checkWaitingJobs.invoke(this);
      startReadyJobs.invoke(this);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
