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
package org.apache.pig.backend.hadoop23;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob.State;

/**
 * extends the hadoop JobControl to remove the hardcoded sleep(5000)
 * as most of this is private we have to use reflection
 *
 * See {@link https://svn.apache.org/repos/asf/hadoop/common/branches/branch-0.23.1/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/lib/jobcontrol/JobControl.java }
 *
 */
public class PigJobControl extends JobControl {
  private static final Log log = LogFactory.getLog(PigJobControl.class);

  private static Field runnerState;
  private static Field jobsInProgress;
  private static Field successfulJobs;
  private static Field failedJobs;

  private static Method failAllJobs;

  private static Method checkState;
  private static Method submit;

  private static boolean initSuccesful;

  static {
    try {

      runnerState = org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl.class.getDeclaredField("runnerState");
      runnerState.setAccessible(true);
      jobsInProgress = org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl.class.getDeclaredField("jobsInProgress");
      jobsInProgress.setAccessible(true);
      successfulJobs = org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl.class.getDeclaredField("successfulJobs");
      successfulJobs.setAccessible(true);
      failedJobs = org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl.class.getDeclaredField("failedJobs");
      failedJobs.setAccessible(true);

      failAllJobs = org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl.class.getDeclaredMethod("failAllJobs", Throwable.class);
      failAllJobs.setAccessible(true);

      checkState = ControlledJob.class.getDeclaredMethod("checkState");
      checkState.setAccessible(true);
      submit = ControlledJob.class.getDeclaredMethod("submit");
      submit.setAccessible(true);

      initSuccesful = true;
    } catch (Exception e) {
      log.warn("falling back to default JobControl (not using hadoop 0.23 ?)", e);
      initSuccesful = false;
    }
  }

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

  private void setRunnerState(ThreadState state) {
    try {
      runnerState.set(this, state);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  private ThreadState getRunnerState() {
    try {
      return (ThreadState)runnerState.get(this);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private State checkState(ControlledJob j) {
    try {
      return (State)checkState.invoke(j);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private State submit(ControlledJob j) {
    try {
      return (State)submit.invoke(j);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private LinkedList<ControlledJob> getJobs(Field field) {
    try {
      return (LinkedList<ControlledJob>)field.get(this);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void failAllJobs(Throwable t) {
    try {
      failAllJobs.invoke(this, t);
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
    try {
      setRunnerState(ThreadState.RUNNING);
      while (true) {
        while (getRunnerState() == ThreadState.SUSPENDED) {
          try {
            Thread.sleep(timeToSleep);
          }
          catch (Exception e) {
            //TODO the thread was interrupted, do something!!!
          }
        }

        synchronized(this) {
          Iterator<ControlledJob> it = getJobs(jobsInProgress).iterator();
          while(it.hasNext()) {
            ControlledJob j = it.next();
            log.debug("Checking state of job "+j);
            switch(checkState(j)) {
            case SUCCESS:
              getJobs(successfulJobs).add(j);
              it.remove();
              break;
            case FAILED:
            case DEPENDENT_FAILED:
              getJobs(failedJobs).add(j);
              it.remove();
              break;
            case READY:
              submit(j);
              break;
            case RUNNING:
            case WAITING:
              //Do Nothing
              break;
            }
          }
        }

        if (getRunnerState() != ThreadState.RUNNING &&
            getRunnerState() != ThreadState.SUSPENDED) {
          break;
        }
        try {
          Thread.sleep(timeToSleep);
        }
        catch (Exception e) {
          //TODO the thread was interrupted, do something!!!
        }
        if (getRunnerState() != ThreadState.RUNNING &&
            getRunnerState() != ThreadState.SUSPENDED) {
          break;
        }
      }
    }catch(Throwable t) {
      log.error("Error while trying to run jobs.",t);
      //Mark all jobs as failed because we got something bad.
      failAllJobs(t);
    }
    setRunnerState(ThreadState.STOPPED);
  }


}
