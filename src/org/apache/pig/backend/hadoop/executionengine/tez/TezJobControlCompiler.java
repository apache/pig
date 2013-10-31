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
package org.apache.pig.backend.hadoop.executionengine.tez;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.Utils;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.python.google.common.collect.Sets;

import com.google.common.collect.Maps;

/**
 * This is compiler class that takes a TezOperPlan and converts it into a
 * JobControl object with the relevant dependency info maintained. The
 * JobControl object is made up of TezJobs each of which has a JobConf.
 */
public class TezJobControlCompiler {
    private static final Log log = LogFactory.getLog(TezJobControlCompiler.class);
    private static final String DAG_JAR_NAME = "dag_job.jar";

    private PigContext pigContext;
    private TezClient tezClient;
    private TezConfiguration tezConf;

    public TezJobControlCompiler(PigContext pigContext, Configuration conf) throws IOException {
        this.pigContext = pigContext;
        this.tezConf = new TezConfiguration(conf);
        this.tezClient = new TezClient(tezConf);
    }

    public DAG buildDAG(TezOperPlan tezPlan, Map<String, LocalResource> localResources)
            throws IOException, YarnException {
        String jobName = pigContext.getProperties().getProperty(PigContext.JOB_NAME, DAG_JAR_NAME);
        DAG tezDag = new DAG(jobName);
        TezDagBuilder dagBuilder = new TezDagBuilder(pigContext, tezPlan, tezDag, localResources);
        dagBuilder.visit();
        return tezDag;
    }

    public JobControl compile(TezOperPlan tezPlan, String grpName) throws JobCreationException {
        int timeToSleep;
        String defaultPigJobControlSleep = pigContext.getExecType().isLocal() ? "100" : "5000";
        String pigJobControlSleep = tezConf.get("pig.jobcontrol.sleep", defaultPigJobControlSleep);
        if (!pigJobControlSleep.equals(defaultPigJobControlSleep)) {
            log.info("overriding default JobControl sleep (" +
                    defaultPigJobControlSleep + ") to " + pigJobControlSleep);
        }

        try {
            timeToSleep = Integer.parseInt(pigJobControlSleep);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid configuration " +
                    "pig.jobcontrol.sleep=" + pigJobControlSleep +
                    " should be a time in ms. default=" + defaultPigJobControlSleep, e);
        }

        JobControl jobCtrl = HadoopShims.newJobControl(grpName, timeToSleep);

        try {
            // TODO: for now, we assume that the whole Tez plan can be always
            // packaged into a single Tez job. But that may be not always true.
            tezConf.set(TezConfiguration.TEZ_AM_STAGING_DIR,
                    TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT);
            TezJob job = getJob(tezPlan);
            jobCtrl.addJob(job);
        } catch (JobCreationException jce) {
            throw jce;
        } catch(Exception e) {
            int errCode = 2017;
            String msg = "Internal error creating job configuration.";
            throw new JobCreationException(msg, errCode, PigException.BUG, e);
        }

        return jobCtrl;
    }

    private TezJob getJob(TezOperPlan tezPlan) throws JobCreationException {
        try {
            ApplicationId appId = tezClient.createApplication();
            Map<String, LocalResource> localResources = Maps.newHashMap();
            FileSystem remoteFs = FileSystem.get(tezConf);
            Path remoteStagingDir = remoteFs.makeQualified(new Path(
                    tezConf.get(TezConfiguration.TEZ_AM_STAGING_DIR), appId.toString()));

            for (URL extraJar : pigContext.extraJars) {
                Path pathInHDFS = Utils.shipToHDFS(pigContext, tezConf, extraJar);
                FileStatus fstat = remoteFs.getFileStatus(pathInHDFS);
                LocalResource extraJarRsrc = LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromPath(fstat.getPath()),
                        LocalResourceType.FILE,
                        LocalResourceVisibility.APPLICATION,
                        fstat.getLen(),
                        fstat.getModificationTime());
                localResources.put(pathInHDFS.getName(), extraJarRsrc);
                pigContext.skipJars.add(extraJar.getPath());
            }

            // Collect all the UDFs registered in tezPlan
            Set<String> udfs = Sets.newHashSet();
            Iterator<TezOperator> it = tezPlan.iterator();
            while (it.hasNext()) {
                udfs.addAll(it.next().UDFs);
            }

            // Create the jar of all functions and classes required
            File jobJar = File.createTempFile("Job", ".jar");
            jobJar.deleteOnExit();
            FileOutputStream fos = new FileOutputStream(jobJar);
            try {
                JarManager.createJar(fos, udfs, pigContext);
            } catch (ClassNotFoundException e) {
                throw new JobCreationException("UDF is not found in classpath: ", e);
            }

            // Ship the job jar to the staging directory on hdfs
            Path remoteJarPath = remoteFs.makeQualified(new Path(remoteStagingDir, DAG_JAR_NAME));
            remoteFs.copyFromLocalFile(new Path(jobJar.getAbsolutePath()), remoteJarPath);
            FileStatus jarFileStatus = remoteFs.getFileStatus(remoteJarPath);

            LocalResource dagJarLocalRsrc = LocalResource.newInstance(
                    ConverterUtils.getYarnUrlFromPath(remoteJarPath),
                    LocalResourceType.FILE,
                    LocalResourceVisibility.APPLICATION,
                    jarFileStatus.getLen(),
                    jarFileStatus.getModificationTime());
            localResources.put(DAG_JAR_NAME, dagJarLocalRsrc);

            DAG tezDag = buildDAG(tezPlan, localResources);
            return new TezJob(tezConf, appId, tezDag, localResources);
        } catch (Exception e) {
            int errCode = 2017;
            String msg = "Internal error creating job configuration.";
            throw new JobCreationException(msg, errCode, PigException.BUG, e);
        }
    }
}

