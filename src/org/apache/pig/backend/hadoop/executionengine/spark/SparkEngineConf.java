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
package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * The object of SparkEngineConf is to solve the initialization problem of PigContext.properties.get("udf.import.list"),
 * UDFContext#udfConfs, UDFContext#clientSysProps in spark mode. These variables can not be
 * serialized because they are ThreadLocal variables. In MR mode, they are serialized in JobConfiguration
 * in JobControlCompiler#getJob and deserialized by JobConfiguration in PigGenericMapBase#setup. But there is no
 * setup() in spark like what in mr, so these variables can be correctly deserialized before spark programs call them.
 * Here we use following solution to solve:
 * these variables are saved in SparkEngineConf#writeObject and available and then initialized
 * in SparkEngineConf#readObject in spark executor thread.
 */
public class SparkEngineConf implements Serializable {

    private static final Log LOG = LogFactory.getLog(SparkEngineConf.class);
    private static String SPARK_UDF_IMPORT_LIST = "pig.spark.udf.import.list";
    private static String SPARK_UDFCONTEXT_UDFCONFS = "pig.spark.udfcontext.udfConfs";
    private static String SPARK_UDFCONTEXT_CLIENTSYSPROPS = "pig.spark.udfcontext.clientSysProps";

    private Properties properties = new Properties();

    public SparkEngineConf() {
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        ArrayList<String> udfImportList = (ArrayList<String>) in.readObject();
        PigContext.setPackageImportList(udfImportList);
        String udfConfsStr = (String) in.readObject();
        String clientSysPropsStr = (String) in.readObject();
        UDFContext.getUDFContext().deserializeForSpark(udfConfsStr, clientSysPropsStr);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        ArrayList<String> udfImportList = Lists.newArrayList(Splitter.on(",").split(properties.getProperty(SPARK_UDF_IMPORT_LIST)));
        out.writeObject(udfImportList);
        //2 threads call SparkEngineConf#writeObject
        //In main thread: SparkLauncher#initialize->SparkUtil#newJobConf
        //                ->ObjectSerializer#serialize-> SparkEngineConf#writeObject
        //In dag-scheduler-event-loop thread: DAGScheduler.submitMissingTasks->JavaSerializationStream.writeObject
        //
        //In main thread,UDFContext#getUDFContext is not empty, we store UDFContext#udfConfs and UDFContext#clientSysProps
        //into properties and serialize them.
        //In dag-scheduler-event-loop thread, UDFContext#getUDFContext is empty, we get value of UDFContext#udfConfs and UDFContext#clientSysProps
        //from properties and serialize them.
        if (!UDFContext.getUDFContext().isUDFConfEmpty()) {
            //In SparkUtil#newJobConf(), sparkEngineConf is serialized in job configuration and will call
            //SparkEngineConf#writeObject(at this time UDFContext#udfConfs and UDFContext#clientSysProps is not null)
            //later spark will call JavaSerializationStream.writeObject to serialize all objects when submit spark
            //jobs(at that time, UDFContext#udfConfs and UDFContext#clientSysProps is null so we need to save their
            //value in SparkEngineConf#properties after these two variables are correctly initialized in
            //SparkUtil#newJobConf, More detailed see PIG-4920
            String udfConfsStr = UDFContext.getUDFContext().serialize();
            String clientSysPropsStr = ObjectSerializer.serialize(UDFContext.getUDFContext().getClientSystemProps());
            this.properties.setProperty(SPARK_UDFCONTEXT_UDFCONFS, udfConfsStr);
            this.properties.setProperty(SPARK_UDFCONTEXT_CLIENTSYSPROPS, clientSysPropsStr);
            out.writeObject(udfConfsStr);
            out.writeObject(clientSysPropsStr);
        } else {
            out.writeObject(this.properties.getProperty(SPARK_UDFCONTEXT_UDFCONFS));
            out.writeObject(this.properties.getProperty(SPARK_UDFCONTEXT_CLIENTSYSPROPS));
        }
    }

    public void setSparkUdfImportListStr(String sparkUdfImportListStr) {
        this.properties.setProperty(SPARK_UDF_IMPORT_LIST, sparkUdfImportListStr);
    }
}
