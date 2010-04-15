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
package org.apache.hadoop.owl.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.mapreduce.OwlInputFormat.OwlOperation;

/** The Class which handles checking for features supported by the underlying storage drivers for
 *  the particular Owl query (only partitions matching the filter are checked for).
 */
class OwlFeatureSupport {

    /**
     * Gets the list of features supported for the given partitions. If any one of the underlying InputFormat's does
     * not support the feature and it cannot be implemented by Owl, then the feature is not returned.
     * @param inputInfo  the owl table input info
     * @return the storage features supported for the partitions selected by the setInput call 
     */
    public static List<OwlOperation> getSupportedFeatures(OwlJobInfo owlJobInfo) throws OwlException {

        List<OwlOperation> owlOperationList = new ArrayList<OwlOperation>();

        List<OwlInputStorageDriver> owlInputStorageDriverList = getUniqueStorageDriver(owlJobInfo/*inputInfo*/);

        // For each OwlOperation, check if the operation is supported for all of the OwlInputStorageDrivers in the above set.
        boolean isSupported;
        for (OwlOperation op: OwlOperation.values()){
            isSupported = true;
            for( OwlInputStorageDriver owlInputStorageDriver : owlInputStorageDriverList){
                try {
                    if (! owlInputStorageDriver.isFeatureSupported(op)) {
                        isSupported = false;
                        break;
                    }
                } catch (IOException e) {
                    throw new OwlException(ErrorType.ERROR_STORAGE_DRIVER_EXCEPTION,e);
                }
            }
            if (isSupported == true)  owlOperationList.add(op);
        }
        return owlOperationList;
    }

    /**
     * Checks if the specified operation is supported for the given partitions. If any one of the underlying InputFormat's does
     * not support the operation and it cannot be implemented by Owl, then returns false. Else returns true.
     * @param inputInfo the owl table input info
     * @param operation the operation to check for
     * @return true, if the feature is supported for selected partitions
     */
    public static boolean isFeatureSupported(OwlJobInfo owlJobInfo,
            OwlOperation operation) throws OwlException {

        List<OwlInputStorageDriver> owlInputStorageDriverList = getUniqueStorageDriver(owlJobInfo);

        // For OwlOperation, check if the operation is supported for all of the OwlInputStorageDrivers in the above set.
        for( OwlInputStorageDriver owlInputStorageDriver : owlInputStorageDriverList){
            try {
                if (! owlInputStorageDriver.isFeatureSupported(operation)) {
                    return false;
                }
            } catch (IOException e) {
                throw new OwlException(ErrorType.ERROR_STORAGE_DRIVER_EXCEPTION,e);
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private static List<OwlInputStorageDriver> getUniqueStorageDriver(OwlJobInfo owlJobInfo)
    throws OwlException {
        if (owlJobInfo == null){
            throw new OwlException(ErrorType.ERROR_INPUT_UNINITIALIZED);
        }
        List<OwlPartitionInfo> owlPartitionInfoList = owlJobInfo.getPartitions();
        List<Class<? extends OwlInputStorageDriver>> owlInputStorageDriverClassObjectList 
        = new ArrayList<Class<? extends OwlInputStorageDriver>>();

        for (OwlPartitionInfo owlPartitionInfo:owlPartitionInfoList ){
            String owlInputDriverClassName = owlPartitionInfo.getLoaderInfo().getInputDriverClass();
            Class<? extends OwlInputStorageDriver> owlInputStorageDriverClass;
            try {
                owlInputStorageDriverClass = (Class<? extends OwlInputStorageDriver>)Class.forName(owlInputDriverClassName);
            } catch (ClassNotFoundException e) {
                throw new OwlException(ErrorType.ERROR_CREATE_STORAGE_DRIVER, owlInputDriverClassName, e);
            }
            // create an unique list of OwlInputDriver class objects
            if (! owlInputStorageDriverClassObjectList.contains(owlInputStorageDriverClass)){
                owlInputStorageDriverClassObjectList.add(owlInputStorageDriverClass);
            }
        }
        // create a list instance out of owlInputStorageDriverObjectList
        List<OwlInputStorageDriver> owlInputStorageDriverList = new ArrayList<OwlInputStorageDriver>();
        for (Class<? extends OwlInputStorageDriver> owlInputStorageDriverClassObject : owlInputStorageDriverClassObjectList){
            try {
                OwlInputStorageDriver owlInputStorageDriver = owlInputStorageDriverClassObject.newInstance();
                owlInputStorageDriverList.add(owlInputStorageDriver);
            } catch (Exception e) {
                throw new OwlException (ErrorType.ERROR_CREATE_STORAGE_DRIVER_INSTANCE, owlInputStorageDriverClassObject.getName(), e);
            }
        }
        return owlInputStorageDriverList;
    }

    /**
     * Set the predicate filter for pushdown to the storage driver.
     * @param job the job object
     * @param predicate the predicate filter, an arbitrary AND/OR filter
     * @return true, if the specified predicate can be filtered for the given partitions
     * @throws IOException the exception
     */
    public static boolean setPredicate(Job job, String predicate) throws IOException {
        String jobString = job.getConfiguration().get(OwlInputFormat.OWL_KEY_JOB_INFO);
        if( jobString == null ) {
            throw new OwlException(ErrorType.ERROR_INPUT_UNINITIALIZED);
        }

        OwlJobInfo owlJobInfo = (OwlJobInfo) SerializeUtil.deserialize(jobString);

        // First check if storage driver supports predicate pushdown, if not, throw OwlException
        if (!isFeatureSupported(/*change to OwlJobInfo*/owlJobInfo, OwlOperation.PREDICATE_PUSHDOWN)){
            throw new OwlException(ErrorType.ERROR_STORAGE_DRIVER_EXCEPTION, "Storage driver doesn't support predicate pushdown.");
        }
        // for each storage driver, check for setPredicate() if any of them is false, return false
        List<OwlInputStorageDriver> owlInputStorageDriverList = getUniqueStorageDriver(owlJobInfo);
        for (OwlInputStorageDriver owlInputStorageDriver : owlInputStorageDriverList){
            // for each storage driver, we need to create a copy of jobcontext to avoid overwriting of the jobcontext by storage driver.
            Job localJob = new Job(job.getConfiguration());
            if (! owlInputStorageDriver.setPredicate(localJob, predicate) ) return false;
        }
        // after making sure every storage driver supports the predicate, finally set predicate into jobcontext
        job.getConfiguration().set(OwlInputFormat.OWL_KEY_JOB_INFO, predicate);
        return true;
    }

}
