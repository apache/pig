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

package org.apache.pig.builtin;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreMetadata;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.backend.hadoop.datastorage.HDirectory;
import org.apache.pig.backend.hadoop.datastorage.HFile;
import org.apache.pig.backend.hadoop.datastorage.HPath;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.util.LRUMap;

/**
 * Reads and Writes metadata using JSON in metafiles next to the data.
 *
 *
 */
public class JsonMetadata implements LoadMetadata, StoreMetadata {

    private static final Log log = LogFactory.getLog(JsonMetadata.class);

    private final String schemaFileName;
    private final String headerFileName;
    private final String statFileName;

    private boolean printHeaders = true;

    private byte fieldDel;
    private byte recordDel;

    private transient LRUMap<ElementDescriptor, Boolean> lookupCache = new LRUMap<ElementDescriptor, Boolean>(100, 1000);

    public JsonMetadata() {
        this(".pig_schema", ".pig_header", ".pig_stats");
    }

    public JsonMetadata(String schemaFileName, String headerFileName, String statFileName) {
        this.schemaFileName = schemaFileName;
        this.headerFileName = headerFileName;
        this.statFileName = statFileName;
    }

    /**.
     * Given a path, which may represent a glob pattern, a directory,
     * comma separated files/glob patterns or a file, this method
     * finds the set of relevant metadata files on the storage system.
     * The algorithm for finding the metadata file is as follows:
     * <p>
     * For each object represented by the path (either directly, or via a glob):
     *   If object is a directory, and path/metaname exists, use that as the metadata file.
     *   Else if parentPath/metaname exists, use that as the metadata file.
     * <p>
     * Resolving conflicts, merging the metadata, etc, is not handled by this method and should be
     * taken care of by downstream code.
     * <p>
     * @param path      Path, as passed in to a LoadFunc (may be a Hadoop glob)
     * @param metaname    Metadata file designation, such as .pig_schema or .pig_stats
     * @param conf      configuration object
     * @return Set of element descriptors for all metadata files associated with the files on the path.
     */
    protected Set<ElementDescriptor> findMetaFile(String path, String metaname, Configuration conf)
        throws IOException {
        Set<ElementDescriptor> metaFileSet = new HashSet<ElementDescriptor>();
        String[] locations = LoadFunc.getPathStrings(path);
        for (String loc : locations) {
            DataStorage storage;

            storage = new HDataStorage(new Path(loc).toUri(), ConfigurationUtil.toProperties(conf));

            String fullPath = FileLocalizer.fullPath(loc, storage);

            if(storage.isContainer(fullPath)) {
                ElementDescriptor metaFilePath = storage.asElement(fullPath, metaname);
                if (exists(metaFilePath)) {
                    metaFileSet.add(metaFilePath);
                }
            } else {
                ElementDescriptor[] descriptors = storage.asCollection(loc);
                for(ElementDescriptor descriptor : descriptors) {
                    ContainerDescriptor container = null;

                    if (descriptor instanceof HFile) {
                        Path descriptorPath = ((HPath) descriptor).getPath();
                        String fileName = descriptorPath.getName();
                        Path parent = descriptorPath.getParent();
                        String parentName = parent.toString();
                        container = new HDirectory((HDataStorage)storage,parent);
                    } else { // descriptor instanceof HDirectory
                        container = (HDirectory)descriptor;
                    }

                    // if no custom schema, try the parent directory
                    ElementDescriptor metaFilePath = storage.asElement(container, metaname);
                    if (exists(metaFilePath)) {
                        metaFileSet.add(metaFilePath);
                    }
                }
            }
        }
        return metaFileSet;
    }

    private boolean exists(ElementDescriptor e) throws IOException {
        if (lookupCache.containsKey(e)) {
            return lookupCache.get(e);
        } else {
         boolean res = e.exists();
         lookupCache.put(e, res);
         return res;
        }
    }

    //------------------------------------------------------------------------
    // Implementation of LoadMetaData interface

    @Override
    public String[] getPartitionKeys(String location, Job job) {
        return null;
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter)
            throws IOException {
    }


    /**
     * For JsonMetadata schema is considered optional
     * This method suppresses (and logs) errors if they are encountered.
     *
     */
    @Override
    public ResourceSchema getSchema(String location, Job job) throws IOException {
        return getSchema(location, job, false);
    }

    /**
     * Read the schema from json metadata file
     * If isSchemaOn parameter is false, the errors are suppressed and logged
     * @param location
     * @param job
     * @param isSchemaOn
     * @return schema
     * @throws IOException
     */
    public ResourceSchema getSchema(String location, Job job, boolean isSchemaOn) throws IOException {
        Configuration conf = job.getConfiguration();
        Set<ElementDescriptor> schemaFileSet = null;
        try {
            schemaFileSet = findMetaFile(location, schemaFileName, conf);
        } catch (IOException e) {
            String msg = "Could not find schema file for "+ location;
            return nullOrException(isSchemaOn, msg, e);
        }

        // TODO we assume that all schemas are the same. The question of merging schemas is left open for now.
        ElementDescriptor schemaFile = null;
        if (!schemaFileSet.isEmpty()) {
            schemaFile = schemaFileSet.iterator().next();
        } else {
            String msg = "Could not find schema file for "+location;
            return nullOrException(isSchemaOn, msg, null);
        }
        log.debug("Found schema file: "+schemaFile.toString());
        ResourceSchema resourceSchema = null;
        try {
            resourceSchema = new ObjectMapper().readValue(schemaFile.open(), ResourceSchema.class);
        } catch (JsonParseException e) {
            String msg = "Unable to load Resource Schema for "+location;
            return nullOrException(isSchemaOn, msg, e);
        } catch (JsonMappingException e) {
            String msg = "Unable to load Resource Schema for "+location;
            return nullOrException(isSchemaOn, msg, e);
        } catch (IOException e) {
            String msg = "Unable to load Resource Schema for "+location;
            return nullOrException(isSchemaOn, msg, e);
        }
        return resourceSchema;
    }

    private ResourceSchema nullOrException(boolean isSchemaOn, String msg,
            IOException e) throws FrontendException {
        if(isSchemaOn){
            throw  new FrontendException(msg, 1131, PigException.INPUT, e);
        }
        //a valid schema file was probably not expected, so just log a
        //debug message and return null
        log.debug(msg);
        return null;
    }

    /**
     * For JsonMetadata stats are considered optional
     * This method suppresses (and logs) errors if they are encountered.
     * @see org.apache.pig.LoadMetadata#getStatistics(String, Job)
     */
    @Override
    public ResourceStatistics getStatistics(String location, Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        Set<ElementDescriptor> statFileSet = null;
        try {
            statFileSet = findMetaFile(location, statFileName, conf);
        } catch (IOException e) {
            log.warn("could not fine stat file for " + location);
            return null;
        }
        ElementDescriptor statFile = null;
        if (!statFileSet.isEmpty()) {
            statFile = statFileSet.iterator().next();
        } else {
            log.warn("Could not find stat file for "+location);
            return null;
        }
        log.debug("Found stat file "+statFile.toString());
        ResourceStatistics resourceStats = null;
        try {
            resourceStats = new ObjectMapper().readValue(statFile.open(), ResourceStatistics.class);
        } catch (JsonParseException e) {
            log.warn("Unable to load Resource Statistics for "+location);
            e.printStackTrace();
        } catch (JsonMappingException e) {
            log.warn("Unable to load Resource Statistics for "+location);
            e.printStackTrace();
        } catch (IOException e) {
            log.warn("Unable to load Resource Statistics for "+location);
            e.printStackTrace();
        }
        return resourceStats;
    }

    //------------------------------------------------------------------------
    // Implementation of StoreMetaData interface

    @Override
    public void storeStatistics(ResourceStatistics stats, String location, Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        DataStorage storage = new HDataStorage(new Path(location).toUri(),
                ConfigurationUtil.toProperties(conf));
        ElementDescriptor statFilePath = storage.asElement(location, statFileName);
        if(!statFilePath.exists() && stats != null) {
            try {
                new ObjectMapper().writeValue(statFilePath.create(), stats);
            } catch (JsonGenerationException e) {
                log.warn("Unable to write Resource Statistics for "+location);
                e.printStackTrace();
            } catch (JsonMappingException e) {
                log.warn("Unable to write Resource Statistics for "+location);
                e.printStackTrace();
            }
        }
    }

    @Override
    public void storeSchema(ResourceSchema schema, String location, Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        DataStorage storage = new HDataStorage(new Path(location).toUri(),
                ConfigurationUtil.toProperties(conf));
        ElementDescriptor schemaFilePath = storage.asElement(location, schemaFileName);
        if(!schemaFilePath.exists() && schema != null) {
            try {
                new ObjectMapper().writeValue(schemaFilePath.create(), schema);
            } catch (JsonGenerationException e) {
                log.warn("Unable to write Resource Statistics for "+location);
                e.printStackTrace();
            } catch (JsonMappingException e) {
                log.warn("Unable to write Resource Statistics for "+location);
                e.printStackTrace();
            }
        }
        if (printHeaders) {
            ElementDescriptor headerFilePath = storage.asElement(location, headerFileName);
            if (!headerFilePath.exists()) {
                OutputStream os = headerFilePath.create();
                try {
                    String[] names = schema.fieldNames();

                    for (int i=0; i < names.length; i++) {
                        os.write(names[i].getBytes("UTF-8"));
                        if (i <names.length-1) {
                            os.write(fieldDel);
                        } else {
                            os.write(recordDel);
                        }
                    }
                } finally {
                    os.close();
                }
            }
        }
    }

    public void setFieldDel(byte fieldDel) {
        this.fieldDel = fieldDel;
    }

    public void setRecordDel(byte recordDel) {
        this.recordDel = recordDel;
    }

}
