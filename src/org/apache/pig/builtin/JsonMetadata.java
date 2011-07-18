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
import org.apache.pig.LoadMetadata;
import org.apache.pig.StoreMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.backend.hadoop.datastorage.HDirectory;
import org.apache.pig.backend.hadoop.datastorage.HFile;
import org.apache.pig.impl.io.FileLocalizer;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Reads and Writes metadata using JSON in metafiles next to the data.
 *
 *
 */
public class JsonMetadata implements LoadMetadata, StoreMetadata {

    private static final Log log = LogFactory.getLog(JsonMetadata.class);

    // These are not static+final because we may want to make these adjustable by users.
    private String schemaFileName = ".pig_schema";
    private String headerFileName = ".pig_header";
    private String statFileName = ".pig_stats";

    private boolean printHeaders = true;

    private byte fieldDel;
    private byte recordDel;

    public JsonMetadata() {

    }

    /**.
     * Given a path, which may represent a glob pattern, a directory, or a file, this method
     * finds the set of relevant metadata files on the storage system. The algorithm for finding the
     * metadata file is as follows:
     * <p>
     * For each file represented by the path (either directly, or via a glob):
     *   If parentPath/prefix.fileName exists, use that as the metadata file.
     *   Else if parentPath/prefix exists, use that as the metadata file.
     * <p>
     * Resolving conflicts, merging the metadata, etc, is not handled by this method and should be
     * taken care of by downstream code.
     * <p>
     * This can go into a util package if metadata files are considered a general enough pattern
     * <p>
     * @param path      Path, as passed in to a LoadFunc (may be a Hadoop glob)
     * @param prefix    Metadata file designation, such as .pig_schema or .pig_stats
     * @param conf      configuration object
     * @return Set of element descriptors for all metadata files associated with the files on the path.
     */
    protected Set<ElementDescriptor> findMetaFile(String path, String prefix, Configuration conf)
        throws IOException {
        DataStorage storage = new HDataStorage(ConfigurationUtil.toProperties(conf));
        String fullPath = FileLocalizer.fullPath(path, storage);
        Set<ElementDescriptor> metaFileSet = new HashSet<ElementDescriptor>();
        if(storage.isContainer(fullPath)) {
            ElementDescriptor metaFilePath = storage.asElement(fullPath, prefix);
            if (metaFilePath.exists()) {
                metaFileSet.add(metaFilePath);
            }
        } else {
            ElementDescriptor[] descriptors = storage.asCollection(path);
            for(ElementDescriptor descriptor : descriptors) {
                String fileName = null, parentName = null;
                ContainerDescriptor parentContainer = null;
                if (descriptor instanceof HFile) {
                    Path descriptorPath = ((HFile) descriptor).getPath();
                    fileName = descriptorPath.getName();
                    Path parent = descriptorPath.getParent();
                    parentName = parent.toString();
                    parentContainer = new HDirectory((HDataStorage)storage,parent);
                }
                ElementDescriptor metaFilePath = storage.asElement(parentName, prefix+"."+fileName);

                // if the file has a custom schema, use it
                if (metaFilePath.exists()) {
                    metaFileSet.add(metaFilePath);
                    continue;
                }

                // if no custom schema, try the parent directory
                metaFilePath = storage.asElement(parentContainer, prefix);
                if (metaFilePath.exists()) {
                    metaFileSet.add(metaFilePath);
                }
            }
        }
        return metaFileSet;
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
        Configuration conf = job.getConfiguration();
        Set<ElementDescriptor> schemaFileSet = null;
        try {
            schemaFileSet = findMetaFile(location, schemaFileName, conf);
        } catch (IOException e) {
            log.warn("Could not find schema file for "+ location);
            return null;
        }

        // TODO we assume that all schemas are the same. The question of merging schemas is left open for now.
        ElementDescriptor schemaFile = null;
        if (!schemaFileSet.isEmpty()) {
            schemaFile = schemaFileSet.iterator().next();
        } else {
            log.warn("Could not find schema file for "+location);
            return null;
        }
        log.debug("Found schema file: "+schemaFile.toString());
        ResourceSchema resourceSchema = null;
        try {
            resourceSchema = new ObjectMapper().readValue(schemaFile.open(), ResourceSchema.class);
        } catch (JsonParseException e) {
            log.warn("Unable to load Resource Schema for "+location);
            e.printStackTrace();
        } catch (JsonMappingException e) {
            log.warn("Unable to load Resource Schema for "+location);
            e.printStackTrace();
        } catch (IOException e) {
            log.warn("Unable to load Resource Schema for "+location);
            e.printStackTrace();
        }
        return resourceSchema;
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
            log.warn("could not fine stat file for "+location);
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
        DataStorage storage = new HDataStorage(ConfigurationUtil.toProperties(conf));
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
        DataStorage storage = new HDataStorage(ConfigurationUtil.toProperties(conf));
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
