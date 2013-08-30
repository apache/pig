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
package org.apache.pig.data;

import static org.apache.pig.PigConfiguration.SHOULD_USE_SCHEMA_TUPLE;
import static org.apache.pig.PigConstants.GENERATED_CLASSES_KEY;
import static org.apache.pig.PigConstants.LOCAL_CODE_DIR;
import static org.apache.pig.PigConstants.SCHEMA_TUPLE_ON_BY_DEFAULT;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.data.SchemaTupleClassGenerator.GenContext;
import org.apache.pig.data.utils.StructuresHelper.Pair;
import org.apache.pig.data.utils.StructuresHelper.SchemaKey;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

/**
 * This class is to be used at job creation time. It provides the API that lets code
 * register Schemas with pig to be generated. It is necessary to register these Schemas
 * and reducers.
 */
public class SchemaTupleFrontend {
    private static final Log LOG = LogFactory.getLog(SchemaTupleFrontend.class);

    private static SchemaTupleFrontend stf;

    /**
     * Schemas registered for generation are held here.
     */
    private static Map<Pair<SchemaKey, Boolean>, Pair<Integer, Set<GenContext>>> schemasToGenerate = Maps.newHashMap();

    private int internalRegisterToGenerateIfPossible(Schema udfSchema, boolean isAppendable, GenContext type) {
        Pair<SchemaKey, Boolean> key = Pair.make(new SchemaKey(udfSchema), isAppendable);
        Pair<Integer, Set<GenContext>> pr = schemasToGenerate.get(key);
        if (pr != null) {
            pr.getSecond().add(type);
            return pr.getFirst();
        }
        if (!SchemaTupleFactory.isGeneratable(udfSchema)) {
            LOG.debug("Given Schema is not generatable: " + udfSchema);
            return -1;
        }
        int id = SchemaTupleClassGenerator.getNextGlobalClassIdentifier();
        Set<GenContext> contexts = Sets.newHashSet();
        contexts.add(GenContext.FORCE_LOAD);
        contexts.add(type);
        schemasToGenerate.put(key, Pair.make(Integer.valueOf(id), contexts));
        LOG.debug("Registering "+(isAppendable ? "Appendable" : "")+"Schema for generation ["
                + udfSchema + "] with id [" + id + "] and context: " + type);
        return id;
    }

    private Map<Pair<SchemaKey, Boolean>, Pair<Integer, Set<GenContext>>> getSchemasToGenerate() {
        return schemasToGenerate;
    }

    private static class SchemaTupleFrontendGenHelper {
        private File codeDir;
        private PigContext pigContext;
        private Configuration conf;

        public SchemaTupleFrontendGenHelper(PigContext pigContext, Configuration conf) {
            codeDir = Files.createTempDir();
            codeDir.deleteOnExit();
            LOG.debug("Temporary directory for generated code created: "
                    + codeDir.getAbsolutePath());
            this.pigContext = pigContext;
            this.conf = conf;
        }

        /**
         * This method copies all class files present in the local temp directory to the distributed cache.
         * All copied files will have a symlink of their name. No files will be copied if the current
         * job is being run from local mode.
         * @param pigContext
         * @param conf
         */
        private void internalCopyAllGeneratedToDistributedCache() {
            LOG.info("Starting process to move generated code to distributed cacche");
            if (pigContext.getExecType().isLocal()) {
                String codePath = codeDir.getAbsolutePath();
                LOG.info("Distributed cache not supported or needed in local mode. Setting key ["
                        + LOCAL_CODE_DIR + "] with code temp directory: " + codePath);
                    conf.set(LOCAL_CODE_DIR, codePath);
                return;
            } else {
                // This let's us avoid NPE in some of the non-traditional pipelines
                String codePath = codeDir.getAbsolutePath();
                conf.set(LOCAL_CODE_DIR, codePath);
            }
            DistributedCache.createSymlink(conf); // we will read using symlinks
            StringBuilder serialized = new StringBuilder();
            boolean first = true;
            // We attempt to copy over every file in the generated code temp directory
            for (File f : codeDir.listFiles()) {
                if (first) {
                    first = false;
                } else {
                    serialized.append(",");
                }
                String symlink = f.getName(); //the class name will also be the symlink
                serialized.append(symlink);
                Path src = new Path(f.toURI());
                Path dst;
                try {
                    dst = FileLocalizer.getTemporaryPath(pigContext);
                } catch (IOException e) {
                    throw new RuntimeException("Error getting temporary path in HDFS", e);
                }
                FileSystem fs;
                try {
                    fs = dst.getFileSystem(conf);
                } catch (IOException e) {
                    throw new RuntimeException("Unable to get FileSystem", e);
                }
                try {
                    fs.copyFromLocalFile(src, dst);
                } catch (IOException e) {
                    throw new RuntimeException("Unable to copy from local filesystem to HDFS, src = "
                            + src + ", dst = " + dst, e);
                }

                String destination = dst.toString() + "#" + symlink;

                try {
                    DistributedCache.addCacheFile(new URI(destination), conf);
                } catch (URISyntaxException e) {
                    throw new RuntimeException("Unable to add file to distributed cache: " + destination, e);
                }
                LOG.info("File successfully added to the distributed cache: " + symlink);
            }
            String toSer = serialized.toString();
            LOG.info("Setting key [" + GENERATED_CLASSES_KEY + "] with classes to deserialize [" + toSer + "]");
            // we must set a key in the job conf so individual jobs know to resolve the shipped classes
            conf.set(GENERATED_CLASSES_KEY, toSer);
        }

        /**
         * This sets into motion the generation of all "registered" Schemas. All code will be generated
         * into the temporary directory.
         * @return true of false depending on if there are any files to copy to the distributed cache
         */
        private boolean generateAll(Map<Pair<SchemaKey, Boolean>, Pair<Integer, Set<GenContext>>> schemasToGenerate) {
            boolean filesToShip = false;
            if (!conf.getBoolean(SHOULD_USE_SCHEMA_TUPLE, SCHEMA_TUPLE_ON_BY_DEFAULT)) {
                LOG.info("Key ["+SHOULD_USE_SCHEMA_TUPLE+"] is false, will not generate code.");
                return false;
            }
            LOG.info("Generating all registered Schemas.");
            for (Map.Entry<Pair<SchemaKey, Boolean>, Pair<Integer, Set<GenContext>>> entry : schemasToGenerate.entrySet()) {
                Pair<SchemaKey, Boolean> keyPair = entry.getKey();
                Schema s = keyPair.getFirst().get();
                Pair<Integer, Set<GenContext>> valuePair = entry.getValue();
                Set<GenContext> contextsToInclude = Sets.newHashSet();
                boolean isShipping = false;
                for (GenContext context : valuePair.getSecond()) {
                    if (!context.shouldGenerate(conf)) {
                        LOG.info("Skipping generation of Schema [" + s + "], as key value [" + context.key() + "] was false.");
                    } else {
                        isShipping = true;
                        contextsToInclude.add(context);
                    }
                }
                if (!isShipping) {
                    continue;
                }
                int id = valuePair.getFirst();
                boolean isAppendable = keyPair.getSecond();
                SchemaTupleClassGenerator.generateSchemaTuple(s, isAppendable, id, codeDir, contextsToInclude.toArray(new GenContext[0]));
                filesToShip = true;
            }
            return filesToShip;
        }
    }

    /**
     * This allows the frontend/backend process to be repeated if on the same
     * JVM (as in testing).
     */
    public static void reset() {
        stf = null;
        schemasToGenerate.clear();
    }

    /**
     * This method "registers" a Schema to be generated. It allows a portions of the code
     * to register a Schema for generation without knowing whether code generation is enabled.
     * A unique ID will be passed back that can be used internally to refer to generated SchemaTuples
     * (such as in the case of serialization and deserialization). The context is necessary to allow
     * the client to restrict where generated code can be used.
     * @param   udfSchema       This is the Schema of a Tuple that we will potentially generate
     * @param   isAppendable    This specifies whether or not we want the SchemaTuple to be appendable
     * @param   context         This is the context in which users should be able to access the SchemaTuple
     * @return  identifier
     */
    public static int registerToGenerateIfPossible(Schema udfSchema, boolean isAppendable, GenContext context) {
        if (stf == null) {
            if (pigContextToReset != null) {
                Properties prop = pigContextToReset.getProperties();
                prop.remove(GENERATED_CLASSES_KEY);
                prop.remove(LOCAL_CODE_DIR);
                pigContextToReset = null;
            }
            SchemaTupleBackend.reset();
            SchemaTupleClassGenerator.resetGlobalClassIdentifier();
            stf = new SchemaTupleFrontend();
        }

        if (udfSchema == null) {
            return -1;
        }

        try {
            udfSchema = udfSchema.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Unable to clone Schema: " + udfSchema, e);
        }
        stripAliases(udfSchema);

        return stf.internalRegisterToGenerateIfPossible(udfSchema, isAppendable, context);
    }

    private static void stripAliases(Schema s) {
        for (Schema.FieldSchema fs : s.getFields()) {
            fs.alias = null;
            if (fs.schema != null) {
                stripAliases(fs.schema);
            }
        }
    }

    /**
     * This must be called when the code has been generated and the generated code needs to be shipped
     * to the cluster, so that it may be used by the mappers and reducers.
     * @param pigContext
     * @param conf
     */
    public static void copyAllGeneratedToDistributedCache(PigContext pigContext, Configuration conf) {
        if (stf == null) {
            LOG.debug("Nothing registered to generate.");
            return;
        }
        SchemaTupleFrontendGenHelper stfgh = new SchemaTupleFrontendGenHelper(pigContext, conf);
        stfgh.generateAll(stf.getSchemasToGenerate());
        stfgh.internalCopyAllGeneratedToDistributedCache();

        Properties prop = pigContext.getProperties();
        String value = conf.get(GENERATED_CLASSES_KEY);
        if (value != null) {
            prop.setProperty(GENERATED_CLASSES_KEY, value);
        } else {
            prop.remove(GENERATED_CLASSES_KEY);
        }
        value = conf.get(LOCAL_CODE_DIR);
        if (value != null) {
            prop.setProperty(LOCAL_CODE_DIR, value);
        } else {
            prop.remove(LOCAL_CODE_DIR);
        }
    }

    private static PigContext pigContextToReset = null;

    /**
     * This is a method which caches a PigContext object that has had
     * relevant key values set by SchemaTupleBackend. This is necessary
     * because in some cases, multiple cycles of jobs might run in the JVM,
     * but the PigContext object may be shared, so we want to make sure to
     * undo any changes we have made to it.
     */
    protected static void lazyReset(PigContext pigContext) {
        pigContextToReset = pigContext;
    }
}
