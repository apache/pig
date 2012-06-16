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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.data.SchemaTupleClassGenerator.GenContext;
import org.apache.pig.data.utils.StructuresHelper.SchemaKey;
import org.apache.pig.data.utils.StructuresHelper.Triple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class SchemaTupleFrontend {
    private static final Log LOG = LogFactory.getLog(SchemaTupleFrontend.class);

    private static SchemaTupleFrontend stf;

    /**
     * Schemas registered for generation are held here.
     */
    private static Map<SchemaKey, Triple<Integer, Boolean, Set<GenContext>>> schemasToGenerate = Maps.newHashMap();

    private int internalRegisterToGenerateIfPossible(Schema udfSchema, boolean isAppendable, GenContext type) {
        SchemaKey sk = new SchemaKey(udfSchema);
        Triple<Integer, Boolean, Set<GenContext>> pr = schemasToGenerate.get(sk);
        if (pr != null) {
            pr.getThird().add(type);
            return pr.getFirst();
        }
        if (!SchemaTupleFactory.isGeneratable(udfSchema)) {
            return -1;
        }
        int id = SchemaTupleClassGenerator.getNextGlobalClassIdentifier();
        Set<GenContext> contexts = Sets.newHashSet();
        contexts.add(type);
        schemasToGenerate.put(sk, Triple.make(Integer.valueOf(id), isAppendable, contexts));
        LOG.info("Registering "+(isAppendable ? "Appendable" : "")+"Schema for generation ["
                + udfSchema + "] with id [" + id + "] and context: " + type);
        return id;
    }

    private Map<SchemaKey, Triple<Integer, Boolean, Set<GenContext>>> getSchemasToGenerate() {
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
            if (pigContext.getExecType() == ExecType.LOCAL) {
                String codePath = codeDir.getAbsolutePath();
                LOG.info("Distributed cache not supported or needed in local mode. Setting key ["
                        + LOCAL_CODE_DIR + "] with code temp directory: " + codePath);
                if (pigContext.getExecType() == ExecType.LOCAL) {
                    conf.set(LOCAL_CODE_DIR, codePath);
                }
                return;
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
            LOG.info("Setting key [" + SchemaTupleBackend.GENERATED_CLASSES_KEY + "] with classes to deserialize [" + toSer + "]");
            // we must set a key in the job conf so individual jobs know to resolve the shipped classes
            conf.set(SchemaTupleBackend.GENERATED_CLASSES_KEY, toSer);
        }

        /**
         * This sets into motion the generation of all "registered" Schemas. All code will be generated
         * into the temporary directory.
         * @return true of false depending on if there are any files to copy to the distributed cache
         */
        private boolean generateAll(Map<SchemaKey, Triple<Integer, Boolean, Set<GenContext>>> schemasToGenerate) {
            boolean filesToShip = false;
            LOG.info("Generating all registered Schemas.");
            for (Map.Entry<SchemaKey, Triple<Integer,Boolean, Set<GenContext>>> entry : schemasToGenerate.entrySet()) {
                Schema s = entry.getKey().get();
                Triple<Integer, Boolean, Set<GenContext>> value = entry.getValue();
                Set<GenContext> contextsToInclude = Sets.newHashSet();
                boolean isShipping = false;
                for (GenContext context : value.getThird()) {
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
                int id = value.getFirst();
                boolean isAppendable = value.getSecond();
                SchemaTupleClassGenerator.generateSchemaTuple(s, isAppendable, id, codeDir, contextsToInclude.toArray(new GenContext[0]));
                filesToShip = true;
            }
            return filesToShip;
        }
    }

    /**
     * This method "registers" a Schema to be generated. It allows a portions of the code
     * to register a Schema for generation without knowing whether code generation is enabled.
     * A unique ID will be passed back, so in the actual M/R job, code needs to make sure that
     * generation was turned on or else the classes will not be present!
     * @param   udfSchema
     * @param   isAppendable
     * @return  identifier
     */
    public static int registerToGenerateIfPossible(Schema udfSchema, boolean isAppendable, GenContext type) {
        if (stf == null) {
            stf = new SchemaTupleFrontend();
        }
        return stf.internalRegisterToGenerateIfPossible(udfSchema, isAppendable, type);
    }

    protected static final String LOCAL_CODE_DIR = "pig.schematuple.local.dir";

    public static void copyAllGeneratedToDistributedCache(PigContext pigContext, Configuration conf) {
        SchemaTupleFrontendGenHelper stfgh = new SchemaTupleFrontendGenHelper(pigContext, conf);
        stfgh.generateAll(stf.getSchemasToGenerate());
        stfgh.internalCopyAllGeneratedToDistributedCache();
    }
}
