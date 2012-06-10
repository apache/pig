package org.apache.pig.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.data.SchemaTuple.SchemaTupleQuickGenerator;
import org.apache.pig.data.utils.HierarchyHelper;
import org.apache.pig.data.utils.MethodHelper;
import org.apache.pig.data.utils.StructuresHelper;
import org.apache.pig.data.utils.MethodHelper.NotImplemented;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class SchemaTupleFactory extends TupleFactory {
    private static final Log LOG = LogFactory.getLog(SchemaTupleFactory.class);

    private SchemaTupleQuickGenerator<? extends SchemaTuple<?>> generator;
    private Class<SchemaTuple<?>> clazz;

    protected SchemaTupleFactory(Class<SchemaTuple<?>> clazz, SchemaTupleQuickGenerator<? extends SchemaTuple<?>> generator) {
        this.clazz = clazz;
        this.generator = generator;
    }

    public static boolean isGeneratable(Schema s) {
        if (s == null) {
            return false;
        }

        for (Schema.FieldSchema fs : s.getFields()) {
            if (fs.type == DataType.BAG || fs.type == DataType.MAP)
                return false;

            if (fs.type == DataType.TUPLE && !isGeneratable(fs.schema))
                return false;
        }

        return true;
    }

    @Override
    public Tuple newTuple() {
        return generator.make();
    }

    @Override
    @NotImplemented
    public Tuple newTuple(int size) {
        throw MethodHelper.methodNotImplemented();
    }

    @Override
    @NotImplemented
    public Tuple newTuple(List c) {
        throw MethodHelper.methodNotImplemented();
    }

    @Override
    @NotImplemented
    public Tuple newTupleNoCopy(List c) {
        throw MethodHelper.methodNotImplemented();
    }

    @Override
    @NotImplemented
    public Tuple newTuple(Object datum) {
        throw MethodHelper.methodNotImplemented();
    }

    @Override
    public Class<SchemaTuple<?>> tupleClass() {
        return clazz;
    }

    public static SchemaTupleFactory getSchemaTupleFactory(Schema s, boolean isAppendable) {
        return loadedSchemaTupleClassesHolder.newSchemaTupleFactory(s, isAppendable);
    }

    public static SchemaTupleFactory getSchemaTupleFactory(int id) {
        return loadedSchemaTupleClassesHolder.newSchemaTupleFactory(id);
    }

    private static LoadedSchemaTupleClassesHolder loadedSchemaTupleClassesHolder = new LoadedSchemaTupleClassesHolder();

    public static LoadedSchemaTupleClassesHolder getLoadedSchemaTupleClassesHolder() {
        return loadedSchemaTupleClassesHolder;
    }

    public static class LoadedSchemaTupleClassesHolder {
        private Set<String> filesToResolve = Sets.newHashSet();

        private URLClassLoader classLoader;
        private Map<StructuresHelper.Pair<StructuresHelper.SchemaKey, Boolean>, SchemaTupleFactory> schemaTupleFactories = Maps.newHashMap();
        private Map<Integer, StructuresHelper.Pair<StructuresHelper.SchemaKey,Boolean>> identifiers = Maps.newHashMap();

        private LoadedSchemaTupleClassesHolder() {
            File fil = SchemaTupleClassGenerator.getGenerateCodeTempDir();
            try {
                classLoader = new URLClassLoader(new URL[] { fil.toURI().toURL() });
            } catch (MalformedURLException e) {
                throw new RuntimeException("Unable to make URLClassLoader for tempDir: " + fil.getAbsolutePath());
            }
        }

        public SchemaTupleFactory newSchemaTupleFactory(Schema s, boolean isAppendable)  {
            return newSchemaTupleFactory(StructuresHelper.Pair.make(new StructuresHelper.SchemaKey(s), isAppendable));
        }

        public SchemaTupleFactory newSchemaTupleFactory(int id) {
            StructuresHelper.Pair<StructuresHelper.SchemaKey, Boolean> pr = identifiers.get(id);
            if (pr == null) {
                LOG.debug("No SchemaTuple present for given identifier: " + id);
            }
            return newSchemaTupleFactory(pr);
        }

        private SchemaTupleFactory newSchemaTupleFactory(StructuresHelper.Pair<StructuresHelper.SchemaKey, Boolean> pr) {
            SchemaTupleFactory stf = schemaTupleFactories.get(pr);
            if (stf == null) {
                LOG.debug("No SchemaTupleFactory present for given SchemaKey/Boolean combination: " + pr);
            }
            return stf;
        }

        public void copyAllFromDistributedCache(Configuration conf) throws IOException {
            String toDeserialize = conf.get(SchemaTupleClassGenerator.GENERATED_CLASSES_KEY);
            if (toDeserialize == null) {
                LOG.info("No classes in in key [" + SchemaTupleClassGenerator.GENERATED_CLASSES_KEY + "] to copy from distributed cache.");
                return;
            }
            LOG.info("Copying files in key ["+SchemaTupleClassGenerator.GENERATED_CLASSES_KEY+"] from distributed cache: " + toDeserialize);
            //FileSystem fs = FileSystem.get(conf);
            for (String s : toDeserialize.split(",")) {
                LOG.info("Attempting to read file: " + s);
                FileInputStream fin = new FileInputStream(new File(s));
                FileOutputStream fos = new FileOutputStream(SchemaTupleClassGenerator.tempFile(s));

                int read;
                byte[] buf = new byte[1024*1024];
                while ((read = fin.read(buf)) > -1) {
                    fos.write(buf, 0, read);
                }
                fin.close();
                fos.close();
                LOG.info("Successfully read file.");
            }
        }

        public void copyAndResolve(Configuration conf, boolean isLocal) throws IOException {
            String shouldGenerate = conf.get(SchemaTupleClassGenerator.SHOULD_GENERATE_KEY);
            if (shouldGenerate == null || !Boolean.parseBoolean(shouldGenerate)) {
                LOG.info("Key [" + SchemaTupleClassGenerator.SHOULD_GENERATE_KEY +"] was not set... aborting generation");
            }
            if (!isLocal) {
                copyAllFromDistributedCache(conf);
            }
            //NEED TO ADD ALL FILES IN TEMPDIR TO "filesToResolve"
            for (File f : SchemaTupleClassGenerator.getGeneratedFiles()) {
                String name = f.getName().split("\\.")[0];
                if (!name.contains("$")) {
                    filesToResolve.add(name);
                    LOG.info("Added class to list of class to resolve: " + name);
                }
            }
            resolveClasses();
        }

        /**
         * Once all of the files are copied from the distributed cache to the local
         * temp directory, this will attempt to resolve those files and add their information.
         */
        @SuppressWarnings("unchecked")
        public void resolveClasses() {
            for (String s : filesToResolve) {
                LOG.info("Attempting to resolve class: " + s);
                Class<?> clazz;
                try {
                    clazz = classLoader.loadClass(s);
                } catch (ClassNotFoundException e1) {
                    throw new RuntimeException("Unable to find class: " + s);
                }
                if (!HierarchyHelper.verifyMustOverride(clazz)) {
                    LOG.error("Class ["+clazz+"] does NOT override all @MustOverride methods!");
                }
                Object o;
                try {
                    o = clazz.newInstance();
                } catch (InstantiationException e) {
                    throw new RuntimeException("Error instantiating file: " + s, e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Error accessing file: " + s, e);
                }
                if (!(o instanceof SchemaTuple<?>)) {
                    return;
                }
                SchemaTuple<?> st = (SchemaTuple<?>)o;
                boolean isAppendable = st instanceof AppendableSchemaTuple<?>;

                int id = st.getSchemaTupleIdentifier();
                Schema schema = st.getSchema();
                StructuresHelper.SchemaKey sk = new StructuresHelper.SchemaKey(schema);

                StructuresHelper.Pair<StructuresHelper.SchemaKey, Boolean> pr = StructuresHelper.Pair.make(sk, isAppendable);

                SchemaTupleFactory stf = new SchemaTupleFactory((Class<SchemaTuple<?>>)clazz, (SchemaTupleQuickGenerator<SchemaTuple<?>>)st.getQuickGenerator());

                schemaTupleFactories .put(pr, stf);
                identifiers.put(id, pr);

                LOG.info("Successfully resolved class.");
            }
        }

        public boolean isRegistered(String className) {
            return SchemaTupleClassGenerator.tempFile(className + ".class").exists();
        }
    }
}
