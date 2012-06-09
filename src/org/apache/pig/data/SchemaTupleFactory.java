package org.apache.pig.data;

import java.io.File;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.data.SchemaTuple.SchemaTupleQuickGenerator;
import org.apache.pig.data.SchemaTupleClassGenerator.Pair;
import org.apache.pig.data.SchemaTupleClassGenerator.SchemaKey;
import org.apache.pig.data.utils.MethodHelper.NotImplemented;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

//TODO Use code generation to avoid having to do clazz.newInstance(), and to be able to directly return the proper SchemaTuple
//TODO can just have a small container class that the generated code extends that has any functionality specific to that
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

    public static RuntimeException methodNotImplemented() {
        StackTraceElement[] ste = Thread.currentThread().getStackTrace();
        StackTraceElement pre = ste[ste.length - 2];
        return new RuntimeException(pre.getMethodName() + " not implemented in " + pre.getClassName());
    }

    @Override
    @NotImplemented
    public Tuple newTuple(int size) {
        throw methodNotImplemented();
    }

    @Override
    @NotImplemented
    public Tuple newTuple(List c) {
        throw methodNotImplemented();
    }

    @Override
    @NotImplemented
    public Tuple newTupleNoCopy(List c) {
        throw methodNotImplemented();
    }

    @Override
    @NotImplemented
    public Tuple newTuple(Object datum) {
        throw methodNotImplemented();
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
        private Map<Pair<SchemaKey, Boolean>, SchemaTupleFactory> schemaTupleFactories = Maps.newHashMap();
        private Map<Integer, Pair<SchemaKey,Boolean>> identifiers = Maps.newHashMap();

        private LoadedSchemaTupleClassesHolder() {
            File fil = SchemaTupleClassGenerator.getGenerateCodeTempDir();
            try {
                classLoader = new URLClassLoader(new URL[] { fil.toURI().toURL() });
            } catch (MalformedURLException e) {
                throw new RuntimeException("Unable to make URLClassLoader for tempDir: " + fil.getAbsolutePath());
            }
        }

        public SchemaTupleFactory newSchemaTupleFactory(Schema s, boolean isAppendable)  {
            return newSchemaTupleFactory(Pair.make(new SchemaKey(s), isAppendable));
        }

        public SchemaTupleFactory newSchemaTupleFactory(int id) {
            Pair<SchemaKey, Boolean> pr = identifiers.get(id);
            if (pr == null) {
                LOG.debug("No SchemaTuple present for given identifier: " + id);
            }
            return newSchemaTupleFactory(pr);
        }

        private SchemaTupleFactory newSchemaTupleFactory(Pair<SchemaKey, Boolean> pr) {
            SchemaTupleFactory stf = schemaTupleFactories.get(pr);
            if (stf == null) {
                LOG.debug("No SchemaTUpleFactory present for given SchemaKey/Boolean combination: " + pr);
            }
            return stf;
        }

        public void copyAllFromDistributedCache(Configuration conf) throws IOException {
            String toDeserialize = conf.get(SchemaTupleClassGenerator.GENERATED_CLASSES_KEY);
            if (toDeserialize == null) {
                return;
            }
            FileSystem fs = FileSystem.get(conf);
            for (String s : toDeserialize.split(",")) {
                copyFromDistributedCache(s, conf, fs);
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

        public void copyFromDistributedCache(String className, Configuration conf, FileSystem fs) throws IOException {
            Path src = new Path(className + ".class");
            Path dst = new Path(SchemaTupleClassGenerator.tempFile(className).getAbsolutePath());
            fs.copyToLocalFile(src, dst);
        }

        /**
         * Once all of the files are copied from the distributed cache to the local
         * temp directory, this will attempt to resolve those files and add their information.
         */
        public void resolveClasses() {
            for (String s : filesToResolve) {
                LOG.info("Attempting to resolve class: " + s);
                Class<?> clazz;
                try {
                    clazz = classLoader.loadClass(s);
                } catch (ClassNotFoundException e1) {
                    throw new RuntimeException("Unable to find class: " + s);
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
                SchemaKey sk = new SchemaKey(schema);

                Pair<SchemaKey, Boolean> pr = Pair.make(sk, isAppendable);

                SchemaTupleFactory stf = new SchemaTupleFactory((Class<SchemaTuple<?>>)clazz, (SchemaTupleQuickGenerator<SchemaTuple<?>>)st.getQuickGenerator());

                schemaTupleFactories .put(pr, stf);
                identifiers.put(id, pr);

                LOG.info("Successfully resolved class.");
            }
        }

        public boolean isRegistered(String className) {
            return SchemaTupleClassGenerator.tempFile(className).exists();
        }
    }
}
