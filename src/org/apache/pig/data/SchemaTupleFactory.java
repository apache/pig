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
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.SchemaTuple.SchemaTupleQuickGenerator;
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

    //TODO need to incorporate the appendable/not appendable question!
    private static Map<Integer, SchemaTupleFactory> cachedSchemaTupleFactoriesById = Maps.newHashMap();
    private static Map<SchemaKey, SchemaTupleFactory> cachedSchemaTupleFactoriesBySchema = Maps.newHashMap();

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

    public static SchemaTupleFactory getSchemaTupleFactory(Schema s) {
        SchemaKey sk = new SchemaKey(s);
        SchemaTupleFactory stf = cachedSchemaTupleFactoriesBySchema.get(sk);

        if (stf != null) {
            return stf;
        }

        try {
            stf = loadedSchemaTupleClassesHolder.newSchemaTupleFactory(s);
        } catch (ExecException e) {
            throw new RuntimeException("Error making new SchemaTupleFactory by schema: " + s, e);
        }

        cachedSchemaTupleFactoriesById.put(((SchemaTuple<?>)stf.newTuple()).getSchemaTupleIdentifier(), stf);
        cachedSchemaTupleFactoriesBySchema.put(sk, stf);

        return stf;
    }

    public static SchemaTupleFactory getSchemaTupleFactory(int id) {
        SchemaTupleFactory stf = cachedSchemaTupleFactoriesById.get(id);

        if (stf != null) {
            return stf;
        }

        try {
            stf = loadedSchemaTupleClassesHolder.newSchemaTupleFactory(id);
        } catch (ExecException e) {
            throw new RuntimeException("Error making new SchemaTupleFactory by id: " + id, e);
        }

        cachedSchemaTupleFactoriesById.put(id, stf);
        cachedSchemaTupleFactoriesBySchema.put(new SchemaKey(((SchemaTuple<?>)stf.newTuple()).getSchema()), stf);

        return stf;
    }

    private static LoadedSchemaTupleClassesHolder loadedSchemaTupleClassesHolder = new LoadedSchemaTupleClassesHolder();

    public static LoadedSchemaTupleClassesHolder getLoadedSchemaTupleClassesHolder() {
        return loadedSchemaTupleClassesHolder;
    }

    public static class LoadedSchemaTupleClassesHolder {
        private Map<Integer, SchemaTupleQuickGenerator<? extends SchemaTuple<?>>> generators = Maps.newHashMap();
        private Map<Integer, Class<SchemaTuple<?>>> classes = Maps.newHashMap();

        private Map<SchemaKey, Integer> lookup = Maps.newHashMap();
        private Set<String> filesToResolve = Sets.newHashSet();

        private URLClassLoader classLoader;

        private LoadedSchemaTupleClassesHolder() {
            File fil = SchemaTupleClassGenerator.getGenerateCodeTempDir();
            try {
                classLoader = new URLClassLoader(new URL[] { fil.toURI().toURL() });
            } catch (MalformedURLException e) {
                throw new RuntimeException("Unable to make URLClassLoader for tempDir: " + fil.getAbsolutePath());
            }
        }

        public SchemaTupleFactory newSchemaTupleFactory(Schema s) throws ExecException {
            SchemaKey sk = new SchemaKey(s);
            Integer id = lookup.get(sk);
            if (id != null) {
                return newSchemaTupleFactory(id);
            }

            throw new ExecException("No mapping present for given Schema: " + s);
        }

        public SchemaTupleFactory newSchemaTupleFactory(int id) throws ExecException {
            Class<SchemaTuple<?>> clazz = classes.get(id);
            SchemaTupleQuickGenerator<? extends SchemaTuple<?>> stGen = generators.get(id);
            if (clazz == null || stGen == null) {
                throw new ExecException("Could not find matching SchemaTuple for id: " + id); //TODO do something else? Return null? A checked exception?
            }
            return new SchemaTupleFactory(clazz, stGen);
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
                int id = st.getSchemaTupleIdentifier();
                Schema schema = st.getSchema();

                classes.put(id, (Class<SchemaTuple<?>>)clazz);
                generators.put(id, (SchemaTupleQuickGenerator<SchemaTuple<?>>)st.getQuickGenerator());
                lookup.put(new SchemaKey(schema), id);
                LOG.info("Successfully resolved class.");
            }
        }

        public boolean isRegistered(String className) {
            return SchemaTupleClassGenerator.tempFile(className).exists();
        }
    }
}
