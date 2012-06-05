package org.apache.pig.data;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.pig.data.utils.MethodHelper.NotImplemented;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.SchemaTuple.SchemaTupleQuickGenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

import com.google.common.collect.Maps;
import com.google.common.collect.Lists;

import org.apache.pig.data.utils.HierarchyHelper;

//TODO Use code generation to avoid having to do clazz.newInstance(), and to be able to directly return the proper SchemaTuple
//TODO can just have a small container class that the generated code extends that has any functionality specific to that
public class SchemaTupleFactory extends TupleFactory {
    private SchemaTupleQuickGenerator generator;
    private Class<SchemaTuple> clazz;

    private static Map<Integer, SchemaTupleFactory> cachedSchemaTupleFactories = Maps.newHashMap();

    protected SchemaTupleFactory(Class<SchemaTuple> clazz, SchemaTupleQuickGenerator generator) {
        this.clazz = clazz;
        this.generator = generator;
    }

    public static boolean isGeneratable(Schema s) {
        if (s == null)
            return false;

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
        generator.make();
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
    public Class<SchemaTuple> tupleClass() {
        return clazz;
    }

    public static SchemaTupleFactory getSchemaTupleFactory(int id) {
        SchemaTupleFactory stf = cachedSchemaTupleFactories.get(id);

        if (stf != null) {
            return stf;
        }

        stf = loadedSchemaTupleClassesHolder.newSchemaTupleFactory(id);

        cachedSchemaTupleFactories.put(id, stf);

        return stf;
    }

    private static LoadedSchemaTupleClassesHolder loadedSchemaTupleClassesHolder = new LoadedSchemaTupleClassesHolder();

    public static LoadedSchemaTupleClassesHolder getLoadedSchemaTupleClassesHolder() {
        return loadedSchemaTupleClassesHolder;
    }

    public static class LoadedSchemaTupleClassesHolder {
        private Map<Integer, SchemaTupleQuickGenerator> generators = Maps.newHashMap();
        private Map<Integer, Class<? extends SchemaTuple>> classes = Maps.newHashMap();

        protected LoadedSchemaTupleClassesHolder() {
        }

        public SchemaTupleFactory newSchemaTupleFactory(int id) {
            Class<? extends SchemaTuple> clazz = classes.get(id);
            SchemaTupleQuickGenerator stGen = generators.get(id);
            if (clazz == null || stGen == null) {
                throw new RuntimeException("Could not find matching SchemaTuple for id: " + id); //TODO do something else? Return null? A checked exception?
            }
            return new SchemaTupleFactory(clazz, stGen);
        }

        public void registerFileFromDistributedCache(String className, Configuration conf) throws IOException {
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream fsdis = fs.open(new Path(className));
            long available = 0;
            long position = 0;
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while ((available = fsdis.available()) > 0) {
                byte[] buf = new byte[available];
                long read = fsdis.read(position, buf, 0, available);
                if (read == -1) {
                    break;
                }
                position += read;
                baos.write(buf, 0, read);
            }

            byte[] buf = baos.toByteArray();

            ClassLoader cl = new SecureClassLoader() {
                @Override
                protected Class<?> findClass(String name) {
                    return super.defineClass(name, buf, 0, buf.length);
                }
            };

            Class<? extends SchemaTuple> clazz = (Class<? extends SchemaTuple>)cl.loadClass(className);
            SchemaTuple st;
            try {
                st = clazz.newInstance();
            } catch (InstantiationException e) {
                throw new ExecException("Unable to instantiate class " + clazz, e);
            } catch (IllegalAccessException e) {
                throw new ExecException("Not allowed to instantiate class " + clazz, e);
            }
            SchemaTupleQuickGenerator stGen = st.getQuickGenerator();
            int id = st.getSchemaTupleIdentifier();

            generators.put(id, stGen);
            classes.put(id, clazz);
        }
    }
}
