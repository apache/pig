package org.apache.pig.data;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.pig.data.utils.MethodHelper.NotImplemented;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Maps;
import com.google.common.collect.Lists;

import org.apache.pig.data.utils.HierarchyHelper;

//TODO Use code generation to avoid having to do clazz.newInstance(), and to be able to directly return the proper SchemaTuple
//TODO can just have a small container class that the generated code extends that has any functionality specific to that
public class SchemaTupleFactory extends TupleFactory {
    private static boolean massLoad = false;
    private static GeneratedSchemaTupleInfoRepository gennedInfo = new GeneratedSchemaTupleInfoRepository();

    private Class<SchemaTuple> clazz;

    private SchemaTupleFactory(Class<SchemaTuple> clazz) {
        this.clazz = clazz;
    }

    private static Map<Schema, SchemaTupleFactory> schemaFactoryMap = Maps.newHashMap();
    private static Map<Integer, SchemaTupleFactory> idFactoryMap = Maps.newHashMap();

    //TODO this should use SchemaTupleClassGenerate.readClasslistFromJar and get the classnames there...but for now it uses the janky method
    private static void massLoad() {
        //gennedInfo = new GeneratedSchemaTupleInfo(SchemaSchemaTupleClassGenerate.readClasslistFromJar());
        try {
            for (int i = 0; true; i++)
                gennedInfo.registerClass("SchemaTuple_" + i);
        } catch (ExecException e) {
            Throwable cause = e.getCause();
            if (!(cause instanceof ClassNotFoundException))
                throw new RuntimeException("Unexpected failure in massLoad()", cause);
        }
        massLoad = true;
    }

    protected static GeneratedSchemaTupleInfoRepository getGeneratedInfo() {
        return gennedInfo;
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
        try {
            return instantiateClass(clazz);
        } catch (ExecException e) {
            throw new RuntimeException("Error creating Tuple", e);
        }
    }

    @Override
    @NotImplemented
    public Tuple newTuple(int size) {
        throw new RuntimeException("newTuple(int) not implemented in SchemaTupleFactory");
    }

    @Override
    public Tuple newTuple(List c) {
        List copy = Lists.newArrayList();
        Collections.copy(c, copy);
        return newTupleNoCopy(copy);
    }

    @Override
    public Tuple newTupleNoCopy(List c) {
        SchemaTuple st = (SchemaTuple)newTuple();
        try {
            return st.set(c);
        } catch (ExecException e) {
            throw new RuntimeException("Unable to fill " + st.getClass() + " with given list " + c);
        }
    }

    @Override
    @NotImplemented
    public Tuple newTuple(Object datum) {
        throw new RuntimeException("newTuple(Object) not implemented in SchemaTupleFactory");
    }

    @Override
    public Class<SchemaTuple> tupleClass() {
        return clazz;
    }

    public static SchemaTuple instantiateClass(Class<SchemaTuple> clazz) throws ExecException {
       try {
            return clazz.newInstance();
        } catch (InstantiationException e) {
            throw new ExecException("Unable to instantiate class " + clazz, e);
        } catch (IllegalAccessException e) {
            throw new ExecException("Not allowed to instantiate class " + clazz, e);
        }
    }

    public static Schema stripAliases(Schema s) {
        for (Schema.FieldSchema fs : s.getFields()) {
            fs.alias = null;
            if (fs.schema != null) {
                stripAliases(fs.schema);
            }
        }

        return s;
    }

    public static SchemaTupleFactory getSchemaTupleFactory(Schema schema) {
        SchemaTupleFactory stf = schemaFactoryMap.get(schema);

        if (stf != null) {
            return stf;
        }

        if (!massLoad) {
            massLoad();
        }

        stf = new SchemaTupleFactory(gennedInfo.getTupleClass(schema));

        schemaFactoryMap.put(schema, stf);

        return stf;
    }

    public static SchemaTupleFactory getSchemaTupleFactory(int id) {
        SchemaTupleFactory stf = schemaFactoryMap.get(id);

        if (stf != null) {
            return stf;
        }

        if (!massLoad) {
            massLoad();
        }

        stf = new SchemaTupleFactory(gennedInfo.getTupleClass(id));

        idFactoryMap.put(id, stf);

        return stf;
    }

    static class GeneratedSchemaTupleInfoRepository {
        private Map<Integer, Class<SchemaTuple>> idClassMap = Maps.newHashMap();
        private Map<Schema, Integer> strippedSchemaIdMap = Maps.newHashMap();
	private Map<Schema, Integer> schemaIdMap = Maps.newHashMap();
        private Map<String, Integer> nameIdMap = Maps.newHashMap();

        public GeneratedSchemaTupleInfoRepository() {}

        public GeneratedSchemaTupleInfoRepository(Iterable<String> classes) {
            for (String className : classes) {
                try {
                    registerClass(className);
                } catch (ExecException e) {
                    throw new RuntimeException("Expected class " + className + " was not in classpath", e);
                }
            }
        }

        public void registerClass(String className) throws ExecException {
            Class<SchemaTuple> clazz;
            Integer hashId = nameIdMap.get(className);

            if (hashId != null)
                return;

            try {
                clazz = (Class<SchemaTuple>)Class.forName(className); //should we be using a different method?
            } catch (ClassNotFoundException e) {
                throw new ExecException("Given class " + className + " not found in classpath", e);
            }

            if (!HierarchyHelper.verifyMustOverride(clazz)) {
                 throw new ExecException("Generated class " + clazz + " does not override all @MustOverride methods in SchemaTuple");
            }

            SchemaTuple st = instantiateClass(clazz);
            int id = st.getSchemaTupleIdentifier();
            Schema schema = st.getSchema();
            schemaIdMap.put(schema, id);

            strippedSchemaIdMap.put(stripAliases(new Schema(schema)), id);

            nameIdMap.put(className, id);
            idClassMap.put(id, clazz);
        }

        public Class<SchemaTuple> getTupleClass(Schema schema) {
            return getTupleClass(schema, false);
        }

        public Class<SchemaTuple> getTupleClass(Schema schema, boolean strict) {
            Integer id;
            if (!strict) {
                schema = stripAliases(new Schema(schema));
                id = strippedSchemaIdMap.get(schema);
            } else {
                id = schemaIdMap.get(schema);
            }

            return idClassMap.get(id);
        }

        public Class<SchemaTuple> getTupleClass(int id) {
            return idClassMap.get(id);
        }
    }
}
