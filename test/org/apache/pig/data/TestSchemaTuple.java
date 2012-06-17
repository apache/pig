package org.apache.pig.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.SchemaTupleClassGenerator.GenContext;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.Utils;
import org.junit.Test;

public class TestSchemaTuple {
    @Test
    public void testCompileAndResolve() throws Exception {
        //frontend
        Properties props = new Properties();
        props.setProperty(SchemaTupleBackend.SHOULD_GENERATE_KEY, "true");

        Configuration conf = ConfigurationUtil.toConfiguration(props);
        PigContext pigContext = new PigContext(ExecType.LOCAL, props);

        Schema udfSchema = Utils.getSchemaFromString("a:int");
        boolean isAppendable = false;
        GenContext context = GenContext.UDF;
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("a:long");
        isAppendable = true;
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("a:chararray,(a:chararray)}");
        isAppendable = false;
        context = GenContext.LOAD;
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("a:int,(a:int,(a:int,(a:int,(a:int,(a:int,(a:int))))))");
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("((int,int),(int,int),(int,int)),((int,int),(int,int),(int,int))");
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        // this compiles and "ships"
        SchemaTupleFrontend.copyAllGeneratedToDistributedCache(pigContext, conf);

        //backend
        SchemaTupleBackend.initialize(conf, true);

        udfSchema = Utils.getSchemaFromString("a:int");
        isAppendable = false;
        context = GenContext.UDF;
        TupleFactory tf = TupleFactory.getInstanceForSchema(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        context = GenContext.JOIN;
        tf = TupleFactory.getInstanceForSchema(udfSchema, isAppendable, context);
        assertNull(tf);

        udfSchema = Utils.getSchemaFromString("a:long");
        context = GenContext.UDF;
        isAppendable = true;

        tf = TupleFactory.getInstanceForSchema(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        isAppendable = false;
        tf = TupleFactory.getInstanceForSchema(udfSchema, isAppendable, context);
        assertNull(tf);

        udfSchema = Utils.getSchemaFromString("a:chararray,(a:chararray)");
        isAppendable = false;
        context = GenContext.LOAD;
        tf = TupleFactory.getInstanceForSchema(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        udfSchema = Utils.getSchemaFromString("(a:charrarray)");
        tf = TupleFactory.getInstanceForSchema(udfSchema, isAppendable, context);
        assertNull(tf);

        udfSchema = Utils.getSchemaFromString("a:int,(a:int,(a:int,(a:int,(a:int,(a:int,(a:int))))))");
        tf = TupleFactory.getInstanceForSchema(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        udfSchema = Utils.getSchemaFromString("((int,int),(int,int),(int,int)),((int,int),(int,int),(int,int))");
        tf = TupleFactory.getInstanceForSchema(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);
    }

    private void putThroughPaces(TupleFactory tf, Schema udfSchema, boolean isAppendable) throws ExecException {
        assertNotNull(tf);
        assertTrue(tf instanceof SchemaTupleFactory);
        assertTrue(tf.newTuple() instanceof SchemaTuple);
        if (isAppendable) {
            assertTrue(tf.newTuple() instanceof AppendableSchemaTuple);
        }

        testNotAppendable(tf, udfSchema);
        if (isAppendable) {
            testAppendable(tf, udfSchema);
        }
    }

    private void testAppendable(TupleFactory tf, Schema udfSchema) {
        SchemaTuple<?> st = (SchemaTuple<?>) tf.newTuple();

        st.append("woah");
        assertEquals(udfSchema.size() + 1, st.size());

    }

    private void testNotAppendable(TupleFactory tf, Schema udfSchema) throws ExecException {
        SchemaTuple<?> st = (SchemaTuple<?>) tf.newTuple();
        Schema.equals(udfSchema, st.getSchema(), false, true);

        assertEquals(udfSchema.size(), st.size());

        fillWithData(st, udfSchema);
    }

    private void fillWithData(SchemaTuple<?> st, Schema udfSchema) throws ExecException {
        int pos = 0;
        for (FieldSchema fs : udfSchema.getFields()) {
            Object val;
            if (fs.type == DataType.TUPLE) {
                val = TupleFactory
                            .getInstanceForSchema(fs.schema, false, GenContext.FORCE_LOAD)
                            .newTuple();
                fillWithData((SchemaTuple<?>)val, fs.schema);
            } else {
                val = randData(fs);
            }
            st.set(pos++, val);
        }
    }

    private Random r = new Random(100L);

    private Object randData(FieldSchema fs) {
        switch (fs.type) {
        case DataType.BOOLEAN: return r.nextBoolean();
        case DataType.BYTEARRAY: return new DataByteArray(new BigInteger(130, r).toByteArray());
        case DataType.CHARARRAY: return new BigInteger(130, r).toString(32);
        case DataType.INTEGER: return r.nextInt();
        case DataType.LONG: return r.nextLong();
        case DataType.FLOAT: return r.nextFloat();
        case DataType.DOUBLE: return r.nextDouble();
        default: throw new RuntimeException("Cannot generate data for given FieldSchema: " + fs);
        }
    }
}
