package org.apache.pig.data;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.data.SchemaTupleClassGenerator.GenContext;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
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

        Schema udfSchema = Utils.getSchemaFromString("int");
        boolean isAppendable = false;
        GenContext context = GenContext.UDF;
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("long");
        isAppendable = true;
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("chararray,(charrarray)");
        isAppendable = false;
        context = GenContext.LOAD;
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("int,(int,(int,(int,(int,(int,(int))))))");
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("((int,int),(int,int),(int,int)),((int,int),(int,int),(int,int))");
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        // this compiles and "ships"
        SchemaTupleFrontend.copyAllGeneratedToDistributedCache(pigContext, conf);

        //backend
        SchemaTupleBackend.initialize(conf, true);

        udfSchema = Utils.getSchemaFromString("int");
        isAppendable = false;
        context = GenContext.UDF;
        TupleFactory tf = TupleFactory.getInstanceForSchema(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        context = GenContext.JOIN;
        tf = TupleFactory.getInstanceForSchema(udfSchema, isAppendable, context);
        assertNull(tf);

        udfSchema = Utils.getSchemaFromString("long");
        context = GenContext.UDF;
        isAppendable = true;

        tf = TupleFactory.getInstanceForSchema(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        isAppendable = false;
        tf = TupleFactory.getInstanceForSchema(udfSchema, isAppendable, context);
        assertNull(tf);

        udfSchema = Utils.getSchemaFromString("chararray,(charrarray)");
        isAppendable = false;
        context = GenContext.LOAD;
        tf = TupleFactory.getInstanceForSchema(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        udfSchema = Utils.getSchemaFromString("(charrarray)");
        tf = TupleFactory.getInstanceForSchema(udfSchema, isAppendable, context);
        assertNull(tf);

        udfSchema = Utils.getSchemaFromString("int,(int,(int,(int,(int,(int,(int))))))");
        tf = TupleFactory.getInstanceForSchema(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        udfSchema = Utils.getSchemaFromString("((int,int),(int,int),(int,int)),((int,int),(int,int),(int,int))");
        tf = TupleFactory.getInstanceForSchema(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);
    }

    private void putThroughPaces(TupleFactory tf, Schema udfSchema, boolean isAppendable) {
        assertNotNull(tf);
        assertTrue(tf instanceof SchemaTupleFactory);
        assertTrue(tf.newTuple() instanceof SchemaTuple);
        SchemaTuple<?> st = (SchemaTuple<?>) tf.newTuple();
        Schema.equals(udfSchema, st.getSchema(), false, true);

    }
}
