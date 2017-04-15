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

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pig.ExecType;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRConfiguration;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.SchemaTupleClassGenerator.GenContext;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.InterRecordReader;
import org.apache.pig.impl.io.InterRecordWriter;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.impl.util.Utils;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TestSchemaTuple {
    private Properties props;
    private Configuration conf;
    private PigContext pigContext;
    private static final BinInterSedes bis = new BinInterSedes();

    @Before
    public void perTestInitialize() {
        props = new Properties();
        props.setProperty(PigConfiguration.PIG_SCHEMA_TUPLE_ENABLED, "true");

        conf = ConfigurationUtil.toConfiguration(props);

        pigContext = new PigContext(ExecType.LOCAL, props);
    }

    @Test
    public void testCompileAndResolve() throws Exception {
        //frontend
        Schema udfSchema = Utils.getSchemaFromString("a:int");
        boolean isAppendable = false;
        GenContext context = GenContext.UDF;
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("a:long");
        isAppendable = true;
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("a:chararray,(a:chararray)");
        isAppendable = false;
        context = GenContext.FOREACH;
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("a:int,(a:int,(a:int,(a:int,(a:int,(a:int,(a:int))))))");
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("((a:int,b:int),(a:int,b:int),(a:int,b:int)),((a:int,b:int),(a:int,b:int),(a:int,b:int))");
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString(
                "a:int, b:long, c:chararray, d:boolean, e:bytearray, f:float, g:double, dt:datetime, bd: bigdecimal, bi: biginteger,"
                +"(a:int, b:long, c:chararray, d:boolean, e:bytearray, f:float, g:double, dt:datetime, bd: bigdecimal, bi: biginteger,"
                +"(a:int, b:long, c:chararray, d:boolean, e:bytearray, f:float, g:double, dt:datetime, bd: bigdecimal, bi: biginteger))");
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString("boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean");
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        udfSchema = Utils.getSchemaFromString(
                "int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger))");
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        isAppendable = true;
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        isAppendable = false;
        udfSchema = Utils.getSchemaFromString("int, b:bag{(int,int,int)}");
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        isAppendable = false;
        udfSchema = Utils.getSchemaFromString("int, m:map[(int,int,int)]");
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        isAppendable = false;
        udfSchema = new Schema();
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        isAppendable = false;
        udfSchema = new Schema(new FieldSchema(null, DataType.BAG));
        SchemaTupleFrontend.registerToGenerateIfPossible(udfSchema, isAppendable, context);

        // this compiles and "ships"
        SchemaTupleFrontend.copyAllGeneratedToDistributedCache(pigContext, conf);

        //backend
        SchemaTupleBackend.initialize(conf, pigContext);

        udfSchema = Utils.getSchemaFromString("a:int");
        isAppendable = false;
        context = GenContext.UDF;
        SchemaTupleFactory tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        context = GenContext.MERGE_JOIN;
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        assertNull(tf);

        udfSchema = Utils.getSchemaFromString("a:long");
        context = GenContext.UDF;
        isAppendable = true;

        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        isAppendable = false;
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        assertNull(tf);

        udfSchema = Utils.getSchemaFromString("a:chararray,(a:chararray)");
        isAppendable = false;
        context = GenContext.FOREACH;
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        udfSchema = Utils.getSchemaFromString("(a:chararray)");
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        assertNull(tf);

        udfSchema = Utils.getSchemaFromString("a:int,(a:int,(a:int,(a:int,(a:int,(a:int,(a:int))))))");
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        udfSchema = Utils.getSchemaFromString("((a:int,b:int),(a:int,b:int),(a:int,b:int)),((a:int,b:int),(a:int,b:int),(a:int,b:int))");
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        udfSchema = Utils.getSchemaFromString(
                "a:int, b:long, c:chararray, d:boolean, e:bytearray, f:float, g:double, dt: datetime, bd: bigdecimal, bi: biginteger,"
                +"(a:int, b:long, c:chararray, d:boolean, e:bytearray, f:float, g:double, dt: datetime, bd: bigdecimal, bi: biginteger,"
                +"(a:int, b:long, c:chararray, d:boolean, e:bytearray, f:float, g:double, dt: datetime, bd: bigdecimal, bi: biginteger))");
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        udfSchema = Utils.getSchemaFromString("boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean,"
                + "boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean, boolean");
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        udfSchema = Utils.getSchemaFromString(
                "int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger)),"
                +"int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger,"
                +"(int, long, chararray, boolean, bytearray, float, double, datetime, bigdecimal, biginteger))");
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        isAppendable = true;
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        isAppendable = false;
        udfSchema = Utils.getSchemaFromString("int, b:bag{(int,int,int)}");
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        isAppendable = false;
        udfSchema = Utils.getSchemaFromString("int, m:map[(int,int,int)]");
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);

        isAppendable = false;
        udfSchema = new Schema();
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        assertNull(tf);

        isAppendable = false;
        udfSchema = new Schema(new FieldSchema(null, DataType.BAG));
        tf = SchemaTupleFactory.getInstance(udfSchema, isAppendable, context);
        putThroughPaces(tf, udfSchema, isAppendable);
    }

    private void putThroughPaces(SchemaTupleFactory tf, Schema udfSchema, boolean isAppendable) throws Exception {
        assertNotNull(tf);
        if (isAppendable) {
            assertTrue(tf.newTuple() instanceof AppendableSchemaTuple);
        }

        testNotAppendable(tf, udfSchema);
        if (isAppendable) {
            testAppendable(tf, udfSchema);
        }
    }

    private void testAppendable(SchemaTupleFactory tf, Schema udfSchema) {
        SchemaTuple<?> st = tf.newTuple();

        st.append("woah");
        assertEquals(udfSchema.size() + 1, st.size());

    }

    private void testNotAppendable(SchemaTupleFactory tf, Schema udfSchema) throws Exception {
        SchemaTuple<?> st = tf.newTuple();
        Schema.equals(udfSchema, st.getSchema(), false, true);

        assertEquals(udfSchema.size(), st.size());

        testHashCodeEqualAsNormalTuple(tf);

        shouldAllBeNull(tf);

        copyThenCompare(tf);

        testSerDe(tf);
        testInterStorageSerDe(tf);
    }

    private void copyThenCompare(SchemaTupleFactory tf) throws ExecException {
        SchemaTuple<?> st = tf.newTuple();
        SchemaTuple<?> st2 = tf.newTuple();
        fillWithData(st);
        st2.set(st);
        assertTrue(st.equals(st2));
        assertEquals(st.compareTo(st2), 0);
        st.set(0, null);
        assertFalse(st.equals(st2));
        assertEquals(st.compareTo(st2), -1);
        assertTrue(st.isNull(0));
        st2.set(0, null);
        assertTrue(st.equals(st2));
        assertEquals(st.compareTo(st2), 0);
    }

    /**
     * This ensures that a fresh Tuple out of a TupleFactory
     * will be full of null fields.
     * @param   tf a TupleFactory
     * @throws  ExecException
     */
    private void shouldAllBeNull(SchemaTupleFactory tf) throws ExecException {
        Tuple t = tf.newTuple();
        for (Object o : t) {
            assertNull(o);
        }
        for (int i = 0; i < t.size(); i++) {
            assertNull(t.get(i));
            assertTrue(t.isNull(i));
        }
    }

    private void fillWithData(SchemaTuple<?> st) throws ExecException {
        Schema udfSchema = st.getSchema();
        int pos = 0;
        for (FieldSchema fs : udfSchema.getFields()) {
            Object val;
            if (fs.type == DataType.TUPLE) {
                val = SchemaTupleFactory
                            .getInstance(fs.schema, false, GenContext.FORCE_LOAD)
                            .newTuple();
                fillWithData((SchemaTuple<?>)val);
            } else {
                val = randData(fs);
            }
            st.set(pos++, val);
        }
    }

    private Random r = new Random(100L);

    private Object randData(FieldSchema fs) throws ExecException {
        switch (fs.type) {
        case DataType.BIGDECIMAL: return new BigDecimal(r.nextDouble());
        case DataType.BIGINTEGER: return new BigInteger(130, r);
        case DataType.BOOLEAN: return r.nextBoolean();
        case DataType.BYTEARRAY: return new DataByteArray(new BigInteger(130, r).toByteArray());
        case DataType.CHARARRAY: return new BigInteger(130, r).toString(32);
        case DataType.INTEGER: return r.nextInt();
        case DataType.LONG: return r.nextLong();
        case DataType.FLOAT: return r.nextFloat();
        case DataType.DOUBLE: return r.nextDouble();
        case DataType.DATETIME: return new DateTime(r.nextLong());
        case DataType.BAG:
            DataBag db = BagFactory.getInstance().newDefaultBag();
            int sz = r.nextInt(100);
            for (int i = 0; i < sz; i++) {
                int tSz = r.nextInt(10);
                Tuple t = TupleFactory.getInstance().newTuple(tSz);
                for (int j = 0; j < tSz; j++) {
                    t.set(j, r.nextInt());
                }
                db.add(t);
            }
            return db;
        case DataType.MAP:
            Map<String, Object> map = Maps.newHashMap();
            int keys = r.nextInt(10);
            for (int i = 0; i < keys; i++) {
                String key = (String) randData(new FieldSchema(null, DataType.CHARARRAY));
                int values = r.nextInt(100);
                for (int j = 0; j < values; j++) {
                    int tSz = r.nextInt(10);
                    Tuple t = TupleFactory.getInstance().newTuple(tSz);
                    for (int k = 0; k < tSz; k++) {
                        t.set(k, r.nextInt());
                    }
                    map.put(key,  t);
                }
            }
            return map;
        default: throw new RuntimeException("Cannot generate data for given FieldSchema: " + fs);
        }
    }

    public void testTypeAwareGetSetting(TupleFactory tf) throws ExecException {
        SchemaTuple<?> st = (SchemaTuple<?>)tf.newTuple();
        checkNullGetThrowsError(st);
    }

    private void checkNullGetThrowsError(SchemaTuple<?> st) throws ExecException {
        Schema schema = st.getSchema();
        int i = 0;
        for (Schema.FieldSchema fs : schema.getFields()) {
            boolean fieldIsNull = false;
            try {
                switch (fs.type) {
                case DataType.BIGDECIMAL: st.getBigDecimal(i); break;
                case DataType.BIGINTEGER: st.getBigInteger(i); break;
                case DataType.BOOLEAN: st.getBoolean(i); break;
                case DataType.BYTEARRAY: st.getBytes(i); break;
                case DataType.CHARARRAY: st.getString(i); break;
                case DataType.INTEGER: st.getInt(i); break;
                case DataType.LONG: st.getLong(i); break;
                case DataType.FLOAT: st.getFloat(i); break;
                case DataType.DOUBLE: st.getDouble(i); break;
                case DataType.DATETIME: st.getDateTime(i); break;
                case DataType.TUPLE: st.getTuple(i); break;
                case DataType.BAG: st.getDataBag(i); break;
                case DataType.MAP: st.getMap(i); break;
                default: throw new RuntimeException("Unsupported FieldSchema in SchemaTuple: " + fs);
                }
            } catch (FieldIsNullException e) {
                fieldIsNull = true;
            }
            assertTrue(fieldIsNull);
            i++;
        }
    }

    /**
     * This tests that InterStorage will work as expected with the Tuples generated by
     * the given TupleFactory.
     * @param tf
     * @throws Exception
     */
    public void testInterStorageSerDe(SchemaTupleFactory tf) throws Exception {
        int sz = 4096;
        List<Tuple> written = new ArrayList<Tuple>(sz);

        File temp = File.createTempFile("tmp", "tmp");
        temp.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(temp);
        DataOutputStream dos = new DataOutputStream(fos);

        InterRecordWriter writer = new InterRecordWriter(dos);

        // We add these lines because a part of the InterStorage logic
        // is the ability to seek to the next Tuple based on a magic set
        // of bytes. This emulates the random byes that will be present
        // at the beginning of a split.
        dos.writeByte(r.nextInt());
        dos.writeByte(r.nextInt());
        dos.writeByte(r.nextInt());
        dos.writeByte(r.nextInt());
        dos.writeByte(r.nextInt());
        dos.writeByte(r.nextInt());

        for (int i = 0; i < sz; i++) {
            SchemaTuple<?> st = (SchemaTuple<?>)tf.newTuple();
            fillWithData(st);
            writer.write(null, st);
            written.add(st);

            dos.writeByte(r.nextInt());
            dos.writeByte(r.nextInt());
            dos.writeByte(r.nextInt());
        }
        writer.close(null);

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");

        TaskAttemptID taskId = HadoopShims.createTaskAttemptID("jt", 1, true, 1, 1);
        conf.set(MRConfiguration.TASK_ID, taskId.toString());

        InputSplit is = new FileSplit(new Path(temp.getAbsolutePath()), 0, temp.length(), null);

        InterRecordReader reader = new InterRecordReader();
        reader.initialize(is, HadoopShims.createTaskAttemptContext(conf, taskId));

        for (int i = 0; i < sz; i++) {
            assertTrue(reader.nextKeyValue());
            SchemaTuple<?> st = (SchemaTuple<?>)reader.getCurrentValue();
            assertEquals(written.get(i), st);
        }
        reader.close();

    }

    public void testSerDe(SchemaTupleFactory tf) throws IOException {
        int sz = 4096;
        List<Tuple> written = new ArrayList<Tuple>(sz);

        File temp = File.createTempFile("tmp", "tmp");
        temp.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(temp);
        DataOutput dos = new DataOutputStream(fos);

        for (int i = 0; i < sz; i++) {
            SchemaTuple<?> st = (SchemaTuple<?>)tf.newTuple();
            fillWithData(st);
            bis.writeDatum(dos, st);
            written.add(st);
        }
        fos.close();

        assertEquals(sz, written.size());

        FileInputStream fis = new FileInputStream(temp);
        DataInput din = new DataInputStream(fis);
        for (int i = 0; i < sz; i++) {
            SchemaTuple<?> st = (SchemaTuple<?>)bis.readDatum(din);
            assertEquals(written.get(i), st);
        }
        fis.close();
    }

    @Test
    public void testFRJoinWithSchemaTuple() throws Exception {
        testJoinType("replicated", false);
    }

    @Test
    public void testMergeJoinWithSchemaTuple() throws Exception {
        testJoinType("merge", true);
    }

    public void testJoinType(String joinType, boolean preSort) throws Exception {
        Properties props = PropertiesUtil.loadDefaultProperties();
        props.setProperty("pig.schematuple", "true");
        PigServer pigServer = new PigServer(ExecType.LOCAL, props);

        Data data = resetData(pigServer);

        data.set("foo1",
            tuple(0, 0),
            tuple(1, 1),
            tuple(2, 2),
            tuple(3, 3),
            tuple(4, 4),
            tuple(5, 5),
            tuple(6, 6),
            tuple(7, 7),
            tuple(8, 8),
            tuple(9, 9)
            );

        data.set("foo2",
            tuple(0, 0),
            tuple(1, 1),
            tuple(2, 2),
            tuple(3, 3),
            tuple(4, 4),
            tuple(5, 5),
            tuple(6, 6),
            tuple(7, 7),
            tuple(8, 8),
            tuple(9, 9)
            );

        pigServer.registerQuery("A = LOAD 'foo1' USING mock.Storage() as (x:int, y:int);");
        pigServer.registerQuery("B = LOAD 'foo2' USING mock.Storage() as (x:int, y:int);");
        if (preSort) {
            pigServer.registerQuery("A = ORDER A BY x ASC;");
            pigServer.registerQuery("B = ORDER B BY x ASC;");
        }
        pigServer.registerQuery("C = JOIN A by x, B by x using '"+joinType+"';");
        pigServer.registerQuery("D = ORDER C BY $0 ASC;");

        Iterator<Tuple> out = pigServer.openIterator("D");
        for (int i = 0; i < 10; i++) {
            if (!out.hasNext()) {
                throw new Exception("Output should have had more elements! Failed on element: " + i);
            }
            assertEquals(tuple(i, i, i, i), out.next());
        }
        assertFalse(out.hasNext());

        pigServer.registerQuery("STORE D INTO 'bar1' USING mock.Storage();");
        pigServer.registerQuery("E = JOIN A by (x, y),  B by (x, y) using '"+joinType+"';");
        pigServer.registerQuery("F = ORDER E BY $0 ASC;");
        pigServer.registerQuery("STORE F INTO 'bar2' USING mock.Storage();");

        List<Tuple> bar1 = data.get("bar1");
        List<Tuple> bar2 = data.get("bar2");

        assertEquals("Output does not have enough elements! List: " + bar1, 10, bar1.size());
        assertEquals("Output does not have enough elements! List: " + bar2, 10, bar2.size());

        for (int i = 0; i < 10; i++) {
            assertEquals(tuple(i, i, i, i), bar1.get(i));
            assertEquals(tuple(i, i, i, i), bar2.get(i));
        }

    }

    public void testHashCodeEqualAsNormalTuple(SchemaTupleFactory tf) {
        SchemaTuple<?> st = tf.newTuple();
        Tuple t = TupleFactory.getInstance().newTuple();
        for (Object o : st) {
            t.append(o);
        }
        assertEquals(t.hashCode(), st.hashCode());
        assertEquals(st, t);
        assertEquals(t, st);
    }

}
