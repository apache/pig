package org.apache.pig.test;

import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaUtil;

public class TestSchemaUtil extends TestCase {

    public void testTupleSchema() {
        try {
            String tupleName = "mytuple";
            String[] fieldNames = new String[] { "field_0", "field_1" };
            Byte[] dataTypes = new Byte[] { DataType.LONG, DataType.CHARARRAY };

            String expected = "{mytuple: (field_0: long,field_1: chararray)}";
            Schema tupleSchema = SchemaUtil.newTupleSchema(tupleName,
                    fieldNames, dataTypes);
            assertEquals(expected, tupleSchema.toString());

            tupleSchema = SchemaUtil.newTupleSchema(tupleName, Arrays
                    .asList(fieldNames), Arrays.asList(dataTypes));
            assertEquals(expected, tupleSchema.toString());

            expected = "{t: (field_0: long,field_1: chararray)}";
            tupleSchema = SchemaUtil.newTupleSchema(fieldNames, dataTypes);
            assertEquals(expected, tupleSchema.toString());

            tupleSchema = SchemaUtil.newTupleSchema(Arrays.asList(fieldNames),
                    Arrays.asList(dataTypes));
            assertEquals(expected, tupleSchema.toString());

            expected = "{t: (f0: long,f1: chararray)}";
            tupleSchema = SchemaUtil.newTupleSchema(dataTypes);
            assertEquals(expected, tupleSchema.toString());

            tupleSchema = SchemaUtil.newTupleSchema(Arrays.asList(dataTypes));
            assertEquals(expected, tupleSchema.toString());
        } catch (FrontendException e) {
            fail();
        }
    }

    public void testBagSchema() {
        try {
            String bagName="mybag";
            String tupleName = "mytuple";
            String[] fieldNames = new String[] { "field_0", "field_1" };
            Byte[] dataTypes = new Byte[] { DataType.LONG, DataType.CHARARRAY };

            String expected = "{mybag: {mytuple: (field_0: long,field_1: chararray)}}";
            Schema bagSchema = SchemaUtil.newBagSchema(bagName,tupleName,
                    fieldNames, dataTypes);
            assertEquals(expected, bagSchema.toString());

            bagSchema = SchemaUtil.newBagSchema(bagName,tupleName, Arrays
                    .asList(fieldNames), Arrays.asList(dataTypes));
            assertEquals(expected, bagSchema.toString());

            expected = "{b: {t: (field_0: long,field_1: chararray)}}";
            bagSchema = SchemaUtil.newBagSchema(fieldNames, dataTypes);
            assertEquals(expected, bagSchema.toString());

            bagSchema = SchemaUtil.newBagSchema(Arrays.asList(fieldNames),
                    Arrays.asList(dataTypes));
            assertEquals(expected, bagSchema.toString());

            expected = "{b: {t: (f0: long,f1: chararray)}}";
            bagSchema = SchemaUtil.newBagSchema(dataTypes);
            assertEquals(expected, bagSchema.toString());

            bagSchema = SchemaUtil.newBagSchema(Arrays.asList(dataTypes));
            assertEquals(expected, bagSchema.toString());
        } catch (FrontendException e) {
            fail();
        }
    }
}
