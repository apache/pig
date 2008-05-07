package org.apache.pig.test;

import java.util.* ;

import org.apache.pig.data.* ;
import org.apache.pig.impl.logicalLayer.schema.* ;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import org.junit.* ;

import junit.framework.Assert;
import junit.framework.TestCase ;

public class TestSchema extends TestCase {
    
    @Test
    public void testSchemaEqual1() {
        
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>() ;
        innerList1.add(new FieldSchema("11a", DataType.INTEGER)) ;
        innerList1.add(new FieldSchema("11b", DataType.LONG)) ;
        
        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>() ;
        innerList2.add(new FieldSchema("11a", DataType.INTEGER)) ;
        innerList2.add(new FieldSchema("11b", DataType.LONG)) ;
        
        Schema innerSchema1 = new Schema(innerList1) ;
        Schema innerSchema2 = new Schema(innerList2) ;
                
        List<FieldSchema> list1 = new ArrayList<FieldSchema>() ;
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY)) ;
        list1.add(new FieldSchema("1b", innerSchema1)) ;
        list1.add(new FieldSchema("1c", DataType.INTEGER)) ;
        
        List<FieldSchema> list2 = new ArrayList<FieldSchema>() ;
        list2.add(new FieldSchema("1a", DataType.BYTEARRAY)) ;
        list2.add(new FieldSchema("1b", innerSchema2)) ;
        list2.add(new FieldSchema("1c", DataType.INTEGER)) ;
        
        Schema schema1 = new Schema(list1) ;
        Schema schema2 = new Schema(list2) ;
        
        Assert.assertTrue(Schema.equals(schema1, schema2, false, false)) ;
        
        innerList2.get(1).alias = "pi" ;
        
        Assert.assertFalse(Schema.equals(schema1, schema2, false, false)) ;
        Assert.assertTrue(Schema.equals(schema1, schema2, false, true)) ;
        
        innerList2.get(1).alias = "11b" ;
        innerList2.get(1).type = DataType.BYTEARRAY ;
        
        Assert.assertFalse(Schema.equals(schema1, schema2, false, false)) ;
        Assert.assertTrue(Schema.equals(schema1, schema2, true, false)) ;
        
        innerList2.get(1).type = DataType.LONG ;
        
        Assert.assertTrue(Schema.equals(schema1, schema2, false, false)) ;
        
        list2.get(0).type = DataType.CHARARRAY ;
        Assert.assertFalse(Schema.equals(schema1, schema2, false, false)) ;
    }
    
    @Test
    public void testMerge1() {
        
        // Generate two schemas
        List<FieldSchema> innerList1 = new ArrayList<FieldSchema>() ;
        innerList1.add(new FieldSchema("11a", DataType.INTEGER)) ; 
        innerList1.add(new FieldSchema("11b", DataType.FLOAT)) ;
        
        List<FieldSchema> innerList2 = new ArrayList<FieldSchema>() ;
        innerList2.add(new FieldSchema("22a", DataType.DOUBLE)) ;
        innerList2.add(new FieldSchema(null, DataType.LONG)) ;
        
        Schema innerSchema1 = new Schema(innerList1) ;
        Schema innerSchema2 = new Schema(innerList2) ;
                
        List<FieldSchema> list1 = new ArrayList<FieldSchema>() ;
        list1.add(new FieldSchema("1a", DataType.BYTEARRAY)) ;
        list1.add(new FieldSchema("1b", innerSchema1)) ;
        list1.add(new FieldSchema("1c", DataType.LONG)) ;
        
        List<FieldSchema> list2 = new ArrayList<FieldSchema>() ;
        list2.add(new FieldSchema("2a", DataType.BYTEARRAY)) ;
        list2.add(new FieldSchema("2b", innerSchema2)) ;
        list2.add(new FieldSchema("2c", DataType.INTEGER)) ;
        
        Schema schema1 = new Schema(list1) ;
        Schema schema2 = new Schema(list2) ;
        
        // Merge
        Schema mergedSchema = schema1.merge(schema2, true) ;
        
        
        // Generate expected schema
        List<FieldSchema> expectedInnerList = new ArrayList<FieldSchema>() ;
        expectedInnerList.add(new FieldSchema("22a", DataType.DOUBLE)) ;
        expectedInnerList.add(new FieldSchema("11b", DataType.FLOAT)) ;
        
        Schema expectedInner = new Schema(expectedInnerList) ;
        
        List<FieldSchema> expectedList = new ArrayList<FieldSchema>() ;
        expectedList.add(new FieldSchema("2a", DataType.BYTEARRAY)) ;
        expectedList.add(new FieldSchema("2b", expectedInner)) ;
        expectedList.add(new FieldSchema("2c", DataType.LONG)) ;
        
        Schema expected = new Schema(expectedList) ;
        
        // Compare
        Assert.assertTrue(Schema.equals(mergedSchema, expected, false, false)) ;
    }
}
