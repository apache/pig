/**
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

package org.apache.hadoop.zebra.types;

import java.io.StringReader;
import junit.framework.Assert;

import org.apache.hadoop.zebra.schema.ColumnType;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.parser.TableSchemaParser;
import org.apache.hadoop.zebra.schema.Schema.ColumnSchema;
import org.junit.Test;

public class TestSchemaCollection {
  @Test
  public void testSchemaValid1() throws ParseException {
    String strSch = "c1:collection(record(f1:int, f2:int)), c2:collection(record(c3:collection(record(f3:float, f4))))";
    TableSchemaParser parser;
    Schema schema;

    parser = new TableSchemaParser(new StringReader(strSch));
    schema = parser.RecordSchema(null);
    System.out.println(schema);

    // test 1st level schema;
    ColumnSchema f1 = schema.getColumn(0);
    Assert.assertEquals("c1", f1.getName());
    Assert.assertEquals(ColumnType.COLLECTION, f1.getType());

    ColumnSchema f2 = schema.getColumn(1);
    Assert.assertEquals("c2", f2.getName());
    Assert.assertEquals(ColumnType.COLLECTION, f2.getType());

    // test 2nd level schema;
    Schema f1Schema = f1.getSchema();
    ColumnSchema f11 = f1Schema.getColumn(0).getSchema().getColumn(0);
    Assert.assertEquals("f1", f11.getName());
    Assert.assertEquals(ColumnType.INT, f11.getType());
    ColumnSchema f12 = f1Schema.getColumn(0).getSchema().getColumn(1);
    Assert.assertEquals("f2", f12.getName());
    Assert.assertEquals(ColumnType.INT, f12.getType());

    Schema f2Schema = f2.getSchema();
    ColumnSchema f21 = f2Schema.getColumn(0).getSchema().getColumn(0);
    Assert.assertEquals("c3", f21.getName());
    Assert.assertEquals(ColumnType.COLLECTION, f21.getType());

    // test 3rd level schema;
    Schema f21Schema = f21.getSchema();
    ColumnSchema f211 = f21Schema.getColumn(0).getSchema().getColumn(0);
    Assert.assertEquals("f3", f211.getName());
    Assert.assertEquals(ColumnType.FLOAT, f211.getType());
    ColumnSchema f212 = f21Schema.getColumn(0).getSchema().getColumn(1);
    Assert.assertEquals("f4", f212.getName());
    Assert.assertEquals(ColumnType.BYTES, f212.getType());
  }

  @Test
  public void testSchemaInvalid1() throws ParseException, Exception {
    try {
      String strSch = "c1:collection(record(f1:int, f2:int)), c2:collection(record(c3:collection(record(f3:float, f4)))))";
      TableSchemaParser parser;
      Schema schema;

      parser = new TableSchemaParser(new StringReader(strSch));
      schema = parser.RecordSchema(null);
      System.out.println(schema);
    } catch (Exception e) {
      String errMsg = e.getMessage();
      String str = "Encountered \" \")\" \") \"\" at line 1, column 98.";
      System.out.println(errMsg);
      System.out.println(str);
      Assert.assertEquals(errMsg.startsWith(str), true);
    }
  }
  
  @Test
  public void testInvalidCollectionSchema() throws ParseException {
	    String[] strSchs = { "c0:int, c1:collection(f1:int, f2:string)",
	    		             "c0:int, c1:collection(f1:int)",
	    		             "c0:int, c1:collection(int)" };
	    for( String strSch : strSchs ) {
		    TableSchemaParser parser = new TableSchemaParser( new StringReader( strSch ) );
		    try {
		    	parser.RecordSchema(null);
		    } catch(ParseException ex) {
		    	System.out.println( "Catch expected exception for schema: " + strSch );
		    	
		    	continue;
		    }
		    Assert.fail( "Exception expected for invalid schemes: "+  strSch );
	    }
  }
  
//  private void printSchema(Schema schema, int indent) {
//	    for( int i = 0; i < schema.getNumColumns(); i++ ) {
//	    	ColumnSchema cs = schema.getColumn( i );
//	    	for( int j = 0; j < indent; j++ )
//	    		System.out.print( "\t" );
//	    	System.out.println( cs.getName() + ": " + cs.getType().toString() );
//	    	if( cs.getSchema() != null )
//	    		printSchema( cs.getSchema(), indent + 1 );
//	    }
//	  
//  }
  
}
