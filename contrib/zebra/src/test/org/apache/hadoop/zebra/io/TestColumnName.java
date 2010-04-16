/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.zebra.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.zebra.io.BasicTable;
import org.apache.hadoop.zebra.io.TableInserter;
import org.apache.hadoop.zebra.io.TableScanner;
import org.apache.hadoop.zebra.io.BasicTable.Reader.RangeSplit;
import org.apache.hadoop.zebra.parser.ParseException;
import org.apache.hadoop.zebra.schema.Schema;
import org.apache.hadoop.zebra.types.TypesUtils;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * Test conventions for column names. Specifically, '_' is allowed as leading character for map keys,
 * but it's disallowed for other fields.
 * 
 */
public class TestColumnName {
	final static String STR_SCHEMA = 
		"f1:bool, r:record(f11:int, f12:long), m:map(string), c:collection(record(f13:double, f14:double, f15:bytes))";
	final static String STR_STORAGE = "[r.f12, f1, m#{b}]; [m#{_a}, r.f11]";

	final static String INVALID_STR_SCHEMA = 
		"_f1:bool, _r:record(f11:int, _f12:long), _m:map(string), _c:collection(record(_f13:double, _f14:double, _f15:bytes))";
	final static String INVALID_STR_STORAGE = "[_r.f12, _f1, _m#{b}]; [_m#{_a}, _r.f11]";

	private static Configuration conf = new Configuration();
	private static FileSystem fs = new LocalFileSystem( new RawLocalFileSystem() );
	private static Path path = new Path( fs.getWorkingDirectory(), "TestColumnName" );
	static {
		conf.setInt("table.output.tfile.minBlock.size", 64 * 1024);
		conf.setInt("table.input.split.minSize", 64 * 1024);
		conf.set("table.output.tfile.compression", "none");
	}

	@BeforeClass
	public static void setUp() throws IOException {
		// drop any previous tables
		BasicTable.drop( path, conf );

		BasicTable.Writer writer = new BasicTable.Writer( path, STR_SCHEMA, STR_STORAGE, conf );
		writer.finish();

		Schema schema = writer.getSchema();
		Tuple tuple = TypesUtils.createTuple( schema );

		BasicTable.Writer writer1 = new BasicTable.Writer(path, conf);
		int part = 0;
		TableInserter inserter = writer1.getInserter("part" + part, true);
		TypesUtils.resetTuple(tuple);

		tuple.set(0, true);

		Tuple tupRecord;
		try {
			tupRecord = TypesUtils.createTuple(schema.getColumnSchema("r")
					.getSchema());
		} catch (ParseException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
		tupRecord.set(0, 1);
		tupRecord.set(1, 1001L);
		tuple.set(1, tupRecord);

		Map<String, String> map = new HashMap<String, String>();
		map.put("_a", "x");
		map.put("b", "y");
		map.put("c", "z");
		tuple.set(2, map);

		DataBag bagColl = TypesUtils.createBag();
		Schema schColl = schema.getColumn(3).getSchema().getColumn(0).getSchema();
		Tuple tupColl1 = TypesUtils.createTuple(schColl);
		Tuple tupColl2 = TypesUtils.createTuple(schColl);
		byte[] abs1 = new byte[3];
		byte[] abs2 = new byte[4];
		tupColl1.set(0, 3.1415926);
		tupColl1.set(1, 1.6);
		abs1[0] = 11;
		abs1[1] = 12;
		abs1[2] = 13;
		tupColl1.set(2, new DataByteArray(abs1));
		bagColl.add(tupColl1);
		tupColl2.set(0, 123.456789);
		tupColl2.set(1, 100);
		abs2[0] = 21;
		abs2[1] = 22;
		abs2[2] = 23;
		abs2[3] = 24;
		tupColl2.set(2, new DataByteArray(abs2));
		bagColl.add(tupColl2);
		tuple.set(3, bagColl);

		int row = 0;
		inserter.insert(new BytesWritable(String.format("k%d%d", part + 1, row + 1)
				.getBytes()), tuple);
		inserter.close();
		writer1.finish();

		writer.close();
	}

	@AfterClass
	public static void tearDownOnce() throws IOException {
	}

	@Test
	public void testInvalidCase() throws IOException {
		Path p = new Path( fs.getWorkingDirectory(), "TestColumnNameInvalid" );
		BasicTable.drop( p, conf );

		try {
			BasicTable.Writer writer = new BasicTable.Writer( p, INVALID_STR_SCHEMA, INVALID_STR_STORAGE, conf );
			writer.finish();
		} catch(IOException ex) {
			// Do nothing. This is expected.
			return;
		}

		Assert.assertTrue( false ); // Test failure.
	}

	@Test
	public void testRead() throws IOException, ParseException {
		String projection = new String("f1, m#{_a|b}, r, m#{c}");
		BasicTable.Reader reader = new BasicTable.Reader(path, conf);
		reader.setProjection(projection);
		// long totalBytes = reader.getStatus().getSize();

		List<RangeSplit> splits = reader.rangeSplit(1);
		reader.close();
		reader = new BasicTable.Reader(path, conf);
		reader.setProjection(projection);
		TableScanner scanner = reader.getScanner(splits.get(0), true);
		BytesWritable key = new BytesWritable();
		Tuple value = TypesUtils.createTuple(scanner.getSchema());

		scanner.getKey(key);
		Assert.assertEquals(key, new BytesWritable("k11".getBytes()));
		scanner.getValue(value);

		Tuple recordTuple = (Tuple) value.get(2);
		Assert.assertEquals(1, recordTuple.get(0));
		Assert.assertEquals(1001L, recordTuple.get(1));
		Assert.assertEquals(true, value.get(0));

		HashMap<String, Object> mapval = (HashMap<String, Object>) value.get(1);
		Assert.assertEquals("x", mapval.get("_a"));
		Assert.assertEquals("y", mapval.get("b"));
		Assert.assertEquals(null, mapval.get("c"));
		mapval = (HashMap<String, Object>) value.get(3);
		Assert.assertEquals("z", mapval.get("c"));
		Assert.assertEquals(null, mapval.get("_a"));
		Assert.assertEquals(null, mapval.get("b"));
		reader.close();
	}

	@Test
	public void testProjectionParsing() throws IOException, ParseException {
		String projection = new String( "f1, m#{_a}, _r, m#{c}, m" );
		BasicTable.Reader reader = new BasicTable.Reader( path, conf );
		try {
			reader.setProjection( projection );
			reader.close();
		} catch(ParseException ex) {
			// Expected.
			return;
		}
		
		Assert.assertTrue( false );
	}
	
}
