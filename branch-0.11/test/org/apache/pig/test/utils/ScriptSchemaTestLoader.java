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
package org.apache.pig.test.utils;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ResourceSchema;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.test.PigStorageWithSchema;

/**
 * 
 * Used to test that the LOLoad operator is setting the script schema correctly.
 * 
 */
public class ScriptSchemaTestLoader extends PigStorageWithSchema {

	// this method requires to be static because pig will instantiate the
	// instance
	// and the unit test will have no other way of directly reaching this
	// variable
	static volatile Schema scriptSchema;

	@Override
	public ResourceSchema getSchema(String location, Job job)
			throws IOException {

		scriptSchema = Utils.getScriptSchema(getUDFContextSignature(),
				job.getConfiguration());

		return null;
	}

	public static Schema getScriptSchema() {
		return scriptSchema;
	}

}
