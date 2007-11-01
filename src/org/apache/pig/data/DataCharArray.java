/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.data;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

/**
 * Indicates an character array (string) datum.
 */

public abstract class DataCharArray extends AtomicDatum {

public enum Encoding { UTF16, NONE };

public static final byte ENC_UTF16 = 0x1;
public static final byte ENC_NONE  = 0x2;

public DataCharArray(Encoding enc) { mEncoding = enc; }

public DataType getType() { return Datum.DataType.CHARARRAY; }

public Encoding getEncoding() { return mEncoding; }

static DataCharArray read(DataInput in) throws IOException
{
	// Assumes that the float indicator has already been read in order to
	// select his function.
	// Read encoding and create appropriate type.
	byte[] b = new byte[1];
	in.readFully(b);
	switch (b[0]) {
	case ENC_UTF16:
		return DataCharArrayUtf16.read(in);

	case ENC_NONE:
		return DataCharArrayNone.read(in);

	default:
		throw new AssertionError("Unknown encoding type " + b[0]);
	}
}

private Encoding mEncoding;

}

