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
package org.apache.pig;

import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;


public interface LoadFunc {
	/**
	 * This interface is used to implement functions to parse records
	 * from a dataset.
	 * 
	 * @author database-systems@research.yahoo
	 *
	 */
	/**
	 * Specifies a portion of an InputStream to read tuples. Because the
	 * starting and ending offsets may not be on record boundaries it is up to
	 * the implementor to deal with figuring out the actual starting and ending
	 * offsets in such a way that an arbitrarily sliced up file will be processed
	 * in its entirety.
	 * <p>
	 * A common way of handling slices in the middle of records is to start at
	 * the given offset and, if the offset is not zero, skip to the end of the
	 * first record (which may be a partial record) before reading tuples.
	 * Reading continues until a tuple has been read that ends at an offset past
	 * the ending offset.
	 * <p>
	 * <b>The load function should not do any buffering on the input stream</b>. Buffering will
	 * cause the offsets returned by is.getPos() to be unreliable.
	 *  
	 * @param fileName the name of the file to be read
	 * @param is the stream representing the file to be processed, and which can also provide its position.
	 * @param offset the offset to start reading tuples.
	 * @param end the ending offset for reading.
	 * @throws IOException
	 */
	public abstract void bindTo(String fileName, BufferedPositionedInputStream is, long offset, long end) throws IOException;
	/**
	 * Retrieves the next tuple to be processed.
	 * @return the next tuple to be processed or null if there are no more tuples
	 * to be processed.
	 * @throws IOException
	 */
	public abstract Tuple getNext() throws IOException;
	
}
