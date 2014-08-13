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
package org.apache.pig.piggybank.evaluation.datetime.truncate;

import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.apache.pig.backend.executionengine.ExecException;

/**
 * ISOHelper provides helper methods for the other classes in this package.
 *
 * <ul>
 * <li>Jodatime: http://joda-time.sourceforge.net/</li>
 * <li>ISO8601 Date Format: http://en.wikipedia.org/wiki/ISO_8601</li>
 * </ul>
 * This class is public so that it can be tested in TestTruncateDateTime. 
 * Otherwise, it would have "package" visibility.
 */
public class ISOHelper {

	/**
	 * Default time zone for use in parsing, regardless of System or JDK's
	 * default time zone.
	 */
	public static final DateTimeZone DEFAULT_DATE_TIME_ZONE = DateTimeZone.UTC;

    /**
	 * @param input a non-null, non-empty Tuple,
	 *  whose first element is a ISO 8601 string representation of a date, time, or dateTime;
	 *  with optional time zone.
	 * @return a DateTime representing the date, 
	 *  with DateTimeZone set to the time zone parsed from the string,
	 *  or else DateTimeZone.defaultTimeZone() if one is set,
	 *  or else UTC.
	 * @throws ExecException if input is a malformed or empty tuple.
	 * This method is public so that it can be tested in TestTruncateDateTime. 
	 * Otherwise, it would have "package" visibility.
	 */
	public static DateTime parseDateTime(Tuple input) throws ExecException {	
	        
	    // Save previous default time zone for restore later.
	    DateTimeZone previousDefaultTimeZone = DateTimeZone.getDefault();

	    // Temporarily set default time zone to UTC, for this parse.
	    DateTimeZone.setDefault(DEFAULT_DATE_TIME_ZONE);

	    String isoDateString = input.get(0).toString();
	    DateTime dt = ISODateTimeFormat.dateTimeParser().withOffsetParsed().parseDateTime(isoDateString);			

	    // restore previous default TimeZone.
	    DateTimeZone.setDefault(previousDefaultTimeZone);

	    return dt;
	}
	
}

