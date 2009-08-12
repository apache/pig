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

package org.apache.pig.impl.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigWarning;

public class CastUtils {

	private static Integer mMaxInt = Integer.valueOf(Integer.MAX_VALUE);

	private static Long mMaxLong = Long.valueOf(Long.MAX_VALUE);
	
	protected static final Log mLog = LogFactory.getLog(CastUtils.class);


	public static Double stringToDouble(String str) {
		if (str == null) {
			return null;
		} else {
			try {
				return Double.parseDouble(str);
			} catch (NumberFormatException e) {
				LogUtils
						.warn(
								CastUtils.class,
								"Unable to interpret value "
										+ str
										+ " in field being "
										+ "converted to double, caught NumberFormatException <"
										+ e.getMessage() + "> field discarded",
								PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED,
								mLog);
				return null;
			}
		}
	}

	public static Float stringToFloat(String str) {
		if (str == null) {
			return null;
		} else {
			try {
				return Float.parseFloat(str);
			} catch (NumberFormatException e) {
				LogUtils
						.warn(
								CastUtils.class,
								"Unable to interpret value "
										+ str
										+ " in field being "
										+ "converted to float, caught NumberFormatException <"
										+ e.getMessage() + "> field discarded",
								PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED,
								mLog);
				return null;
			}
		}
	}

	public static Integer stringToInteger(String str) {
		if (str == null) {
			return null;
		} else {
			try {
				return Integer.parseInt(str);
			} catch (NumberFormatException e) {
				// It's possible that this field can be interpreted as a double.
				// Unfortunately Java doesn't handle this in Integer.valueOf. So
				// we need to try to convert it to a double and if that works
				// then
				// go to an int.
				try {
					Double d = Double.valueOf(str);
					// Need to check for an overflow error
					if (d.doubleValue() > mMaxInt.doubleValue() + 1.0) {
						LogUtils.warn(CastUtils.class, "Value " + d
								+ " too large for integer",
								PigWarning.TOO_LARGE_FOR_INT, mLog);
						return null;
					}
					return Integer.valueOf(d.intValue());
				} catch (NumberFormatException nfe2) {
					LogUtils
							.warn(
									CastUtils.class,
									"Unable to interpret value "
											+ str
											+ " in field being "
											+ "converted to int, caught NumberFormatException <"
											+ e.getMessage()
											+ "> field discarded",
									PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED,
									mLog);
					return null;
				}
			}
		}
	}

	public static Long stringToLong(String str) {
		if (str == null) {
			return null;
		} else {
			try {
				return Long.parseLong(str);
			} catch (NumberFormatException e) {
				// It's possible that this field can be interpreted as a double.
				// Unfortunately Java doesn't handle this in Long.valueOf. So
				// we need to try to convert it to a double and if that works
				// then
				// go to an long.
				try {
					Double d = Double.valueOf(str);
					// Need to check for an overflow error
					if (d.doubleValue() > mMaxLong.doubleValue() + 1.0) {
						LogUtils.warn(CastUtils.class, "Value " + d
								+ " too large for long",
								PigWarning.TOO_LARGE_FOR_INT, mLog);
						return null;
					}
					return Long.valueOf(d.longValue());
				} catch (NumberFormatException nfe2) {
					LogUtils
							.warn(
									CastUtils.class,
									"Unable to interpret value "
											+ str
											+ " in field being "
											+ "converted to long, caught NumberFormatException <"
											+ nfe2.getMessage()
											+ "> field discarded",
									PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED,
									mLog);
					return null;
				}
			}
		}
	}

}
