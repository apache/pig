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
package org.apache.hadoop.owl.backend;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.owl.common.ErrorType;
import org.apache.hadoop.owl.common.LogHandler;
import org.apache.hadoop.owl.common.OwlException;
import org.apache.hadoop.owl.common.OwlUtil;
import org.apache.hadoop.owl.entity.PartitionKeyEntity;
import org.apache.hadoop.owl.protocol.OwlPartitionKey;

/** Class which provides utility functions for interval data type support */ 
public class IntervalUtil {

    /**
     * Parses the date string.
     * 
     * @param dateString the date string
     * @return the long value for the date
     * @throws OwlException the owl exception
     */
    public static Long parseDateString(String dateString) throws OwlException {
        try {
            DateFormat dfm = new SimpleDateFormat(OwlUtil.DATE_FORMAT);
            return Long.valueOf(dfm.parse(dateString).getTime());
        }
        catch(Exception e) {
            throw new OwlException(ErrorType.ERROR_DATE_FORMAT, e);
        }
    }

    /**
     * Gets the number of seconds in each interval unit for specified interval type. For "2 minutes", it should return 120.
     * 
     * @param partKey the partition key
     * @return the number of seconds in the interval
     * @throws OwlException the owl exception
     */
    public static long getIntervalSeconds(PartitionKeyEntity partKey) throws OwlException {
        OwlPartitionKey.IntervalFrequencyUnit freqUnit = OwlPartitionKey.IntervalFrequencyUnit.fromCode(
                partKey.getIntervalFrequencyUnit().intValue());

        switch( freqUnit ) {
        case SECONDS :
            return partKey.getIntervalFrequency().intValue();
        case MINUTES :
            return partKey.getIntervalFrequency().intValue() * 60;
        case HOURS :
            return partKey.getIntervalFrequency().intValue() * 60 * 60;
        }

        throw new OwlException(ErrorType.ERROR_UNKNOWN_ENUM_TYPE, "IntervalFrequencyUnit " + freqUnit);
    }

    /**
     * Gets the interval offset value for the specified value of date.
     * 
     * @param partKey the partition key
     * @param stringValue the string value
     * @return the interval offset
     * @throws OwlException the owl exception
     */
    public static int getIntervalOffset(PartitionKeyEntity partKey, String stringValue) throws OwlException {
        Long longValue = parseDateString(stringValue);
        Long intervalStart = partKey.getIntervalStart();

        if( longValue.longValue() < intervalStart.longValue() ) {
            throw new OwlException(ErrorType.ERROR_DATESTAMP_VALUE, "Key <" + partKey.getName() + "> value <" + stringValue + ">");
        }

        long secondsDifference = (longValue.longValue() - intervalStart.longValue()) / 1000;
        int intervalOffset = (int) (secondsDifference / getIntervalSeconds(partKey));

        LogHandler.getLogger("server").debug("Interval offset " + intervalOffset + " for " + stringValue +
                ", intervalStart " + new Date(intervalStart.longValue()));
        return intervalOffset;
    }

    /**
     * Gets the start time from specified offset value.
     * 
     * @param partKey the partition key
     * @param offset the offset value
     * @return the interval start time as a long value 
     * @throws OwlException the owl exception
     */
    public static Long getTimeFromOffset(PartitionKeyEntity partKey, int offset) throws OwlException { 
        Long intervalStart = partKey.getIntervalStart();

        long offsetSeconds = offset * getIntervalSeconds(partKey);
        return Long.valueOf(intervalStart.longValue() + offsetSeconds * 1000);
    }

    /**
     * Checks if the specified value is exactly same as an interval start time.
     * 
     * @param partKey the partition key
     * @param stringValue the string value
     * @return true, if time is exactly same as an interval start time
     * @throws OwlException the owl exception
     */
    public static boolean isIntervalStart(PartitionKeyEntity partKey, String stringValue) throws OwlException {
        Long longValue = parseDateString(stringValue);

        //Find the start time for the interval in which given time belongs
        int offset = getIntervalOffset(partKey, stringValue);
        Long intervalStart = getTimeFromOffset(partKey, offset);

        return longValue.longValue() == intervalStart.longValue();
    }
}
