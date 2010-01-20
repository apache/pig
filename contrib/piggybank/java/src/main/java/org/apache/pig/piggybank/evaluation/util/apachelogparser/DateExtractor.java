/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the
 * NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.pig.piggybank.evaluation.util.apachelogparser;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * DateExtractor has four different constructors which each allow for different functionality. The
 * incomingDateFormat ("dd/MMM/yyyy:HH:mm:ss Z" by default) is used to match the date string that gets passed in from the
 * log. The outgoingDateFormat ("yyyy-MM-dd" by default) is used to format the returned string.
 * 
 * Different constructors exist for each combination; please use the appropriate respective constructor.
 * 
 * Note that any data that exists in the SimpleDateFormat schema can be supported. For example, if you were
 * starting with the default incoming format and wanted to extract just the year, you would use the single
 * string constructor DateExtractor("yyyy").
 * 
 * From pig latin you will need to use aliases to use a non-default format, like
 * 
 * define MyDateExtractor org.apache.pig.piggybank.evaluation.util.apachelogparser.DateExtractor("yyyy-MM");
 * 
 * A = FOREACH row GENERATE DateExtractor(dayTime);
 * 
 * If a string cannot be parsed, null will be returned and an error message printed to stderr.
 *
 * By default, the DateExtractor uses the GMT timezone. You can use the three-parameter constructor to override the
 * timezone.
 */
public class DateExtractor extends EvalFunc<String> {
    private static String DEFAULT_INCOMING_DATE_FORMAT = "dd/MMM/yyyy:HH:mm:ss Z";
    private static String DEFAULT_OUTGOING_DATE_FORMAT = "yyyy-MM-dd";
    private static String DEFAULT_TZ_ID="GMT";
    private SimpleDateFormat incomingDateFormat;
    private SimpleDateFormat outgoingDateFormat;

    /**
     * forms the formats based on default incomingDateFormat and default outgoingDateFormat
     * 
     * @param outgoingDateString outgoingDateFormat is based on outgoingDateString
     */
    public DateExtractor() {
        this(DEFAULT_INCOMING_DATE_FORMAT, DEFAULT_OUTGOING_DATE_FORMAT, DEFAULT_TZ_ID);
    }

    /**
     * forms the formats based on passed outgoingDateString and the default incomingDateFormat
     * 
     * @param outgoingDateString outgoingDateFormat is based on outgoingDateString
     */
    public DateExtractor(String outgoingDateString) {
        this(DEFAULT_INCOMING_DATE_FORMAT, outgoingDateString, "GMT");
    }

    /**
     * forms the formats based on passed incomingDateString and outgoingDateString
     * 
     * @param incomingDateString incomingDateFormat is based on incomingDateString
     * @param outgoingDateString outgoingDateFormat is based on outgoingDateString
     * 
     */
    public DateExtractor(String incomingDateString, String outgoingDateString) {
        this(incomingDateString, outgoingDateString, DEFAULT_TZ_ID);
    }

    /**
     * forms the formats based on passed incomingDateString and outgoingDateString
     * 
     * @param incomingDateString incomingDateFormat is based on incomingDateString
     * @param outgoingDateString outgoingDateFormat is based on outgoingDateString
     * @param timeZoneID time zone id in which dates should be expressed.
     * 
     */
    public DateExtractor(String incomingDateString, String outgoingDateString, String timeZoneID) {
        TimeZone tz = TimeZone.getTimeZone(timeZoneID);
        incomingDateFormat = new SimpleDateFormat(incomingDateString);
        outgoingDateFormat = new SimpleDateFormat(outgoingDateString);
        incomingDateFormat.setTimeZone(tz);
        outgoingDateFormat.setTimeZone(tz);
    }
    
    @Override
    public String exec(Tuple input) throws IOException {
      if (input == null || input.size() == 0)
        return null;
      String str="";
      try{
        str = (String)input.get(0);
        Date date = incomingDateFormat.parse(str);
        return outgoingDateFormat.format(date);
        
      } catch (ParseException pe) {
        System.err.println("piggybank.evaluation.util.apachelogparser.DateExtractor: unable to parse date "+str);
        return null;
      } catch(Exception e){
        throw new IOException("Caught exception processing input row ", e);
      }
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), 
            new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));

        return funcList;
    }
}

