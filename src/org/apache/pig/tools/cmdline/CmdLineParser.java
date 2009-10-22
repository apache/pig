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
 *
 * A simple command line parser.  This parser allows the caller to register a set of
 * valid arguments.  Each argument can have both a long and short format (e.g. -h and
 * -hod).  Note that -- (double dashes) is not required for long options as in
 * standard GNU, though it is accepted.
 *
 * The caller then calls getNextOpt().  This returns a character, matching the short
 * form of the argument.  The caller can then obtain the value by calling one of the
 * getVal functions.  
 *
 * $Header:$
 */

package org.apache.pig.tools.cmdline;

import java.text.ParseException;
import java.util.HashMap;
import java.lang.AssertionError;

public class CmdLineParser
{

public enum ValueExpected {REQUIRED, OPTIONAL, NOT_ACCEPTED};
public static final char EndOfOpts = '-';

/**
 * @param args String array of arguments passed to the program.
 */
public CmdLineParser(String[] args)
{
    mArgs = args;
    mArgNum = 0;
    mShort = new HashMap<Character, ValueExpected>();
    mLong = new HashMap<String, Character>();
}

/**
 * Register a command line option.
 * @param c Single character designator for this option.  It cannot be '-'.
 * @param s Full word designator for this option.  This can be null, in which case
 * no long designator will exist for this option.
 * @param ve If REQUIRED, a value will be expected with this option.  If
 * OPTIONAL a value will be accepted if it is seen.
 * @throws AssertionError if there is no short option, or if this option has already been
 * used.
 */
public void registerOpt(char c, String s, ValueExpected ve)
{
    if (c == '-') {
        throw new AssertionError("CmdLineParser:  '-' is not a legal single character designator.");
    }

    Character cc = Character.valueOf(c);
    if (mShort.put(cc, ve) != null) {
        throw new AssertionError("CmdLineParser:  You have already registered option " + cc.toString());
    }

    if (mLong != null) {
        if (mLong.put(s, cc) != null) {
            throw new AssertionError("CmdLineParser:  You have already registered option " + s);
        }
    }
}

/**
 * Get the next option.
 * @return The short designator for the next argument.  If there are no more arguments
 * than the special designator CmdLineParser.EndOfOpts will be returned.
 * @throws ParseException if an unknown option is found or an option that
 * expects a value does not have one or a value that does not expect a value does have
 * one.
 */
public char getNextOpt() throws ParseException
{
    if (mArgNum >= mArgs.length) return EndOfOpts;

    int offset = 1;
    mVal = null;
    try {
        String arg = mArgs[mArgNum];
        // If it doesn't start with a dash, we'll assume we've reached the end of the
        // arguments.  We need to decrement mArgNum because the finally at the end is
        // going to increment it.
        if (arg.charAt(0) != '-') {
            mArgNum--;
            return EndOfOpts;
        }
    
        // Strip any dashes off of the beginning
        for (int i = 1; i < arg.length() && arg.charAt(i) == '-'; i++) offset++;
    
        // If they passed us a - or -- then quit
        if (offset == arg.length()) return EndOfOpts;

        Character cc = null;
        if (arg.substring(offset).length() == 1) {
            cc = Character.valueOf(arg.substring(offset).charAt(0));
        } else {
            cc = mLong.get(arg.substring(offset));
            if (cc == null) {
                Integer ii = Integer.valueOf(mArgNum + 1);
                String errMsg = "Found unknown option (" + arg + ") at position " +
                    ii.toString();
                throw new ParseException(errMsg, mArgNum);
            }
        }

        ValueExpected ve = mShort.get(cc);
        if (ve == null) {
            Integer ii = Integer.valueOf(mArgNum + 1);
            String errMsg = "Found unknown option (" + arg + ") at position " +
                ii.toString();
            throw new ParseException(errMsg, mArgNum);
        }

        switch (ve) {
        case NOT_ACCEPTED: 
            return cc.charValue();

        case REQUIRED: 
            // If it requires an option, make sure there is one.
            if (mArgNum + 1 >= mArgs.length || mArgs[mArgNum + 1].charAt(0) == '-') {
                String errMsg = "Option " + arg +
                    " requires a value but you did not provide one.";
                throw new ParseException(errMsg, mArgNum);
            }
            mVal = mArgs[++mArgNum];
            return cc.charValue();

        case OPTIONAL:
            if (mArgNum + 1 < mArgs.length && mArgs[mArgNum + 1].charAt(0) != '-') {
                mVal = mArgs[++mArgNum];
            }
            return cc.charValue();

        default:
            throw new AssertionError("Unknown valueExpected state");

        }

    } finally {
        mArgNum++;
    }
}

/**
 * Get any remaining arguments.
 * @return In general this function will null.
 * Only if the caller passed a '-' or '--' followed by other arguments.  In that case
 * the remainder of the args array will be returned.
 */
public String[] getRemainingArgs()
{
    if (mArgNum == mArgs.length) return null;

    String[] remainders = new String[mArgs.length - mArgNum];
    System.arraycopy(mArgs, mArgNum, remainders, 0, remainders.length);
    return remainders;
}

/**
 * Get the value, as a string.
 * @return The value associated with the current option.  If there is no value,
 * then null will be returned.
 */
public String getValStr()
{
    return mVal;
}

/**
 * Get the value, as an Integer.
 * @return The value associated with the current option.  If there is not value, then
 * null will be returned.
 * @throws NumberFormatException if the value cannot be converted to an integer.
 */
public Integer getValInt() throws NumberFormatException
{
    if (mVal == null) return null;
    else return new Integer(mVal);
}

private String[] mArgs;
private HashMap<Character, ValueExpected> mShort;
private HashMap<String, Character> mLong;
private int mArgNum;
private String mVal;

}

