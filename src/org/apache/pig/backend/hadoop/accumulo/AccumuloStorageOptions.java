/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.hadoop.accumulo;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;

public class AccumuloStorageOptions {
    public static final Option CASTER_OPTION = new Option(
            "c",
            "caster",
            true,
            "Implementation of LoadStoreCaster to use typically UTF8StringConverter or AccumuloBinaryConverter"),
            AUTHORIZATIONS_OPTION = new Option("auths", "authorizations", true,
                    "Comma-separated list of authorizations to use"),
            START_ROW_OPTION = new Option("s", "start", true,
                    "The row to begin reading from, inclusive"),
            END_ROW_OPTION = new Option("e", "end", true,
                    "The row to read until, inclusive"),
            MUTATION_BUFFER_SIZE_OPTION = new Option("buff",
                    "mutation-buffer-size", true,
                    "Number of bytes to buffer when writing data"),
            WRITE_THREADS_OPTION = new Option("wt", "write-threads", true,
                    "Number of threads to use when writing data"),
            MAX_LATENCY_OPTION = new Option("ml", "max-latency", true,
                    "Maximum latency in milliseconds before Mutations are flushed to Accumulo"),
            COLUMN_SEPARATOR_OPTION = new Option("sep", "separator", true,
                    "Separator string to use when parsing columns"),
            COLUMN_IGNORE_WHITESPACE_OPTION = new Option("iw",
                    "ignore-whitespace", true,
                    "Whether or not whitespace should be stripped from column list");

    private Options options;
    private GnuParser parser;

    public AccumuloStorageOptions() {
        parser = new GnuParser();
        options = new Options();

        options.addOption(CASTER_OPTION);
        options.addOption(AUTHORIZATIONS_OPTION);
        options.addOption(START_ROW_OPTION);
        options.addOption(END_ROW_OPTION);
        options.addOption(MUTATION_BUFFER_SIZE_OPTION);
        options.addOption(WRITE_THREADS_OPTION);
        options.addOption(MAX_LATENCY_OPTION);
        options.addOption(COLUMN_SEPARATOR_OPTION);
        options.addOption(COLUMN_IGNORE_WHITESPACE_OPTION);
    }

    public String getHelpMessage() {
        return "[(-c|--caster) LoadStoreCasterImpl] [(-auths|--authorizations auth1,auth2,auth3] [(-s|--start) startrow]"
                + " [(-e|--end) endrow] [(-buff|--mutation-buffer-size) bytes] [(-wt|--write-threads) threads] [(-ml|--max-latency) seconds]"
                + " [(-sep|--separator) ,] [(-iw|--ignore-whitespace) true|false]";
    }

    public CommandLine getCommandLine(String args) throws ParseException {
        String[] splitArgs = StringUtils.split(args);
        try {
            return parser.parse(options, splitArgs);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(getHelpMessage(), options);
            throw e;
        }
    }

    public boolean hasAuthorizations(CommandLine cli) {
        return cli.hasOption(AUTHORIZATIONS_OPTION.getOpt());
    }

    public Authorizations getAuthorizations(CommandLine cli) {
        return new Authorizations(cli.getOptionValue(
                AUTHORIZATIONS_OPTION.getOpt(), ""));
    }

    public long getLong(CommandLine cli, Option o) {
        String value = cli.getOptionValue(o.getOpt());

        return (null == value) ? null : Long.parseLong(value);
    }

    public int getInt(CommandLine cli, Option o) {
        String value = cli.getOptionValue(o.getOpt());

        return (null == value) ? null : Integer.parseInt(value);
    }
}
