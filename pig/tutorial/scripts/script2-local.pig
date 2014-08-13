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

-- Temporal Query Phrase Popularity (local mode)

-- This script processes a search query log file from the Excite search engine and finds search phrases that occur with particular high frequency during certain times of the day. 

-- Register the tutorial JAR file so that the included UDFs can be called in the script.
REGISTER ./tutorial.jar;

-- Use the PigStorage function to load the excite log file into the raw bag as an array of records.
-- Input: (user,time,query) 
raw = LOAD 'excite-small.log' USING PigStorage('\t') AS (user: chararray, time: chararray, query: chararray);

-- Call the NonURLDetector UDF to remove records if the query field is empty or a URL.
clean1 = FILTER raw BY org.apache.pig.tutorial.NonURLDetector(query);

-- Call the ToLower UDF to change the query field to lowercase.
clean2 = FOREACH clean1 GENERATE user, time, org.apache.pig.tutorial.ToLower(query) as query;

-- Because the log file only contains queries for a single day, we are only interested in the hour.
-- The excite query log timestamp format is YYMMDDHHMMSS.
-- Call the ExtractHour UDF to extract the hour (HH) from the time field.
houred = FOREACH clean2 GENERATE user, org.apache.pig.tutorial.ExtractHour(time) as hour, query;

-- Call the NGramGenerator UDF to compose the n-grams of the query.
ngramed1 = FOREACH houred GENERATE user, hour, flatten(org.apache.pig.tutorial.NGramGenerator(query)) as ngram;

-- Use the DISTINCT command to get the unique n-grams for all records.
ngramed2 = DISTINCT ngramed1;

-- Use the GROUP command to group records by n-gram and hour.
hour_frequency1 = GROUP ngramed2 BY (ngram, hour);

-- Use the COUNT function to get the count (occurrences) of each n-gram.
hour_frequency2 = FOREACH hour_frequency1 GENERATE flatten($0), COUNT($1) as count;

-- Use the FOREACH-GENERATE command to assign names to the fields. 
hour_frequency3 = FOREACH hour_frequency2 GENERATE $0 as ngram, $1 as hour, $2 as count;

-- Use the FILTER command to get the n-grams for hour 00 .
hour00 = FILTER hour_frequency2 BY hour eq '00';

-- Use the FILTER command to get the n-grams for hour 12
hour12 = FILTER hour_frequency3 BY hour eq '12';

-- Use the JOIN command to get the n-grams that appear in both hours.
same = JOIN hour00 BY $0, hour12 BY $0;

-- Use the FOREACH-GENERATE command to record their frequency.
same1 = FOREACH same GENERATE hour00::group::ngram as ngram, $2 as count00, $5 as count12;

-- Use the PigStorage function to store the results. 
-- Output: (n-gram, count_at_hour_00, count_at_hour_12)
STORE same1 INTO 'script2-local-results.txt' USING PigStorage();
