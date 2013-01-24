/**
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

register /grid/0/dev/hadoopqa/jars/zebra.jar;

--a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
--a2 = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
 
--sort1 = order a1 by a parallel 6;
--sort2 = order a2 by a parallel 5;

--store sort1 into 'asort1' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c,d]');
--store sort2 into 'asort2' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c,d]');
--store sort1 into 'asort3' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c,d]');
--store sort2 into 'asort4' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c,d]');

joinl = LOAD 'asort1,asort2' USING org.apache.hadoop.zebra.pig.TableLoader('a,b,c,d', 'sorted');

joinr = LOAD 'asort3,asort4' USING org.apache.hadoop.zebra.pig.TableLoader('a,b,c,d', 'sorted');


joina = join joinl by a, joinr by a using "merge" ;
dump joina;
--E = foreach joina  generate $0 as count,  $1 as seed,  $2 as int1,  $3 as str2, $4 as long1;
--joinE = order E by long1 parallel 25;

--limitedVals = LIMIT joinE 10;
--dump limitedVals;

--store joinE into 'join_jira' using org.apache.hadoop.zebra.pig.TableStorer('');                     
