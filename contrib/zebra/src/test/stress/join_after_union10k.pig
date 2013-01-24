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

register $zebraJar;
--fs -rmr $outputDir


a1 = LOAD '$inputDir/10k1' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,long1');
a2 = LOAD '$inputDir/10k2' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,long1');
a3 = LOAD '$inputDir/10k3' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,long1');
a4 = LOAD '$inputDir/10k4' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,long1');

sort1 = order a1 by long1 parallel 6;
sort2 = order a2 by long1 parallel 5;
sort3 = order a3 by long1 parallel 7;
sort4 = order a4 by long1 parallel 4;

store sort1 into '$outputDir/sortedlong110k1' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,long1]');
store sort2 into '$outputDir/sortedlong110k2' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,long1]');
store sort3 into '$outputDir/sortedlong110k3' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,long1]');
store sort4 into '$outputDir/sortedlong110k4' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,long1]');


joinl = LOAD '$outputDir/sortedlong110k1,$outputDir/sortedlong110k2' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,long1', 'sorted');
joinll = order joinl by long1 parallel 7;
store joinll into '$outputDir/union10kl' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,long1]');


joinr = LOAD '$outputDir/sortedlong110k3,$outputDir/sortedlong110k4' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,long1', 'sorted');
joinrr = order joinr by long1 parallel 4;
store joinrr into '$outputDir/union10kr' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,long1]');


rec1 = load '$outputDir/union10kl' using org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,long1', 'sorted');
rec2 = load '$outputDir/union10kr' using org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,long1', 'sorted');


joina = join rec1 by long1, rec2 by long1 using "merge" ;

E = foreach joina  generate $0 as count,  $1 as seed,  $2 as int1,  $3 as str2, $4 as long1;
joinE = order E by long1 parallel 25;

limitedVals = LIMIT joina 10;
dump limitedVals;

store joinE into '$outputDir/join_after_union_10k' using org.apache.hadoop.zebra.pig.TableStorer('');  
