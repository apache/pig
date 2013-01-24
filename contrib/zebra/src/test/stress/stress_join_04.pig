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


--a1 = LOAD '$inputDir/unsorted1' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,byte1');
--a2 = LOAD '$inputDir/unsorted2' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,byte1');

--sort1 = order a1 by byte1,int1;
--sort2 = order a2 by byte1,int1;

--store sort1 into '$outputDir/sortedbyteint1' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,byte1]');
--store sort2 into '$outputDir/sortedbyteint2' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,byte1]');

rec1 = load '$outputDir/sortedbyteint1' using org.apache.hadoop.zebra.pig.TableLoader();
rec2 = load '$outputDir/sortedbyteint2' using org.apache.hadoop.zebra.pig.TableLoader();

joina = join rec1 by (byte1,int1), rec2 by (byte1,int1) using "merge" ;

E = foreach joina  generate $0 as count,  $1 as seed,  $2 as int1,  $3 as str2, $4 as byte1;


--limitedVals = LIMIT E 5;
--dump limitedVals;

--store E into '$outputDir/join4' using org.apache.hadoop.zebra.pig.TableStorer('');


join4 = load '$outputDir/join4' using org.apache.hadoop.zebra.pig.TableLoader();
orderjoin = order join4 by byte1,int1;
store orderjoin into '$outputDir/join4_order' using org.apache.hadoop.zebra.pig.TableStorer('');

