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


a1 = LOAD '$inputDir/25Munsorted3' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,byte1');
a2 = LOAD '$inputDir/25Munsorted4' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,byte1');

sort1 = order a1 by byte1;
sort2 = order a2 by byte1;

store sort1 into '$outputDir/sortedbyte3' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,byte1]');
store sort2 into '$outputDir/sortedbyte4' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,byte1]');

rec1 = load '$outputDir/sortedbyte3' using org.apache.hadoop.zebra.pig.TableLoader();
rec2 = load '$outputDir/sortedbyte4' using org.apache.hadoop.zebra.pig.TableLoader();

joina = LOAD '$outputDir/sortedbyte3,$outputDir/sortedbyte4' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,byte1', 'sorted');
    

store joina into '$outputDir/union2_2' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,byte1]');
