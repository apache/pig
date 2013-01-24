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


a1 = LOAD '$inputDir/$unsorted1' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,int2,str1,str2,byte1,byte2,float1,long1,double1,m1,r1,c1');

store a1 into '$outputDir/unsorted1' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,int2,str1,str2,byte1,byte2,float1,long1,double1];[m1#{a}];[r1,c1]');

sort1 = ORDER a1 BY int2;

store sort1 into '$outputDir/sorted111' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,int2,str1,str2,byte1,byte2,float1,long1,double1];[m1#{a}];[r1,c1]'); 
