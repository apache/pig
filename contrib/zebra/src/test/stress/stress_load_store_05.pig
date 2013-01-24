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



a1 = LOAD '$inputDir/unsorted1' USING org.apache.hadoop.zebra.pig.TableLoader('int1,int2,str1,str2,byte1,byte2,r1');
--limitedVals = LIMIT a1 10;
--dump limitedVals;

store a1 into '$outputDir/r1' using org.apache.hadoop.zebra.pig.TableStorer('[int1];[int2];[byte2];[str2,str1,r1]');

a2 = LOAD '$outputDir/r1' USING org.apache.hadoop.zebra.pig.TableLoader('byte2,int2,int1,str1,str2,r1');
--limitedVals = LIMIT a2 10;
--dump limitedVals;


store a2 into '$outputDir/r1_2' using org.apache.hadoop.zebra.pig.TableStorer('[int1];[int2];[byte2];[str2,str1,r1]');  

a3 = LOAD '$outputDir/r1_2' USING org.apache.hadoop.zebra.pig.TableLoader('byte2,int2,int1,str1,str2,r1');
--limitedVals = LIMIT a2 10;
--dump limitedVals;


store a3 into '$outputDir/r1_1' using org.apache.hadoop.zebra.pig.TableStorer('[int1];[int2];[byte2];[str2,str1,r1]');

--if only store once, and compare r1 with r1_1, table one has column number 6, table two has 5 (default column for table one)
--now we should compare r1_1 and r1_2 . they should be identia
