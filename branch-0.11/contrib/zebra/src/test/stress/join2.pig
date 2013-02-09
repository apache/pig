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



rec1 = load '$outputDir/u3' using org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,long1', 'sorted');
rec2 = load '$outputDir/u4' using org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,long1', 'sorted');


joina = join rec1 by long1, rec2 by long1 using "merge" ;

E = foreach joina  generate $0 as count,  $1 as seed,  $2 as int1,  $3 as str2, $4 as long1;
joinE = order E by long1 parallel 25;



store joinE into '$outputDir/j2' using org.apache.hadoop.zebra.pig.TableStorer('');
                                                 
