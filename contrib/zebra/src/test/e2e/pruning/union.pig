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
a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

a2 = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
                      
a1order = order a1 by a;  
a2order = order a2 by a;   
       
b =  foreach a1order  generate m1#'a' as ms1;
c =  foreach a2order generate m1#'b' as ms2;

--store b  into 'u1' using org.apache.hadoop.zebra.pig.TableStorer('');    
--store c  into 'u2' using org.apache.hadoop.zebra.pig.TableStorer('');    

-- records1 should have only two columns, ms1 and ms2, for column a will return empty
--records1 = LOAD 'u1,u2' USING org.apache.hadoop.zebra.pig.TableLoader ('source_table,ms1,a, ms2','sorted');
records1 = LOAD 'u1,u2' USING org.apache.hadoop.zebra.pig.TableLoader ('ms1,a, ms2');
--joina = join rec1 by a, rec2 by a2 using "merge" ;     
dump records1;
             
