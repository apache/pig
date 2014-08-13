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

--Load
A = load 'x.file' using org.apache.pig.PigStorage ( ',' ) as ( a:int, b:long );
A = load 'x.file' as c:int;
A=LoAD 'myfile.txt' using PigTextLoader() as ( a:int, b, c );
A = load 'xxx' as ( a : INT, b : LONG );
A = LOAD 'myfile.txt' USING PigTextLoader( 'arg1', 'arg2' ) as ( a:INT, b : long, c : bytearray, d : chararray, e : float, f : double);
aa = load '/data/intermediate/pow/elcarobootstrap/account/full/weekly/data/$date' using org.apache.pig.PigStorage('\n');
A = load 'xxx' as (a:int, b:long, c:bag{});

--Filter
B = FILTER A by $0 == 100 OR $0 < 5 parallel 20;
B = FILTER ( load 'x.file' as c:int ) by c == 40;
bb = filter aa by $4 eq '' or $4 eq 'NULL' or $4 eq 'ss' parallel 400;
B = filter A by NOT( ( a  > 5 ) OR ( b < 100 AND a < -1 ) AND d matches 'abc' );
inactiveAccounts = filter a by ($1 neq '') and ($1 == '2') parallel 400;

--Distinct
C = DISTINCT B parallel 10;
C = DISTINCT B partition by org.apache.pig.RandomPartitioner;

--Foreach
D = foreach bb { generate $0,$12,$7; }
D = foreach bb { generate $0,$12,$7; };
D = foreach C generate $0;
D = foreach ( load 'x' as (a:bag{}, b:chararray, c:int) ) { E = c; S = order a by $0; generate $1, COUNT( S ); }
countInactiveAcct = foreach grpInactiveAcct { generate COUNT( inactiveAccounts ); }
E = foreach A generate a as b:int;
I = foreach A generate flatten(c);

--sample
E = sample D 0.9;

--limit
F = limit E 100;

--order by
G = ORDER F by $2;
G = order F by * DESC;
E = order B by $0 ASC;

--define
define myudf org.apache.pig.TextLoader( 'test', 'data' );
define CMD `ls -l`;

--group
D = cogroup A by $0 inner, B by $0 outer;
grpInactiveAcct = group inactiveAccounts all;
B = GROUP A ALL using 'collected';

--cube
C = CUBE A BY CUBE(a, b);
CC = CUBE A BY ROLLUP(*);

--join
E = join A by $0, B by $0 using 'replicated';
H = join A by u, B by u;
I = foreach H generate A::u, B::u;

--croos
F = Cross A, B;

--store
store C into 'output.txt';
store countInactiveAcct into '/user/kaleidoscope/pow_stats/20080228/acct_stats/InactiveAcctCount';
store inactiveAccounts into '/user/kaleidoscope/pow_stats/20080228/acct/InactiveAcct';

--split
Split A into X if $0 > 0, Y if $0 == 0;

--union
H = union onschema A, B;

--stream
C = stream A through CMD;


--rank

R = rank A;
R = rank A by a;
R = rank A by a DESC;
R = rank A by a DESC, b;