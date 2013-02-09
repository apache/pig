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

-- single line comment

%default date '20080228'
%declare destLocation '\'/user/kaleidoscope/pow_stats/$date/acct/InactiveAcct\''

aa = load '/data/intermediate/pow/elcarobootstrap/account/full/weekly/data/$date' using org.apache.pig.PigStorage('\n');
bb = filter aa by (ARITY == '16') and ( $4 eq '' or $4 eq 'NULL' or $4 eq 'ss') parallel 400;
a = foreach bb generate $0,$12,$7, $1.$1;

define myudf org.apache.pig.TextLoader( 'test', 'data' );

inactiveAccounts = filter a by ($1 neq '') and ($1 == '2') parallel 400;
store inactiveAccounts into '/user/kaleidoscope/pow_stats/20080228/acct/InactiveAcct';
grpInactiveAcct = group inactiveAccounts all;
countInactiveAcct = foreach grpInactiveAcct { generate COUNT( inactiveAccounts ); }
store countInactiveAcct into '/user/kaleidoscope/pow_stats/20080228/acct_stats/InactiveAcctCount';
A = load 'xxx' as ( a : INT, b : LONG );
A = LOAD 'myfile.txt' USING PigTextLoader( 'arg1', 'arg2' ) as ( a:INT, b : long, c : bytearray, d : chararray, e : float, f : double);

B = filter A by NOT( ( a  > 5 ) OR ( b < 100 AND a < -1 ) AND d matches 'abc' );

E = order B by $0 ASC;

F = order E by * DESC;

C = DISTINCT B partition by org.apache.pig.RandomPartitioner;

store C into 'output0.txt';

A = load 'xxx' as (a:int, b:long, c:bag{});
B = foreach A {S = order c by $0; generate $0; };

D = cogroup A by $0 inner, B by $0 outer;

E = join A by $0, B by $0 using 'replicated';

F = Cross A, B;

G = Split A into X if $0 > 0, Y if $0 == 2L;

H = union onschema A, B;

B = GROUP A ALL using 'collected';

I = foreach A generate flatten(B::c);

J = CUBE A BY CUBE($0, $1), ROLLUP($2, $3);

CMD = `ls -l`;
A = stream through CMD;

B = limit A 100;

A = LOAD 'data' AS (f1:int,f2:int,f3:int);

X = SAMPLE A 0.01;

R = rank A by f1;

R = rank A by f1 ASC, f2 DESC, f3;

R = rank A by *;

R = rank A by * DESC DENSE;






