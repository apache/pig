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
