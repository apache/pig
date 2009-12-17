register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

a2 = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

n2 = foreach a1 generate a,b,c,d,e,f;

--this store will fail since there is no b and c
--store n2 into 'foreach1' using org.apache.hadoop.zebra.pig.TableStorer('[a];[b];[c]');

--this store will pass
store n2 into 'foreach2' using org.apache.hadoop.zebra.pig.TableStorer('');

--this load will pass, the second column 'b' will return empty string
a3 = load 'foreach2' using org.apache.hadoop.zebra.pig.TableLoader();
dump a3;
--store a2 into 'foreach2' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f,r1,m1]');

