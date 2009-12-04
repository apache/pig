register /grid/0/dev/hadoopqa/jars/zebra.jar;
a = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

b = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);


c = join a by a, b by a;
d = foreach c generate a::a as a, a::b as b, b::c as c;
describe d;
--store d into 'join1' using org.apache.hadoop.zebra.pig.TableStorer('[a];[b];[c]');
store d into 'join2' using org.apache.hadoop.zebra.pig.TableStorer('');
