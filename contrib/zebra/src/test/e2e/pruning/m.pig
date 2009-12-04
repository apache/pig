register /grid/0/dev/hadoopqa/jars/zebra.jar;
a = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
b = store a into 'm1' using org.apache.hadoop.zebra.pig.TableStorer('');
c = load 'm1' using org.apache.hadoop.zebra.pig.TableLoader('m1#{a}');
store c  into 'm2' using org.apache.hadoop.zebra.pig.TableStorer('');


