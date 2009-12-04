register /grid/0/dev/hadoopqa/jars/zebra.jar;
a = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
b = order a by a;
c = foreach b generate a as a, b as b;
describe c;
--There should be 2 columns in orderby1 table
store c into 'orderby1' using org.apache.hadoop.zebra.pig.TableStorer('');
