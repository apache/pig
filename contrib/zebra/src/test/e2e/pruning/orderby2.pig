register /grid/0/dev/hadoopqa/jars/zebra.jar;
a = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
b = order a by m1#'a';

c = foreach b generate a as a, m1#'a' as ms1;
describe c;
--dump c;
--There should be 2 columns in orderby1 table
store c into 'orderby2' using org.apache.hadoop.zebra.pig.TableStorer('');
