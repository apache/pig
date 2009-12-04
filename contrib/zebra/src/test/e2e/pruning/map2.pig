register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

a2 = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

n1 = foreach a1 generate a,b,m1#'a' as ms1;;
n2 = foreach a2 generate a,b,m1#'b' as ms2;

j = join n1 by (a,b), n2 by (a,b);
dump j; 
--check table j should only have 6 columns, a,b, and m1#{a} of n1 and a,b, m1#{a} of n2
store j into 'maps1' using org.apache.hadoop.zebra.pig.TableStorer('');


