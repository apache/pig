register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

a2 = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

n1 = foreach a1 generate a;
n2 = foreach a2 generate b;


--this store will pass
store n1 into 'mforeach11' using org.apache.hadoop.zebra.pig.TableStorer('[a]');
store n2 into 'mforeach12' using org.apache.hadoop.zebra.pig.TableStorer('');

a3 = load 'mforeach11' using org.apache.hadoop.zebra.pig.TableLoader();
dump a3;

a4 = load 'mforeach12' using org.apache.hadoop.zebra.pig.TableLoader();
dump a4;
