register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

a2 = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

n1 = foreach a1 generate a,c,e;
n2 = foreach a2 generate b,d,f;


--this store will pass
store n1 into 'mforeach21' using org.apache.hadoop.zebra.pig.TableStorer('');
store n2 into 'mforeach22' using org.apache.hadoop.zebra.pig.TableStorer('');

a3 = load 'mforeach21' using org.apache.hadoop.zebra.pig.TableLoader();
dump a3;

a4 = load 'mforeach22' using org.apache.hadoop.zebra.pig.TableLoader();
dump a4;
