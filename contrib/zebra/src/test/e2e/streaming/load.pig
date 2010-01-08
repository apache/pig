-- The script converts simple.txt to simple table 

register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load 'simple.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray);

dump a1;

store a1 into 'simple-table' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f]');
a2 = load 'simple-table' using org.apache.hadoop.zebra.pig.TableLoader();
dump a2;

