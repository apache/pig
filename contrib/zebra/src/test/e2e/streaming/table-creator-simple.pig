-- The script converts simple.txt to simple table 

register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load 'simple.txt' as (a2:int, b2:float,c2:long,d2:double,e2:chararray,f2:bytearray);

dump a1;

store a1 into 'simple-table2' using org.apache.hadoop.zebra.pig.TableStorer('[a2,b2,c2];[d2,e2,f2]');


