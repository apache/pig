-- The script converts simple.txt to simple table 

register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load 'complex.txt' as (r1(f1:chararray,f2:chararray),m1:map[]);

dump a1;

store a1 into 'complex-table' using org.apache.hadoop.zebra.pig.TableStorer('[r1];[m1]');


