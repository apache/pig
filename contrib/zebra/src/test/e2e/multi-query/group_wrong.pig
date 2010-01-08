register /grid/0/dev/hadoopqa/jars/zebra-111.jar;
A = load 'filter.txt' as (name:chararray, age:int);
B = group A by name;
C = foreach B generate group, COUNT(A.name) as cnt;
Store C into 'group1' using org.apache.hadoop.zebra.pig.TableStorer('[group];[cnt]');
