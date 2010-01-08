register /grid/0/dev/hadoopqa/hadoop/lib/zebra.jar;
A = load 'filter.txt' as (name:chararray, age:int);

B = group A by name;
C = foreach B generate group as group1, COUNT(A.name) as myname;
Store C into 'group1' using org.apache.hadoop.zebra.pig.TableStorer('[group1];[myname]');
D = group A by age;
E = foreach D generate group as group2, COUNT(A.age) as myage;
Store E into 'group2' using org.apache.hadoop.zebra.pig.TableStorer('[group2];[myage]');
 
