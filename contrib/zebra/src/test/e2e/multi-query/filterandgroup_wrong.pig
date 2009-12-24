register /grid/0/dev/hadoopqa/hadoop/lib/zebra.jar;
A = load 'filter.txt' as (name:chararray, age:int);

B = filter A by age < 20;
--dump B;
store B into 'filter1' using org.apache.hadoop.zebra.pig.TableStorer('[name];[age]');

C = filter A by age >= 20;
--dump C;
Store C into 'filter2' using org.apache.hadoop.zebra.pig.TableStorer('[name];[age]');

D = group A by age;
E = foreach D generate group as group1,  COUNT(A.age) as myage;
--E = foreach D generate group;
Store E into 'filterandgroup' using org.apache.hadoop.zebra.pig.TableStorer('[group1];[myage]');
