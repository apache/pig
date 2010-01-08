register /grid/0/dev/hadoopqa/hadoop/lib/zebra.jar;
A = load 'filter.txt' as (name:chararray, age:int);

B = filter A by age < 20;
--dump B;
store B into 'filter1' using org.apache.hadoop.zebra.pig.TableStorer('[name];[age]');

C = filter A by age >= 20;
--dump C;
Store C into 'filter2' using org.apache.hadoop.zebra.pig.TableStorer('[name];[age]');

