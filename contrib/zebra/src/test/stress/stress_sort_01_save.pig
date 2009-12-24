

register /grid/0/dev/hadoopqa/jars/zebra.jar;

a1 = LOAD '/user/hadoopqa/zebra/data/unsorted1' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2');

store a1 into '/user/hadoopqa/zebra/temp/unsorted1' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2]');

sort1 = ORDER a1 BY str2;

store sort1 into '/user/hadoopqa/zebra/temp/sorted1' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2]'); 
