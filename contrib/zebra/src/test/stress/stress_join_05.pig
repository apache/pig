
register /grid/0/dev/hadoopqa/jars/zebra.jar;

a1 = LOAD '/user/hadoopqa/zebra/data/unsorted1' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2');
a2 = LOAD '/user/hadoopqa/zebra/data/unsorted2' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2');

sort1 = order a1 by str2;
sort2 = order a2 by str2;

store sort1 into '/user/hadoopqa/zebra/temp/sorted1' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2]');
store sort2 into '/user/hadoopqa/zebra/temp/sorted2' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2]');

rec1 = load '/user/hadoopqa/zebra/temp/sorted1' using org.apache.hadoop.zebra.pig.TableLoader();
rec2 = load '/user/hadoopqa/zebra/temp/sorted2' using org.apache.hadoop.zebra.pig.TableLoader();

joina = join rec1 by str2, rec2 by str2 using "merge" ;

limitedVals = LIMIT joina 5;
dump limitedVals;

--store joina into '/user/hadoopqa/zebra/temp/join1' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2];[count,seed,int1,str2]');
