register $zebraJar;
--fs -rmr $outputDir


a1 = LOAD '$inputDir/$unsorted1' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,int2,str1,str2,byte1,byte2,float1,long1,double1,m1,r1,c1');

store a1 into '$outputDir/unsorted1' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,int2,str1,str2,byte1,byte2,float1,long1,double1];[m1#{a}];[r1,c1]');

sort1 = ORDER a1 BY int2;

store sort1 into '$outputDir/sorted111' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,int2,str1,str2,byte1,byte2,float1,long1,double1];[m1#{a}];[r1,c1]'); 
