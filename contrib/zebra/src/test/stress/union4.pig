register $zebraJar;
--fs -rmr $outputDir


a1 = LOAD '$inputDir/50Munsorted3' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,long1');
a2 = LOAD '$inputDir/50Munsorted4' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,long1');

sort1 = order a1 by long1 parallel 6;
sort2 = order a2 by long1 parallel 5;

store sort1 into '$outputDir/50MS3' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,long1]');
store sort2 into '$outputDir/50MS4' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,long1]');


union1 = LOAD '$outputDir/50MS3,$outputDir/50MS4' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,long1', 'sorted');
orderunion1 = order union1 by long1 parallel 7;
store orderunion1 into '$outputDir/u4' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,long1]');    
