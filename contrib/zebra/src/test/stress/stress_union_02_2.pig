register $zebraJar;
--fs -rmr $outputDir


a1 = LOAD '$inputDir/25Munsorted3' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,byte1');
a2 = LOAD '$inputDir/25Munsorted4' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,byte1');

sort1 = order a1 by byte1;
sort2 = order a2 by byte1;

store sort1 into '$outputDir/sortedbyte3' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,byte1]');
store sort2 into '$outputDir/sortedbyte4' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,byte1]');

rec1 = load '$outputDir/sortedbyte3' using org.apache.hadoop.zebra.pig.TableLoader();
rec2 = load '$outputDir/sortedbyte4' using org.apache.hadoop.zebra.pig.TableLoader();

joina = LOAD '$outputDir/sortedbyte3,$outputDir/sortedbyte4' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,byte1', 'sorted');
    

store joina into '$outputDir/union2_2' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,byte1]');
