register $zebraJar;
--fs -rmr $outputDir


a1 = LOAD '$inputDir/25Munsorted1' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,byte1');
a2 = LOAD '$inputDir/25Munsorted2' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,byte1');

sort1 = order a1 by byte1;
sort2 = order a2 by byte1;

--store sort1 into '$outputDir/sortedbyte1' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,byte1]');
--store sort2 into '$outputDir/sortedbyte2' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,byte1]');

rec1 = load '$outputDir/sortedbyte1' using org.apache.hadoop.zebra.pig.TableLoader();
rec2 = load '$outputDir/sortedbyte2' using org.apache.hadoop.zebra.pig.TableLoader();

joina = LOAD '$outputDir/sortedbyte1,$outputDir/sortedbyte2' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,byte1', 'sorted');
    
joinaa = order joina by byte1;
store joinaa into '$outputDir/union2' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,byte1]');
