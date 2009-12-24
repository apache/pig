
register $zebraJar;
--fs -rmr $outputDir


--a1 = LOAD '$inputDir/unsorted1' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,byte1');
--a2 = LOAD '$inputDir/unsorted2' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,byte1');

--sort1 = order a1 by byte1,int1;
--sort2 = order a2 by byte1,int1;

--store sort1 into '$outputDir/sortedbyteint1' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,byte1]');
--store sort2 into '$outputDir/sortedbyteint2' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2,byte1]');

rec1 = load '$outputDir/sortedbyteint1' using org.apache.hadoop.zebra.pig.TableLoader();
rec2 = load '$outputDir/sortedbyteint2' using org.apache.hadoop.zebra.pig.TableLoader();

joina = join rec1 by (byte1,int1), rec2 by (byte1,int1) using "merge" ;

E = foreach joina  generate $0 as count,  $1 as seed,  $2 as int1,  $3 as str2, $4 as byte1;


--limitedVals = LIMIT E 5;
--dump limitedVals;

--store E into '$outputDir/join4' using org.apache.hadoop.zebra.pig.TableStorer('');


join4 = load '$outputDir/join4' using org.apache.hadoop.zebra.pig.TableLoader();
orderjoin = order join4 by byte1,int1;
store orderjoin into '$outputDir/join4_order' using org.apache.hadoop.zebra.pig.TableStorer('');

