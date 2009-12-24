
register $zebraJar;
--fs -rmr $outputDir


--a1 = LOAD '$inputDir/unsorted1' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2');
--a2 = LOAD '$inputDir/unsorted2' USING org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2');

--sort1 = order a1 by str2;
--sort2 = order a2 by str2;

--store sort1 into '$outputDir/sorted11' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2]');
--store sort2 into '$outputDir/sorted21' using org.apache.hadoop.zebra.pig.TableStorer('[count,seed,int1,str2]');

rec1 = load '$outputDir/sorted11' using org.apache.hadoop.zebra.pig.TableLoader('','sorted');
rec2 = load '$outputDir/sorted21' using org.apache.hadoop.zebra.pig.TableLoader('','sorted');

joina = join rec1 by str2, rec2 by str2 using "merge" ;

E = foreach joina  generate $0 as count,  $1 as seed,  $2 as int1,  $3 as str2;


store E into '$outputDir/testjoin21' using org.apache.hadoop.zebra.pig.TableStorer('');
