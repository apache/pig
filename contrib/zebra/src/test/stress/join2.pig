register $zebraJar;
--fs -rmr $outputDir



rec1 = load '$outputDir/u3' using org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,long1', 'sorted');
rec2 = load '$outputDir/u4' using org.apache.hadoop.zebra.pig.TableLoader('count,seed,int1,str2,long1', 'sorted');


joina = join rec1 by long1, rec2 by long1 using "merge" ;

E = foreach joina  generate $0 as count,  $1 as seed,  $2 as int1,  $3 as str2, $4 as long1;
joinE = order E by long1 parallel 25;



store joinE into '$outputDir/j2' using org.apache.hadoop.zebra.pig.TableStorer('');
                                                 
