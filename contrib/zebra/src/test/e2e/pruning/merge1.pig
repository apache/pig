register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

a2 = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
                      
a1order = order a1 by a,b;  
a2order = order a2 by a,b;   
       

d = foreach a2order  generate a,b;
store a1order into 'prune_ab11' using org.apache.hadoop.zebra.pig.TableStorer('[a,b];[d,e,f,r1,m1]');    
store d into 'prune_ab21' using org.apache.hadoop.zebra.pig.TableStorer('[a,b]');

rec1 = load 'prune_ab11' using org.apache.hadoop.zebra.pig.TableLoader(); 
rec2 = load 'prune_ab21' using org.apache.hadoop.zebra.pig.TableLoader(); 
joina = join rec1 by (a,b), rec2 by (a,b) using "merge" ;     

dump joina;
--table merge1 should only have 8 columns from left hand, 2 columns from right hand. 
store joina into 'merge1' using org.apache.hadoop.zebra.pig.TableStorer('');


             
