register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

a2 = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
                      
a1order = order a1 by a,b;  
a2order = order a2 by a,b;   
       

d = foreach a1order  generate a,b,c;
store d into 'prune_ab1' using org.apache.hadoop.zebra.pig.TableStorer('[a];[b];[c]');    
store a2order into 'prune_ab2' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f,r1,m1]');

rec1 = load 'prune_ab1' using org.apache.hadoop.zebra.pig.TableLoader(); 
rec2 = load 'prune_ab2' using org.apache.hadoop.zebra.pig.TableLoader(); 
joina = join rec1 by (a,b), rec2 by (a,b) using "merge" ;     

dump joina;
--table merge should only have 3 columns from left hand, 8 columns from right hand. 
store joina into 'merge' using org.apache.hadoop.zebra.pig.TableStorer('');


             
