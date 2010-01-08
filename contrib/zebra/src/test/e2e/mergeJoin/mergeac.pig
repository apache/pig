register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

a2 = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
--dump a1;                
                      
a1order = order a1 by a,c;  
a2order = order a2 by a,c;   
       

store a1order into 'ac1' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f,r1,m1]');    
store a2order into 'ac2' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f,r1,m1]');

rec1 = load 'ac1' using org.apache.hadoop.zebra.pig.TableLoader(); 
rec2 = load 'ac2' using org.apache.hadoop.zebra.pig.TableLoader(); 
joina = join rec1 by (a,c), rec2 by (a,c) using "merge" ;     
dump joina;
             
