register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

a2 = load 'reverse.txt' as (m1:map[],r1(f1:chararray,f2:chararray),f:bytearray,e:chararray,d:double,c:long,b:float,a:int);     
--dump a1;                
                      
a1order = order a1 by e;  
a2order = order a2 by e;   
       

store a1order into 'new11' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f,r1,m1]');    
store a2order into 'new22' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f,r1,m1]');

rec1 = load 'new11' using org.apache.hadoop.zebra.pig.TableLoader(); 
rec2 = load 'new22' using org.apache.hadoop.zebra.pig.TableLoader(); 
joina = join rec1 by (e), rec2 by (e) using "merge" ;     
dump joina;
             
