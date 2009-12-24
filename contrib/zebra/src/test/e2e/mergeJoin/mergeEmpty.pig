register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load '1.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);

a2 = load 'empty.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
--dump a1;                
                      
a1order = order a1 by a;  
a2order = order a2 by a;   
       

store a1order into 'a1' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f,r1,m1]');    
store a2order into 'empty' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f,r1,m1]');

rec1 = load 'a1' using org.apache.hadoop.zebra.pig.TableLoader(); 
rec2 = load 'empty' using org.apache.hadoop.zebra.pig.TableLoader(); 
joina = join rec1 by a, rec2 by a using "merge" ;     
dump joina;
             
