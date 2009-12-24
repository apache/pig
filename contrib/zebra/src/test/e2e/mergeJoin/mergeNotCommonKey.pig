register /grid/0/dev/hadoopqa/jars/zebra.jar;
a1 = load '3.txt' as (e:chararray,f:chararray,r1:chararray);

a2 = load '2.txt' as (a:int, b:float,c:long,d:double,e:chararray,f:bytearray,r1(f1:chararray,f2:chararray),m1:map[]);
--dump a1;                
                      
a1order = order a1 by e;  
a2order = order a2 by a;   
       

--store a1order into 'notCommonSortKey1' using org.apache.hadoop.zebra.pig.TableStorer('[e,f,r1]');    
store a2order into 'notCommonSortKey2' using org.apache.hadoop.zebra.pig.TableStorer('[a,b,c];[d,e,f,r1,m1]');

rec1 = load 'notCommonSortKey1' using org.apache.hadoop.zebra.pig.TableLoader(); 
rec2 = load 'notCommonSortKey2' using org.apache.hadoop.zebra.pig.TableLoader(); 
joina = join rec1 by e, rec2 by a using "merge" ;     
dump joina;

--ERROR 1107: Cannot merge join keys, incompatible types

