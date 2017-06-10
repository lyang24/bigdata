a = load 'studenttab10k' as (name:chararray, age:int, gpa:double); 
b = group a by age;
c = foreach b generate group, COUNT(a), AVG(a.age), AVG(a.gpa); 
dump c;
