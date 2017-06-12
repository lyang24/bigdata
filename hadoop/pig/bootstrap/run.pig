register target/bootstrap-0.0.1-SNAPSHOT.jar

a = load 'words' using com.example.pig.Bootstrap();
opt = filter a by $0 is not null;
b = group a all;
c = foreach b generate COUNT(a);
dump b;
dump c;
store opt into 'output';
