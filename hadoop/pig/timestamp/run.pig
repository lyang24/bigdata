
register target/timestamp-0.0.1-SNAPSHOT.jar 
register geoip-api-1.3.1.jar

a = LOAD 'access_logs' using PigStorage(' ') AS (ip:chararray, country: chararray,dash: chararray, timestp: chararray ); 
b = filter a by ip is not null and timestp is not null;
c = foreach b generate ip, timestp;
d = foreach c generate com.example.pig.TimeStamp(ip, timestp);
e = foreach d generate $0.year as year, $0.month as month, $0.date as date, $0.country as country, $0.city as city;
f = group e by (year, month, date);
g = filter f by $0.year is not null and $0.month is not null and $0.date is not null;
store g into 'output';




