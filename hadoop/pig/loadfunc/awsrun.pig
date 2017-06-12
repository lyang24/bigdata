register s3n://nutchdbloader-0.0.1-SNAPSHOT.jar
register s3n://hadoop-aws-2.7.3.jar
register s3n://nutch-1.12.jar

loaded = load 's3n://nutchcrawler/nutchdb/segments/*/parse_data/part-*/data' using com.example.NutchParsedDataLoader();
filtered = filter loaded by $0 is not null;
store filtered into 'output';
