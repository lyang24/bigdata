/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package com.example;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.FileInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class WordCount {
  enum Word{one, two, findChinese,findDict,noDict};

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private HashMap<String, String> dictionary = new HashMap<String, String>();
    

    //step up the distributed cache
    public void setup(Context context) 
            throws IOException, InterruptedException{
      Path[] dicts = context.getLocalCacheFiles();
      if (dicts != null && dicts.length > 0){
          context.getCounter(Word.findDict).increment(1);
          for (Path dict: dicts){
            //BufferedReader bufferedReader = new BufferedReader(new FileReader(dict.toString()));
            
            //use InputStreamReader to solve 乱码
            InputStreamReader isr = new InputStreamReader(new FileInputStream(dict.toString()), "utf-8");
            BufferedReader bufferedReader = new BufferedReader(isr);

            String pair = null;
            while ((pair = bufferedReader.readLine()) != null){
              context.getCounter("wordCount", pair);
            String[] pairSplit = pair.split(" ");
            context.getCounter("wordCount", pairSplit[0]);
            dictionary.put(pairSplit[0], pairSplit[1]);
         }
            bufferedReader.close();
        }

      }else{
        System.err.println("No dict found!!!!!!!!!!");
        context.getCounter(Word.noDict).increment(1);
      }

    }



    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      // StringTokenizer itr = new StringTokenizer(value.toString());
      // while (itr.hasMoreTokens()) {
      //   word.set(itr.nextToken());
      //   context.write(word, one);
      // }
      String line = value.toString().trim();

      line = line.replaceAll("[^\u4e00-\u9fa5a-zA-Z]", " ");
      String[] words = line.split("\\s+");
      for (String word: words){
        if (word.equals("一")){
          context.getCounter(Word.one).increment(1);
        } 
        if (word.equals("二")){
          context.getCounter(Word.two).increment(1);
        }
          
        //check if the word is in dict
        if (dictionary.containsKey(word)){
            context.getCounter(Word.findChinese).increment(1);
            context.write(new Text(dictionary.get(word)), one);
          } else{
          context.write(new Text(word),one);
        }
      }
    }
  }
  
  //Custom partitioner
  public static class CustomPartitioner extends Partitioner<Text, IntWritable> {
    private String partitionKey;

    public int getPartition(Text key, IntWritable value, int numPartitions) {
        
        if(numPartitions == 2){
            partitionKey = key.toString();
            if (partitionKey.matches("^[abAB].*$")){
                return 0;} else {
                return 1;}
        } else{
            System.err.println("Can only handle 2 paritions");
            return 1;
        }
    }

}


  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }




  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    //conf.setInt("mapred.max.split.size", 100);
    conf.setInt("mapreduce.job.reduces", 2);

    //  //compress output file
    conf.set("mapreduce.output.fileoutputformat.compress", "true");
    conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK"); 
    conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec"); 


    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //Combiner
    //job.setCombinerClass(CIntSumReducer.class)

    //Custom partitioner
    job.setPartitionerClass(CustomPartitioner.class);

    //Store into sequence file
    // job.setOutputFormatClass(SequenceFileOutputFormat.class);

    //Add distributed cache
    job.addCacheFile(new Path("file:///src/week1/wordcount/cache/dict.txt").toUri());


    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
