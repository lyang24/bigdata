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
package com.example;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import dk.brics.automaton.Automaton;
import dk.brics.automaton.RegExp;

public class WordCount {
	enum WordRange{A_M, N_Z};
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    // pass in configure
    int multiplier = -1;
    Automaton automaton = new RegExp("h(.*)").toAutomaton();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	if (multiplier == -1){
    		multiplier = context.getConfiguration().getInt("multiplier", -1);
    	}
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    	  String w = itr.nextToken();
    	  if (automaton.run(w)){
    		  word.set(w);
    		  context.write(word, new IntWritable(multiplier));
    	  }
    	  if (word.toString().toUpperCase().compareTo("N") < 0){
    		  context.getCounter(WordRange.A_M).increment(1);
    	  }else {
    		  context.getCounter(WordRange.N_Z).increment(1);
    	  }
        word.set(itr.nextToken());
        //context.write(word, one);
        context.write(word, new IntWritable(multiplier));
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
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    //set config
    conf.setInt("multiplier", 2);
    //control mapper number
    conf.setInt("mapred.max.slit.size", 8);
    //control reducer number
    conf.setInt("mapred.reduce.tasks", 2);
    
    
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    
    //add combiner
    job.setCombinerClass(IntSumReducer.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
