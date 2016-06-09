package com.kapil.wordcount.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class WordCountExecute {

public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.setBoolean("mapreduce.map.output.compress", true);
		conf.setClass("mapreduce.map.output.compress.codec", GzipCodec.class, CompressionCodec.class);

		
		Job job = Job.getInstance();
		job.setJarByClass(WordCountExecute.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPaths(job, "./WordCount.txt"); 
		//provide output folder path below
		FileOutputFormat.setOutputPath(job, new Path(""));
		    
		job.waitForCompletion(true);
	}
}
