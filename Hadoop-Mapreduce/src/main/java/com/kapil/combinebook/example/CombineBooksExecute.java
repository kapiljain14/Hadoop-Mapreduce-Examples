package com.kapil.combinebook.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CombineBooksExecute {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		/*
		 * if (args.length != 2) {
		 * System.err.println("Usage: CombineBooks <in> <out>"); System.exit(2);
		 * }
		 */
		Job job = Job.getInstance(conf);
		job.setJarByClass(CombineBooksExecute.class);
		job.setMapperClass(CombineBooksMapper.class);
		job.setReducerClass(CombineBooksReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPaths(job, "./CombineBooks.txt");
		//provide output folder path below
		FileOutputFormat.setOutputPath(job, new Path(""));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
