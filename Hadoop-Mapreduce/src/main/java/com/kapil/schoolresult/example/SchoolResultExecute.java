package com.kapil.schoolresult.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SchoolResultExecute {
	public static void main(String[] args) throws Exception {

		/*
		 * String[] otherArgs = new GenericOptionsParser(conf,
		 * args).getRemainingArgs(); if (otherArgs.length != 2) {
		 * System.err.println("Usage: CountSchoolsBasedOnTestTakers <in> <out>"
		 * ); System.exit(2); }
		 */
		Job job = Job.getInstance();
		job.setJarByClass(SchoolResultExecute.class);
		job.setMapperClass(SchoolResultMapper.class);
		job.setReducerClass(SchoolResultReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPaths(job, "./SchoolResult.txt");
		//provide output folder path below
		FileOutputFormat.setOutputPath(job, new Path(""));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
