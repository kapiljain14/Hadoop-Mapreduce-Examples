package com.kapil.wordmean.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Charsets;

public class WordMeanExecute extends Configured implements Tool {

	private double mean = 0;
	private final static Text COUNT = new Text("count");
	private final static Text LENGTH = new Text("length");

	/**
	 * Reads the output file and parses the summation of lengths, and the word
	 * count, to perform a quick calculation of the mean.
	 * 
	 * @param path
	 *            The path to find the output file in. Set in main to the output
	 *            directory.
	 * @throws IOException
	 *             If it cannot access the output directory, we throw an
	 *             exception.
	 */
	private double readAndCalcMean(Path path, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path file = new Path(path, "part-r-00000");

		if (!fs.exists(file))
			throw new IOException("Output not found!");

		BufferedReader br = null;

		// average = total sum / number of elements;
		try {
			br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));

			long count = 0;
			long length = 0;

			String line;
			while ((line = br.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(line);

				// grab type
				String type = st.nextToken();

				// differentiate
				if (type.equals(COUNT.toString())) {
					String countLit = st.nextToken();
					count = Long.parseLong(countLit);
				} else if (type.equals(LENGTH.toString())) {
					String lengthLit = st.nextToken();
					length = Long.parseLong(lengthLit);
				}
			}

			double theMean = (((double) length) / ((double) count));
			System.out.println("The mean is: " + theMean);
			return theMean;
		} finally {
			if (br != null) {
				br.close();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new WordMeanExecute(), args);
	}

	public int run(String[] args) throws Exception {
		/*
		 * if (args.length != 2) {
		 * System.err.println("Usage: wordmean <in> <out>"); return 0; }
		 */

		Configuration conf = getConf();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "word mean");
		job.setJarByClass(WordMeanExecute.class);
		job.setMapperClass(WordMeanMapper.class);
		job.setCombinerClass(WordMeanReducer.class);
		job.setReducerClass(WordMeanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPaths(job, "./WordMean.txt");
		//Set output path folder before run
		Path outputpath = new Path("");
		FileOutputFormat.setOutputPath(job, outputpath);
		boolean result = job.waitForCompletion(true);
		mean = readAndCalcMean(outputpath, conf);

		return (result ? 0 : 1);
	}

	/**
	 * Only valuable after run() called.
	 * 
	 * @return Returns the mean value.
	 */
	public double getMean() {
		return mean;
	}

}
