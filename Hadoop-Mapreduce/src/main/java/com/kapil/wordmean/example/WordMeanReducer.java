package com.kapil.wordmean.example;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Performs integer summation of all the values for each key.
 */
public class WordMeanReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	private LongWritable sum = new LongWritable();

	/**
	 * Sums all the individual values within the iterator and writes them to the
	 * same key.
	 * 
	 * @param key
	 *            This will be one of 2 constants: LENGTH_STR or COUNT_STR.
	 * @param values
	 *            This will be an iterator of all the values associated with
	 *            that key.
	 */
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		int theSum = 0;
		for (LongWritable val : values) {
			theSum += val.get();
		}
		sum.set(theSum);
		context.write(key, sum);
	}
}