package com.kapil.wordmean.example;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMeanMapper extends Mapper<Object, Text, Text, LongWritable> {

	private LongWritable wordLen = new LongWritable();
	private final static Text COUNT = new Text("count");
	private final static Text LENGTH = new Text("length");
	private final static LongWritable ONE = new LongWritable(1);

	/**
	 * Emits 2 key-value pairs for counting the word and its length. Outputs are
	 * (Text, LongWritable).
	 * 
	 * @param value
	 *            This will be a line of text coming in from our input file.
	 */
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			String string = itr.nextToken();
			this.wordLen.set(string.length());
			context.write(LENGTH, this.wordLen);
			context.write(COUNT, ONE);
		}
	}
}