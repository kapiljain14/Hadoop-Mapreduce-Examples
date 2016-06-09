package com.kapil.schoolresult.example;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SchoolResultMapper extends Mapper<Object, Text, IntWritable, Text> {

	// private final static IntWritable range = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split(",");
		word.set(split[1]); // school name

		if (split[3].matches("(.*)[a-z](.*)") | split[3].matches("(.*)[A-Z](.*)")) {
			split[3] = "0";
		}

		int testTakers = new Integer(split[3]).intValue();
		int range = 0;

		if (testTakers <= 100)
			range = 1;
		if (testTakers > 100 && testTakers <= 200)
			range = 2;
		if (testTakers > 200 && testTakers <= 300)
			range = 3;
		if (testTakers > 300 && testTakers <= 400)
			range = 4;
		if (testTakers > 400 && testTakers <= 500)
			range = 5;
		if (testTakers > 500 && testTakers <= 600)
			range = 6;
		if (testTakers > 600 && testTakers <= 700)
			range = 7;
		if (testTakers > 700 && testTakers <= 800)
			range = 8;
		if (testTakers > 800 && testTakers <= 900)
			range = 9;
		if (testTakers > 900 && testTakers <= 1000)
			range = 10;
		if (testTakers > 1000 && testTakers <= 1100)
			range = 11;
		// count.set("" + text.length());
		context.write(new IntWritable(range), word);

	}

}
