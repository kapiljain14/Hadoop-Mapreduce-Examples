package com.kapil.schoolresult.example;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SchoolResultReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	public void reduce(IntWritable key, Iterator<Text> values, Context context)
			throws IOException, InterruptedException {
		String schoolNames = " ";

		while (values.hasNext()) {
			String newschools = values.next().toString();
			schoolNames = schoolNames + "----->" + newschools;
		}
		context.write(key, new Text(schoolNames));
	}

}
