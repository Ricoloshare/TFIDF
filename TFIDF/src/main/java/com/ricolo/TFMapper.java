package com.ricolo;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TFMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private String fileName = "";
	Text text = new Text();
	IntWritable intWrite = new IntWritable(1);
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		InputSplit file = context.getInputSplit();
		fileName = ((FileSplit)file).getPath().getName();
	}
	 @Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		 
		// get words
		StringTokenizer words = new StringTokenizer(value.toString());
		
		// get k2 v2(v2 = 1)
		while(words.hasMoreTokens()) {
			text.set(String.join(":", fileName, words.nextToken()));
			context.write(text, intWrite);
		}
	}
}
