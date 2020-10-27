package com.ricolo;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text>{
	private Text textKay = new Text();
	private Text textValue = new Text();
	static public int totalFile = 1;
	 @Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		 
		// get words
		StringTokenizer words = new StringTokenizer(value.toString());
		String word = words.nextToken();
		
		if(word.indexOf(":") == -1) {
			totalFile = Integer.parseInt(words.nextToken());
		 }else {
			String df = words.nextToken();
			textKay.set(word.split(":")[1]);
			textValue.set(String.join(":", df, word.split(":")[0])); // word df filename
			context.write(textKay, textValue); 
		 }
		
		
	}
}
