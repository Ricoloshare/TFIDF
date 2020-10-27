package com.ricolo;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TFIDFReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		if(values == null) {
			return;
		}
		int fileCount = 0;
		ArrayList<String> lists = new ArrayList<String>();
		for(Text value: values) {
			lists.add(value.toString());
			fileCount++;
		}
		
		Double idfValue = Math.log10(1.0 * TFIDFMapper.totalFile / (fileCount + 1));
		Double tfidfValue;
		Text text = new Text();
		for(String value : lists) {
			tfidfValue = idfValue * Double.parseDouble(value.split(":")[0]);
			text.set(String.join(":", value.split(":")[1],key.toString()));
			context.write(text, new Text(tfidfValue.toString()));
		}
	}
}
