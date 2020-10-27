package com.ricolo;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFReducer extends Reducer<Text, IntWritable, Text, Text>{
	private int sum;
	Text v = new Text();
	private Map<String, Integer> keyMap = new ConcurrentHashMap<String, Integer>();
	private Map<String, Integer> docuMap = new ConcurrentHashMap<String, Integer>();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		//sum
		if(values == null) {
			return;
		}
		sum = 0;
		for(IntWritable value: values) {
			sum += value.get();	 // the total number of the same word
		}
		// use HashMap(key: fileName, value: sum)
		//to count the total number of the document
		String docuName = key.toString().split(":")[0];
		if(docuMap.containsKey(docuName)) {
			docuMap.put(docuName, sum + docuMap.get(docuName));
		}else {
			docuMap.put(docuName, sum);
		}
		
		keyMap.put(key.toString(), sum); //Save the total number of the same word

	}
	
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Double tf;
		// TF = (The number of times the word appears in an ducument / the total number of words in an articles)
		for(Map.Entry<String, Integer> entry: keyMap.entrySet()) {
			String fileName = entry.getKey().split(":")[0]; // get HashMap key 
			tf = 1.0 * entry.getValue() / docuMap.get(fileName);  // Calculation TF value
			context.write(new Text(entry.getKey()), new Text(String.valueOf(tf.isInfinite() ? Double.MIN_VALUE : tf)));
		}
		context.write(new Text("totalDocu"),new Text(docuMap.size()+""));
	}
}
