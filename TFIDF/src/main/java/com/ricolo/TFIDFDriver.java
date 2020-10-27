package com.ricolo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFIDFDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);
		
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(TFIDFDriver.class);

		
		job.setMapperClass(TFMapper.class);
		job.setReducerClass(TFReducer.class);

		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		FileInputFormat.setInputPaths(job, new Path("file:///D:\\in\\"));
//		FileInputFormat.setInputPaths(job, new Path("hdfs://node01:8020/test"));
//		Path path = new Path("hdfs://node01:8020/output");
		Path path = new Path("file:///D:\\output\\");
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
		FileOutputFormat.setOutputPath(job, path);
		
	
		ControlledJob ctrlJob = new ControlledJob(configuration);
        ctrlJob.setJob(job);
		
		Job job1 = Job.getInstance(configuration);
		
		job1.setJarByClass(TFIDFDriver.class);
		
		job1.setMapperClass(TFIDFMapper.class);
		job1.setReducerClass(TFIDFReducer.class);

		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		
		FileInputFormat.setInputPaths(job1, path);
//		Path path1 = new Path("hdfs://node01:8020/outputEnd");
		Path path1 = new Path("file:///D:\\outputEnd\\");
		if (fs.exists(path1)) {
            fs.delete(path1, true);
        }
		FileOutputFormat.setOutputPath(job1, path1);
		
		ControlledJob ctrlJob1 = new ControlledJob(configuration);
        ctrlJob1.setJob(job1);
		
        //ctrlJob1 depend ctrlJob
        ctrlJob1.addDependingJob(ctrlJob);

       
        JobControl jobCtrl = new JobControl("myCtrl");
       
        jobCtrl.addJob(ctrlJob);
        jobCtrl.addJob(ctrlJob1);


        Thread thread = new Thread(jobCtrl);
        thread.start();
        while (true) {
            if (jobCtrl.allFinished()) {
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                break;
            }
        }
	}
}
