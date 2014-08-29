package com.neel.hadoopMR.MedianStd;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MinMaxCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

        Configuration conf = new Configuration();
        FileSystem hdfs =FileSystem.get(conf);
	    Job job = new Job();
	    job.setJarByClass(MinMaxCount.class);
	    job.setJobName("Max Min temperature");
	   
	    FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/ncdc_full_data"));
	    hdfs.delete( new Path("hdfs://localhost:54310/ncdcout/minmaxmedstdcount"));
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/ncdcout/minmaxmedstdcount"));
	    
	    job.setMapperClass(MinMaxCountMedStdMapper.class);
	    job.setReducerClass(MinMaxMedStdReducer.class);
	
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(MinMaxavgStdCountTuple.class);
	   // job.setNumReduceTasks(2);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}