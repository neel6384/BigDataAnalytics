package com.neel.hadoopMR.MaxTemp;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature {

  public static void main(String[] args) throws Exception {
  /*  if (args.length != 2) {
      System.err.println("Usage: MaxTemperature <input path> <output path>");
      System.exit(-1);
    }*/
    
    Job job = new Job();
    Configuration conf = new Configuration();
    		
    conf.set("mapred.child.java.opts", "-Xmx1024m -Xss600m");
    job.setJarByClass(MaxTemperature.class);
    job.setJobName("Max temperature");
   
   
    FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310//ncdc_full_data"));
      
    FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310//ncdcout/maxminavgreducer"));
    
    job.setMapperClass(MaxTemperatureMapper.class);
    job.setReducerClass(MaxTemperatureReducer.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    //job.setNumReduceTasks();
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}