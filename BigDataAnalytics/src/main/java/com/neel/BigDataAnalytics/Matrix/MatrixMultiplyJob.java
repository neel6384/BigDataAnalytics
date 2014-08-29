package com.neel.BigDataAnalytics.Matrix;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.common.HadoopUtil;

public class MatrixMultiplyJob {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		 Configuration conf1 = new Configuration();
		 Configuration conf2 = new Configuration();
		
	        Job job = new Job(conf1, "MatrixMatrixMultiplicationStep1");
	        job.setJarByClass(MatrixMultiplyJob.class);
	        
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(Text.class);
	        
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(FloatWritable.class);
	 
	        job.setMapperClass(MatrixMapperFirst.class);
	        job.setReducerClass(FirstReducer.class);
	 
	        job.setInputFormatClass(TextInputFormat.class);
	       // job.setOutputFormatClass(TextOutputFormat.class);
	        job.setOutputFormatClass(SequenceFileOutputFormat.class);
	        
	        HadoopUtil.delete(conf1, new Path("hdfs://localhost:54310/Matrix_op/"));
	        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/Matrix/"));
	        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/Matrix_op/"));
	 
	        job.waitForCompletion(true);
	       
	        Job job2 = new Job(conf2, "MatrixMatrixMultiplicationStep2");
	        job2.setJarByClass(MatrixMultiplyJob.class);
	       
	        job2.setOutputKeyClass(Text.class);
	        job2.setOutputValueClass(FloatWritable.class);
	        
	        //job2.setMapperClass(IdentiryMapper.class);
	        job2.setReducerClass(SecondReducer.class);
	 
	       //job2.setInputFormatClass(TextInputFormat.class);
	        job2.setInputFormatClass(SequenceFileInputFormat.class);
	        job2.setOutputFormatClass(TextOutputFormat.class);
	        
	        HadoopUtil.delete(conf1, new Path("hdfs://localhost:54310/Matrix_opf/"));
	        FileInputFormat.addInputPath(job2, new Path("hdfs://localhost:54310/Matrix_op/part-r-00000/"));
	        FileOutputFormat.setOutputPath(job2, new Path("hdfs://localhost:54310/Matrix_opf/"));
	 
	        job2.waitForCompletion(true);
	        
	   
	        
	}

}
