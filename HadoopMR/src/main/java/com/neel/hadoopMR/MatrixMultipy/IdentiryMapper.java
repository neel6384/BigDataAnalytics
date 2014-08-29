package com.neel.hadoopMR.MatrixMultipy;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class IdentiryMapper extends Mapper<Text, FloatWritable, Text, FloatWritable> {
	
	public void map(Text key,FloatWritable val ,Context context){
	
		try {
			context.write(key, val);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
