package com.neel.hadoopMR.MatrixMultipy;

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text column =  new Text();
	private Text value = new Text();
	
	public void map(LongWritable key,Text val ,Context context){
		
		String line = val.toString();
		String tokens[] = line.split(",");
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName().toString();
		//System.out.println(fileName);
		if(fileName.equals("file1")){
			column.set(tokens[1]);
			value.set(fileName + ","+ tokens[1] + ","+tokens[2]);
			System.out.println(value.toString());
		}
		else if (fileName.equals("file2")){
			column.set(tokens[0]);
			value.set(fileName + ","+ tokens[0] + ","+tokens[2]);
			System.out.println(value.toString());
		}
		try {
			context.write(column, value);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
